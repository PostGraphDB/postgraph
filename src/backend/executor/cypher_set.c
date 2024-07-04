/*
 * Copyright (C) 2023-2024 PostGraphDB
 *  
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 */

#include "postgres.h"

#include "miscadmin.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

#include "utils/cypher_tuplestore.h"
#include "access/cypher_heapam.h"
#include "executor/cypher_executor.h"
#include "executor/cypher_utils.h"
#include "nodes/cypher_nodes.h"
#include "utils/gtype.h"
#include "utils/graphid.h"
#include "utils/vertex.h"
#include "utils/edge.h"
#include "catalog/ag_label.h"

static void begin_cypher_set(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *exec_cypher_set(CustomScanState *node);
static void end_cypher_set(CustomScanState *node);
static void rescan_cypher_set(CustomScanState *node);

static void process_update_list(CustomScanState *node);
static HeapTuple update_entity_tuple(ResultRelInfo *resultRelInfo, TupleTableSlot *elemTupleSlot,
                                     EState *estate, HeapTuple old_tuple);

const CustomExecMethods cypher_set_exec_methods = {SET_SCAN_STATE_NAME,
    begin_cypher_set, exec_cypher_set, end_cypher_set, rescan_cypher_set,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};

static void begin_cypher_set(CustomScanState *node, EState *estate, int eflags) {
    cypher_set_custom_scan_state *css =
        (cypher_set_custom_scan_state *)node;
    Plan *subplan;

    Assert(list_length(css->cs->custom_plans) == 1);

    subplan = linitial(css->cs->custom_plans);
    node->ss.ps.lefttree = ExecInitNode(subplan, estate, eflags);

    ExecAssignExprContext(estate, &node->ss.ps);

    //ExecInitScanTupleSlot(estate, &node->ss, ExecGetResultType(node->ss.ps.lefttree), &TTSOpsHeapTuple);
	
    ExecInitScanTupleSlot(estate, &node->ss, ExecGetResultType(node->ss.ps.lefttree), &TTSOpsMinimalTuple);
	//ExecCreateScanSlotFromOuterPlan(estate, &node->ss, &TTSOpsMinimalTuple);

    //node->ss.ps.plan->targetlist = css->targetList;

    ExecInitResultTupleSlotTL(node, &TTSOpsMinimalTuple);
/*
    if (!CYPHER_CLAUSE_IS_TERMINAL(css->flags)) {
        //TupleDesc tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
        TupleDesc tupdesc = node->ss.ps.ps_ResultTupleDesc;
        ExecAssignProjectionInfo(&node->ss.ps, tupdesc);
    }
*/
    Oid xid = GetCurrentTransactionId();

    if (estate->es_output_cid == 0)
        estate->es_output_cid = estate->es_snapshot->curcid;

    Increment_Estate_CommandId(estate);


}

static HeapTuple update_entity_tuple(ResultRelInfo *resultRelInfo, TupleTableSlot *elemTupleSlot,
                                     EState *estate, HeapTuple old_tuple) {
    HeapTuple tuple = NULL;
    LockTupleMode lockmode;
    TM_FailureData hufd;
    CYPHER_TM_Result lock_result;
    Buffer buffer;
    bool update_indexes;
    CYPHER_TM_Result   result;

    ResultRelInfo **saved_resultRelsInfo  = estate->es_result_relations;
    estate->es_result_relations = &resultRelInfo;

    lockmode = ExecUpdateLockMode(estate, resultRelInfo);

    lock_result = cypher_heap_lock_tuple(resultRelInfo->ri_RelationDesc, old_tuple,
                                  estate->es_snapshot->curcid, lockmode,
                                  LockWaitBlock, false, &buffer, &hufd);

    if (lock_result == CYPHER_TM_Ok) {
        ExecOpenIndices(resultRelInfo, false);
        ExecStoreVirtualTuple(elemTupleSlot);
        tuple = ExecFetchSlotHeapTuple(elemTupleSlot, true, NULL);
        tuple->t_self = old_tuple->t_self;

        // Check the constraints of the tuple
        tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
        if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
            ExecConstraints(resultRelInfo, elemTupleSlot, estate);

        result = table_tuple_update(resultRelInfo->ri_RelationDesc,
                                    &tuple->t_self, elemTupleSlot,
                                    estate->es_snapshot->curcid,
                                    estate->es_snapshot,
                                    estate->es_crosscheck_snapshot,
                                    true /* wait for commit */ ,
                                    &hufd, &lockmode, &update_indexes);

        if (result == CYPHER_TM_SelfModified)
        {
            if (hufd.cmax != estate->es_output_cid)
                ereport(ERROR,
                        (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                         errmsg("tuple to be updated was already modified")));

            ExecCloseIndices(resultRelInfo);
            estate->es_result_relations = saved_resultRelsInfo;

            return tuple;
        }

        if (result != CYPHER_TM_Ok)
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Entity failed to be updated: %i", result)));

        // Insert index entries for the tuple
        if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
          ExecInsertIndexTuples(resultRelInfo, elemTupleSlot, estate, false, false, NULL, NIL);

        ExecCloseIndices(resultRelInfo);
    }
    else if (lock_result == CYPHER_TM_SelfModified)
    {/*
        if (hufd.cmax != estate->es_output_cid)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                     errmsg("tuple to be updated was already modified")));
        }*/
    }
    else
    {
        /*
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("Entity failed to be updated: %i", lock_result)));
                */
    }

    ReleaseBuffer(buffer);

    estate->es_result_relations = saved_resultRelsInfo;

    return tuple;
}

/*
 * When the CREATE clause is the last cypher clause, consume all input from the
 * previous clause(s) in the first call of exec_cypher_create.
 */
static void process_all_tuples(CustomScanState *node)
{
    cypher_set_custom_scan_state *css = (cypher_set_custom_scan_state *)node;
    TupleTableSlot *slot;
    EState *estate = css->css.ss.ps.state;

    do
    {
        process_update_list(node);
        Decrement_Estate_CommandId(estate)
        slot = ExecProcNode(node->ss.ps.lefttree);
        Increment_Estate_CommandId(estate)
    } while (!TupIsNull(slot));
}

TupleTableSlot *populate_vertex_tts_1(TupleTableSlot *elemTupleSlot, graphid id, gtype_value *properties)
{
    bool properties_isnull;

    if (id == NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("vertex id field cannot be NULL")));

    elemTupleSlot->tts_values[vertex_tuple_id] = GRAPHID_GET_DATUM(id);
    elemTupleSlot->tts_isnull[vertex_tuple_id] = false;

    elemTupleSlot->tts_values[vertex_tuple_properties] =
        GTYPE_P_GET_DATUM(gtype_value_to_gtype(properties));
    elemTupleSlot->tts_isnull[vertex_tuple_properties] = properties == NULL;

    elemTupleSlot->tts_values[2] = NULL;
    elemTupleSlot->tts_isnull[2] = true;


    return elemTupleSlot;
}

TupleTableSlot *populate_edge_tts_1(TupleTableSlot *elemTupleSlot, graphid id, graphid startid,
    graphid endid, gtype_value *properties)
{
    bool properties_isnull;

    properties_isnull = properties == NULL;

    elemTupleSlot->tts_values[edge_tuple_id] = GRAPHID_GET_DATUM(id);
    elemTupleSlot->tts_isnull[edge_tuple_id] = false;

    elemTupleSlot->tts_values[edge_tuple_start_id] = GRAPHID_GET_DATUM(startid);
    elemTupleSlot->tts_isnull[edge_tuple_start_id] = false;

    elemTupleSlot->tts_values[edge_tuple_end_id] = GRAPHID_GET_DATUM(endid);
    elemTupleSlot->tts_isnull[edge_tuple_end_id] = false;

    elemTupleSlot->tts_values[edge_tuple_properties] =
        GTYPE_P_GET_DATUM(gtype_value_to_gtype(properties));
    elemTupleSlot->tts_isnull[edge_tuple_properties] = properties_isnull;

    return elemTupleSlot;
}


static void process_update_list(CustomScanState *node)
{
    cypher_set_custom_scan_state *css = (cypher_set_custom_scan_state *)node;
    ExprContext *econtext = css->css.ss.ps.ps_ExprContext;
    TupleTableSlot *scanTupleSlot = econtext->ecxt_scantuple;
    ListCell *lc;
    EState *estate = css->css.ss.ps.state;
    int *luindex = NULL;
    int lidx = 0;

    luindex = palloc0(sizeof(int) * scanTupleSlot->tts_nvalid);

    foreach (lc, css->set_list->set_items)
    {
        cypher_update_item *update_item = NULL;

        update_item = (cypher_update_item *)lfirst(lc);
        luindex[update_item->entity_position - 1] = lidx;

        lidx++;
    }

    lidx = 0;

    foreach (lc, css->set_list->set_items)
    {
        gtype_value *altered_properties;
        gtype *original_properties;
        graphid id;
        gtype *new_property_value;
        TupleTableSlot *slot;
        ResultRelInfo *resultRelInfo;
        ScanKeyData scan_keys[1];
        TableScanDesc scan_desc;
        char *label_name;
        cypher_update_item *update_item;
        Datum new_entity;
        HeapTuple heap_tuple;
        char *clause_name = css->set_list->clause_name;
        int cid;

        update_item = (cypher_update_item *)lfirst(lc);


        if (scanTupleSlot->tts_isnull[update_item->entity_position - 1])
            continue;

        bool remove_property;
        if (update_item->remove_item)
            remove_property = true;
        else
            remove_property = scanTupleSlot->tts_isnull[update_item->prop_position - 1];

        if (remove_property)
            new_property_value = NULL;
        else
            new_property_value = DATUM_GET_GTYPE_P(scanTupleSlot->tts_values[update_item->prop_position - 1]);


        if (scanTupleSlot->tts_tupleDescriptor->attrs[update_item->entity_position -1].atttypid == VERTEXOID) {

            vertex *v = DATUM_GET_VERTEX(scanTupleSlot->tts_values[update_item->entity_position - 1]);

            id = *((int64 *)(&v->children[0])); 

            label_name =  extract_vertex_label(v);

            original_properties = extract_vertex_properties(v);

            altered_properties = alter_property_value(original_properties, update_item->prop_name,
                                                  new_property_value, remove_property);

            resultRelInfo = create_entity_result_rel_info(estate, css->set_list->graph_name, label_name);

            slot = ExecInitExtraTupleSlot(estate, RelationGetDescr(resultRelInfo->ri_RelationDesc), &TTSOpsHeapTuple);

	    new_entity = VERTEX_GET_DATUM(create_vertex(id, css->graph_oid, gtype_value_to_gtype(altered_properties)));

            slot = populate_vertex_tts_1(slot, id, altered_properties);
	} 
	else if (scanTupleSlot->tts_tupleDescriptor->attrs[update_item->entity_position -1].atttypid == EDGEOID) {
            edge *v = DATUM_GET_EDGE(scanTupleSlot->tts_values[update_item->entity_position - 1]);

            id = *((int64 *)(&v->children[0]));
	    graphid startid = *((int64 *)(&v->children[2]));
	    graphid endid = *((int64 *)(&v->children[4]));

            label_name =  extract_edge_label(v);

            original_properties = extract_edge_properties(v);

            altered_properties = alter_property_value(original_properties, update_item->prop_name,
                                                  new_property_value, remove_property);

            resultRelInfo = create_entity_result_rel_info(estate, css->set_list->graph_name, label_name);

            slot = ExecInitExtraTupleSlot(estate, RelationGetDescr(resultRelInfo->ri_RelationDesc), &TTSOpsHeapTuple);

            new_entity = EDGE_GET_DATUM(create_edge(id, startid, endid, css->graph_oid, gtype_value_to_gtype(altered_properties)));

            slot = populate_edge_tts_1(slot, id, startid, endid, altered_properties);
	
	} 
        else
        {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("%s clause can only update vertex and edges", clause_name)));
        }

        scanTupleSlot->tts_values[update_item->entity_position - 1] = new_entity;

        if (luindex[update_item->entity_position - 1] == lidx)
        {
            ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_GRAPHIDEQ,
                        GRAPHID_GET_DATUM(id));

            scan_desc = table_beginscan(resultRelInfo->ri_RelationDesc,
                                        estate->es_snapshot, 1, scan_keys);

            heap_tuple = cypher_heap_getnext(scan_desc, ForwardScanDirection);

            if (HeapTupleIsValid(heap_tuple))
                heap_tuple = update_entity_tuple(resultRelInfo, slot, estate, heap_tuple);

            table_endscan(scan_desc);
        }

        estate->es_snapshot->curcid = cid;

        ExecCloseIndices(resultRelInfo);
        table_close(resultRelInfo->ri_RelationDesc, RowExclusiveLock);

        lidx++;	
    }

}

static TupleTableSlot *exec_cypher_set(CustomScanState *node)
{
    cypher_set_custom_scan_state *css = (cypher_set_custom_scan_state *)node;
    ResultRelInfo **saved_resultRelsInfo;
    EState *estate = css->css.ss.ps.state;
    ExprContext *econtext = css->css.ss.ps.ps_ExprContext;
    TupleTableSlot *slot;
    TupleTableSlot *stored_slot;

    if (css->slot == NULL)
        css->slot = MakeSingleTupleTableSlot(css->css.ss.ps.ps_ResultTupleDesc, &TTSOpsMinimalTuple);


    if (!css->done || CYPHER_CLAUSE_IS_TERMINAL(css->flags)) {
       if(!CYPHER_CLAUSE_IS_TERMINAL(css->flags))
            css->tuple_store = tuplestore_begin_heap(false, false, work_mem);
        else 
            css->tuple_store = NULL;

        saved_resultRelsInfo = estate->es_result_relations;

        //Process the subtree first
        Decrement_Estate_CommandId(estate);
        slot = ExecProcNode(node->ss.ps.lefttree);
        Increment_Estate_CommandId(estate);

        if (TupIsNull(slot))
            return NULL;

        econtext->ecxt_scantuple =
            node->ss.ps.lefttree->ps_ProjInfo->pi_exprContext->ecxt_scantuple;

        if (CYPHER_CLAUSE_IS_TERMINAL(css->flags)) {
            estate->es_result_relations = saved_resultRelsInfo;

            process_all_tuples(node);

            return NULL;
        } else {
            estate->es_result_relations = saved_resultRelsInfo;
//return slot;
  //      }

            do {
                process_update_list(node);

                TupleTableSlot *temp = econtext->ecxt_scantuple;
                css->slot = econtext->ecxt_scantuple;
                /*
                econtext->ecxt_scantuple = ExecProject(node->ss.ps.lefttree->ps_ProjInfo);
                css->slot = ExecProject(node->ss.ps.ps_ProjInfo);
                */

                tuplestore_putvalues(css->tuple_store, css->slot->tts_tupleDescriptor, 
                                     css->slot->tts_values, css->slot->tts_isnull);
        
                econtext->ecxt_scantuple = temp;

                Decrement_Estate_CommandId(estate)
                slot = ExecProcNode(node->ss.ps.lefttree);
                Increment_Estate_CommandId(estate)

econtext->ecxt_scantuple =
            node->ss.ps.lefttree->ps_ProjInfo->pi_exprContext->ecxt_scantuple;

            } while (!TupIsNull(slot) );

            css->done = true;
            tuplestore_rescan(css->tuple_store);
        }

    }

    css->slot = MakeSingleTupleTableSlot(css->slot->tts_tupleDescriptor, &TTSOpsMinimalTuple);
    ExecClearTuple(css->slot);
    bool res = tuplestore_gettupleslot(css->tuple_store, true, true, css->slot);

    //css->slot->tts_ops = (const TupleTableSlotOps *)&TTSOpsVirtual;
    //memcpy(&css->slot->tts_ops, &TTSOpsVirtual, 4);
    //econtext->ecxt_scantuple = css->slot;
    //node->ss.ps.lefttree->ps_ProjInfo->pi_exprContext->ecxt_scantuple = css->slot;

    if (!res)
      return NULL;
    return css->slot;
}

static void end_cypher_set(CustomScanState *node)
{
    CommandCounterIncrement();

    ExecEndNode(node->ss.ps.lefttree);
}

static void rescan_cypher_set(CustomScanState *node)
{
    cypher_set_custom_scan_state *css = (cypher_set_custom_scan_state *)node;
    char *clause_name = css->set_list->clause_name;

     ereport(ERROR,
             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("cypher %s clause cannot be rescaned",
                             clause_name),
                      errhint("its unsafe to use joins in a query with a Cypher %s clause", clause_name)));
}

Node *create_cypher_set_plan_state(CustomScan *cscan)
{
    cypher_set_custom_scan_state *cypher_css = palloc0(sizeof(cypher_set_custom_scan_state));
    cypher_update_information *set_list;
    char *serialized_data;
    Const *c;

    cypher_css->cs = cscan;

    // get the serialized data structure from the Const and deserialize it.
    c = linitial(cscan->custom_private);
    serialized_data = (char *)c->constvalue;
    set_list = stringToNode(serialized_data);

    Assert(is_ag_node(set_list, cypher_update_information));

    cypher_css->set_list = set_list;
    cypher_css->flags = set_list->flags;
    cypher_css->graph_oid = set_list->graph_oid;

    cypher_css->targetList = cscan->custom_scan_tlist;
    cypher_css->css.ss.ps.type = T_CustomScanState;
    cypher_css->css.methods = &cypher_set_exec_methods;

    cypher_css->done = false;
    cypher_css->slot = NULL;
    
    return (Node *)cypher_css;
}
