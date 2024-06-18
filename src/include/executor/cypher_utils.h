/*
 * Copyright (C) 2023 PostGraphDB
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
 * Portions Copyright (c) 1996-2010, Bitnine Global
 */
#ifndef AG_CYPHER_UTILS_H
#define AG_CYPHER_UTILS_H

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"

#include "utils/tuplestore.h"

#include "nodes/cypher_nodes.h"
#include "utils/gtype.h"

// declaration of a useful postgres macro that isn't in a header file
#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)	 PointerGetDatum(X)

/*
 * When executing the children of the CREATE, SET, REMOVE, and
 * DELETE clasues, we need to alter the command id in the estate
 * and the snapshot. That way we can hide the modified tuples from
 * the sub clauses that should not know what their parent clauses are
 * doing.
 */
#define Increment_Estate_CommandId(estate) \
    estate->es_output_cid++; \
    estate->es_snapshot->curcid++;

#define Decrement_Estate_CommandId(estate) \
    estate->es_output_cid--; \
    estate->es_snapshot->curcid--;

typedef struct cypher_create_custom_scan_state
{
    CustomScanState css;
    CustomScan *cs;
    List *pattern;
    List *path_values;
    uint32 flags;
    TupleTableSlot *slot;
    Oid graph_oid;
} cypher_create_custom_scan_state;

typedef struct cypher_set_custom_scan_state
{
    CustomScanState css;
    CustomScan *cs;
    cypher_update_information *set_list;
    int flags;
    Oid graph_oid;
    Tuplestorestate *tuple_store;
    bool done;
    TupleTableSlot *slot;
} cypher_set_custom_scan_state;

typedef struct cypher_delete_custom_scan_state
{
    CustomScanState css;
    CustomScan *cs;
    cypher_delete_information *delete_data;
    int flags;
    List *edge_labels;
    Oid graph_oid;
} cypher_delete_custom_scan_state;

typedef struct cypher_merge_custom_scan_state
{
    CustomScanState css;
    CustomScan *cs;
    cypher_merge_information *merge_information;
    int flags;
    cypher_create_path *path;
    List *path_values;
    Oid graph_oid;
    AttrNumber merge_function_attr;
    bool created_new_path;
    bool found_a_path;
    CommandId base_currentCommandId;
} cypher_merge_custom_scan_state;

TupleTableSlot *populate_vertex_tts(TupleTableSlot *elemTupleSlot,
                                    gtype_value *id, gtype_value *properties);
TupleTableSlot *populate_edge_tts(
    TupleTableSlot *elemTupleSlot, gtype_value *id, gtype_value *startid,
    gtype_value *endid, gtype_value *properties);

ResultRelInfo *create_entity_result_rel_info(EState *estate, char *graph_name,
                                             char *label_name);
void destroy_entity_result_rel_info(ResultRelInfo *result_rel_info);

bool entity_exists(EState *estate, Oid graph_oid, graphid id);
HeapTuple insert_entity_tuple(ResultRelInfo *resultRelInfo,
                              TupleTableSlot *elemTupleSlot,
                              EState *estate);
HeapTuple insert_entity_tuple_cid(ResultRelInfo *resultRelInfo,
                                  TupleTableSlot *elemTupleSlot,
                                  EState *estate, CommandId cid);

#endif
