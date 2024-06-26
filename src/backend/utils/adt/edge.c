/*
 * PostGraph
 * Copyright (C) 2023 by PostGraph
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgraph.h"

#include "miscadmin.h"
#include "funcapi.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "utils/fmgrprotos.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

#include "catalog/ag_label.h"
#include "commands/label_commands.h"
#include "utils/ag_cache.h"
#include "utils/gtype.h"
#include "utils/graphid.h"
#include "utils/edge.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);
static Datum get_vertex(Oid graph_oid, int64 graphid);
static int edge_btree_fast_cmp(Datum x, Datum y, SortSupport ssup);

/*
 * I/O routines for vertex type
 */
PG_FUNCTION_INFO_V1(edge_in);
Datum edge_in(PG_FUNCTION_ARGS) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Use build_edge()")));

    return NULL;
}

PG_FUNCTION_INFO_V1(edge_out);
Datum edge_out(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);
    StringInfo str = makeStringInfo();

    // id
    appendStringInfoString(str, "{\"id\": ");
    graphid id = *((int64 *)(&v->children[0]));
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(id))));

    // start_id
    appendStringInfoString(str, ", \"start_id\": ");
    graphid startid = *((int64 *)(&v->children[2]));
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(startid))));

    // end_id
    appendStringInfoString(str, ", \"end_id\": ");
    graphid endid = *((int64 *)(&v->children[4]));
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(endid))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    appendStringInfoString(str, extract_edge_label(v));

    // properties
    appendStringInfoString(str, "\", \"properties\": ");
    gtype *agt = extract_edge_properties(v);
    gtype_to_cstring(str, &agt->root, 0);


    appendStringInfoString(str, "}");

    PG_RETURN_CSTRING(str->data);
}

void append_edge_to_string(StringInfoData *str, edge *v) {

    // id
    appendStringInfoString(str, "{\"id\": ");
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(*((int64 *)(&v->children[0]))))));

    // start_id
    appendStringInfoString(str, ", \"start_id\": ");
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(*((int64 *)(&v->children[2]))))));

    // end_id
    appendStringInfoString(str, ", \"end_id\": ");
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(*((int64 *)(&v->children[4]))))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    appendStringInfoString(str, extract_edge_label(v));

    // properties
    appendStringInfoString(str, "\", \"properties\": ");
    gtype *agt = extract_edge_properties(v);
    gtype_to_cstring(str, &agt->root, 0);

    appendStringInfoString(str, "}");

}

PG_FUNCTION_INFO_V1(build_edge);
Datum
build_edge(PG_FUNCTION_ARGS) {
    graphid id = AG_GETARG_GRAPHID(0);
    graphid start_id = AG_GETARG_GRAPHID(1);
    graphid end_id = AG_GETARG_GRAPHID(2);
    Oid graph_oid = PG_GETARG_OID(3);
    //char *label = PG_GETARG_CSTRING(3);
    gtype *properties = AG_GET_ARG_GTYPE_P(4);

    if (!AGT_ROOT_IS_OBJECT(properties))
        PG_RETURN_NULL();
    
    AG_RETURN_EDGE(create_edge(id, start_id, end_id, graph_oid, properties));
}
 
edge *
create_edge(graphid id,graphid start_id,graphid end_id, Oid graph_oid, gtype *properties) {
    StringInfoData buffer;
    initStringInfo(&buffer);

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    // id
    append_to_buffer(&buffer, (char *)&id, sizeof(graphid));

    // start_id
    append_to_buffer(&buffer, (char *)&start_id, sizeof(graphid));

    // end_id
    append_to_buffer(&buffer, (char *)&end_id, sizeof(graphid));

    // graph_oid
    append_to_buffer(&buffer, (char *)&graph_oid, sizeof(Oid));

    // properties
    append_to_buffer(&buffer, properties, VARSIZE(properties));

    edge *v = (edge *)buffer.data;

    SET_VARSIZE(v, buffer.len);

    return v;
}

// -> operator
PG_FUNCTION_INFO_V1(edge_property_access_gtype);
Datum
edge_property_access_gtype(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);
    gtype *agt = extract_edge_properties(v);
    gtype *key = AG_GET_ARG_GTYPE_P(1);
    gtype_value *key_value;

    if (!AGT_ROOT_IS_SCALAR(key))
        PG_RETURN_NULL();

    key_value = get_ith_gtype_value_from_container(&key->root, 0);

    if (key_value->type == AGTV_INTEGER)
        AG_RETURN_GTYPE_P(gtype_array_element_impl(fcinfo, agt, key_value->val.int_value, false));
    else if (key_value->type == AGTV_STRING)
        AG_RETURN_GTYPE_P(gtype_object_field_impl(fcinfo, agt, key_value->val.string.val,
                                                    key_value->val.string.len, false));
    else
        PG_RETURN_NULL();
}



/*
 * Equality Operators (=, <>)
 */
PG_FUNCTION_INFO_V1(edge_eq);
Datum
edge_eq(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] == (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(edge_ne);
Datum
edge_ne(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] != (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(edge_lt);
Datum
edge_lt(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] < (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(edge_le);
Datum
edge_le(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] <= (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(edge_gt);
Datum
edge_gt(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] > (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(edge_ge);
Datum
edge_ge(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    PG_RETURN_BOOL((int64)lhs->children[0] >= (int64)rhs->children[0]);
}

/*
 * B-Tree Support
 */
PG_FUNCTION_INFO_V1(edge_btree_cmp);
Datum edge_btree_cmp(PG_FUNCTION_ARGS) {
    edge *lhs = AG_GET_ARG_EDGE(0);
    edge *rhs = AG_GET_ARG_EDGE(1);

    graphid lgid = (int64)lhs->children[0];
    graphid rgid = (int64)rhs->children[0];

    if (lgid > rgid)
        PG_RETURN_INT32(1);
    else if (lgid == rgid)
        PG_RETURN_INT32(0);
    else
        PG_RETURN_INT32(-1);
}

PG_FUNCTION_INFO_V1(edge_btree_sort);
Datum edge_btree_sort(PG_FUNCTION_ARGS) {
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

    ssup->comparator = edge_btree_fast_cmp;
    PG_RETURN_VOID();
}

static int edge_btree_fast_cmp(Datum x, Datum y, SortSupport ssup) {
    edge *lhs = DATUM_GET_EDGE(x);
    edge *rhs = DATUM_GET_EDGE(y);

    graphid lgid = (int64)lhs->children[0];
    graphid rgid = (int64)rhs->children[0];

    if (lgid > rgid)
        return 1;
    else if (lgid == rgid)
        return 0;
    else
        return -1;
}




/*
 * Functions
 */
PG_FUNCTION_INFO_V1(edge_id);
Datum
edge_id(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = {.int_value = *((int64 *)(&v->children[0])) } };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(edge_start_id);
Datum
edge_start_id(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = {.int_value = *((int64 *)(&v->children[2])) } };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(edge_end_id);
Datum
edge_end_id(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = {.int_value = *((int64 *)(&v->children[4])) } };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(edge_label);
Datum
edge_label(PG_FUNCTION_ARGS) {
    edge *v = AG_GET_ARG_EDGE(0);

    char *label = extract_edge_label(v);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { strlen(label), label } };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(edge_properties);
Datum
edge_properties(PG_FUNCTION_ARGS) {
    edge *e = AG_GET_ARG_EDGE(0);

    AG_RETURN_GTYPE_P(extract_edge_properties(e));
}

PG_FUNCTION_INFO_V1(edge_startnode);
Datum edge_startnode(PG_FUNCTION_ARGS) {
    edge *e = AG_GET_ARG_EDGE(0);

    graphid id = *((int64 *)(&e->children[2]));
    
    Datum result = get_vertex(EXTRACT_EDGE_GRAPH_OID(e), id);

    PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(edge_endnode);
Datum edge_endnode(PG_FUNCTION_ARGS) {
    edge *e = AG_GET_ARG_EDGE(0);

    graphid id = *((int64 *)(&e->children[4]));

    Datum result = get_vertex(EXTRACT_EDGE_GRAPH_OID(e), id);

    PG_RETURN_POINTER(result);
}


static Datum get_vertex(Oid graph_oid, int64 graphid)
{
    ScanKeyData scan_keys[1];
    Relation graph_vertex_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum properties, result;
    int32 labelid = (graphid >> ENTRY_ID_BITS);
    
    label_cache_data *lcd = search_label_graph_oid_cache(graph_oid, labelid);

    Snapshot snapshot = GetActiveSnapshot();

    ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(graphid));

    graph_vertex_label = table_open(lcd->relation, ShareLock);
    scan_desc = table_beginscan(graph_vertex_label, snapshot, 1, scan_keys);
    tuple = heap_getnext(scan_desc, ForwardScanDirection);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("id %lu does not exist", graphid)));

    tupdesc = RelationGetDescr(graph_vertex_label);

    properties = column_get_datum(tupdesc, tuple, 1, "properties", GTYPEOID, true);

    table_endscan(scan_desc);
    table_close(graph_vertex_label, ShareLock);

    return VERTEX_GET_DATUM(create_vertex(graphid, graph_oid, DATUM_GET_GTYPE_P(properties)));
}

PG_FUNCTION_INFO_V1(edge_unnest);
/*
 * Function to convert an array of vertices into a set of rows. It is used for
 * Cypher `UNWIND` clause, but considering the situation in which the user can
 * directly use this function in vanilla PGSQL, put a second parameter related
 * to this.
 */
Datum edge_unnest(PG_FUNCTION_ARGS)
{
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    bool block_types = PG_GETARG_BOOL(1);
    ReturnSetInfo *rsi;
    ArrayMetaState *my_extra;
    Tuplestorestate *tuple_store;
    TupleDesc tupdesc;
    TupleDesc ret_tdesc;
    MemoryContext old_cxt, tmp_cxt;
    Datum value;
    bool isnull;

    if(ARR_NDIM(array) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unnest only supports 1 dimensional arrays")));

    if (ARR_NDIM(array) < 1)
        PG_RETURN_NULL();


    if (ARR_ELEMTYPE(array) != EDGEOID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid element type for unnest")));

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    rsi->returnMode = SFRM_Materialize;

    /* it's a simple type, so don't use get_call_result_type() */
    tupdesc = rsi->expectedDesc;

    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

    ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(ret_tdesc);
    tuple_store = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, work_mem);
    MemoryContextSwitchTo(old_cxt);

    tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "age_unnest temporary cxt", ALLOCSET_DEFAULT_SIZES);

    ArrayIterator array_iterator = array_create_iterator(array, 0, NULL);
    while (array_iterate(array_iterator, &value, &isnull))
    {
        HeapTuple tuple;
        Datum values[1];
        bool nulls[1] = {false};

        if (isnull)
            continue;

        /* use the tmp context so we can clean up after each tuple is done */
        old_cxt = MemoryContextSwitchTo(tmp_cxt);

        values[0] = value;

        tuple = heap_form_tuple(ret_tdesc, values, nulls);

        tuplestore_puttuple(tuple_store, tuple);

        MemoryContextSwitchTo(old_cxt);
        MemoryContextReset(tmp_cxt);
    }

    array_free_iterator(array_iterator);

    /* Avoid leaking memory when handed toasted input */
    PG_FREE_IF_COPY(array, 0);

    MemoryContextDelete(tmp_cxt);

    rsi->setResult = tuple_store;
    rsi->setDesc = ret_tdesc;

    PG_RETURN_NULL();
}

/*
 * Aggregate Functions
 */
PG_FUNCTION_INFO_V1(edge_collect_transfn);
Datum
edge_collect_transfn(PG_FUNCTION_ARGS)
{
        Oid arg1_typeid = EDGEOID;
        MemoryContext aggcontext;
        ArrayBuildState *state;
        Datum           elem;

        if (arg1_typeid == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("could not determine input data type")));

        /*
         * Note: we do not need a run-time check about whether arg1_typeid is a
         * valid array element type, because the parser would have verified that
         * while resolving the input/result types of this polymorphic aggregate.
         */

        if (!AggCheckCallContext(fcinfo, &aggcontext))
        {
                /* cannot be called directly because of internal-type argument */
                elog(ERROR, "array_agg_transfn called in non-aggregate context");
        }

        if (PG_ARGISNULL(0))
                state = initArrayResult(arg1_typeid, aggcontext, false);
        else
                state = (ArrayBuildState *) PG_GETARG_POINTER(0);

        elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);

        state = accumArrayResult(state, elem, PG_ARGISNULL(1), arg1_typeid, aggcontext);

        /*
         * The transition type for array_agg() is declared to be "internal", which
         * is a pass-by-value type the same size as a pointer.  So we can safely
         * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
         */
        PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(edge_collect_finalfn);
Datum
edge_collect_finalfn(PG_FUNCTION_ARGS)
{
    Datum           result;
    ArrayBuildState *state;
    int                     dims[1];
    int                     lbs[1];

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);

    if (state == NULL)
        PG_RETURN_NULL();               /* returns null iff no input values */

    dims[0] = state->nelems;
    lbs[0] = 1;

    /*
     * Make the result.  We cannot release the ArrayBuildState because
     * sometimes aggregate final functions are re-executed.  Rather, it is
     * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
     * so.
     */
    result = makeMdArrayResult(state, 1, dims, lbs, CurrentMemoryContext, false);

    PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(edge_collect_transfn_w_limit);
Datum
edge_collect_transfn_w_limit(PG_FUNCTION_ARGS)
{
        Oid arg1_typeid = EDGEOID;
        MemoryContext aggcontext;
        ArrayBuildState *state;
        Datum           elem;

        if (arg1_typeid == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("could not determine input data type")));

        /*
         * Note: we do not need a run-time check about whether arg1_typeid is a
         * valid array element type, because the parser would have verified that
         * while resolving the input/result types of this polymorphic aggregate.
         */

        if (!AggCheckCallContext(fcinfo, &aggcontext))
        {
                /* cannot be called directly because of internal-type argument */
                elog(ERROR, "array_agg_transfn called in non-aggregate context");
        }

        if (PG_ARGISNULL(0))
                state = initArrayResult(arg1_typeid, aggcontext, false);
        else
                state = (ArrayBuildState *) PG_GETARG_POINTER(0);

        if (PG_ARGISNULL(2))
            PG_RETURN_POINTER(state);

        gtype *limit = PG_GETARG_DATUM(2);

        if (!GTYPE_CONTAINER_IS_SCALAR(&limit->root))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("limit argument of collect must be an integer")));

        gtype_value *gtv = get_ith_gtype_value_from_container(&limit->root, 0);
        if (gtv->type != AGTV_INTEGER)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("limit argument of collect must be an integer")));

        if (state->nelems >= gtv->val.int_value)
            PG_RETURN_POINTER(state);

        elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);

        state = accumArrayResult(state, elem, PG_ARGISNULL(1), arg1_typeid, aggcontext);

        /*
         * The transition type for array_agg() is declared to be "internal", which
         * is a pass-by-value type the same size as a pointer.  So we can safely
         * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
         */
        PG_RETURN_POINTER(state);
}

static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

char *
extract_edge_label(edge *e) {
    graphid id = EXTRACT_EDGE_ID(e);
    int32 label_id = get_graphid_label_id(id);

    Oid graph_oid = EXTRACT_EDGE_GRAPH_OID(e);

    label_cache_data *cache_data = search_label_graph_oid_cache(graph_oid, label_id);
    char *label = NameStr(cache_data->name);

    if (IS_AG_DEFAULT_LABEL(label))
        return "";

    return label;
}

gtype *
extract_edge_properties(edge *v) {
    char *bytes = (char *)v;

    return (gtype *)&bytes[VARHDRSZ + (3 * sizeof(graphid)) + sizeof(Oid)];
}
