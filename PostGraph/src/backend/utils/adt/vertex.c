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
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

#include "commands/label_commands.h"
#include "utils/gtype.h"
#include "utils/graphid.h"
#include "utils/vertex.h"
#include "utils/ag_cache.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);

/*
 * I/O routines for vertex type
 */
PG_FUNCTION_INFO_V1(vertex_in);
Datum vertex_in(PG_FUNCTION_ARGS) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Use build_vertex()")));

    return NULL;
}

PG_FUNCTION_INFO_V1(vertex_out);
Datum vertex_out(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    StringInfo str = makeStringInfo();

    // id
    appendStringInfoString(str, "{\"id\": ");
    graphid id = EXTRACT_VERTEX_ID(v);
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(id))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    char *label = extract_vertex_label(v);
    appendStringInfoString(str, label);

    // properties
    appendStringInfoString(str, "\", \"properties\": ");
    gtype *agt = extract_vertex_properties(v);
    gtype_to_cstring(str, &agt->root, 0);

    appendStringInfoString(str, "}");

    PG_RETURN_CSTRING(str->data);
}

void append_vertex_to_string(StringInfoData *buffer, vertex *v){

    // id
    appendStringInfoString(buffer, "{\"id\": ");
    graphid id = *((int64 *)(&v->children[0]));
    appendStringInfoString(buffer, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(id))));

    // label
    appendStringInfoString(buffer, ", \"label\": \"");
    appendStringInfoString(buffer, extract_vertex_label(v));

    // properties
    appendStringInfoString(buffer, "\", \"properties\": ");
    gtype *agt = extract_vertex_properties(v);
    gtype_to_cstring(buffer, &agt->root, 0);

    appendStringInfoString(buffer, "}");

}

PG_FUNCTION_INFO_V1(build_vertex);
Datum
build_vertex(PG_FUNCTION_ARGS) {
    graphid id = AG_GETARG_GRAPHID(0);
    Oid graph_oid = PG_GETARG_OID(1);
    gtype *properties = AG_GET_ARG_GTYPE_P(2);

    if (!AGT_ROOT_IS_OBJECT(properties))
        PG_RETURN_NULL();

    AG_RETURN_VERTEX(create_vertex(id, graph_oid, properties));
}
  
vertex *
create_vertex(graphid id, Oid graph_oid, gtype *properties) {
    StringInfoData buffer;
    initStringInfo(&buffer);

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    // id
    append_to_buffer(&buffer, (char *)&id, sizeof(graphid));

    // label
    append_to_buffer(&buffer, (char *)&graph_oid, sizeof(Oid));

    // properties
    append_to_buffer(&buffer, properties, VARSIZE(properties));

    vertex *v = (vertex *)buffer.data;

    SET_VARSIZE(v, buffer.len);

    return v;
}


/*
 * Equality Operators (=, <>)
 */
PG_FUNCTION_INFO_V1(vertex_eq);
Datum
vertex_eq(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] == (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_ne);
Datum
vertex_ne(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] != (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_lt);
Datum
vertex_lt(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] < (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_le);
Datum
vertex_le(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] <= (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_gt);
Datum
vertex_gt(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] > (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_ge);
Datum
vertex_ge(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    PG_RETURN_BOOL((int64)lhs->children[0] >= (int64)rhs->children[0]);
}

PG_FUNCTION_INFO_V1(vertex_btree_cmp);
Datum
vertex_btree_cmp(PG_FUNCTION_ARGS) {
    vertex *lhs = AG_GET_ARG_VERTEX(0);
    vertex *rhs = AG_GET_ARG_VERTEX(1);

    graphid lgid = (int64)lhs->children[0];
    graphid rgid = (int64)rhs->children[0];

    if (lgid < rgid)
        PG_RETURN_INT32(-1);
    else if (lgid > rgid)
        PG_RETURN_INT32(-1);
    
    PG_RETURN_INT32(0);
}

/*
 * Operators
 *
 * All operators on a vertex actually happen on the vertex's properties, thats why the first step in all these
 * functions is to extract the properties.
 */
// -> operator
PG_FUNCTION_INFO_V1(vertex_property_access);
Datum
vertex_property_access(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);
                                             
    AG_RETURN_GTYPE_P(gtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), false));
}

// -> operator
PG_FUNCTION_INFO_V1(vertex_property_access_gtype);
Datum
vertex_property_access_gtype(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
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

    PG_RETURN_NULL();
}


// ->> operator
PG_FUNCTION_INFO_V1(vertex_property_access_text);
Datum
vertex_property_access_text(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);
                                             
    AG_RETURN_GTYPE_P(gtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), true));
}

// @> operator
PG_FUNCTION_INFO_V1(vertex_contains);
Datum 
vertex_contains(PG_FUNCTION_ARGS) {                                    
    vertex *v = AG_GET_ARG_VERTEX(0);                                 
    gtype *properties = extract_vertex_properties(v);
    gtype *constraints = AG_GET_ARG_GTYPE_P(1);

    gtype_iterator *constraint_it = gtype_iterator_init(&constraints->root);
    gtype_iterator *property_it = gtype_iterator_init(&properties->root);
    
    PG_RETURN_BOOL(gtype_deep_contains(&property_it, &constraint_it));
}   
    

// <@ operator
PG_FUNCTION_INFO_V1(vertex_contained_by);  
Datum 
vertex_contained_by(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(1);
    gtype *properties = extract_vertex_properties(v);
    gtype *constraints = AG_GET_ARG_GTYPE_P(0);

    gtype_iterator *constraint_it = gtype_iterator_init(&constraints->root);
    gtype_iterator *property_it = gtype_iterator_init(&properties->root);

    PG_RETURN_BOOL(gtype_deep_contains(&constraint_it, &property_it));
}

// ? operator
PG_FUNCTION_INFO_V1(vertex_exists);
Datum
vertex_exists(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);

    /*
     * We only match Object keys (which are naturally always Strings), or
     * In particular, we do not match non-string scalar elements. 
     * Existence of a key/element is only considered at the
     * top level.  No recursion occurs.
     */
    gtype_value agtv = { .type = AGTV_STRING, .val.string = {VARSIZE_ANY_EXHDR(key), VARDATA_ANY(key)} };

    gtype_value *val = find_gtype_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, &agtv);

    PG_RETURN_BOOL(val != NULL);
}

// ?| operator
PG_FUNCTION_INFO_V1(vertex_exists_any);
Datum
vertex_exists_any(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
    ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls, &elem_count);

    for (int i = 0; i < elem_count; i++) {
        if (key_nulls[i])
            continue;

        gtype_value agtv = { .type = AGTV_STRING, .val.string = { VARSIZE(key_datums[i]) - VARHDRSZ, VARDATA_ANY(key_datums[i]) } };

        if (find_gtype_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, &agtv) != NULL)
            PG_RETURN_BOOL(true);
    }

    PG_RETURN_BOOL(false);
}

// ?& operator
PG_FUNCTION_INFO_V1(vertex_exists_all);
Datum
vertex_exists_all(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    gtype *agt = extract_vertex_properties(v);
    ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls, &elem_count);

    for (int i = 0; i < elem_count; i++) {
        if (key_nulls[i])
            continue;

        gtype_value agtv = { .type = AGTV_STRING, .val.string = { VARSIZE(key_datums[i]) - VARHDRSZ, VARDATA_ANY(key_datums[i]) } };

        if (find_gtype_value_from_container(&agt->root, GT_FOBJECT | GT_FARRAY, &agtv) == NULL)
            PG_RETURN_BOOL(false);
    }

    PG_RETURN_BOOL(true);
}

/*
 * Functions
 */
// id function
PG_FUNCTION_INFO_V1(vertex_id);
Datum
vertex_id(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = {.int_value = *((int64 *)(&v->children[0])) } };    

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(vertex_properties);
Datum
vertex_properties(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    AG_RETURN_GTYPE_P(extract_vertex_properties(v));
}

PG_FUNCTION_INFO_V1(vertex_label);
Datum
vertex_label(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    char *label = extract_vertex_label(v);

    gtype_value gtv = {
            .type = AGTV_STRING,
            .val.string = { strlen(label), label }
            };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(vertex_unnest);
/*
 * Function to convert an array of vertices into a set of rows. It is used for
 * Cypher `UNWIND` clause, but considering the situation in which the user can
 * directly use this function in vanilla PGSQL, put a second parameter related
 * to this.
 */
Datum vertex_unnest(PG_FUNCTION_ARGS)
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


    if (ARR_ELEMTYPE(array) != VERTEXOID)
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
PG_FUNCTION_INFO_V1(vertex_collect_transfn);
Datum
vertex_collect_transfn(PG_FUNCTION_ARGS)
{
        Oid arg1_typeid = VERTEXOID;
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

        state = accumArrayResult(state,
                                                         elem,
                                                         PG_ARGISNULL(1),
                                                         arg1_typeid,
                                                         aggcontext);

        /*
         * The transition type for array_agg() is declared to be "internal", which
         * is a pass-by-value type the same size as a pointer.  So we can safely
         * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
         */
        PG_RETURN_POINTER(state);
}


PG_FUNCTION_INFO_V1(vertex_collect_transfn_w_limit);
Datum
vertex_collect_transfn_w_limit(PG_FUNCTION_ARGS)
{
        Oid arg1_typeid = VERTEXOID;
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


PG_FUNCTION_INFO_V1(vertex_collect_finalfn);
Datum
vertex_collect_finalfn(PG_FUNCTION_ARGS)
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
        result = makeMdArrayResult(state, 1, dims, lbs,
                                                           CurrentMemoryContext,
                                                           false);

        PG_RETURN_DATUM(result);
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

char *
extract_vertex_label(vertex *v) {
    graphid id = EXTRACT_VERTEX_ID(v);
    int32 label_id = get_graphid_label_id(id);

    Oid graph_oid = EXTRACT_VERTEX_GRAPH_OID(v);

    label_cache_data *cache_data = search_label_graph_oid_cache(graph_oid, label_id);
    char *label = NameStr(cache_data->name);

    if (IS_AG_DEFAULT_LABEL(label))
	    return "";

    return label;
}

gtype *
extract_vertex_properties(vertex *v) {
    char *bytes = (char *)v;

    return (gtype *)&bytes[VARHDRSZ + sizeof(graphid) + sizeof(Oid)];
}
