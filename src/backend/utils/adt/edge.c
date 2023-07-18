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

#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "utils/fmgrprotos.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"

#include "catalog/ag_label.h"
#include "utils/gtype.h"
#include "utils/graphid.h"
#include "utils/edge.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);
static Datum get_vertex(const char *graph, const char *vertex_label, int64 graphid);

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
    graphid startid = *((int64 *)(&v->children[4]));
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
    char *label = PG_GETARG_CSTRING(3);
    gtype *properties = AG_GET_ARG_GTYPE_P(4);

    if (!AGT_ROOT_IS_OBJECT(properties))
        PG_RETURN_NULL();

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

    // label
    int len = strlen(label);
    append_to_buffer(&buffer, (char *)&len, sizeof(agtentry));
    append_to_buffer(&buffer, label, len);

    // properties
    append_to_buffer(&buffer, properties, VARSIZE(properties));

    edge *v = (edge *)buffer.data;

    SET_VARSIZE(v, buffer.len);

    AG_RETURN_EDGE(v);
}
 
edge *
create_edge(graphid id,graphid start_id,graphid end_id, char *label, gtype *properties) {
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

    // label
    int len = strlen(label);
    append_to_buffer(&buffer, (char *)&len, sizeof(agtentry));
    append_to_buffer(&buffer, label, len);

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

    gtype_value gtv = {
        .type = AGTV_STRING,
        .val.string = { extract_edge_label_length(v), extract_edge_label(v) }
    };

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
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val, agtv_object->val.string.len);

    edge *e = AG_GET_ARG_EDGE(1);
    graph_oid = *((int64 *)(&e->children[2]));

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);
    result = get_vertex(graph_name, label_name, graph_oid);

    return result;
}


PG_FUNCTION_INFO_V1(edge_endnode);
Datum edge_endnode(PG_FUNCTION_ARGS) {
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val, agtv_object->val.string.len);

    edge *e = AG_GET_ARG_EDGE(1);
    graph_oid = *((int64 *)(&e->children[4]));

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);
    result = get_vertex(graph_name, label_name, graph_oid);

    return result;
}


static Datum get_vertex(const char *graph, const char *vertex_label, int64 graphid)
{
    ScanKeyData scan_keys[1];
    Relation graph_vertex_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum id, properties, result;

    Oid graph_namespace_oid = get_namespace_oid(graph, false);
    Oid vertex_label_table_oid = get_relname_relid(vertex_label, graph_namespace_oid);
    Snapshot snapshot = GetActiveSnapshot();

    ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(graphid));

    graph_vertex_label = table_open(vertex_label_table_oid, ShareLock);
    scan_desc = table_beginscan(graph_vertex_label, snapshot, 1, scan_keys);
    tuple = heap_getnext(scan_desc, ForwardScanDirection);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("graphid cde %lu does not exist", graphid)));

    tupdesc = RelationGetDescr(graph_vertex_label);
    if (tupdesc->natts != 2)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid number of attributes for %s.%s", graph,
                        vertex_label )));

    id = column_get_datum(tupdesc, tuple, 0, "id", GRAPHIDOID, true);
    properties = column_get_datum(tupdesc, tuple, 1, "properties", GTYPEOID, true);
    /* reconstruct the vertex */
    result = VERTEX_GET_DATUM(create_vertex(DATUM_GET_GRAPHID(id), vertex_label, DATUM_GET_GTYPE_P(properties)));
    /* end the scan and close the relation */
    table_endscan(scan_desc);
    table_close(graph_vertex_label, ShareLock);
    /* return the vertex datum */
    return result;
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

char *
extract_edge_label(edge *v) {
    char *bytes = (char *)v;
    char *label_addr = &bytes[VARHDRSZ + ( 3 * sizeof(graphid)) + sizeof(agtentry)];
    int *label_length = (int *)&bytes[VARHDRSZ + ( 3 * sizeof(graphid))];

    return pnstrdup(label_addr, *label_length);
}

int
extract_edge_label_length(edge *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + ( 3 * sizeof(graphid))];

    return *label_length;
}



gtype *
extract_edge_properties(edge *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + (3 * sizeof(graphid))];

    return (gtype *)&bytes[VARHDRSZ + (3 * sizeof(graphid)) + sizeof(agtentry) + ((*label_length) * sizeof(char))];
}
