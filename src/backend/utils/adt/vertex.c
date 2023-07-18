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

#include "utils/fmgrprotos.h"
#include "utils/varlena.h"

#include "utils/gtype.h"
#include "utils/graphid.h"
#include "utils/vertex.h"

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
    graphid id = *((int64 *)(&v->children[0]));
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(id))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    appendStringInfoString(str, extract_vertex_label(v));

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
    char *label = PG_GETARG_CSTRING(1);
    gtype *properties = AG_GET_ARG_GTYPE_P(2);

    if (!AGT_ROOT_IS_OBJECT(properties))
        PG_RETURN_NULL();

    StringInfoData buffer;
    initStringInfo(&buffer);

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    // id
    append_to_buffer(&buffer, (char *)&id, sizeof(graphid));

    // label
    int len = strlen(label);
    append_to_buffer(&buffer, (char *)&len, sizeof(agtentry));
    append_to_buffer(&buffer, label, len);

    // properties
    append_to_buffer(&buffer, properties, VARSIZE(properties));

    vertex *v = (vertex *)buffer.data;

    SET_VARSIZE(v, buffer.len);

    AG_RETURN_VERTEX(v);
}
  
vertex *
create_vertex(graphid id, char *label, gtype *properties) {
    StringInfoData buffer;
    initStringInfo(&buffer);

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    // id
    append_to_buffer(&buffer, (char *)&id, sizeof(graphid));

    // label
    int len = strlen(label);
    append_to_buffer(&buffer, (char *)&len, sizeof(agtentry));
    append_to_buffer(&buffer, label, len);

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
    else
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

    gtype_value *val = find_gtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv);

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

        if (find_gtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv) != NULL)
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

        if (find_gtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv) == NULL)
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

    AG_RETURN_GRAPHID((graphid)v->children[0]);
}

PG_FUNCTION_INFO_V1(age_vertex_id);
Datum
age_vertex_id(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = {.int_value = *((int64 *)(&v->children[0])) } };    

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(vertex_label);
Datum
vertex_label(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    char *label = extract_vertex_label(v);

    Datum d = string_to_gtype(label);

    PG_RETURN_POINTER(d);
}

PG_FUNCTION_INFO_V1(vertex_properties);
Datum
vertex_properties(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    AG_RETURN_GTYPE_P(extract_vertex_properties(v));
}

PG_FUNCTION_INFO_V1(age_vertex_label);
Datum
age_vertex_label(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    gtype_value gtv = {
            .type = AGTV_STRING,
            .val.string = { extract_vertex_label_length(v), extract_vertex_label(v) }
            };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

char *
extract_vertex_label(vertex *v) {
    char *bytes = (char *)v;
    char *label_addr = &bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry)];
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return pnstrdup(label_addr, *label_length);
}

int
extract_vertex_label_length(vertex *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + ( sizeof(graphid))];

    return *label_length;
}

gtype *
extract_vertex_properties(vertex *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return (gtype *)&bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry) + ((*label_length) * sizeof(char))];
}
