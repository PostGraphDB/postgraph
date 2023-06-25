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

#include "utils/agtype.h"
#include "utils/graphid.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);
static agtype *extract_properties(vertex *v);
static char *extract_label(vertex *v);

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
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum((int64)v->children[0]))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    appendStringInfoString(str, extract_label(v));

    // properties
    appendStringInfoString(str, "\", \"properties\": ");
    agtype *agt = extract_properties(v);
    agtype_to_cstring(str, &agt->root, 0);


    appendStringInfoString(str, "}");

    PG_RETURN_CSTRING(str->data);
}

void append_vertex_to_string(StringInfoData *buffer, vertex *v){

    // id
    appendStringInfoString(buffer, "{\"id\": ");
    appendStringInfoString(buffer, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum((int64)v->children[0]))));

    // label
    appendStringInfoString(buffer, ", \"label\": \"");
    appendStringInfoString(buffer, extract_label(v));

    // properties
    appendStringInfoString(buffer, "\", \"properties\": ");
    agtype *agt = extract_properties(v);
    agtype_to_cstring(buffer, &agt->root, 0);


    appendStringInfoString(buffer, "}");

}

PG_FUNCTION_INFO_V1(build_vertex);
Datum
build_vertex(PG_FUNCTION_ARGS) {
    graphid id = AG_GETARG_GRAPHID(0);
    char *label = PG_GETARG_CSTRING(1);
    agtype *properties = AG_GET_ARG_AGTYPE_P(2);

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
    agtype *agt = extract_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);
                                             
    AG_RETURN_AGTYPE_P(agtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), false));
}

// ->> operator
PG_FUNCTION_INFO_V1(vertex_property_access_text);
Datum
vertex_property_access_text(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    agtype *agt = extract_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);
                                             
    AG_RETURN_AGTYPE_P(agtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), true));
}

// @> operator
PG_FUNCTION_INFO_V1(vertex_contains);
Datum 
vertex_contains(PG_FUNCTION_ARGS) {                                    
    vertex *v = AG_GET_ARG_VERTEX(0);                                 
    agtype *properties = extract_properties(v);
    agtype *constraints = AG_GET_ARG_AGTYPE_P(1);

    agtype_iterator *constraint_it = agtype_iterator_init(&constraints->root);
    agtype_iterator *property_it = agtype_iterator_init(&properties->root);
    
    PG_RETURN_BOOL(agtype_deep_contains(&property_it, &constraint_it));
}   
    

// <@ operator
PG_FUNCTION_INFO_V1(vertex_contained_by);  
Datum 
vertex_contained_by(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(1);
    agtype *properties = extract_properties(v);
    agtype *constraints = AG_GET_ARG_AGTYPE_P(0);

    agtype_iterator *constraint_it = agtype_iterator_init(&constraints->root);
    agtype_iterator *property_it = agtype_iterator_init(&properties->root);

    PG_RETURN_BOOL(agtype_deep_contains(&constraint_it, &property_it));
}

// ? operator
PG_FUNCTION_INFO_V1(vertex_exists);
Datum
vertex_exists(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    agtype *agt = extract_properties(v);
    text *key = PG_GETARG_TEXT_PP(1);

    /*
     * We only match Object keys (which are naturally always Strings), or
     * In particular, we do not match non-string scalar elements. 
     * Existence of a key/element is only considered at the
     * top level.  No recursion occurs.
     */
    agtype_value agtv = { .type = AGTV_STRING, .val.string = {VARSIZE_ANY_EXHDR(key), VARDATA_ANY(key)} };

    agtype_value *val = find_agtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv);

    PG_RETURN_BOOL(val != NULL);
}

// ?| operator
PG_FUNCTION_INFO_V1(vertex_exists_any);
Datum
vertex_exists_any(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    agtype *agt = extract_properties(v);
    ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls, &elem_count);

    for (int i = 0; i < elem_count; i++) {
        if (key_nulls[i])
            continue;

        agtype_value agtv = { .type = AGTV_STRING, .val.string = { VARSIZE(key_datums[i]) - VARHDRSZ, VARDATA_ANY(key_datums[i]) } };

        if (find_agtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv) != NULL)
            PG_RETURN_BOOL(true);
    }

    PG_RETURN_BOOL(false);
}

// ?& operator
PG_FUNCTION_INFO_V1(vertex_exists_all);
Datum
vertex_exists_all(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    agtype *agt = extract_properties(v);
    ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls, &elem_count);

    for (int i = 0; i < elem_count; i++) {
        if (key_nulls[i])
            continue;

        agtype_value agtv = { .type = AGTV_STRING, .val.string = { VARSIZE(key_datums[i]) - VARHDRSZ, VARDATA_ANY(key_datums[i]) } };

        if (find_agtype_value_from_container(&agt->root, AGT_FOBJECT | AGT_FARRAY, &agtv) == NULL)
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

PG_FUNCTION_INFO_V1(vertex_label);
Datum
vertex_label(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    char *label = extract_label(v);

    Datum d = string_to_agtype(label);

    PG_RETURN_POINTER(d);
}

PG_FUNCTION_INFO_V1(vertex_properties);
Datum
vertex_properties(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);

    AG_RETURN_AGTYPE_P(extract_properties(v));
}



static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

static char *
extract_label(vertex *v) {
    char *bytes = (char *)v;
    char *label_addr = &bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry)];
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return pnstrdup(label_addr, *label_length);
}

static agtype *
extract_properties(vertex *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return (agtype *)&bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry) + ((*label_length) * sizeof(char))];
}
