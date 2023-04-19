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

/*
 * I/O routines for vertex type
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

PG_FUNCTION_INFO_V1(vertex_in);
Datum vertex_in(PG_FUNCTION_ARGS) {
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Use build_vertex()")));

    return NULL;
}

PG_FUNCTION_INFO_V1(vertex_out);
Datum vertex_out(PG_FUNCTION_ARGS) {
    vertex *v = AG_GET_ARG_VERTEX(0);
    StringInfo str = makeStringInfo();

    // id
    appendStringInfoString(str, "{\"id\": ");
    appendStringInfoString(str, DatumGetCString(DirectFunctionCall1( int8out, Int64GetDatum((int64)v->children[0]))));

    // label
    appendStringInfoString(str, ", \"label\": \"");
    appendStringInfoString(str, extract_label(v));

    // properties
    appendStringInfoString(str, "\", \"properties\": ");
    agtype *agt = extract_properties(v);
    agtype_to_cstring(str, &agt->root, 0);


    appendStringInfoString(str, "}::vertex");

    PG_RETURN_CSTRING(str->data);
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
        
static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}

static char *extract_label(vertex *v) {
    char *bytes = (char *)v;
    char *label_addr = &bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry)];
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return pnstrdup(label_addr, *label_length);
}

static agtype *extract_properties(vertex *v) {
    char *bytes = (char *)v;
    int *label_length = (int *)&bytes[VARHDRSZ + sizeof(graphid)];

    return (agtype *)&bytes[VARHDRSZ + sizeof(graphid) + sizeof(agtentry) + ((*label_length) * sizeof(char))];
}
