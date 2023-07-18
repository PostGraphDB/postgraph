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
 */
#include "postgraph.h"

#include "utils/fmgrprotos.h"
#include "utils/varlena.h"

#include "utils/edge.h"
#include "utils/variable_edge.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);

/*
 * I/O routines for vertex type
 */
PG_FUNCTION_INFO_V1(variable_edge_in);
Datum variable_edge_in(PG_FUNCTION_ARGS) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Use build_traversal()")));

    AG_RETURN_VARIABLE_EDGE(NULL);
}

PG_FUNCTION_INFO_V1(variable_edge_out);
Datum variable_edge_out(PG_FUNCTION_ARGS) {
    VariableEdge *v = AG_GET_ARG_VARIABLE_EDGE(0);
    StringInfo str = makeStringInfo();

    appendStringInfoString(str, "[");
 //      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
   //                 errmsg("%i", v->children[0])));


    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
	if (i % 2 == 1) {
	    appendStringInfoString(str, ", ");
            append_vertex_to_string(str, (vertex *)ptr);
	    appendStringInfoString(str, ", ");
        } else {
            append_edge_to_string(str, (edge *)ptr);
	}
    } 

    appendStringInfoString(str, "]");
    PG_RETURN_CSTRING(str->data);
}

PG_FUNCTION_INFO_V1(build_variable_edge);
Datum
build_variable_edge(PG_FUNCTION_ARGS) {
    StringInfoData buffer;
    initStringInfo(&buffer);
    Datum *args;
    bool *nulls;
    Oid *types;
    prentry nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    int32 size;

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    append_to_buffer(&buffer, (char *)&nargs, sizeof(prentry));

    for (int i = 0; i < nargs; i++) {
        if (i % 2 == 1) {
            if (types[i] != VERTEXOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arguement %i build_traversal() must be a vertex", i)));
            if (i + 1 == nargs)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("VariableEdges must end with an edge")));

            append_to_buffer(&buffer, DATUM_GET_VERTEX(args[i]), VARSIZE(args[i]));
	}
	else {

            if (types[i] != EDGEOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arguement %i build_traversal() must be an edge", i)));

            append_to_buffer(&buffer, DATUM_GET_EDGE(args[i]), VARSIZE(args[i]));
	}
    }

    VariableEdge *p = (VariableEdge *)buffer.data;

    SET_VARSIZE(p, buffer.len);

    AG_RETURN_VARIABLE_EDGE(p);
}

/*
 * Comparison Operators
 */

// match 2 VLE edges 
PG_FUNCTION_INFO_V1(match_vles);
Datum match_vles(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    edge *left_edge = (edge *)&lhs->children[1];

    char *ptr = &rhs->children[1];
    for (int i = 0; i < rhs->children[0] - 1; i++, ptr = ptr + VARSIZE(ptr));
    edge *right_edge = (edge *)ptr;

    graphid left_start =  *((int64 *)(&left_edge->children[2]));
    graphid left_end =  *((int64 *)(&left_edge->children[4]));

    graphid right_start =  *((int64 *)(&right_edge->children[2]));
    graphid right_end =  *((int64 *)(&right_edge->children[4]));

PG_RETURN_BOOL(left_start == right_start || left_end == right_start ||
		left_start == right_end || left_end == right_end);
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}       
