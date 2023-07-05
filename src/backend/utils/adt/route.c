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
#include "utils/route.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);

/*
 * I/O routines for vertex type
 */
PG_FUNCTION_INFO_V1(route_in);
Datum route_in(PG_FUNCTION_ARGS) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Use build_route()")));

    AG_RETURN_ROUTE(NULL);
}

PG_FUNCTION_INFO_V1(route_out);
Datum route_out(PG_FUNCTION_ARGS) {
    route *v = AG_GET_ARG_ROUTE(0);
    StringInfo str = makeStringInfo();

    appendStringInfoString(str, "[");
    
    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
	
	if (i % 2 == 0) {
            append_vertex_to_string(str, (vertex *)ptr);
        } else {
	    appendStringInfoString(str, ", ");
            append_edge_to_string(str, (edge *)ptr);
	    appendStringInfoString(str, ", ");
	}
    } 

    appendStringInfoString(str, "]");
    PG_RETURN_CSTRING(str->data);
}

PG_FUNCTION_INFO_V1(build_route);
Datum
build_route(PG_FUNCTION_ARGS) {
    StringInfoData buffer;
    initStringInfo(&buffer);
    Datum *args;
    bool *nulls;
    Oid *types;
    pentry nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    int32 size;

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    // length
    reserve_from_buffer(&buffer, sizeof(pentry));

    int cnt = 0;
    for (int i = 0; i < nargs; i++) {
        if (i % 2 == 0) {
            if (types[i] != VERTEXOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arguement %i build_route() must be a vertex", i)));
            cnt++;
            append_to_buffer(&buffer, DATUM_GET_VERTEX(args[i]), VARSIZE_ANY(args[i]));
	}
	else {

            if (types[i] != EDGEOID && types[i] != VARIABLEEDGEOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arguement %i build_route() must be an edge", i)));
            if (i + 1 == nargs)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("routes must end with a vertex")));

            if (types[i] == EDGEOID) {
                append_to_buffer(&buffer, DATUM_GET_EDGE(args[i]), VARSIZE_ANY(args[i]));
	        cnt++;
	    } else {
		VariableEdge *v = DATUM_GET_VARIABLE_EDGE(args[i]);
                char *ptr = &v->children[1];
                for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
                     append_to_buffer(&buffer, DATUM_GET_EDGE(ptr), VARSIZE_ANY(ptr));
		     cnt++;
                }
	    }
	}
    }

    route *p = (route *)buffer.data;

    p->children[0] = cnt;
    SET_VARSIZE(p, buffer.len);

    AG_RETURN_ROUTE(p);
}

static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}       
