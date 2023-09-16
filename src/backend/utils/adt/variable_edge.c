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
static int variable_edge_btree_fast_cmp(Datum x, Datum y, SortSupport ssup);

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
static int32 compare_variable_edge_orderability(VariableEdge *lhs, VariableEdge *rhs) {

    char *lhs_ptr = &lhs->children[1];
    char *rhs_ptr = &rhs->children[1];

    for (int i = 0; i < lhs->children[0] && i < rhs->children[0]; i++) {

        if (i % 2 == 0) {
            graphid left_id = EXTRACT_EDGE_ID(lhs_ptr);
            graphid right_id = EXTRACT_EDGE_ID(rhs_ptr);

            if (left_id > right_id)
                return 1;
             else if (left_id < right_id)
                return -1;
        } else {
            graphid left_id = EXTRACT_VERTEX_ID(lhs_ptr);
            graphid right_id = EXTRACT_VERTEX_ID(rhs_ptr);

            if (left_id > right_id)
                return 1;
             else if (left_id < right_id)
                return -1;
        }

        lhs_ptr = lhs_ptr + VARSIZE(lhs_ptr);
        rhs_ptr = rhs_ptr + VARSIZE(rhs_ptr);
    }

    if (lhs->children[0] > rhs->children[0])
        return 1;
    else if (lhs->children[0] == rhs->children[0])
        return 0;
    return -1;
}



PG_FUNCTION_INFO_V1(variable_edge_eq);
Datum variable_edge_eq(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    if (rhs->children[0] != lhs->children[0])
         PG_RETURN_BOOL(false);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) == 0);
}

PG_FUNCTION_INFO_V1(variable_edge_ne);
Datum variable_edge_ne(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    if (rhs->children[0] != lhs->children[0])
         PG_RETURN_BOOL(true);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) != 0);
}

PG_FUNCTION_INFO_V1(variable_edge_gt);
Datum variable_edge_gt(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) > 0);
}

PG_FUNCTION_INFO_V1(variable_edge_ge);
Datum variable_edge_ge(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) >= 0);
}

PG_FUNCTION_INFO_V1(variable_edge_lt);
Datum variable_edge_lt(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) < 0);
}

PG_FUNCTION_INFO_V1(variable_edge_le);
Datum variable_edge_le(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    PG_RETURN_BOOL(compare_variable_edge_orderability(lhs, rhs) <= 0);
}

/*
 * B-Tree Support
 */
PG_FUNCTION_INFO_V1(variable_edge_btree_cmp);
Datum variable_edge_btree_cmp(PG_FUNCTION_ARGS) {
    VariableEdge *lhs = AG_GET_ARG_VARIABLE_EDGE(0);
    VariableEdge *rhs = AG_GET_ARG_VARIABLE_EDGE(1);

    PG_RETURN_INT32(compare_variable_edge_orderability(lhs, rhs));
}

PG_FUNCTION_INFO_V1(variable_edge_btree_sort);
Datum variable_edge_btree_sort(PG_FUNCTION_ARGS) {
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

    ssup->comparator = variable_edge_btree_fast_cmp;
    PG_RETURN_VOID();
}

static int variable_edge_btree_fast_cmp(Datum x, Datum y, SortSupport ssup) {
    VariableEdge *lhs = DATUM_GET_VARIABLE_EDGE(x);
    VariableEdge *rhs = DATUM_GET_VARIABLE_EDGE(y);

    return compare_variable_edge_orderability(lhs, rhs);
}

PG_FUNCTION_INFO_V1(edge_contained_in_variable_edge);
Datum edge_contained_in_variable_edge(PG_FUNCTION_ARGS) {
    edge *e = AG_GET_ARG_EDGE(0);
    VariableEdge *variable_edge = AG_GET_ARG_VARIABLE_EDGE(1);

    graphid left_id = EXTRACT_EDGE_ID(e);

    char *ptr = &variable_edge->children[1];
    for (int i = 0; i < variable_edge->children[0] - 1; i++, ptr = ptr + VARSIZE(ptr)) {
	 if (i % 2 == 0) {
             edge *right_edge = (edge *)ptr;
	     graphid right_id = EXTRACT_EDGE_ID(right_edge);
	     if (left_id == right_id)
                 PG_RETURN_BOOL(true);
	 }
    }

    PG_RETURN_BOOL(false);
}

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

/*
 * Functions
 */
PG_FUNCTION_INFO_V1(variable_edge_edges);
Datum variable_edge_edges(PG_FUNCTION_ARGS) {
    VariableEdge *v = AG_GET_ARG_VARIABLE_EDGE(0);

    int size = (v->children[0] + 1) / 2;
    Datum *array_value = (Datum *) palloc(sizeof(Datum) * size);

    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
        if (i % 2 == 1) {
            continue;
        } else {
            array_value[i/2] = EDGE_GET_DATUM((edge *)ptr);
        }
    }

    PG_RETURN_ARRAYTYPE_P(construct_array(array_value, size, EDGEOID, -1, false, TYPALIGN_INT));
}


PG_FUNCTION_INFO_V1(variable_edge_nodes);
Datum variable_edge_nodes(PG_FUNCTION_ARGS) {
    VariableEdge *v = AG_GET_ARG_VARIABLE_EDGE(0);

    int size = (v->children[0] - 1) / 2;
    Datum *array_value = (Datum *) palloc(sizeof(Datum) * size);

    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
        if (i % 2 == 1) {
            array_value[i/2] = VERTEX_GET_DATUM((vertex *)ptr);
        } else {
            continue;
        }
    }

    PG_RETURN_ARRAYTYPE_P(construct_array(array_value, size, VERTEXOID, -1, false, TYPALIGN_INT));
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}       
