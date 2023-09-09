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
#include "utils/traversal.h"
#include "utils/vertex.h"

static void append_to_buffer(StringInfo buffer, const char *data, int len);
static int traversal_btree_fast_cmp(Datum x, Datum y, SortSupport ssup);

/*
 * I/O routines for vertex type
 */
PG_FUNCTION_INFO_V1(traversal_in);
Datum traversal_in(PG_FUNCTION_ARGS) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Use build_traversal()")));

    AG_RETURN_TRAVERSAL(NULL);
}

PG_FUNCTION_INFO_V1(traversal_out);
Datum traversal_out(PG_FUNCTION_ARGS) {
    traversal *v = AG_GET_ARG_TRAVERSAL(0);
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

PG_FUNCTION_INFO_V1(build_traversal);
Datum
build_traversal(PG_FUNCTION_ARGS) {
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
					 errmsg("arguement %i build_traversal() must be a vertex", i)));
            cnt++;
            append_to_buffer(&buffer, DATUM_GET_VERTEX(args[i]), VARSIZE_ANY(args[i]));
	}
	else {

            if (types[i] != EDGEOID && types[i] != VARIABLEEDGEOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("arguement %i build_traversal() must be an edge", i)));
            if (i + 1 == nargs)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("traversals must end with a vertex")));

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

    traversal *p = (traversal *)buffer.data;

    p->children[0] = cnt;
    SET_VARSIZE(p, buffer.len);

    AG_RETURN_TRAVERSAL(p);
}

PG_FUNCTION_INFO_V1(traversal_edges);
Datum traversal_edges(PG_FUNCTION_ARGS) {
    traversal *v = AG_GET_ARG_TRAVERSAL(0);
    Datum *array_value;

    int size = (v->children[0] - 1) / 2;
    array_value = (Datum *) palloc(sizeof(Datum) * size);


    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {

        if (i % 2 == 0) {
            continue;
        } else {
	    array_value[(i - 1) / 2] = EDGE_GET_DATUM((edge *)ptr);
        }
    }

    ArrayType *result = construct_array(array_value, size, EDGEOID, -1, false, TYPALIGN_INT);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(traversal_nodes);
Datum traversal_nodes(PG_FUNCTION_ARGS) {
    traversal *v = AG_GET_ARG_TRAVERSAL(0);
    Datum *array_value;

    int size = (v->children[0] + 1) / 2;
    array_value = (Datum *) palloc(sizeof(Datum) * size);


    char *ptr = &v->children[1];
    for (int i = 0; i < v->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {

        if (i % 2 == 0) {
	    array_value[i/2] = VERTEX_GET_DATUM((vertex *)ptr);
        } else {
	    continue;
        }
    }

    ArrayType *result = construct_array(array_value, size, VERTEXOID, -1, false, TYPALIGN_INT);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(traversal_size);
Datum traversal_size(PG_FUNCTION_ARGS) {
    traversal *v = AG_GET_ARG_TRAVERSAL(0);

    gtype_value gtv = { .type = AGTV_INTEGER, .val = { .int_value = v->children[0] } };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

static int32 compare_traversal_orderability(traversal *lhs, traversal *rhs) {
    char *lhs_ptr = TRAVERSAL_GET_FIRST_ENTITY(lhs);
    char *rhs_ptr = TRAVERSAL_GET_FIRST_ENTITY(rhs);

    for (int i = 0; i < lhs->children[0] && i < rhs->children[0]; i++) {

        if (i % 2 == 1) {
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

/*
 * Comparison Operators
 */
PG_FUNCTION_INFO_V1(traversal_eq);
Datum traversal_eq(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    if (rhs->children[0] != lhs->children[0])
         PG_RETURN_BOOL(false);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) == 0);
}

PG_FUNCTION_INFO_V1(traversal_ne);
Datum traversal_ne(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    if (rhs->children[0] != lhs->children[0])
         PG_RETURN_BOOL(true);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) != 0);
}

PG_FUNCTION_INFO_V1(traversal_gt);
Datum traversal_gt(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) > 0);
}

PG_FUNCTION_INFO_V1(traversal_ge);
Datum traversal_ge(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) >= 0);
}

PG_FUNCTION_INFO_V1(traversal_lt);
Datum traversal_lt(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) < 0);
}

PG_FUNCTION_INFO_V1(traversal_le);
Datum traversal_le(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    PG_RETURN_BOOL(compare_traversal_orderability(lhs, rhs) <= 0);
}

/*
 * B-Tree Support
 */
PG_FUNCTION_INFO_V1(traversal_btree_cmp);
Datum traversal_btree_cmp(PG_FUNCTION_ARGS) {
    traversal *lhs = AG_GET_ARG_TRAVERSAL(0);
    traversal *rhs = AG_GET_ARG_TRAVERSAL(1);

    PG_RETURN_INT32(compare_traversal_orderability(lhs, rhs));
}

PG_FUNCTION_INFO_V1(traversal_btree_sort);
Datum traversal_btree_sort(PG_FUNCTION_ARGS) {
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

    ssup->comparator = traversal_btree_fast_cmp;
    PG_RETURN_VOID();
}

static int traversal_btree_fast_cmp(Datum x, Datum y, SortSupport ssup) {
    traversal *lhs = DATUM_GET_TRAVERSAL(x);
    traversal *rhs = DATUM_GET_TRAVERSAL(y);

    return compare_traversal_orderability(lhs, rhs);
}


static void
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}       
