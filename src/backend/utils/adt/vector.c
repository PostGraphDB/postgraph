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
 *
 * Portions Copyright (c) 2021-2023, pgvector
 */

#include "postgres.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "port.h" /* for strtof() */
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#include "utils/gtype.h"
#include "utils/vector.h"

#define STATE_DIMS(x) (ARR_DIMS(x)[0] - 1)
#define CreateStateDatums(dim) palloc(sizeof(Datum) * (dim + 1))

static gtype_value *
InitVectorGType(int dim)
{
    int size = VECTOR_SIZE(dim) + sizeof(enum gtype_value_type) + sizeof(float8);

    gtype_value *result = (gtype_value *) palloc0(size);

    result->val.vector.dim = dim;

    return result;
}


/*
 * Ensure same dimensions
 */
static inline void
CheckDims(Vector * a, Vector * b)
{
    if (a->dim != b->dim)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("different vector dimensions %d and %d", a->dim, b->dim)));
}

/*
 * Ensure expected dimensions
 */
static inline void
CheckExpectedDim(int32 typmod, int dim)
{
    if (typmod != -1 && typmod != dim)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("expected %d dimensions, not %d", typmod, dim)));
}

/*
 * Ensure valid dimensions
 */
static inline void
CheckDim(int dim)
{
    if (dim < 1)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("vector must have at least 1 dimension")));

    if (dim > VECTOR_MAX_DIM)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));
}

/*
 * Ensure finite elements
 */
static inline void
CheckElement(float value)
{
    if (isnan(value))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("NaN not allowed in vector")));

    if (isinf(value))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("infinite value not allowed in vector")));
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool
vector_isspace(char ch)
{
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f')
        return true;
    return false;
}

/*
 * Check state array
 */
static float8 *
CheckStateArray(ArrayType *statearray, const char *caller)
{
    if (ARR_NDIM(statearray) != 1 || ARR_DIMS(statearray)[0] < 1 ||
        ARR_HASNULL(statearray) || ARR_ELEMTYPE(statearray) != FLOAT8OID)
        elog(ERROR, "%s: expected state array", caller);
    return (float8 *) ARR_DATA_PTR(statearray);
}

gtype_value *
gtype_vector_in(char *str, int32 typmod)
{
    float8 x[VECTOR_MAX_DIM];
    int dim = 0;
    char *pt;
    char *stringEnd;
    gtype_value *result;
    char *lit = pstrdup(str);

    while (vector_isspace(*str))
        str++;

    if (*str != '[')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("malformed vector literal: \"%s\"", lit),
                 errdetail("Vector contents must start with \"[\".")));

    str++;
    pt = strtok(str, ",");
    stringEnd = pt;

    while (pt != NULL && *stringEnd != ']')
    {
        if (dim == VECTOR_MAX_DIM)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));

        while (vector_isspace(*pt))
            pt++;

        /* Check for empty string like float4in */
        if (*pt == '\0')
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type vector: \"%s\"", lit)));

        /* Use strtof like float4in to avoid a double-rounding problem */
        x[dim] = strtod(pt, &stringEnd);
        CheckElement(x[dim]);
        dim++;

        if (stringEnd == pt)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type vector: \"%s\"", lit)));

        while (vector_isspace(*stringEnd))
            stringEnd++;

        if (*stringEnd != '\0' && *stringEnd != ']')
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type vector: \"%s\"", lit)));
        pt = strtok(NULL, ",");
    }

    if (stringEnd == NULL || *stringEnd != ']')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("malformed vector literal: \"%s\"", lit),
                 errdetail("Unexpected end of input.")));

    stringEnd++;

    /* Only whitespace is allowed after the closing brace */
    while (vector_isspace(*stringEnd))
        stringEnd++;

    if (*stringEnd != '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("malformed vector literal: \"%s\"", lit),
                 errdetail("Junk after closing right brace.")));

    /* Ensure no consecutive delimiters since strtok skips */
    for (pt = lit + 1; *pt != '\0'; pt++)
    {
        if (pt[-1] == ',' && *pt == ',')
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("malformed vector literal: \"%s\"", lit)));
    }

    if (dim < 1)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("vector must have at least 1 dimension")));

    pfree(lit);

    CheckExpectedDim(typmod, dim);

    result = InitVectorGType(dim);
    for (int i = 0; i < dim; i++)
        result->val.vector.x[i] = x[i];

    result->type = AGTV_VECTOR;
    return result;
}

/*
 * Convert internal representation to textual representation
 */
char *
vector_out(Vector *vector)
{
    int dim = vector->dim;
    char *buf;
    char *ptr;
    int n;

    /*
     * Need:
     *
     * dim * (FLOAT_SHORTEST_DECIMAL_LEN - 1) bytes for
     * float_to_shortest_decimal_bufn
     *
     * dim - 1 bytes for separator
     *
     * 3 bytes for [, ], and \0
     */
    buf = (char *) palloc(FLOAT_SHORTEST_DECIMAL_LEN * dim + 2);
    ptr = buf;

    *ptr = '[';
    ptr++;
    for (int i = 0; i < dim; i++) {
        if (i > 0) {
            *ptr = ',';
            ptr++;
        }

        n = float_to_shortest_decimal_bufn(vector->x[i], ptr);

    }
    *ptr = ']';
    ptr++;
    *ptr = '\0';

    return buf;
}

PG_FUNCTION_INFO_V1(l2_distance);
Datum l2_distance(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (!AGT_IS_VECTOR(lhs) || !AGT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("<-> requires vector arguments")));



    if (AGT_ROOT_COUNT(lhs) != AGT_ROOT_COUNT(rhs))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("different vector dimensions %i and %i", AGT_ROOT_COUNT(lhs), AGT_ROOT_COUNT(rhs))));

    gtype_iterator *lhs_it, *rhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    rhs_it = gtype_iterator_init(&rhs->root);
    gtype_iterator_token type;

    gtype_value lgtv, rgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);
    gtype_iterator_next(&rhs_it, &rgtv, true);

    Assert (type == WAGT_BEGIN_VECTOR);

    float8 distance = 0.0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WAGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);
        
	float8 lhs_f = lgtv.val.float_value;
	float8 rhs_f = rgtv.val.float_value;

	float8 diff = lhs_f - rhs_f;
	distance += diff * diff;
    }

    gtype_value gtv = {
        .type = AGTV_FLOAT,
	.val.float_value = sqrt(distance)
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


