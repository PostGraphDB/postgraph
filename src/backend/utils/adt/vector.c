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

typedef float8 (*vector_dist_per_elem_calculation) (float8 distance, float8 lhs, float8 rhs);
typedef float8 (*vector_dist_final) (float8 distance);

float8 calc_l2_distance(float8 distance, float8 lhs, float8 rhs){
     float8 diff = lhs - rhs;
     distance += diff * diff;
     return distance;
}

float8 calc_l2_squared_distance_final(float8 distance) {
    return sqrt(distance);
}

float8 calc_inner_product(float8 distance, float8 lhs, float8 rhs){
     return distance += lhs * rhs;
}

float8 calc_negative_inner_product_final(float8 distance) {
    return distance * -1;
}

float8 calc_spherical_distance(float8 distance, float8 lhs, float8 rhs){
     return distance += lhs * rhs;
}

float8 calc_l1_distance(float8 distance, float8 lhs, float8 rhs){
     return distance += fabs(lhs - rhs);
}

gtype_value *
InitVectorGType(int dim)
{
  //  int size = VECTOR_SIZE(dim) + sizeof(enum gtype_value_type) + sizeof(float8);

    gtype_value *result = (gtype_value *) palloc0(sizeof(gtype_value));

    result->val.vector.dim = dim;

    result->val.vector.x = palloc0(sizeof(float8) * dim);

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

static Datum vector_dist_function(gtype *lhs, gtype *rhs, vector_dist_per_elem_calculation per_elem,  vector_dist_final final, char *function) {
    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s requires vector arguments", function)));

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

    Assert (type == WGT_BEGIN_VECTOR);

    float8 distance = 0.0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        distance = per_elem(distance, lgtv.val.float_value, rgtv.val.float_value);
    }

    if (final != NULL)
	    distance = final(distance);

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = distance
    };

    return gtype_value_to_gtype(&gtv);
}

PG_FUNCTION_INFO_V1(l2_distance);
Datum l2_distance(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P(vector_dist_function(lhs, rhs, calc_l2_distance,  NULL, "l2_distance"));
}

PG_FUNCTION_INFO_V1(gtype_l2_squared_distance);
Datum gtype_l2_squared_distance(PG_FUNCTION_ARGS) {

    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    
    AG_RETURN_GTYPE_P(vector_dist_function(lhs, rhs, calc_l2_distance, calc_l2_squared_distance_final, "l2_squared_distance"));
}


PG_FUNCTION_INFO_V1(gtype_inner_product);
Datum gtype_inner_product(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P(vector_dist_function(lhs, rhs, calc_inner_product,  NULL, "inner_product"));
}

PG_FUNCTION_INFO_V1(gtype_negative_inner_product);
Datum gtype_negative_inner_product(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P(vector_dist_function(lhs, rhs, calc_inner_product,  calc_negative_inner_product_final, "negative_inner_product"));
}


PG_FUNCTION_INFO_V1(gtype_cosine_distance);
Datum gtype_cosine_distance(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
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

    Assert (type == WGT_BEGIN_VECTOR);

    float8 distance = 0.0, norma = 0.0, normb = 0.0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        float8 lhs_f = lgtv.val.float_value;
        float8 rhs_f = rgtv.val.float_value;

        distance += lhs_f * rhs_f;
	norma += lhs_f * lhs_f;
	normb += rhs_f * rhs_f;
    }

    float8 similarity = distance / sqrt(norma * normb);

    if (similarity > 1)
        similarity = 1.0;
    else if (similarity < -1)
        similarity = -1.0;

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = 1.0 - similarity
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_spherical_distance);
Datum gtype_spherical_distance(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
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

    Assert (type == WGT_BEGIN_VECTOR);

    float8 distance = 0.0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        distance = calc_spherical_distance(distance, lgtv.val.float_value, rgtv.val.float_value);
    }

    if (distance > 1)
        distance = 1;
    else if (distance < -1)
        distance = -1;

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = acos(distance) / M_PI
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_l1_distance);
Datum gtype_l1_distance(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P(vector_dist_function(lhs, rhs, calc_l1_distance,  NULL, "l1_distance"));
}

PG_FUNCTION_INFO_V1(gtype_dims);
Datum gtype_dims(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);

    if (!GT_IS_VECTOR(lhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("<-> requires vector arguments")));

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = AGT_ROOT_COUNT(lhs)
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_norm);
Datum gtype_norm(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);

    if (!GT_IS_VECTOR(lhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("<-> requires vector arguments")));

    gtype_iterator *lhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    gtype_iterator_token type;

    gtype_value lgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);

    Assert (type == WGT_BEGIN_VECTOR);

    float8 norm = 0.0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        float8 lhs_f = lgtv.val.float_value;

        norm += lhs_f * lhs_f;
    }

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = sqrt(norm)
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


gtype_value *gtype_vector_add(gtype *lhs, gtype *rhs) {

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("+ requires vector arguments")));

    if (AGT_ROOT_COUNT(lhs) != AGT_ROOT_COUNT(rhs))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("different vector dimensions %i and %i", AGT_ROOT_COUNT(lhs), AGT_ROOT_COUNT(rhs))));

    int dim = AGT_ROOT_COUNT(lhs);

    gtype_iterator *lhs_it, *rhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    rhs_it = gtype_iterator_init(&rhs->root);
    gtype_iterator_token type;

    gtype_value lgtv, rgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);
    gtype_iterator_next(&rhs_it, &rgtv, true);

    Assert (type == WGT_BEGIN_VECTOR);

    gtype_value *result = InitVectorGType(dim);
    result->type = AGTV_VECTOR;
    int i = 0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        float8 lhs_f = lgtv.val.float_value;
        float8 rhs_f = rgtv.val.float_value;

        result->val.vector.x[i] = lhs_f + rhs_f;

	if (isinf(result->val.vector.x[i]))
            float_overflow_error();
	i++;
    }

    return result;
}


gtype_value *gtype_vector_sub(gtype *lhs, gtype *rhs) {

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("- requires vector arguments")));

    if (AGT_ROOT_COUNT(lhs) != AGT_ROOT_COUNT(rhs))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("different vector dimensions %i and %i", AGT_ROOT_COUNT(lhs), AGT_ROOT_COUNT(rhs))));

    int dim = AGT_ROOT_COUNT(lhs);

    gtype_iterator *lhs_it, *rhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    rhs_it = gtype_iterator_init(&rhs->root);
    gtype_iterator_token type;

    gtype_value lgtv, rgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);
    gtype_iterator_next(&rhs_it, &rgtv, true);

    Assert (type == WGT_BEGIN_VECTOR);

    gtype_value *result = InitVectorGType(dim);
    result->type = AGTV_VECTOR;
    int i = 0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        float8 lhs_f = lgtv.val.float_value;
        float8 rhs_f = rgtv.val.float_value;

        result->val.vector.x[i] = lhs_f - rhs_f;

        if (isinf(result->val.vector.x[i]))
            float_overflow_error();
        i++;
    }

    return result;
}

gtype_value *gtype_vector_mul(gtype *lhs, gtype *rhs) {

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("- requires vector arguments")));

    if (AGT_ROOT_COUNT(lhs) != AGT_ROOT_COUNT(rhs))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("different vector dimensions %i and %i", AGT_ROOT_COUNT(lhs), AGT_ROOT_COUNT(rhs))));

    int dim = AGT_ROOT_COUNT(lhs);

    gtype_iterator *lhs_it, *rhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    rhs_it = gtype_iterator_init(&rhs->root);
    gtype_iterator_token type;

    gtype_value lgtv, rgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);
    gtype_iterator_next(&rhs_it, &rgtv, true);

    Assert (type == WGT_BEGIN_VECTOR);

    gtype_value *result = InitVectorGType(dim);
    result->type = AGTV_VECTOR;
    int i = 0;
    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        float8 lhs_f = lgtv.val.float_value;
        float8 rhs_f = rgtv.val.float_value;

        result->val.vector.x[i] = lhs_f * rhs_f;

        if (isinf(result->val.vector.x[i]))
            float_overflow_error();

	if (result->val.vector.x[i] == 0 && !(lhs_f == 0 || rhs_f == 0))
            float_underflow_error();

        i++;
    }

    return result;
}


int32 gtype_vector_cmp(gtype *lhs, gtype *rhs) {

    if (!GT_IS_VECTOR(lhs) || !GT_IS_VECTOR(rhs))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cmp requires vector arguments")));

    if (AGT_ROOT_COUNT(lhs) != AGT_ROOT_COUNT(rhs))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("different vector dimensions %i and %i", AGT_ROOT_COUNT(lhs), AGT_ROOT_COUNT(rhs))));

    int dim = AGT_ROOT_COUNT(lhs);

    gtype_iterator *lhs_it, *rhs_it;
    lhs_it = gtype_iterator_init(&lhs->root);
    rhs_it = gtype_iterator_init(&rhs->root);
    gtype_iterator_token type;

    gtype_value lgtv, rgtv;
    type = gtype_iterator_next(&lhs_it, &lgtv, true);
    gtype_iterator_next(&rhs_it, &rgtv, true);

    Assert (type == WGT_BEGIN_VECTOR);

    while ((type = gtype_iterator_next(&lhs_it, &lgtv, false)) != WGT_END_VECTOR) {
        gtype_iterator_next(&rhs_it, &rgtv, true);

        float8 lhs_f = lgtv.val.float_value;
        float8 rhs_f = rgtv.val.float_value;

	if (lhs_f <  rhs_f)
            return -1;
	else if (lhs_f > rhs_f)
            return 1;
    }

    return 0;
}


