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
 *
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 */

#include "postgraph.h"
    
// Postgres
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/rangetypes.h"
#include "utils/numeric.h"
#include "utils/palloc.h"

// PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"

/*
 * Given a string representing the flags for the range type, return the flags
 * represented as a char.
 */
static char
range_parse_flags(const char *flags_str)
{
    char flags = 0;

    if (flags_str[0] == '\0' || flags_str[1] == '\0' || flags_str[2] != '\0')
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("invalid range bound flags"),
                        errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".")));
        
    switch (flags_str[0])
    {
        case '[':
            flags |= RANGE_LB_INC;
            break;
        case '(':
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("invalid range bound flags"),
                            errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".")));
    }

    switch (flags_str[1])
    {
        case ']':
            flags |= RANGE_UB_INC;
            break;
        case ')':
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("invalid range bound flags"),
                            errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".")));
    }

    return flags;
}


PG_FUNCTION_INFO_V1(gtype_intrange_2_args);
Datum gtype_intrange_2_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = INT8RANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_INT, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_intrange_3_args);
Datum gtype_intrange_3_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = INT8RANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_INT, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_numrange_2_args);
Datum gtype_numrange_2_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = NUMRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_NUM, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_numrange_3_args);
Datum gtype_numrange_3_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = NUMRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_NUM, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tsrange_2_args);
Datum gtype_tsrange_2_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = TSRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_TS, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tsrange_3_args);
Datum gtype_tsrange_3_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = TSRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_TS, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tstzrange_2_args);
Datum gtype_tstzrange_2_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = TSTZRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_TSTZ, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tstzrange_3_args);
Datum gtype_tstzrange_3_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = TSTZRANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_TSTZ, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_daterange_2_args);
Datum gtype_daterange_2_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = DATERANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_DATE, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_daterange_3_args);
Datum gtype_daterange_3_args(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = DATERANGEOID;
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));

    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(0);
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(1);
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &lower, &upper, false);

    gtype_value gtv = { .type=AGTV_RANGE_DATE, .val.range=range};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

