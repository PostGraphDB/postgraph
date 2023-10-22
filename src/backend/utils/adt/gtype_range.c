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
    
// Postgres
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rangetypes.h"

// PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"

static Datum _make_range(PG_FUNCTION_ARGS, Datum d1, Datum d2, Oid rngtypid, enum gtype_value_type gt_type);

/*
 * Given a string representing the flags for the range type, return the flags
 * represented as a char.
 */
static char
range_parse_flags(const char *flags_str) {
    char flags = 0;

    if (flags_str[0] == '\0' || flags_str[1] == '\0' || flags_str[2] != '\0')
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("invalid range bound flags"),
                        errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\".")));
        
    switch (flags_str[0]) {
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

    switch (flags_str[1]) {
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

PG_FUNCTION_INFO_V1(gtype_intrange);
Datum gtype_intrange(PG_FUNCTION_ARGS) {
    PG_RETURN_DATUM(_make_range(fcinfo,  
                                PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(0),
                                PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_INT8_DATUM(1),
                                INT8RANGEOID,
				AGTV_RANGE_INT));
}

PG_FUNCTION_INFO_V1(gtype_numrange);
Datum gtype_numrange(PG_FUNCTION_ARGS) {
    PG_RETURN_DATUM(_make_range(fcinfo,  
                                PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(0),
                                PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_NUMERIC_DATUM(1),
                                NUMRANGEOID,
				AGTV_RANGE_NUM));
}

PG_FUNCTION_INFO_V1(gtype_tsrange);
Datum gtype_tsrange(PG_FUNCTION_ARGS) {
    PG_RETURN_DATUM(_make_range(fcinfo,  
                                PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(0),
                                PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMP_DATUM(1),
                                TSRANGEOID,
				AGTV_RANGE_TS));
}

PG_FUNCTION_INFO_V1(gtype_tstzrange);
Datum gtype_tstzrange(PG_FUNCTION_ARGS) {
    PG_RETURN_DATUM(_make_range(fcinfo,  
                                PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(0),
                                PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_TIMESTAMPTZ_DATUM(1),
                                TSTZRANGEOID, 
				AGTV_RANGE_TSTZ));
}

PG_FUNCTION_INFO_V1(gtype_daterange);
Datum gtype_daterange(PG_FUNCTION_ARGS) {
    PG_RETURN_DATUM(_make_range(fcinfo, 
			        PG_ARGISNULL(0) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(0),
				PG_ARGISNULL(1) ? (Datum) 0 : GT_ARG_TO_DATE_DATUM(1),
				DATERANGEOID,
				AGTV_RANGE_DATE));
}

static Datum _make_range(PG_FUNCTION_ARGS, Datum d1, Datum d2, Oid rngtypid, enum gtype_value_type gt_type) {
    RangeType *range;
    TypeCacheEntry *typcache;
    RangeBound lower;
    RangeBound upper;
    char flags;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if (PG_NARGS() == 3 && PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("range constructor flags argument must not be null")));

    lower.val = d1;
    lower.infinite = PG_ARGISNULL(0);

    upper.val = d2;
    upper.infinite = PG_ARGISNULL(1);

    if (PG_NARGS() != 3) {
        lower.inclusive = true;
        lower.lower = true;

        upper.inclusive = false;
        upper.lower = false;
    } else {
        char flags = range_parse_flags(GT_TO_STRING(AG_GET_ARG_GTYPE_P(2)));
        lower.inclusive = (flags & RANGE_LB_INC) != 0;
        lower.lower = true;

        upper.inclusive = (flags & RANGE_UB_INC) != 0;
        upper.lower = false;
    }

    gtype_value gtv = { .type=AGTV_RANGE_DATE, .val.range=make_range(typcache, &lower, &upper, false)};

    return GTYPE_P_GET_DATUM(gtype_value_to_gtype(&gtv));
}	
