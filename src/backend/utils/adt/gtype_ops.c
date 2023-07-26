/*
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
 * Functions for operators in Cypher expressions.
 */

#include "postgres.h"

#include <math.h>

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#include "utils/gtype.h"

static void ereport_op_str(const char *op, gtype *lhs, gtype *rhs);
static gtype *gtype_concat(gtype *agt1, gtype *agt2);
static gtype_value *iterator_concat(gtype_iterator **it1,
                                     gtype_iterator **it2,
                                     gtype_parse_state **state);
static void concat_to_gtype_string(gtype_value *result, char *lhs, int llen,
                                    char *rhs, int rlen);
static char *get_string_from_gtype_value(gtype_value *agtv, int *length);

static void concat_to_gtype_string(gtype_value *result, char *lhs, int llen,
                                    char *rhs, int rlen)
{
    int length = llen + rlen;
    char *buffer = result->val.string.val;

    Assert(llen >= 0 && rlen >= 0);
    check_string_length(length);
    buffer = palloc(length);

    strncpy(buffer, lhs, llen);
    strncpy(buffer + llen, rhs, rlen);

    result->type = AGTV_STRING;
    result->val.string.len = length;
    result->val.string.val = buffer;
}

static char *get_string_from_gtype_value(gtype_value *agtv, int *length)
{
    Datum number;
    char *string;

    switch (agtv->type)
    {
    case AGTV_INTEGER:
        number = DirectFunctionCall1(int8out,
                                     Int8GetDatum(agtv->val.int_value));
        string = DatumGetCString(number);
        *length = strlen(string);
        return string;
    case AGTV_FLOAT:
        number = DirectFunctionCall1(float8out,
                                     Float8GetDatum(agtv->val.float_value));
        string = DatumGetCString(number);
        *length = strlen(string);

        if (is_decimal_needed(string))
        {
            char *str = palloc(*length + 2);
            strncpy(str, string, *length);
            strncpy(str + *length, ".0", 2);
            *length += 2;
            string = str;
        }
        return string;
    case AGTV_STRING:
        *length = agtv->val.string.len;
        return agtv->val.string.val;

    case AGTV_NUMERIC:
        string = DatumGetCString(DirectFunctionCall1(numeric_out,
                     PointerGetDatum(agtv->val.numeric)));
        *length = strlen(string);
        return string;

    case AGTV_NULL:
    case AGTV_BOOL:
    case AGTV_ARRAY:
    case AGTV_OBJECT:
    case AGTV_BINARY:
    default:
        *length = 0;
        return NULL;
    }
    return NULL;
}

Datum get_numeric_datum_from_gtype_value(gtype_value *agtv)
{
    switch (agtv->type)
    {
    case AGTV_INTEGER:
        return DirectFunctionCall1(int8_numeric,
                                   Int8GetDatum(agtv->val.int_value));
    case AGTV_FLOAT:
        return DirectFunctionCall1(float8_numeric,
                                   Float8GetDatum(agtv->val.float_value));
    case AGTV_NUMERIC:
        return NumericGetDatum(agtv->val.numeric);

    default:
        break;
    }

    return 0;
}

bool is_numeric_result(gtype_value *lhs, gtype_value *rhs)
{
    if (((lhs->type == AGTV_NUMERIC || rhs->type == AGTV_NUMERIC) &&
         (lhs->type == AGTV_INTEGER || lhs->type == AGTV_FLOAT ||
          rhs->type == AGTV_INTEGER || rhs->type == AGTV_FLOAT )) ||
        (lhs->type == AGTV_NUMERIC && rhs->type == AGTV_NUMERIC))
        return true;
    return false;
}

PG_FUNCTION_INFO_V1(gtype_add);

/* gtype addition and concat function for + operator */
Datum gtype_add(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    /* If both are not scalars */
    if (!(AGT_ROOT_IS_SCALAR(lhs) && AGT_ROOT_IS_SCALAR(rhs)))
    {
        Datum agt;

        /* It can't be a scalar and an object */
        if ((AGT_ROOT_IS_SCALAR(lhs) && AGT_ROOT_IS_OBJECT(rhs)) ||
            (AGT_ROOT_IS_OBJECT(lhs) && AGT_ROOT_IS_SCALAR(rhs)) ||
            /* It can't be two objects */
            (AGT_ROOT_IS_OBJECT(lhs) && AGT_ROOT_IS_OBJECT(rhs)))
            ereport_op_str("+", lhs, rhs);

        agt = GTYPE_P_GET_DATUM(gtype_concat(lhs, rhs));

        PG_RETURN_DATUM(agt);
    }

    /* Both are scalar */
    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    /*
     * One or both values is a string OR one is a string and the other is
     * either an integer, float, or numeric. If so, concatenate them.
     */
    if ((agtv_lhs->type == AGTV_STRING || agtv_rhs->type == AGTV_STRING) &&
        (agtv_lhs->type == AGTV_INTEGER || agtv_lhs->type == AGTV_FLOAT ||
         agtv_lhs->type == AGTV_NUMERIC || agtv_lhs->type == AGTV_STRING ||
         agtv_rhs->type == AGTV_INTEGER || agtv_rhs->type == AGTV_FLOAT ||
         agtv_rhs->type == AGTV_NUMERIC || agtv_rhs->type == AGTV_STRING))
    {
        int llen = 0;
        char *lhs = get_string_from_gtype_value(agtv_lhs, &llen);
        int rlen = 0;
        char *rhs = get_string_from_gtype_value(agtv_rhs, &rlen);

        concat_to_gtype_string(&agtv_result, lhs, llen, rhs, rlen);
    }
    /* Both are integers - regular addition */
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = agtv_lhs->val.int_value + agtv_rhs->val.int_value;
    }
    /* Both are floats - regular addition */
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value + agtv_rhs->val.float_value;
    }
    /* The left is a float, the right is an integer - regular addition */
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value + agtv_rhs->val.int_value;
    }
    /* The right is a float, the left is an integer - regular addition */
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.int_value + agtv_rhs->val.float_value;
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_add, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else if (agtv_lhs->type == AGTV_TIMESTAMP && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMP;
	agtv_result.val.int_value = DatumGetTimestamp(DirectFunctionCall2(timestamp_pl_interval,
				TimestampGetDatum(agtv_lhs->val.int_value),
				IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_TIMESTAMPTZ && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMPTZ;
        agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
                                TimestampTzGetDatum(agtv_lhs->val.int_value),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_DATE && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMP;
        agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(date_pl_interval,
                                DateADTGetDatum(agtv_lhs->val.date),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }   
    else if (agtv_lhs->type == AGTV_TIME && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIME;
        agtv_result.val.int_value = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval,
                                TimeADTGetDatum(agtv_lhs->val.int_value),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_TIMETZ && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMETZ;
        TimeTzADT *time = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval,
                                TimeTzADTPGetDatum(&agtv_lhs->val.timetz),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));

       agtv_result.val.timetz.time = time->time;
       agtv_result.val.timetz.zone = time->zone;
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    } 
    else
        /* Not a covered case, error out */
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_add")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_sub);

/*
 * gtype subtraction function for - operator
 */
Datum gtype_sub(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(lhs)) || !(AGT_ROOT_IS_SCALAR(rhs)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = agtv_lhs->val.int_value - agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value - agtv_rhs->val.float_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value - agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.int_value - agtv_rhs->val.float_value;
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_sub, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else if (agtv_lhs->type == AGTV_TIMESTAMP && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMP;
        agtv_result.val.int_value = DatumGetTimestamp(DirectFunctionCall2(timestamp_mi_interval,
                                TimestampGetDatum(agtv_lhs->val.int_value),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_TIMESTAMPTZ && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMPTZ;
        agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
                                TimestampTzGetDatum(agtv_lhs->val.int_value),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_DATE && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMESTAMP;
        agtv_result.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(date_mi_interval,
                                DateADTGetDatum(agtv_lhs->val.date),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_TIME && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIME;
        agtv_result.val.int_value = DatumGetTimeADT(DirectFunctionCall2(time_mi_interval,
                                TimeADTGetDatum(agtv_lhs->val.int_value),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));
    }
    else if (agtv_lhs->type == AGTV_TIMETZ && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_TIMETZ;
        TimeTzADT *time = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_mi_interval,
                                TimeTzADTPGetDatum(&agtv_lhs->val.timetz),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));

       agtv_result.val.timetz.time = time->time;
       agtv_result.val.timetz.zone = time->zone;
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mi,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                IntervalPGetDatum(&agtv_rhs->val.interval)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_sub")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_neg);

/*
 * gtype negation function for unary - operator
 */
Datum gtype_neg(PG_FUNCTION_ARGS)
{
    gtype *v = AG_GET_ARG_GTYPE_P(0);
    gtype_value *agtv_value;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(v)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_value = get_ith_gtype_value_from_container(&v->root, 0);

    if (agtv_value->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = -agtv_value->val.int_value;
    }
    else if (agtv_value->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = -agtv_value->val.float_value;
    }
    else if (agtv_value->type == AGTV_NUMERIC)
    {
        Datum numd, vald;

        vald = NumericGetDatum(agtv_value->val.numeric);
        numd = DirectFunctionCall1(numeric_uminus, vald);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else if (agtv_value->type == AGTV_INTERVAL)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall1(interval_um,
                                IntervalPGetDatum(&agtv_value->val.interval)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
	    
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter type for gtype_neg")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_mul);

/*
 * gtype multiplication function for * operator
 */
Datum gtype_mul(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(lhs)) || !(AGT_ROOT_IS_SCALAR(rhs)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = agtv_lhs->val.int_value * agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value * agtv_rhs->val.float_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value * agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.int_value * agtv_rhs->val.float_value;
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_mul, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum(agtv_rhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum((float8)agtv_rhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else if (agtv_rhs->type == AGTV_INTERVAL && agtv_lhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_rhs->val.interval),
                                Float8GetDatum(agtv_lhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else if (agtv_rhs->type == AGTV_INTERVAL && agtv_lhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_mul,
                                IntervalPGetDatum(&agtv_rhs->val.interval),
                                Float8GetDatum((float8)agtv_lhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_mul")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_div);

/*
 * gtype division function for / operator
 */
Datum gtype_div(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(lhs)) || !(AGT_ROOT_IS_SCALAR(rhs)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        if (agtv_rhs->val.int_value == 0)
        {
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
            PG_RETURN_NULL();
        }

        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = agtv_lhs->val.int_value /
                                    agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        if (agtv_rhs->val.float_value == 0)
        {
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
            PG_RETURN_NULL();
        }

        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value /
                                      agtv_rhs->val.float_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        if (agtv_rhs->val.int_value == 0)
        {
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
            PG_RETURN_NULL();
        }

        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.float_value /
                                      agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        if (agtv_rhs->val.float_value == 0)
        {
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
            PG_RETURN_NULL();
        }

        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = agtv_lhs->val.int_value /
                                      agtv_rhs->val.float_value;
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_div, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_div,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum(agtv_rhs->val.float_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else if (agtv_lhs->type == AGTV_INTERVAL && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTERVAL;
        Interval *interval = DatumGetIntervalP(DirectFunctionCall2(interval_div,
                                IntervalPGetDatum(&agtv_lhs->val.interval),
                                Float8GetDatum((float8)agtv_rhs->val.int_value)));

       agtv_result.val.interval.time = interval->time;
       agtv_result.val.interval.day = interval->day;
       agtv_result.val.interval.month = interval->month;
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_div")));

     AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_mod);

/*
 * gtype modulo function for % operator
 */
Datum gtype_mod(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(lhs)) || !(AGT_ROOT_IS_SCALAR(rhs)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = agtv_lhs->val.int_value %
                                    agtv_rhs->val.int_value;
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = fmod(agtv_lhs->val.float_value,
                                           agtv_rhs->val.float_value);
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = fmod(agtv_lhs->val.float_value,
                                           agtv_rhs->val.int_value);
    }
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = fmod(agtv_lhs->val.int_value,
                                           agtv_rhs->val.float_value);
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_mod, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_mod")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_pow);

/*
 * gtype power function for ^ operator
 */
Datum gtype_pow(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    if (!(AGT_ROOT_IS_SCALAR(lhs)) || !(AGT_ROOT_IS_SCALAR(rhs)))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("must be scalar value, not array or object")));

        PG_RETURN_NULL();
    }

    agtv_lhs = get_ith_gtype_value_from_container(&lhs->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = pow(agtv_lhs->val.int_value,
                                          agtv_rhs->val.int_value);
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = pow(agtv_lhs->val.float_value,
                                          agtv_rhs->val.float_value);
    }
    else if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_INTEGER)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = pow(agtv_lhs->val.float_value,
                                          agtv_rhs->val.int_value);
    }
    else if (agtv_lhs->type == AGTV_INTEGER && agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = pow(agtv_lhs->val.int_value,
                                          agtv_rhs->val.float_value);
    }
    /* Is this a numeric result */
    else if (is_numeric_result(agtv_lhs, agtv_rhs))
    {
        Datum numd, lhsd, rhsd;

        lhsd = get_numeric_datum_from_gtype_value(agtv_lhs);
        rhsd = get_numeric_datum_from_gtype_value(agtv_rhs);
        numd = DirectFunctionCall2(numeric_power, lhsd, rhsd);

        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = DatumGetNumeric(numd);
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input parameter types for gtype_pow")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_eq);

Datum gtype_eq(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) == 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_ne);

Datum gtype_ne(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result = true;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) != 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_lt);

Datum gtype_lt(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) < 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_gt);

Datum gtype_gt(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) > 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_le);

Datum gtype_le(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) <= 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_ge);

Datum gtype_ge(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *gtype_rhs = AG_GET_ARG_GTYPE_P(1);
    bool result;

    result = (compare_gtype_containers_orderability(&gtype_lhs->root,
                                                     &gtype_rhs->root) >= 0);

    PG_FREE_IF_COPY(gtype_lhs, 0);
    PG_FREE_IF_COPY(gtype_rhs, 1);

    PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(gtype_contains);
/*
 * <@ operator for gtype. Returns true if the right gtype path/value entries
 * contained at the top level within the left gtype value
 */
Datum gtype_contains(PG_FUNCTION_ARGS)
{
    gtype_iterator *constraint_it, *property_it;
    gtype *properties, *constraints;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    {
        PG_RETURN_NULL();
    }

    properties = AG_GET_ARG_GTYPE_P(0);
    constraints = AG_GET_ARG_GTYPE_P(1);

    constraint_it = gtype_iterator_init(&constraints->root);
    property_it = gtype_iterator_init(&properties->root);

    PG_RETURN_BOOL(gtype_deep_contains(&property_it, &constraint_it));
}


PG_FUNCTION_INFO_V1(gtype_contained_by);
/*
 * <@ operator for gtype. Returns true if the left gtype path/value entries
 * contained at the top level within the right gtype value
 */
Datum gtype_contained_by(PG_FUNCTION_ARGS)
{
    gtype_iterator *constraint_it, *property_it;
    gtype *properties, *constraints;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    {
        PG_RETURN_NULL();
    }

    properties = AG_GET_ARG_GTYPE_P(0);
    constraints = AG_GET_ARG_GTYPE_P(1);

    constraint_it = gtype_iterator_init(&constraints->root);
    property_it = gtype_iterator_init(&properties->root);

    PG_RETURN_BOOL(gtype_deep_contains(&constraint_it, &property_it));
}

PG_FUNCTION_INFO_V1(gtype_exists);
/*
 * ? operator for gtype. Returns true if the string exists as top-level keys
 */
Datum gtype_exists(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    text *key = PG_GETARG_TEXT_PP(1);
    gtype_value aval;
    gtype_value *v = NULL;

    /*
     * We only match Object keys (which are naturally always Strings), or
     * string elements in arrays.  In particular, we do not match non-string
     * scalar elements.  Existence of a key/element is only considered at the
     * top level.  No recursion occurs.
     */
    aval.type = AGTV_STRING;
    aval.val.string.val = VARDATA_ANY(key);
    aval.val.string.len = VARSIZE_ANY_EXHDR(key);

    v = find_gtype_value_from_container(&agt->root,
                                         AGT_FOBJECT | AGT_FARRAY,
                                         &aval);

    PG_RETURN_BOOL(v != NULL);
}

PG_FUNCTION_INFO_V1(gtype_exists_any);
/*
 * ?| operator for gtype. Returns true if any of the array strings exist as
 * top-level keys
 */
Datum gtype_exists_any(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    ArrayType *keys = PG_GETARG_ARRAYTYPE_P(1);
    int i;
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls,
                      &elem_count);

    for (i = 0; i < elem_count; i++)
    {
        gtype_value strVal;

        if (key_nulls[i])
        {
            continue;
        }

        strVal.type = AGTV_STRING;
        strVal.val.string.val = VARDATA(key_datums[i]);
        strVal.val.string.len = VARSIZE(key_datums[i]) - VARHDRSZ;

        if (find_gtype_value_from_container(&agt->root,
                                        AGT_FOBJECT | AGT_FARRAY,
                                        &strVal) != NULL)
        {
            PG_RETURN_BOOL(true);
        }
    }

    PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(gtype_exists_all);
/*
 * ?& operator for gtype. Returns true if all of the array strings exist as
 * top-level keys
 */
Datum gtype_exists_all(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
    int i;
    Datum *key_datums;
    bool *key_nulls;
    int elem_count;

    deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls,
                      &elem_count);

    for (i = 0; i < elem_count; i++)
    {
        gtype_value strVal;

        if (key_nulls[i])
        {
            continue;
        }

        strVal.type = AGTV_STRING;
        strVal.val.string.val = VARDATA(key_datums[i]);
        strVal.val.string.len = VARSIZE(key_datums[i]) - VARHDRSZ;

        if (find_gtype_value_from_container(&agt->root,
                                        AGT_FOBJECT | AGT_FARRAY,
                                        &strVal) == NULL)
        {
            PG_RETURN_BOOL(false);
        }
    }

    PG_RETURN_BOOL(true);
}

static gtype *gtype_concat(gtype *agt1, gtype *agt2)
{
    gtype_parse_state *state = NULL;
    gtype_value *res;
    gtype_iterator *it1;
    gtype_iterator *it2;

    /*
     * If one of the gtype is empty, just return the other if it's not scalar
     * and both are of the same kind.  If it's a scalar or they are of
     * different kinds we need to perform the concatenation even if one is
     * empty.
     */
    if (AGT_ROOT_IS_OBJECT(agt1) == AGT_ROOT_IS_OBJECT(agt2))
    {
        if (AGT_ROOT_COUNT(agt1) == 0 && !AGT_ROOT_IS_SCALAR(agt2))
            return agt2;
        else if (AGT_ROOT_COUNT(agt2) == 0 && !AGT_ROOT_IS_SCALAR(agt1))
            return agt1;
    }

    it1 = gtype_iterator_init(&agt1->root);
    it2 = gtype_iterator_init(&agt2->root);

    res = iterator_concat(&it1, &it2, &state);

    Assert(res != NULL);

    return (gtype_value_to_gtype(res));
}

/*
 * Iterate over all gtype objects and merge them into one.
 * The logic of this function copied from the same hstore function,
 * except the case, when it1 & it2 represents jbvObject.
 * In that case we just append the content of it2 to it1 without any
 * verifications.
 */
static gtype_value *iterator_concat(gtype_iterator **it1,
                                     gtype_iterator **it2,
                                     gtype_parse_state **state)
{
    gtype_value v1, v2, *res = NULL;
    gtype_iterator_token r1, r2, rk1, rk2;

    r1 = rk1 = gtype_iterator_next(it1, &v1, false);
    r2 = rk2 = gtype_iterator_next(it2, &v2, false);

    /*
     * Both elements are objects.
     */
    if (rk1 == WAGT_BEGIN_OBJECT && rk2 == WAGT_BEGIN_OBJECT)
    {
        /*
         * Append the all tokens from v1 to res, except last WAGT_END_OBJECT
         * (because res will not be finished yet).
         */
        push_gtype_value(state, r1, NULL);
        while ((r1 = gtype_iterator_next(it1, &v1, true)) != WAGT_END_OBJECT)
            push_gtype_value(state, r1, &v1);

        /*
         * Append the all tokens from v2 to res, include last WAGT_END_OBJECT
         * (the concatenation will be completed).
         */
        while ((r2 = gtype_iterator_next(it2, &v2, true)) != 0)
            res = push_gtype_value(state, r2,
                                    r2 != WAGT_END_OBJECT ? &v2 : NULL);
    }

    /*
     * Both elements are arrays (either can be scalar).
     */
    else if (rk1 == WAGT_BEGIN_ARRAY && rk2 == WAGT_BEGIN_ARRAY)
    {
        push_gtype_value(state, r1, NULL);

        while ((r1 = gtype_iterator_next(it1, &v1, true)) != WAGT_END_ARRAY)
        {
            Assert(r1 == WAGT_ELEM);
            push_gtype_value(state, r1, &v1);
        }

        while ((r2 = gtype_iterator_next(it2, &v2, true)) != WAGT_END_ARRAY)
        {
            Assert(r2 == WAGT_ELEM);
            push_gtype_value(state, WAGT_ELEM, &v2);
        }

        res = push_gtype_value(state, WAGT_END_ARRAY,
                                NULL /* signal to sort */);
    }
    /* have we got array || object or object || array? */
    else if (((rk1 == WAGT_BEGIN_ARRAY && !(*it1)->is_scalar) &&
              rk2 == WAGT_BEGIN_OBJECT) ||
             (rk1 == WAGT_BEGIN_OBJECT &&
              (rk2 == WAGT_BEGIN_ARRAY && !(*it2)->is_scalar)))
    {
        gtype_iterator **it_array = rk1 == WAGT_BEGIN_ARRAY ? it1 : it2;
        gtype_iterator **it_object = rk1 == WAGT_BEGIN_OBJECT ? it1 : it2;

        bool prepend = (rk1 == WAGT_BEGIN_OBJECT);

        push_gtype_value(state, WAGT_BEGIN_ARRAY, NULL);

        if (prepend)
        {
            push_gtype_value(state, WAGT_BEGIN_OBJECT, NULL);
            while ((r1 = gtype_iterator_next(it_object, &v1, true)) != 0)
                push_gtype_value(state, r1,
                                  r1 != WAGT_END_OBJECT ? &v1 : NULL);

            while ((r2 = gtype_iterator_next(it_array, &v2, true)) != 0)
                res = push_gtype_value(state, r2,
                                        r2 != WAGT_END_ARRAY ? &v2 : NULL);
        }
        else
        {
            while ((r1 = gtype_iterator_next(it_array, &v1, true)) !=
                   WAGT_END_ARRAY)
                push_gtype_value(state, r1, &v1);

            push_gtype_value(state, WAGT_BEGIN_OBJECT, NULL);
            while ((r2 = gtype_iterator_next(it_object, &v2, true)) != 0)
                push_gtype_value(state, r2,
                                  r2 != WAGT_END_OBJECT ? &v2 : NULL);

            res = push_gtype_value(state, WAGT_END_ARRAY, NULL);
        }
    }
    else
    {
        /*
         * This must be scalar || object or object || scalar, as that's all
         * that's left. Both of these make no sense, so error out.
         */
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid concatenation of gtype objects")));
    }

    return res;
}

static void ereport_op_str(const char *op, gtype *lhs, gtype *rhs)
{
    const char *msgfmt;
    const char *lstr;
    const char *rstr;

    AssertArg(rhs != NULL);

    if (lhs == NULL)
    {
        msgfmt = "invalid expression: %s%s%s";
        lstr = "";
    }
    else
    {
        msgfmt = "invalid expression: %s %s %s";
        lstr = gtype_to_cstring(NULL, &lhs->root, VARSIZE(lhs));
    }
    rstr = gtype_to_cstring(NULL, &rhs->root, VARSIZE(rhs));

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg(msgfmt, lstr, op, rstr)));
}
