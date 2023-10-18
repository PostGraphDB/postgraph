/*
 * Used to support gtype temporal values.
 *
 * Copyright (C) 2023 PostGraphDB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "postgraph.h"

#include "common/int128.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"

#include "utils/gtype.h"
#include "utils/gtype_temporal.h"
#include "utils/gtype_typecasting.h"

int
timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2) {                                
    TimeOffset t1, t2;

    // Primary sort is by true (GMT-equivalent) time
    t1 = time1->time + (time1->zone * USECS_PER_SEC);
    t2 = time2->time + (time2->zone * USECS_PER_SEC);

    if (t1 > t2)
        return 1;
    if (t1 < t2)
        return -1;

    /*
     * If same GMT time, sort by timezone; we only want to say that two
     * timetz's are equal if both the time and zone parts are equal.
     */
    if (time1->zone > time2->zone)
        return 1;
    if (time1->zone < time2->zone)
        return -1;

    return 0;
}


static inline INT128
interval_cmp_value(const Interval *interval)
{
    INT128 span;
    int64 dayfraction;
    int64 days;

    /*
     * Separate time field into days and dayfraction, then add the month and
     * day fields to the days part.  We cannot overflow int64 days here.
     */
    dayfraction = interval->time % USECS_PER_DAY;
    days = interval->time / USECS_PER_DAY;
    days += interval->month * INT64CONST(30);
    days += interval->day;
        
    // Widen dayfraction to 128 bits
    span = int64_to_int128(dayfraction);

    // Scale up days to microseconds, forming a 128-bit product
    int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

    return span;
}

int
interval_cmp_internal(Interval *interval1, Interval *interval2)
{
    INT128 span1 = interval_cmp_value(interval1);
    INT128 span2 = interval_cmp_value(interval2);
 
    return int128_compare(span1, span2);
}

PG_FUNCTION_INFO_V1(gtype_age_today);
Datum gtype_age_today(PG_FUNCTION_ARGS)
{
    Timestamp ts;
    gtype *arg1 = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result, *agtv1;
    Interval *i;

    if (is_gtype_null(arg1))
        PG_RETURN_NULL();
    agtv1 = get_ith_gtype_value_from_container(&arg1->root, 0);

    ts = TimestampGetDatum(GetCurrentTransactionStartTimestamp());
    ts = DatumGetTimestamp(DirectFunctionCall2(timestamp_trunc, cstring_to_text_with_len("day",3), ts));

    if (agtv1->type != AGTV_TIMESTAMP)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("age(gtype) only supports timestamps")));

    i = DatumGetIntervalP(DirectFunctionCall2(timestamp_mi,
                                              TimestampGetDatum(agtv1->val.int_value),
                                              ts));

    agtv_result.type = AGTV_INTERVAL;
    agtv_result.val.interval.time = i->time;
    agtv_result.val.interval.day = i->day;
    agtv_result.val.interval.month = i->month;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_age_w2args);
Datum
gtype_age_w2args(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0); 

    Interval *i;
    if (lhs_value->type == AGTV_TIMESTAMP && rhs_value->type == AGTV_TIMESTAMP)
         i = DatumGetIntervalP(DirectFunctionCall2(timestamp_age,
				    TimestampGetDatum(lhs_value->val.int_value),
				    TimestampGetDatum(rhs_value->val.int_value)));
    else if (lhs_value->type == AGTV_TIMESTAMPTZ && rhs_value->type == AGTV_TIMESTAMPTZ)
         i = DatumGetIntervalP(DirectFunctionCall2(timestamptz_age,
                                    TimestampTzGetDatum(lhs_value->val.int_value),
                                    TimestampTzGetDatum(rhs_value->val.int_value)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for age(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_extract);
Datum
gtype_extract(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (lhs_value->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for EXTRACT arg 1 must be a string"),
                        errhint("You may have to use explicit casts.")));

    char *field = lhs_value->val.string.val;
    
    gtype_value gtv;
    if (rhs_value->type == AGTV_TIMESTAMP) {
        gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_timestamp,
                                               CStringGetTextDatum(field),
                                               TimestampGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_TIMESTAMPTZ) {
        gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_timestamptz,
                                              CStringGetTextDatum(field),
                                              TimestampTzGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_DATE) {
         gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_date,
                                              CStringGetTextDatum(field),
                                              DateADTGetDatum(rhs_value->val.date)));
    } else if (rhs_value->type == AGTV_TIME) {
         gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_time,
                                              CStringGetTextDatum(field),
                                              TimeADTGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_TIMETZ) {
         gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_timetz,
                                              CStringGetTextDatum(field),
                                              TimeTzADTPGetDatum(&rhs_value->val.timetz)));
    } else if (rhs_value->type == AGTV_INTERVAL) {
         gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(extract_interval,
                                              CStringGetTextDatum(field),
                                              IntervalPGetDatum(&rhs_value->val.interval)));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for EXTRACT(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    gtv.type = AGTV_NUMERIC;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_date_part);
Datum
gtype_date_part(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (lhs_value->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for EXTRACT arg 1 must be a string"),
                        errhint("You may have to use explicit casts.")));

    char *field = lhs_value->val.string.val;

    gtype_value gtv;
    if (rhs_value->type == AGTV_TIMESTAMP) {
        gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(timestamp_part,
                                               CStringGetTextDatum(field),
                                               TimestampGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_TIMESTAMPTZ) {
        gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(timestamptz_part,
                                              CStringGetTextDatum(field),
                                              TimestampTzGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_DATE) {
	Datum ts = gtype_to_timestamptz_internal(rhs_value);
        gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(timestamptz_part,
                                              CStringGetTextDatum(field), ts));
    } else if (rhs_value->type == AGTV_TIME) {
         gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(time_part,
                                              CStringGetTextDatum(field),
                                              TimeADTGetDatum(rhs_value->val.int_value)));
    } else if (rhs_value->type == AGTV_TIMETZ) {
         gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(timetz_part,
                                              CStringGetTextDatum(field),
                                              TimeTzADTPGetDatum(&rhs_value->val.timetz)));
    } else if (rhs_value->type == AGTV_INTERVAL) {
         gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(interval_part,
                                              CStringGetTextDatum(field),
                                              IntervalPGetDatum(&rhs_value->val.interval)));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_part(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    gtv.type = AGTV_FLOAT;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_make_date);

Datum gtype_make_date(PG_FUNCTION_ARGS)
{
    gtype_value agtv, *agtv_year, *agtv_month, *agtv_day;
    gtype *agt_year = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_month = AG_GET_ARG_GTYPE_P(1);
    gtype *agt_day = AG_GET_ARG_GTYPE_P(2);

    if (is_gtype_null(agt_year) || is_gtype_null(agt_month) || is_gtype_null(agt_day))
        PG_RETURN_NULL();

    agtv_year = get_ith_gtype_value_from_container(&agt_year->root, 0);
    agtv_month = get_ith_gtype_value_from_container(&agt_month->root, 0);
    agtv_day = get_ith_gtype_value_from_container(&agt_day->root, 0);


    if (agtv_year->type != AGTV_INTEGER || agtv_month->type != AGTV_INTEGER || agtv_day->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_date(gtype, gtype, gtype) only supports integer arugments")));


    agtv.type = AGTV_DATE;
    agtv.val.date = DatumGetDateADT(DirectFunctionCall3(make_date,
                                                               Int32GetDatum((int32)agtv_year->val.int_value),
                                                               Int32GetDatum((int32)agtv_month->val.int_value),
                                                               Int32GetDatum((int32)agtv_day->val.int_value)));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_make_time);

Datum gtype_make_time(PG_FUNCTION_ARGS)
{
    gtype_value agtv, *agtv_hour, *agtv_minute, *agtv_second;
    gtype *agt_hour = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_minute = AG_GET_ARG_GTYPE_P(1);
    gtype *agt_second = AG_GET_ARG_GTYPE_P(2);

    if (is_gtype_null(agt_hour) || is_gtype_null(agt_minute) || is_gtype_null(agt_second))
        PG_RETURN_NULL();

    agtv_hour = get_ith_gtype_value_from_container(&agt_hour->root, 0);
    agtv_minute = get_ith_gtype_value_from_container(&agt_minute->root, 0);
    agtv_second = get_ith_gtype_value_from_container(&agt_second->root, 0);


    if (agtv_hour->type != AGTV_INTEGER || agtv_minute->type != AGTV_INTEGER || agtv_second->type != AGTV_FLOAT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_time(gtype, gtype, gtype) only supports integer arugments")));


    agtv.type = AGTV_TIME;
    agtv.val.int_value = DatumGetTimeADT(DirectFunctionCall3(make_time,
                                                          Int32GetDatum((int32)agtv_hour->val.int_value),
                                                          Int32GetDatum((int32)agtv_minute->val.int_value),
                                                          Float8GetDatum(agtv_second->val.float_value)));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_make_timestamp);

Datum gtype_make_timestamp(PG_FUNCTION_ARGS)
{
    gtype_value agtv;
    gtype_value *agtv_hour, *agtv_minute, *agtv_second;
    gtype_value *agtv_year, *agtv_month, *agtv_day;

    gtype *agt_year = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_month = AG_GET_ARG_GTYPE_P(1);
    gtype *agt_day = AG_GET_ARG_GTYPE_P(2);
    gtype *agt_hour = AG_GET_ARG_GTYPE_P(3);
    gtype *agt_minute = AG_GET_ARG_GTYPE_P(4);
    gtype *agt_second = AG_GET_ARG_GTYPE_P(5);

    if (is_gtype_null(agt_year) || is_gtype_null(agt_month) || is_gtype_null(agt_day) ||
        is_gtype_null(agt_hour) || is_gtype_null(agt_minute) || is_gtype_null(agt_second))
        PG_RETURN_NULL();

    agtv_year = get_ith_gtype_value_from_container(&agt_year->root, 0);
    agtv_month = get_ith_gtype_value_from_container(&agt_month->root, 0);
    agtv_day = get_ith_gtype_value_from_container(&agt_day->root, 0);
    agtv_hour = get_ith_gtype_value_from_container(&agt_hour->root, 0);
    agtv_minute = get_ith_gtype_value_from_container(&agt_minute->root, 0);
    agtv_second = get_ith_gtype_value_from_container(&agt_second->root, 0);

    if (agtv_year->type != AGTV_INTEGER || agtv_month->type != AGTV_INTEGER || agtv_day->type != AGTV_INTEGER ||
        agtv_hour->type != AGTV_INTEGER || agtv_minute->type != AGTV_INTEGER || agtv_second->type != AGTV_FLOAT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_time(gtype, gtype, gtype) only supports integer arugments")));


    agtv.type = AGTV_TIMESTAMP;
    agtv.val.int_value = DatumGetTimestamp(DirectFunctionCall6(make_timestamp,
                                                               Int32GetDatum((int32)agtv_year->val.int_value),
                                                               Int32GetDatum((int32)agtv_month->val.int_value),
                                                               Int32GetDatum((int32)agtv_day->val.int_value),
                                                               Int32GetDatum((int32)agtv_hour->val.int_value),
                                                               Int32GetDatum((int32)agtv_minute->val.int_value),
                                                               Float8GetDatum(agtv_second->val.float_value)));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_make_timestamptz);

Datum gtype_make_timestamptz(PG_FUNCTION_ARGS)
{
    gtype_value agtv;
    gtype_value *agtv_hour, *agtv_minute, *agtv_second;
    gtype_value *agtv_year, *agtv_month, *agtv_day;

    gtype *agt_year = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_month = AG_GET_ARG_GTYPE_P(1);
    gtype *agt_day = AG_GET_ARG_GTYPE_P(2);
    gtype *agt_hour = AG_GET_ARG_GTYPE_P(3);
    gtype *agt_minute = AG_GET_ARG_GTYPE_P(4);
    gtype *agt_second = AG_GET_ARG_GTYPE_P(5);

    if (is_gtype_null(agt_year) || is_gtype_null(agt_month) || is_gtype_null(agt_day) ||
        is_gtype_null(agt_hour) || is_gtype_null(agt_minute) || is_gtype_null(agt_second))
        PG_RETURN_NULL();

    agtv_year = get_ith_gtype_value_from_container(&agt_year->root, 0);
    agtv_month = get_ith_gtype_value_from_container(&agt_month->root, 0);
    agtv_day = get_ith_gtype_value_from_container(&agt_day->root, 0);
    agtv_hour = get_ith_gtype_value_from_container(&agt_hour->root, 0);
    agtv_minute = get_ith_gtype_value_from_container(&agt_minute->root, 0);
    agtv_second = get_ith_gtype_value_from_container(&agt_second->root, 0);

    if (agtv_year->type != AGTV_INTEGER || agtv_month->type != AGTV_INTEGER || agtv_day->type != AGTV_INTEGER ||
        agtv_hour->type != AGTV_INTEGER || agtv_minute->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_timestamptz expected an integer arugment")));


    if (agtv_second->type != AGTV_FLOAT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_timestamptz second arguement expects a float")));

    agtv.type = AGTV_TIMESTAMPTZ;
    agtv.val.int_value = DatumGetTimestampTz(DirectFunctionCall6(make_timestamp,
                                                               Int32GetDatum((int32)agtv_year->val.int_value),
                                                               Int32GetDatum((int32)agtv_month->val.int_value),
                                                               Int32GetDatum((int32)agtv_day->val.int_value),
                                                               Int32GetDatum((int32)agtv_hour->val.int_value),
                                                               Int32GetDatum((int32)agtv_minute->val.int_value),
                                                               Float8GetDatum(agtv_second->val.float_value)));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_make_timestamptz_wtimezone);

Datum gtype_make_timestamptz_wtimezone(PG_FUNCTION_ARGS)
{
    gtype_value agtv;
    gtype_value *agtv_hour, *agtv_minute, *agtv_second;
    gtype_value *agtv_year, *agtv_month, *agtv_day;
    gtype_value *agtv_timezone;
    gtype *agt_year = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_month = AG_GET_ARG_GTYPE_P(1);
    gtype *agt_day = AG_GET_ARG_GTYPE_P(2);
    gtype *agt_hour = AG_GET_ARG_GTYPE_P(3);
    gtype *agt_minute = AG_GET_ARG_GTYPE_P(4);
    gtype *agt_second = AG_GET_ARG_GTYPE_P(5);
    gtype *agt_timezone = AG_GET_ARG_GTYPE_P(6);

    if (is_gtype_null(agt_year) || is_gtype_null(agt_month) || is_gtype_null(agt_day) ||
        is_gtype_null(agt_hour) || is_gtype_null(agt_minute) || is_gtype_null(agt_second) ||
        is_gtype_null(agt_timezone))
        PG_RETURN_NULL();

    agtv_year = get_ith_gtype_value_from_container(&agt_year->root, 0);
    agtv_month = get_ith_gtype_value_from_container(&agt_month->root, 0);
    agtv_day = get_ith_gtype_value_from_container(&agt_day->root, 0);
    agtv_hour = get_ith_gtype_value_from_container(&agt_hour->root, 0);
    agtv_minute = get_ith_gtype_value_from_container(&agt_minute->root, 0);
    agtv_second = get_ith_gtype_value_from_container(&agt_second->root, 0);
    agtv_timezone = get_ith_gtype_value_from_container(&agt_timezone->root, 0);

    if (agtv_year->type != AGTV_INTEGER || agtv_month->type != AGTV_INTEGER || agtv_day->type != AGTV_INTEGER ||
        agtv_hour->type != AGTV_INTEGER || agtv_minute->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_timestamptz expected an integer arugment")));


    if (agtv_second->type != AGTV_FLOAT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_timestamptz second arguement expects a float")));

    if (agtv_timezone->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("make_timestamptz timezone arguemnent must be a string")));


    agtv.type = AGTV_TIMESTAMPTZ;
    agtv.val.int_value = DatumGetTimestampTz(DirectFunctionCall7(make_timestamptz_at_timezone,
                                                               Int32GetDatum((int32)agtv_year->val.int_value),
                                                               Int32GetDatum((int32)agtv_month->val.int_value),
                                                               Int32GetDatum((int32)agtv_day->val.int_value),
                                                               Int32GetDatum((int32)agtv_hour->val.int_value),
                                                               Int32GetDatum((int32)agtv_minute->val.int_value),
                                                               Float8GetDatum(agtv_second->val.float_value),
                                                               PointerGetDatum(cstring_to_text_with_len(agtv_timezone->val.string.val,
                                                                                                        agtv_timezone->val.string.len))));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_isfinite);
Datum gtype_isfinite(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *agtv, agtv_result;
    bool result = false;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    agtv = get_ith_gtype_value_from_container(&agt->root, 0);

    if (agtv->type == AGTV_DATE)
        result = DatumGetBool(DirectFunctionCall1(date_finite, DateADTGetDatum(agtv->val.date)));
    else if (agtv->type == AGTV_INTERVAL)
        result = true;
    else if(agtv->type == AGTV_TIMESTAMP || agtv->type == AGTV_TIMESTAMPTZ)
    {
        result = DatumGetBool(DirectFunctionCall1(timestamp_finite, TimestampGetDatum(agtv->val.int_value)));
    }
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for isfinite(gtype)"),
                        errhint("You may have to use explicit casts.")));


    agtv_result.type = AGTV_BOOL;
    agtv_result.val.boolean = result;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_justify_days);
Datum
gtype_justify_days(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *gt_value = get_ith_gtype_value_from_container(&gt->root, 0);

    gtype_value gtv;
    if (gt_value->type != AGTV_INTERVAL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for justify_days(gtype)"),
                        errhint("You may have to use explicit casts.")));

    Interval *i = DatumGetIntervalP(DirectFunctionCall1(interval_justify_days,
                                              IntervalPGetDatum(&gt_value->val.interval)));
    gtv.type = AGTV_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_justify_hours);
Datum
gtype_justify_hours(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *gt_value = get_ith_gtype_value_from_container(&gt->root, 0);

    gtype_value gtv;
    if (gt_value->type != AGTV_INTERVAL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for justify_days(gtype)"),
                        errhint("You may have to use explicit casts.")));

    Interval *i = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
                                              IntervalPGetDatum(&gt_value->val.interval)));
    gtv.type = AGTV_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_justify_interval);
Datum
gtype_justify_interval(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *gt_value = get_ith_gtype_value_from_container(&gt->root, 0);

    gtype_value gtv;
    if (gt_value->type != AGTV_INTERVAL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for justify_days(gtype)"),
                        errhint("You may have to use explicit casts.")));

    Interval *i = DatumGetIntervalP(DirectFunctionCall1(interval_justify_interval,
                                              IntervalPGetDatum(&gt_value->val.interval)));
    gtv.type = AGTV_INTERVAL;
    gtv.val.interval.time = i->time;
    gtv.val.interval.day = i->day;
    gtv.val.interval.month = i->month;
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

}

PG_FUNCTION_INFO_V1(gtype_date_trunc);
Datum
gtype_date_trunc(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (lhs_value->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for EXTRACT arg 1 must be a string"),
                        errhint("You may have to use explicit casts.")));

    char *field = lhs_value->val.string.val;

    gtype_value gtv;
    if (rhs_value->type == AGTV_TIMESTAMP) {
        gtv.val.int_value = DatumGetTimestamp(DirectFunctionCall2(timestamp_trunc,
                                               CStringGetTextDatum(field),
                                               TimestampGetDatum(rhs_value->val.int_value)));
	gtv.type = AGTV_TIMESTAMP;
	AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else if (rhs_value->type == AGTV_TIMESTAMPTZ) {
        gtv.val.int_value = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_trunc,
                                              CStringGetTextDatum(field),
                                              TimestampTzGetDatum(rhs_value->val.int_value)));
        gtv.type = AGTV_TIMESTAMPTZ;
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else if (rhs_value->type == AGTV_INTERVAL) {
         Interval *i = DatumGetIntervalP(DirectFunctionCall2(interval_trunc,
                                              CStringGetTextDatum(field),
                                              IntervalPGetDatum(&rhs_value->val.interval)));
        gtv.type = AGTV_INTERVAL;
        gtv.val.interval.time = i->time;
        gtv.val.interval.day = i->day;
        gtv.val.interval.month = i->month;

	AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_trun(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(gtype_date_trunc_zone);
Datum
gtype_date_trunc_zone(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);
    gtype *zone = AG_GET_ARG_GTYPE_P(2);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);
    gtype_value *zone_value = get_ith_gtype_value_from_container(&zone->root, 0);

    if (lhs_value->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_trunc arg 1 must be a string"),
                        errhint("You may have to use explicit casts.")));

    char *field = lhs_value->val.string.val;

    if (lhs_value->type != AGTV_STRING)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_trunc arg 3 must be a string"),
                        errhint("You may have to use explicit casts.")));

    char *zone_str = zone_value->val.string.val;


    gtype_value gtv;
    if (rhs_value->type == AGTV_TIMESTAMPTZ) {
        gtv.val.int_value = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_trunc_zone,
                                              CStringGetTextDatum(field),
                                              TimestampTzGetDatum(rhs_value->val.int_value),
					      CStringGetTextDatum(zone_str)));
        gtv.type = AGTV_TIMESTAMPTZ;
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_trun(gtype, gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(gtype_date_bin);
Datum
gtype_date_bin(PG_FUNCTION_ARGS) {
    gtype *stride = AG_GET_ARG_GTYPE_P(0);
    gtype *source = AG_GET_ARG_GTYPE_P(1);
    gtype *origin = AG_GET_ARG_GTYPE_P(2);

    if (is_gtype_null(stride) || is_gtype_null(source) || is_gtype_null(origin))
        PG_RETURN_NULL();

    gtype_value *stride_value = get_ith_gtype_value_from_container(&stride->root, 0);    
    gtype_value *source_value = get_ith_gtype_value_from_container(&source->root, 0);
    gtype_value *origin_value = get_ith_gtype_value_from_container(&origin->root, 0);

    if ((source_value->type == AGTV_TIMESTAMP || source_value->type == AGTV_DATE) &&
        (origin_value->type == AGTV_TIMESTAMP || origin_value->type == AGTV_DATE)) {
         Datum source_ts, origin_ts;

         if (source_value->type == AGTV_DATE)
             source_ts = gtype_to_timestamp_internal(source_value);
	 else
             source_ts = TimestampGetDatum(source_value->val.int_value);

	 if (origin_value->type == AGTV_DATE)
             origin_ts = gtype_to_timestamp_internal(origin_value);
         else
             origin_ts = TimestampGetDatum(origin_value->val.int_value);

	 
         Timestamp ts = DatumGetTimestamp(DirectFunctionCall3(timestamp_bin,
                                    IntervalPGetDatum(&stride_value->val.interval),
                                    source_ts, origin_ts));

         gtype_value gtv;
         gtv.type = AGTV_TIMESTAMP;
         gtv.val.int_value = ts;

         AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else if (source_value->type == AGTV_TIMESTAMPTZ && origin_value->type == AGTV_TIMESTAMPTZ) {
         TimestampTz ts = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_bin,
				    IntervalPGetDatum(&stride_value->val.interval),
                                    TimestampTzGetDatum(source_value->val.int_value),
                                    TimestampTzGetDatum(origin_value->val.int_value)));
         gtype_value gtv;
         gtv.type = AGTV_TIMESTAMPTZ;
         gtv.val.int_value = ts;

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else 
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for date_bin(gtype, gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));

    PG_RETURN_NULL();
}

enum overlap_type {
    ovt_timestamp,
    ovt_time,
    ovt_timetz
} overlap_type;

PG_FUNCTION_INFO_V1(gtype_overlaps);

Datum gtype_overlaps(PG_FUNCTION_ARGS)
{
    gtype *arg1 = AG_GET_ARG_GTYPE_P(0);
    gtype *arg2 = AG_GET_ARG_GTYPE_P(1);
    gtype *arg3 = AG_GET_ARG_GTYPE_P(2);
    gtype *arg4 = AG_GET_ARG_GTYPE_P(3);
    enum overlap_type type;
    bool result;
    gtype_value agtv_result;

    Datum d1;
    if (GT_IS_DATE(arg1) || GT_IS_TIMESTAMP(arg1) || GT_IS_TIMESTAMPTZ(arg1)) {
        d1 = GT_TO_TIMESTAMP_DATUM(arg1);
        type = ovt_timestamp;
    } else if (GT_IS_TIME(arg1)) {
        d1 = GT_TO_TIME_DATUM(arg1);
        type = ovt_time;
    } else if (GT_IS_TIMETZ(arg1)) {
        d1 = GT_TO_TIMETZ_DATUM(arg1);
        type = ovt_timetz;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid argument type for overlap expression")));
    }

    Datum d2;
    if (GT_IS_INTERVAL(arg2)) {
        if (type == ovt_timestamp)
            d2 = DirectFunctionCall2(timestamp_pl_interval, d1, GT_TO_INTERVAL_DATUM(arg2));
        else if (type == ovt_time)
            d2 = DirectFunctionCall2(time_pl_interval, d1, GT_TO_INTERVAL_DATUM(arg2));
        else 
            d2 = DirectFunctionCall2(timetz_pl_interval, d1, GT_TO_INTERVAL_DATUM(arg2));
    } else {
        if (type == ovt_timestamp)
            d2 = GT_TO_TIMESTAMP_DATUM(arg2);
        else if (type == ovt_time)
            d2 = GT_TO_TIME_DATUM(arg2);
        else 
            d2 = GT_TO_TIMETZ_DATUM(arg2);
    }

    Datum d3;
    if (type == ovt_timestamp)
        d3 = GT_TO_TIMESTAMP_DATUM(arg3);
    else if (type == ovt_time)
        d3 = GT_TO_TIME_DATUM(arg3);
    else 
        d3 = GT_TO_TIMETZ_DATUM(arg3);


    Datum d4;
    if (GT_IS_INTERVAL(arg4)) {
        if (type == ovt_timestamp)
            d4 = DirectFunctionCall2(timestamp_pl_interval, d3, GT_TO_INTERVAL_DATUM(arg4));
        else if (type == ovt_time)
            d4 = DirectFunctionCall2(time_pl_interval, d3, GT_TO_INTERVAL_DATUM(arg4));
        else 
            d4 = DirectFunctionCall2(timetz_pl_interval, d3, GT_TO_INTERVAL_DATUM(arg4));
    } else {
        if (type == ovt_timestamp)
            d4 = GT_TO_TIMESTAMP_DATUM(arg4);
        else if (type == ovt_time)
            d4 = GT_TO_TIME_DATUM(arg4);
        else 
            d4 = GT_TO_TIMETZ_DATUM(arg4);
    }

    if (type == ovt_timestamp)
        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp, d1, d2, d3, d4));
    else if (type == ovt_time)
        result = DatumGetBool(DirectFunctionCall4(overlaps_time, d1, d2, d3, d4));
    else 
        result = DatumGetBool(DirectFunctionCall4(overlaps_timetz, d1, d2, d3, d4));
        

    agtv_result.type = AGTV_BOOL;
    agtv_result.val.boolean = result;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

