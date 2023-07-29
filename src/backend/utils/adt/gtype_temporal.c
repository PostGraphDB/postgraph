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

#include "postgres.h"

#include "access/xact.h"
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


PG_FUNCTION_INFO_V1(gtype_overlaps);

Datum gtype_overlaps(PG_FUNCTION_ARGS)
{
    gtype *arg1 = AG_GET_ARG_GTYPE_P(0);
    gtype *arg2 = AG_GET_ARG_GTYPE_P(1);
    gtype *arg3 = AG_GET_ARG_GTYPE_P(2);
    gtype *arg4 = AG_GET_ARG_GTYPE_P(3);

    bool result;
    gtype_value agtv_result;
    gtype_value *agtv1, *agtv2, *agtv3, *agtv4;

    if (is_gtype_null(arg1) || is_gtype_null(arg2) || is_gtype_null(arg3) || is_gtype_null(arg4))
        PG_RETURN_NULL();

    agtv1 = get_ith_gtype_value_from_container(&arg1->root, 0);
    agtv2 = get_ith_gtype_value_from_container(&arg2->root, 0);
    agtv3 = get_ith_gtype_value_from_container(&arg3->root, 0);
    agtv4 = get_ith_gtype_value_from_container(&arg4->root, 0);

    if( agtv1->type == AGTV_DATE && agtv2->type == AGTV_DATE
        && agtv3->type == AGTV_DATE && agtv4->type == AGTV_DATE)
    {
        Timestamp ts1,te1,ts2,te2;
        ts1 = date2timestamp_no_overflow(agtv1->val.date);
        te1 = date2timestamp_no_overflow(agtv2->val.date);
        ts2 = date2timestamp_no_overflow(agtv3->val.date);
        te2 = date2timestamp_no_overflow(agtv4->val.date);

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                TimestampGetDatum(ts1),
                                                TimestampGetDatum(te1),
                                                TimestampGetDatum(ts2),
                                                TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_DATE && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_DATE && agtv4->type == AGTV_INTERVAL)
    {
        Timestamp ts1,te1,ts2,te2;
        ts1 = date2timestamp_no_overflow(agtv1->val.date);
        te1 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(ts1), IntervalPGetDatum(&agtv2->val.interval)));
        ts2 = date2timestamp_no_overflow(agtv3->val.date);
        te2 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(ts2), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                TimestampGetDatum(ts1),
                                                TimestampGetDatum(te1),
                                                TimestampGetDatum(ts2),
                                                TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_DATE && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_DATE && agtv4->type == AGTV_DATE)
    {
        Timestamp ts1,te1,ts2,te2;
        ts1 = date2timestamp_no_overflow(agtv1->val.date);
        te1 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(ts1), IntervalPGetDatum(&agtv2->val.interval)));
        ts2 = date2timestamp_no_overflow(agtv3->val.date);
        te2 = date2timestamp_no_overflow(agtv4->val.date);

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                TimestampGetDatum(ts1),
                                                TimestampGetDatum(te1),
                                                TimestampGetDatum(ts2),
                                                TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_DATE && agtv2->type == AGTV_DATE
        && agtv3->type == AGTV_DATE && agtv4->type == AGTV_INTERVAL)
    {
        Timestamp ts1,te1,ts2,te2;
        ts1 = date2timestamp_no_overflow(agtv1->val.date);
        te1 = date2timestamp_no_overflow(agtv2->val.date);
        ts2 = date2timestamp_no_overflow(agtv3->val.date);
        te2 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(ts2), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                TimestampGetDatum(ts1),
                                                TimestampGetDatum(te1),
                                                TimestampGetDatum(ts2),
                                                TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIMESTAMP && agtv2->type == AGTV_TIMESTAMP
        && agtv3->type == AGTV_TIMESTAMP && agtv4->type == AGTV_TIMESTAMP)
    {
        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                  TimestampGetDatum(agtv1->val.int_value),
                                                  TimestampGetDatum(agtv2->val.int_value),
                                                  TimestampGetDatum(agtv3->val.int_value),
                                                  TimestampGetDatum(agtv4->val.int_value)));
    }
    else if( agtv1->type == AGTV_TIMESTAMP && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIMESTAMP && agtv4->type == AGTV_INTERVAL)
    {
        Timestamp te1,te2;
        te1 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(agtv1->val.int_value), IntervalPGetDatum(&agtv2->val.interval)));
        te2 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(agtv3->val.int_value), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                  TimestampGetDatum(agtv1->val.int_value),
                                                  TimestampGetDatum(te1),
                                                  TimestampGetDatum(agtv3->val.int_value),
                                                  TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIMESTAMP && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIMESTAMP && agtv4->type == AGTV_TIMESTAMP)
    {
        Timestamp te1;
        te1 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(agtv1->val.int_value), IntervalPGetDatum(&agtv2->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                  TimestampGetDatum(agtv1->val.int_value),
                                                  TimestampGetDatum(te1),
                                                  TimestampGetDatum(agtv3->val.int_value),
                                                  TimestampGetDatum(agtv4->val.int_value)));
    }
    else if( agtv1->type == AGTV_TIMESTAMP && agtv2->type == AGTV_TIMESTAMP
        && agtv3->type == AGTV_TIMESTAMP && agtv4->type == AGTV_INTERVAL)
    {
        Timestamp te2;
        te2 = TimestampGetDatum(DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(agtv3->val.int_value), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timestamp,
                                                  TimestampGetDatum(agtv1->val.int_value),
                                                  TimestampGetDatum(agtv2->val.int_value),
                                                  TimestampGetDatum(agtv3->val.int_value),
                                                  TimestampGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIME && agtv2->type == AGTV_TIME
        && agtv3->type == AGTV_TIME && agtv4->type == AGTV_TIME)
    {
        result = DatumGetBool(DirectFunctionCall4(overlaps_time,
                                                  TimeADTGetDatum(agtv1->val.int_value),
                                                  TimeADTGetDatum(agtv2->val.int_value),
                                                  TimeADTGetDatum(agtv3->val.int_value),
                                                  TimeADTGetDatum(agtv4->val.int_value)));
    }
    else if( agtv1->type == AGTV_TIME && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIME && agtv4->type == AGTV_INTERVAL)
    {
        TimeADT te1,te2;
        te1 = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval, TimeADTGetDatum(agtv1->val.int_value), IntervalPGetDatum(&agtv2->val.interval)));
        te2 = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval, TimeADTGetDatum(agtv3->val.int_value), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_time,
                                                  TimeADTGetDatum(agtv1->val.int_value),
                                                  TimeADTGetDatum(te1),
                                                  TimeADTGetDatum(agtv3->val.int_value),
                                                  TimeADTGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIME && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIME && agtv4->type == AGTV_TIME)
    {
        TimeADT te1;
        te1 = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval, TimeADTGetDatum(agtv1->val.int_value), IntervalPGetDatum(&agtv2->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_time,
                                                  TimeADTGetDatum(agtv1->val.int_value),
                                                  TimeADTGetDatum(te1),
                                                  TimeADTGetDatum(agtv3->val.int_value),
                                                  TimeADTGetDatum(agtv4->val.int_value)));
    }
    else if( agtv1->type == AGTV_TIME && agtv2->type == AGTV_TIME
        && agtv3->type == AGTV_TIME && agtv4->type == AGTV_INTERVAL)
    {
        TimeADT te2;
        te2 = DatumGetTimeADT(DirectFunctionCall2(time_pl_interval, TimeADTGetDatum(agtv3->val.int_value), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_time,
                                                  TimeADTGetDatum(agtv1->val.int_value),
                                                  TimeADTGetDatum(agtv2->val.int_value),
                                                  TimeADTGetDatum(agtv3->val.int_value),
                                                  TimeADTGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIMETZ && agtv2->type == AGTV_TIMETZ
        && agtv3->type == AGTV_TIMETZ && agtv4->type == AGTV_TIMETZ)
    {
        result = DatumGetBool(DirectFunctionCall4(overlaps_timetz,
                                                  TimeTzADTPGetDatum(&agtv1->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv2->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv3->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv4->val.timetz)));
    }
    else if( agtv1->type == AGTV_TIMETZ && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIMETZ && agtv4->type == AGTV_INTERVAL)
    {
        TimeTzADT *te1,*te2;
        te1 = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval, TimeTzADTPGetDatum(&agtv1->val.timetz), IntervalPGetDatum(&agtv2->val.interval)));
        te2 = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval, TimeTzADTPGetDatum(&agtv3->val.timetz), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timetz,
                                                  TimeTzADTPGetDatum(&agtv1->val.timetz),
                                                  TimeTzADTPGetDatum(te1),
                                                  TimeTzADTPGetDatum(&agtv3->val.timetz),
                                                  TimeTzADTPGetDatum(te2)));
    }
    else if( agtv1->type == AGTV_TIMETZ && agtv2->type == AGTV_INTERVAL
        && agtv3->type == AGTV_TIMETZ && agtv4->type == AGTV_TIMETZ)
    {
        TimeTzADT *te1;
        te1 = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval, TimeTzADTPGetDatum(&agtv1->val.timetz), IntervalPGetDatum(&agtv2->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timetz,
                                                  TimeTzADTPGetDatum(&agtv1->val.timetz),
                                                  TimeTzADTPGetDatum(te1),
                                                  TimeTzADTPGetDatum(&agtv3->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv4->val.timetz)));
    }
    else if( agtv1->type == AGTV_TIMETZ && agtv2->type == AGTV_TIMETZ
        && agtv3->type == AGTV_TIMETZ && agtv4->type == AGTV_INTERVAL)
    {
        TimeTzADT *te2;
        te2 = DatumGetTimeTzADTP(DirectFunctionCall2(timetz_pl_interval, TimeTzADTPGetDatum(&agtv3->val.timetz), IntervalPGetDatum(&agtv4->val.interval)));

        result = DatumGetBool(DirectFunctionCall4(overlaps_timetz,
                                                  TimeTzADTPGetDatum(&agtv1->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv2->val.timetz),
                                                  TimeTzADTPGetDatum(&agtv3->val.timetz),
                                                  TimeTzADTPGetDatum(te2)));
    }
    else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid argument type for overlap expression")));

    agtv_result.type = AGTV_BOOL;
    agtv_result.val.boolean = result;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

