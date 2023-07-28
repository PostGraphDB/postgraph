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
#include "utils/date.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"

#include "utils/gtype.h"
#include "utils/gtype_temporal.h"

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


