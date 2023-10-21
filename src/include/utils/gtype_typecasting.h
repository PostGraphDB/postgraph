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

#ifndef PGRAPH_GTYPE_TYPECASTING_H
#define PGRAPH_GTYPE_TYPECASTING_H

#include "utils/timestamp.h"

typedef Datum (*coearce_function) (gtype_value *);
Datum convert_to_scalar(coearce_function func, gtype *agt, char *type);


#define GT_TO_INT4_DATUM(arg) \
    convert_to_scalar(gtype_to_int4_internal, arg, "int4")
#define GT_TO_INT8_DATUM(arg) \
    convert_to_scalar(gtype_to_int8_internal, arg, "int")
#define GT_TO_TEXT_DATUM(arg) \
    convert_to_scalar(gtype_to_text_internal, (arg), "text")
#define GT_TO_TIMESTAMP_DATUM(arg) \
    convert_to_scalar(gtype_to_timestamp_internal, (arg), "timestamp")
#define GT_TO_TIMESTAMPTZ_DATUM(arg) \
    convert_to_scalar(gtype_to_timestamptz_internal, (arg), "timestamptz")
#define GT_TO_DATE_DATUM(arg) \
    convert_to_scalar(gtype_to_date_internal, (arg), "date")
#define GT_TO_TIME_DATUM(arg) \
    convert_to_scalar(gtype_to_time_internal, (arg), "time")
#define GT_TO_TIMETZ_DATUM(arg) \
    convert_to_scalar(gtype_to_timetz_internal, (arg), "time")
#define GT_TO_INTERVAL_DATUM(arg) \
    convert_to_scalar(gtype_to_interval_internal, (arg), "interval")
#define GT_TO_CIDR_DATUM(arg) \
    convert_to_scalar(gtype_to_cidr_internal, (arg), "cidr")
#define GT_TO_INET_DATUM(arg) \
    convert_to_scalar(gtype_to_inet_internal, (arg), "inet")
#define GT_TO_MAC_DATUM(arg) \
    convert_to_scalar(gtype_to_macaddr_internal, (arg), "mac")
#define GT_TO_MAC8_DATUM(arg) \
    convert_to_scalar(gtype_to_macaddr8_internal, (arg), "mac8")

#define GT_ARG_TO_INT4_DATUM(arg) \
    convert_to_scalar(gtype_to_int4_internal, AG_GET_ARG_GTYPE_P(arg), "int4")
#define GT_ARG_TO_INT8_DATUM(arg) \
    convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(arg), "int")
#define GT_ARG_TO_TEXT_DATUM(arg) \
    convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(arg), "text")
#define GT_ARG_TO_INET_DATUM(arg) \
    convert_to_scalar(gtype_to_inet_internal, AG_GET_ARG_GTYPE_P(arg), "inet")
#define GT_ARG_TO_MAC8_DATUM(arg) \
    convert_to_scalar(gtype_to_macaddr8_internal, AG_GET_ARG_GTYPE_P(arg), "mac8")

Datum gtype_to_int8_internal(gtype_value *gtv);
Datum gtype_to_int4_internal(gtype_value *gtv);
Datum gtype_to_int2_internal(gtype_value *gtv);
Datum gtype_to_float8_internal(gtype_value *gtv);
Datum gtype_to_float4_internal(gtype_value *gtv);
Datum gtype_to_numeric_internal(gtype_value *gtv);
Datum gtype_to_string_internal(gtype_value *gtv);
Datum gtype_to_text_internal(gtype_value *gtv);
Datum gtype_to_boolean_internal(gtype_value *gtv);
Datum gtype_to_date_internal(gtype_value *gtv);
Datum gtype_to_time_internal(gtype_value *gtv);
Datum gtype_to_timetz_internal(gtype_value *gtv);
Datum gtype_to_timestamptz_internal(gtype_value *gtv);
Datum gtype_to_timestamp_internal(gtype_value *gtv);
Datum gtype_to_interval_internal(gtype_value *gtv);
Datum gtype_to_inet_internal(gtype_value *gtv);
Datum gtype_to_cidr_internal(gtype_value *gtv);
Datum gtype_to_macaddr_internal(gtype_value *gtv);
Datum gtype_to_macaddr8_internal(gtype_value *gtv);
Datum gtype_to_point_internal(gtype_value *gtv);
Datum gtype_to_path_internal(gtype_value *gtv);
Datum gtype_to_box2d_internal(gtype_value *gtv);
Datum gtype_to_box3d_internal(gtype_value *gtv);
Datum gtype_to_spheroid_internal(gtype_value *gtv);
Datum gtype_to_geometry_internal(gtype_value *gtv);
Datum gtype_to_int_range_internal(gtype_value *gtv);

Datum _gtype_toinet(Datum arg);

#endif
