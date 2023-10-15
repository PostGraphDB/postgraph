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

Datum gtype_to_int8_internal(gtype_value *agtv);
Datum gtype_to_int4_internal(gtype_value *agtv);
Datum gtype_to_int2_internal(gtype_value *agtv);
Datum gtype_to_float8_internal(gtype_value *agtv);
Datum gtype_to_numeric_internal(gtype_value *agtv);
Datum gtype_to_string_internal(gtype_value *agtv);
Datum gtype_to_text_internal(gtype_value *agtv);
Datum gtype_to_boolean_internal(gtype_value *agtv);
Datum gtype_to_date_internal(gtype_value *agtv);
Datum gtype_to_time_internal(gtype_value *agtv);
Datum gtype_to_timetz_internal(gtype_value *agtv);
Datum gtype_to_timestamptz_internal(gtype_value *agtv);
Datum gtype_to_timestamp_internal(gtype_value *agtv);
Datum gtype_to_box2d_internal(gtype_value *agtv);
Datum gtype_to_box3d_internal(gtype_value *agtv);
Datum gtype_to_spheroid_internal(gtype_value *agtv);
Datum gtype_to_geometry_internal(gtype_value *agtv);

Datum _gtype_toinet(Datum arg);

#endif
