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

#ifndef PGRAPH_GTYPE_TEMPORAL_H
#define PGRAPH_GTYPE_TEMPORAL_H

#include "utils/timestamp.h"

int timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2);
int interval_cmp_internal(Interval *interval1, Interval *interval2);
//TimestampTz timestamp2timestamptz(Timestamp timestamp);

#endif
