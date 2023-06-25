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
 */

/*
 * Declarations for agtype data type support.
 */

#ifndef POSTGRAPH_PARTIAL_ROUTE_H
#define POSTGRAPH_PARTIAL_ROUTE_H

#include "access/htup_details.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/syscache.h"

#include "catalog/ag_namespace.h"
#include "catalog/pg_type.h"
#include "utils/graphid.h"

/* Convenience macros */
#define DATUM_GET_PARTIAL_ROUTE(d) ((vertex *)PG_DETOAST_DATUM(d))
#define PARTIAL_ROUTE_GET_DATUM(p) PointerGetDatum(p)
#define AG_GET_ARG_PARTIAL_ROUTE(x) DATUM_GET_PARTIAL_ROUTE(PG_GETARG_DATUM(x))
#define AG_RETURN_PARTIAL_ROUTE(x) PG_RETURN_POINTER(x)


typedef uint32 prentry;

/*
 * An vertex, within an agtype Datum.
 *
 * An array has one child for each element, stored in array order.
 */
typedef struct
{
    int32 vl_len_; // varlena header (do not touch directly!)
    prentry children[FLEXIBLE_ARRAY_MEMBER];
} partial_route;

#endif
