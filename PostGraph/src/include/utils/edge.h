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
 */

/*
 * Declarations for edge data type support.
 */
#ifndef AG_EDGE_H
#define AG_EDGE_H

#include "access/htup_details.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/fmgrprotos.h"
#include "utils/varlena.h"

#include "catalog/ag_namespace.h"
#include "catalog/pg_type.h"
#include "utils/graphid.h"
#include "utils/gtype.h"

/* Convenience macros */
#define DATUM_GET_EDGE(d) ((edge *)PG_DETOAST_DATUM(d))
#define EDGE_GET_DATUM(p) PointerGetDatum(p)
#define AG_GET_ARG_EDGE(x) DATUM_GET_EDGE(PG_GETARG_DATUM(x))
#define AG_RETURN_EDGE(x) PG_RETURN_POINTER(x)


typedef uint32 eentry;


#define EXTRACT_EDGE_ID(e) \
    (*((int64 *)(&((edge *)e)->children[0])))

#define EXTRACT_EDGE_STARTID(e) \
    (*((int64 *)(&((edge *)e)->children[2])))

#define EXTRACT_EDGE_ENDID(e) \
    (*((int64 *)(&((edge *)e)->children[4])))

#define EXTRACT_EDGE_GRAPH_OID(v) \
    (*((Oid *)(&((edge *)v)->children[6])))


/*
 * An edge, within an gtype Datum.
 *
 * An array has one child for each element, stored in array order.
 */
typedef struct
{
    int32 vl_len_;
    eentry children[FLEXIBLE_ARRAY_MEMBER];
} edge;

void append_edge_to_string(StringInfoData *buffer, edge *v);
char *extract_edge_label(edge *v);
gtype *extract_edge_properties(edge *v);
Datum build_edge(PG_FUNCTION_ARGS);
edge *create_edge(graphid id,graphid start_id,graphid end_id, Oid graph_oid, gtype *properties);
int extract_edge_label_length(edge *v);

#define EDGEOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("edge"), ObjectIdGetDatum(postgraph_namespace_id())))

#define EDGEARRAYOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("_edge"), ObjectIdGetDatum(postgraph_namespace_id())))


#endif
