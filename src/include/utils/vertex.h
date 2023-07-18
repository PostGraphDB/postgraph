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
 *
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 */

/*
 * Declarations for gtype data type support.
 */

#ifndef AG_VERTEX_H
#define AG_VERTEX_H

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
#include "utils/gtype.h"

/* Convenience macros */
#define DATUM_GET_VERTEX(d) ((vertex *)PG_DETOAST_DATUM(d))
#define VERTEX_GET_DATUM(p) PointerGetDatum(p)
#define AG_GET_ARG_VERTEX(x) DATUM_GET_VERTEX(PG_GETARG_DATUM(x))
#define AG_RETURN_VERTEX(x) PG_RETURN_POINTER(x)


typedef uint32 ventry;

/*
 * An vertex, within an gtype Datum.
 *
 * An array has one child for each element, stored in array order.
 */
typedef struct
{
    int32 vl_len_; // varlena header (do not touch directly!)
    ventry children[FLEXIBLE_ARRAY_MEMBER];
} vertex;

void append_vertex_to_string(StringInfoData *buffer, vertex *v);
char *extract_vertex_label(vertex *v);
gtype *extract_vertex_properties(vertex *v);
Datum build_vertex(PG_FUNCTION_ARGS);
vertex *create_vertex(graphid id, char *label, gtype *properties);	
int extract_vertex_label_length(vertex *v);

#define VERTEXOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("vertex"), ObjectIdGetDatum(postgraph_namespace_id())))

#define VERTEXARRAYOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("_vertex"), ObjectIdGetDatum(postgraph_namespace_id())))

#endif
