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
edge *create_edge(graphid id,graphid start_id,graphid end_id, char *label, gtype *properties);
int extract_edge_label_length(edge *v);

#define EDGEOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("edge"), ObjectIdGetDatum(postgraph_namespace_id())))

#define EDGEARRAYOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("_edge"), ObjectIdGetDatum(postgraph_namespace_id())))


#endif
