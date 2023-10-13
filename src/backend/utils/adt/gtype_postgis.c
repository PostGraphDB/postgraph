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


#include "postgres.h"

//Postgres
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

//PostGIS

//PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"


PG_FUNCTION_INFO_V1(LWGEOM_asEWKT);

PG_FUNCTION_INFO_V1(gtype_asEWKT);
Datum 
gtype_asEWKT(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_asEWKT, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));
    
    gtype_value gtv = { .type = AGTV_STRING, .val.string = {VARSIZE_ANY_EXHDR(d), VARDATA_ANY(d)} };
        
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}
