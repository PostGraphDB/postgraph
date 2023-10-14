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
#include "libpgcommon/lwgeom_pg.h"
#include "liblwgeom/liblwgeom.h"

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

PG_FUNCTION_INFO_V1(LWGEOM_x_point);

PG_FUNCTION_INFO_V1(gtype_x_point);
Datum
gtype_x_point(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));
    POINT4D pt;

    if (gserialized_get_type(geom) != POINTTYPE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Argument to ST_X() must have type POINT")));

    if (gserialized_peek_first_point(geom, &pt) == LW_FAILURE)
                PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = pt.x };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_y_point);

PG_FUNCTION_INFO_V1(gtype_y_point);
Datum
gtype_y_point(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));
    POINT4D pt;

    if (gserialized_get_type(geom) != POINTTYPE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Argument to ST_Y() must have type POINT")));

    if (gserialized_peek_first_point(geom, &pt) == LW_FAILURE)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = pt.y };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_z_point);

PG_FUNCTION_INFO_V1(gtype_z_point);
Datum
gtype_z_point(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));
    POINT4D pt;

    if (gserialized_get_type(geom) != POINTTYPE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Argument to ST_Z() must have type POINT")));

    if (!gserialized_has_z(geom) || (gserialized_peek_first_point(geom, &pt) == LW_FAILURE))
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = pt.z };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_m_point);

PG_FUNCTION_INFO_V1(gtype_m_point);
Datum
gtype_m_point(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));
    POINT4D pt;

    if (gserialized_get_type(geom) != POINTTYPE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Argument to ST_M() must have type POINT")));

    if (!gserialized_has_m(geom) || (gserialized_peek_first_point(geom, &pt) == LW_FAILURE))
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = pt.m };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(ST_Intersection);

PG_FUNCTION_INFO_V1(gtype_st_intersection);
Datum
gtype_st_intersection(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");


    Datum d = DirectFunctionCall3(ST_Intersection, d1, d2, d3);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ST_ClosestPointOfApproach);
Datum
gtype_ST_ClosestPointOfApproach(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);

    GSERIALIZED *d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    GSERIALIZED *d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");

    LWGEOM *g0 = lwgeom_from_gserialized(d1);
    LWGEOM *g1 = lwgeom_from_gserialized(d2);
    float8 mindist;
    float8 m = lwgeom_tcpa(g0, g1, &mindist);
    
    lwgeom_free(g0);
    lwgeom_free(g1);
    
    if ( m < 0 )
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = m };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ST_DistanceCPA);
Datum
gtype_ST_DistanceCPA(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);

    GSERIALIZED *d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    GSERIALIZED *d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");

    LWGEOM *g0 = lwgeom_from_gserialized(d1);
    LWGEOM *g1 = lwgeom_from_gserialized(d2);
    float8 mindist;
    float8 m = lwgeom_tcpa(g0, g1, &mindist);

    lwgeom_free(g0);
    lwgeom_free(g1);

    if ( m < 0 )
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = mindist };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

