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
#include "libpgcommon/lwgeom_pg.h"

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

PG_FUNCTION_INFO_V1(LWGEOM_addBBOX);

PG_FUNCTION_INFO_V1(gtype_addBBOX);
Datum
gtype_addBBOX(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_addBBOX, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_dropBBOX);

PG_FUNCTION_INFO_V1(gtype_dropBBOX);
Datum
gtype_dropBBOX(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_dropBBOX, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

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


PG_FUNCTION_INFO_V1(ST_IsValidTrajectory);
PG_FUNCTION_INFO_V1(gtype_st_isvalidtrajectory);
Datum
gtype_st_isvalidtrajectory(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");


    Datum d = DirectFunctionCall1(ST_IsValidTrajectory, d1);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = DatumGetBool(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(ST_CPAWithin);
PG_FUNCTION_INFO_V1(gtype_ST_CPAWithin);
Datum
gtype_ST_CPAWithin(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");

    Datum d = DirectFunctionCall3(ST_IsValidTrajectory, d1, d2, d3);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = DatumGetBool(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}
                        
/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/                      
GSERIALIZED* geometry_serialize(LWGEOM *lwgeom)
{
        size_t ret_size;
        GSERIALIZED *g;
        
        g = gserialized_from_lwgeom(lwgeom, &ret_size);
        SET_VARSIZE(g, ret_size);
        return g;
}

/*
* Generate a field of random points within the area of a
* polygon or multipolygon. Throws an error for other geometry
* types.
*/
PG_FUNCTION_INFO_V1(gtype_st_generatepoints);
Datum
gtype_st_generatepoints(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    GSERIALIZED *gser_input = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    GSERIALIZED *gser_result;
    LWGEOM *lwgeom_input;
    LWGEOM *lwgeom_result;
    int32 npoints = DatumGetInt32(convert_to_scalar(gtype_to_int4_internal, gt_2, "int4"));
    int32 seed = 0;

    if (npoints < 0)
        PG_RETURN_NULL();

    if (PG_NARGS() > 2 && ! PG_ARGISNULL(2)) {
	gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
        seed = DatumGetInt32(convert_to_scalar(gtype_to_int4_internal, gt_3, "int4"));
        if (seed < 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("ST_GeneratePoints: seed must be greater than zero")));

    }

    // Types get checked in the code, we'll keep things small out there
    lwgeom_input = lwgeom_from_gserialized(gser_input);
    lwgeom_result = (LWGEOM*)lwgeom_to_points(lwgeom_input, npoints, seed);
    lwgeom_free(lwgeom_input);
    PG_FREE_IF_COPY(gser_input, 0);

    if (!lwgeom_result)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geometry_serialize(lwgeom_result) };
    lwgeom_free(lwgeom_result);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}



PG_FUNCTION_INFO_V1(gtype_ST_Simplify);
Datum gtype_ST_Simplify(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);

    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    double dist = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, gt_2, "float"));
    GSERIALIZED *result;
    int type = gserialized_get_type(geom);
    LWGEOM *in;
    bool preserve_collapsed = false;
    int modified = LW_FALSE;

    /* Can't simplify points! */
    if ( type == POINTTYPE || type == MULTIPOINTTYPE ) {
         gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geom };
         AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    }

    /* Handle optional argument to preserve collapsed features */
    if (PG_NARGS() > 2 && !PG_ARGISNULL(2))
        preserve_collapsed = DatumGetBool(convert_to_scalar(gtype_to_boolean_internal,
					                    AG_GET_ARG_GTYPE_P(2), "boolean"));

    in = lwgeom_from_gserialized(geom);

    modified = lwgeom_simplify_in_place(in, dist, preserve_collapsed);
    if (!modified)
        PG_RETURN_POINTER(geom);

    if (!in || lwgeom_is_empty(in))
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geometry_serialize(in) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_affine);
Datum gtype_affine(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
    GSERIALIZED *ret;
    AFFINE affine;

    affine.afac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float"));
    affine.bfac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(2), "float"));
    affine.cfac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(3), "float"));
    affine.dfac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(4), "float"));
    affine.efac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(5), "float"));
    affine.ffac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(6), "float"));
    affine.gfac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(7), "float"));
    affine.hfac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(8), "float"));
    affine.ifac = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(9), "float"));
    affine.xoff = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(10), "float"));
    affine.yoff = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(11), "float"));
    affine.zoff = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(12), "float"));

    POSTGIS_DEBUG(2, "LWGEOM_affine called.");

    lwgeom_affine(lwgeom, &affine);

    // COMPUTE_BBOX TAINTING
    if (lwgeom->bbox)
        lwgeom_refresh_bbox(lwgeom);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geometry_serialize(lwgeom) };

    /* Release memory */
    lwgeom_free(lwgeom);
    PG_FREE_IF_COPY(geom, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_makepoint);
Datum gtype_makepoint(PG_FUNCTION_ARGS)
{
    double x, y, z, m;
    LWPOINT *point;
    GSERIALIZED *result;

    POSTGIS_DEBUG(2, "gtype_makepoint called");

    x = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(0), "float"));
    y = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float"));

    if (PG_NARGS() == 2)
                point = lwpoint_make2d(SRID_UNKNOWN, x, y);
    else if (PG_NARGS() == 3) {
        z = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(2), "float"));
        point = lwpoint_make3dz(SRID_UNKNOWN, x, y, z);
    } else if (PG_NARGS() == 4) {
        z = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(2), "float"));
        m = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(3), "float"));
        point = lwpoint_make4d(SRID_UNKNOWN, x, y, z, m);
    } else {
        elog(ERROR, "gtype_makepoint: unsupported number of args: %d", PG_NARGS());
    }

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geometry_serialize((LWGEOM *)point) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_makepoint3dm);
Datum gtype_makepoint3dm(PG_FUNCTION_ARGS)
{
    double x, y, m;
    LWPOINT *point;

    POSTGIS_DEBUG(2, "LWGEOM_makepoint3dm called.");

    x = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(0), "float"));
    y = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float"));
    m = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(2), "float"));

    point = lwpoint_make3dm(SRID_UNKNOWN, x, y, m);
    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geometry_serialize((LWGEOM *)point) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}
