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
#include "access/gist.h"
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

Datum
PostGraphDirectFunctionCall1(PGFunction func, Oid collation, bool *is_null, Datum arg1)
{
        LOCAL_FCINFO(fcinfo, 3);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;

        Datum result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull) {
            *is_null = true;
            return NULL;
        }

        *is_null = false;

        return result;
}


Datum
PostGraphDirectFunctionCall3(PGFunction func, Oid collation, bool *is_null, Datum arg1, Datum arg2, Datum arg3)
{
        LOCAL_FCINFO(fcinfo, 3);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;
        fcinfo->args[2].value = arg3;
        fcinfo->args[2].isnull = false;

        Datum result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull) {
            *is_null = true;
            return NULL;
        }

        *is_null = false;

        return result;
}

Datum
PostGraphDirectFunctionCall4(PGFunction func, Oid collation, bool *is_null, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
{
        LOCAL_FCINFO(fcinfo, 4);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        InitFunctionCallInfoData(*fcinfo, NULL, 4, collation, NULL, NULL);

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;
        fcinfo->args[2].value = arg3;
        fcinfo->args[2].isnull = false;
        fcinfo->args[3].value = arg4;
        fcinfo->args[3].isnull = false;

        Datum result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull) {
            *is_null = true;
            return NULL;
        }

        *is_null = false;

        return result;
}

Datum
PostGraphDirectFunctionCall5(PGFunction func, Oid collation, bool *is_null, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
        LOCAL_FCINFO(fcinfo, 4);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        InitFunctionCallInfoData(*fcinfo, NULL, 4, collation, NULL, NULL);

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;
        fcinfo->args[2].value = arg3;
        fcinfo->args[2].isnull = false;
        fcinfo->args[3].value = arg4;
        fcinfo->args[3].isnull = false;
        fcinfo->args[4].value = arg5;
        fcinfo->args[4].isnull = false;

        Datum result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull) {
            *is_null = true;
            return NULL;
        }

        *is_null = false;

        return result;
}


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

PG_FUNCTION_INFO_V1(BOX3D_xmin);
PG_FUNCTION_INFO_V1(gtype_xmin);
Datum
gtype_xmin(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_xmin, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(BOX3D_xmax);
PG_FUNCTION_INFO_V1(gtype_xmax);
Datum
gtype_xmax(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_xmax, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(BOX3D_ymin);
PG_FUNCTION_INFO_V1(gtype_ymin);
Datum
gtype_ymin(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_ymin, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(BOX3D_zmin);
PG_FUNCTION_INFO_V1(gtype_zmin);
Datum
gtype_zmin(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_zmin, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(BOX3D_ymax);
PG_FUNCTION_INFO_V1(gtype_ymax);
Datum
gtype_ymax(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_ymax, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(BOX3D_zmax);
PG_FUNCTION_INFO_V1(gtype_zmax);
Datum
gtype_zmax(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(BOX3D_zmax, convert_to_scalar(gtype_to_box3d_internal, gt_1, "box3d"));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


/*
 * 2D Geometry Operators
 */
// @
PG_FUNCTION_INFO_V1(gserialized_within_2d);
PG_FUNCTION_INFO_V1(gtype_within_2d);
Datum
gtype_within_2d(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");

    Datum d = DirectFunctionCall2(gserialized_within_2d, d1, d2);

    PG_RETURN_BOOL(DatumGetBool(d));
}

PG_FUNCTION_INFO_V1(gserialized_overleft_2d);

#define GEOMETRIC2DOPERATOR( type, postgis_type) \
PG_FUNCTION_INFO_V1(gtype_##postgis_type); \
Datum \
gtype_##postgis_type(PG_FUNCTION_ARGS) { \
    gtype *lhs = AG_GET_ARG_GTYPE_P(0); \
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);\
\
    if (GT_IS_BOX(lhs) && GT_IS_BOX(rhs)) \
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(box_##type, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs)))); \
    else if (GT_IS_POLYGON(lhs) && GT_IS_POLYGON(rhs)) \
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(poly_##type, GT_TO_POLYGON_DATUM(lhs), GT_TO_POLYGON_DATUM(rhs)))); \
    else if (GT_IS_CIRCLE(lhs) && GT_IS_CIRCLE(rhs)) \
       PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2(circle_##type, GT_TO_CIRCLE_DATUM(lhs), GT_TO_CIRCLE_DATUM(rhs)))); \
\
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, lhs, "geometry"); \
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, rhs, "geometry"); \
\
    Datum d = DirectFunctionCall2(gserialized_##postgis_type, d1, d2); \
\
    PG_RETURN_BOOL(DatumGetBool(d)); \
} \
/* keep compiler quiet - no extra ; */                                                     \
extern int no_such_variable

// ~=
PG_FUNCTION_INFO_V1(gserialized_same_2d);
GEOMETRIC2DOPERATOR(same, same_2d);

// &<
GEOMETRIC2DOPERATOR(overleft, overleft_2d);
// <<|
PG_FUNCTION_INFO_V1(gserialized_below_2d);
GEOMETRIC2DOPERATOR(below, below_2d);
// &<|
PG_FUNCTION_INFO_V1(gserialized_overbelow_2d);
GEOMETRIC2DOPERATOR(overbelow, overbelow_2d);
// &>
PG_FUNCTION_INFO_V1(gserialized_overright_2d);
GEOMETRIC2DOPERATOR(overright, overright_2d);
// |&>
PG_FUNCTION_INFO_V1(gserialized_overabove_2d);
GEOMETRIC2DOPERATOR(overabove, overabove_2d);
// |>>
PG_FUNCTION_INFO_V1(gserialized_above_2d);
GEOMETRIC2DOPERATOR(above, above_2d);


/*
 * 2D GIST Index Support Functions
 */
PG_FUNCTION_INFO_V1(gserialized_gist_consistent_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_consistent_2d);
Datum
gtype_gserialized_gist_consistent_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    Datum d3 = PG_GETARG_DATUM(2);
    Datum d4 = PG_GETARG_DATUM(3);
    Datum d5 = PG_GETARG_DATUM(4);
    bool is_null;

    Datum d = PostGraphDirectFunctionCall5(gserialized_gist_consistent_2d, 100, &is_null, d1, d2, d3, d4, d5);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}
   
PG_FUNCTION_INFO_V1(gserialized_gist_distance_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_distance_2d);
Datum
gtype_gserialized_gist_distance_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    Datum d3 = PG_GETARG_DATUM(2);
    Datum d4 = PG_GETARG_DATUM(3);

    bool is_null;

    Datum d = PostGraphDirectFunctionCall4(gserialized_gist_distance_2d, 100, &is_null, d1, d2, d3, d4);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_gist_compress_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_compress_2d);
Datum
gtype_gserialized_gist_compress_2d(PG_FUNCTION_ARGS) {
    //Datum d1 = PG_GETARG_DATUM(0);
    GISTENTRY *entry_in = (GISTENTRY*)PG_GETARG_POINTER(0);
    entry_in->key = convert_to_scalar(gtype_to_geometry_internal, entry_in->key, "geometry");
    //Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");

    bool is_null;

    Datum d = PostGraphDirectFunctionCall1(gserialized_gist_compress_2d, 100, &is_null, PointerGetDatum(entry_in));

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_gist_penalty_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_penalty_2d);
Datum
gtype_gserialized_gist_penalty_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = PG_GETARG_DATUM(1);
    Datum d3 = PG_GETARG_DATUM(2);

    bool is_null;

    Datum d = PostGraphDirectFunctionCall3(gserialized_gist_penalty_2d, 100, &is_null, d1, d2, d3);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_gist_union_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_union_2d);
Datum
gtype_gserialized_gist_union_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = PG_GETARG_DATUM(1);

    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_gist_union_2d, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_gist_same_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_same_2d);
Datum
gtype_gserialized_gist_same_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = PG_GETARG_DATUM(1);
    Datum d3 = PG_GETARG_DATUM(2);

    bool is_null;

    Datum d = PostGraphDirectFunctionCall3(gserialized_gist_same_2d, 100, &is_null, d1, d2, d3);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_gserialized_gist_decompress_2d);
Datum gtype_gserialized_gist_decompress_2d(PG_FUNCTION_ARGS)
{
        POSTGIS_DEBUG(5, "[GIST] 'decompress' function called");
        /* We don't decompress. Just return the input. */
        PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

PG_FUNCTION_INFO_V1(gserialized_gist_sortsupport_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_sortsupport_2d);
Datum
gtype_gserialized_gist_sortsupport_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);

    bool is_null;

    PostGraphDirectFunctionCall1(gserialized_gist_sortsupport_2d, 100, &is_null, d1);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(gserialized_gist_picksplit_2d);
PG_FUNCTION_INFO_V1(gtype_gserialized_gist_picksplit_2d);
Datum
gtype_gserialized_gist_picksplit_2d(PG_FUNCTION_ARGS) {
    Datum d1 = PG_GETARG_DATUM(0);
    Datum d2 = PG_GETARG_DATUM(1);

    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_gist_picksplit_2d, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_overlaps);
PG_FUNCTION_INFO_V1(gtype_gserialized_overlaps);
Datum
gtype_gserialized_overlaps(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_overlaps, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_contains);
PG_FUNCTION_INFO_V1(gtype_gserialized_contains);
Datum
gtype_gserialized_contains(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_contains, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_within);
PG_FUNCTION_INFO_V1(gtype_gserialized_within);
Datum
gtype_gserialized_within(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_within, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gserialized_same);
PG_FUNCTION_INFO_V1(gtype_gserialized_same);
Datum
gtype_gserialized_same(PG_FUNCTION_ARGS) { 
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(gserialized_same, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(d);
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


PG_FUNCTION_INFO_V1(LWGEOM_length_linestring);
PG_FUNCTION_INFO_V1(gtype_length_linestring);
Datum
gtype_length_linestring(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall1(LWGEOM_length_linestring, 100, &is_null, d1);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.boolean = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(LWGEOM_length2d_linestring);
PG_FUNCTION_INFO_V1(gtype_length2d_linestring);
Datum
gtype_length2d_linestring(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall1(LWGEOM_length2d_linestring, 100, &is_null, d1);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.boolean = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_length_ellipsoid_linestring);
PG_FUNCTION_INFO_V1(gtype_length_ellipsoid_linestring);
Datum
gtype_length_ellipsoid_linestring(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_spheroid_internal, AG_GET_ARG_GTYPE_P(1), "sphereoid");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(LWGEOM_length2d_linestring, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.boolean = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_length2d_ellipsoid);
PG_FUNCTION_INFO_V1(gtype_length2d_ellipsoid);
Datum
gtype_length2d_ellipsoid(PG_FUNCTION_ARGS) {
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(0), "geometry");
    Datum d2 = convert_to_scalar(gtype_to_spheroid_internal, AG_GET_ARG_GTYPE_P(1), "sphereoid");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(LWGEOM_length2d_ellipsoid, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.boolean = DatumGetFloat8(d) };

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

PG_FUNCTION_INFO_V1(ST_Scale);
PG_FUNCTION_INFO_V1(gtype_st_scale_2_args);
Datum
gtype_st_scale_2_args(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");


    Datum d = DirectFunctionCall2(ST_Scale, d1, d2);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_st_scale);
Datum 
gtype_st_scale(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    Datum d3 = convert_to_scalar(gtype_to_geometry_internal, gt_3, "geometry");


    Datum d = DirectFunctionCall3(ST_Scale, d1, d2, d3);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

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

PG_FUNCTION_INFO_V1(LWGEOM_snaptogrid_pointoff);
PG_FUNCTION_INFO_V1(gtype_snaptogrid_pointoff);
Datum
gtype_snaptogrid_pointoff(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
    gtype *gt_4 = AG_GET_ARG_GTYPE_P(3);
    gtype *gt_5 = AG_GET_ARG_GTYPE_P(4);
    gtype *gt_6 = AG_GET_ARG_GTYPE_P(5);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");
    Datum d4 = convert_to_scalar(gtype_to_float8_internal, gt_4, "float");
    Datum d5 = convert_to_scalar(gtype_to_float8_internal, gt_5, "float");
    Datum d6 = convert_to_scalar(gtype_to_float8_internal, gt_6, "float");

    Datum d = DirectFunctionCall6(LWGEOM_snaptogrid_pointoff, d1, d2, d3, d4, d5, d6);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.boolean = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_snaptogrid);
PG_FUNCTION_INFO_V1(gtype_snaptogrid);
Datum
gtype_snaptogrid(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
    gtype *gt_4 = AG_GET_ARG_GTYPE_P(3);
    gtype *gt_5 = AG_GET_ARG_GTYPE_P(4);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_float8_internal, gt_2, "float");
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");
    Datum d4 = convert_to_scalar(gtype_to_float8_internal, gt_4, "float");
    Datum d5 = convert_to_scalar(gtype_to_float8_internal, gt_5, "float");

    Datum d = DirectFunctionCall5(LWGEOM_snaptogrid, d1, d2, d3, d4, d5);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.boolean = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ST_IsPolygonCW);
Datum
gtype_ST_IsPolygonCW(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED* geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    LWGEOM* input;
    bool is_clockwise;

    input = lwgeom_from_gserialized(geom);

    is_clockwise = lwgeom_is_clockwise(input);

    lwgeom_free(input);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = is_clockwise };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ST_IsPolygonCCW);
Datum
gtype_ST_IsPolygonCCW(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED* geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    LWGEOM* input;
    bool is_ccw;

    input = lwgeom_from_gserialized(geom);
    lwgeom_reverse_in_place(input);
    is_ccw = lwgeom_is_clockwise(input);

    lwgeom_free(input);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = is_ccw };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


/**
 * Compute the azimuth of segment defined by the two
 * given Point geometries.
 * @return NULL on exception (same point).
 *              Return radians otherwise.
 */
PG_FUNCTION_INFO_V1(gtype_azimuth);
Datum
gtype_azimuth(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED* geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    LWPOINT *lwpoint;
    POINT2D p1, p2;
    double result;
    int32_t srid;

    /* Extract first point */
    lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(geom));
    if (!lwpoint)
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Argument must be POINT geometries")));   
   
    srid = lwpoint->srid;
    if (!getPoint2d_p(lwpoint->point, 0, &p1))
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Error extracting point")));   
   
    lwpoint_free(lwpoint);

    /* Extract second point */
    geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, AG_GET_ARG_GTYPE_P(1), "geometry"));
    lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(geom));
    if (!lwpoint)
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Argument must be POINT geometries")));   
    
    if (lwpoint->srid != srid)
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Operation on mixed SRID geometries")));   
    
    if (!getPoint2d_p(lwpoint->point, 0, &p2))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Error extracting point")));	    
    
    lwpoint_free(lwpoint);
    PG_FREE_IF_COPY(geom, 1);

    /* Standard return value for equality case */
    if ((p1.x == p2.x) && (p1.y == p2.y))
        PG_RETURN_NULL();

    /* Compute azimuth */
    if (!azimuth_pt_pt(&p1, &p2, &result))
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = result };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

}

PG_FUNCTION_INFO_V1(LWGEOM_distance_ellipsoid);
PG_FUNCTION_INFO_V1(gtype_distance_ellipsoid);
Datum gtype_distance_ellipsoid(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");

    POSTGIS_DEBUG(2, "gtype_distance_ellipsoid called");

    Datum d;
    if (PG_NARGS() == 2) {
        d = DirectFunctionCall2(LWGEOM_distance_ellipsoid, d1, d2);
    } else {
        Datum d3 = convert_to_scalar(gtype_to_spheroid_internal, AG_GET_ARG_GTYPE_P(2), "geometry");
        d = DirectFunctionCall3(LWGEOM_distance_ellipsoid, d1, d2, d3);
    }

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_2d);

PG_FUNCTION_INFO_V1(gtype_force_2d);
Datum
gtype_force_2d(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_force_2d, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_3dz);

PG_FUNCTION_INFO_V1(gtype_force_3dz);
Datum
gtype_force_3dz(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_float8_internal, gt_2, "float");

    Datum d = DirectFunctionCall2(LWGEOM_force_3dz, d1, d2);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_3dm);

PG_FUNCTION_INFO_V1(gtype_force_3dm);
Datum
gtype_force_3dm(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_float8_internal, gt_2, "float");

    Datum d = DirectFunctionCall2(LWGEOM_force_3dm, d1, d2);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_4d);

PG_FUNCTION_INFO_V1(gtype_force_4d);
Datum
gtype_force_4d(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);

    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_float8_internal, gt_2, "float");
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");

    Datum d = DirectFunctionCall3(LWGEOM_force_4d, d1, d2, d3);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_collection);

PG_FUNCTION_INFO_V1(gtype_force_collection);
Datum
gtype_force_collection(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_force_collection, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(LWGEOM_force_multi);

PG_FUNCTION_INFO_V1(gtype_force_multi);
Datum
gtype_force_multi(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(LWGEOM_force_multi, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(convexhull);

PG_FUNCTION_INFO_V1(gtype_convexhull);
Datum
gtype_convexhull(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d = DirectFunctionCall1(convexhull, convert_to_scalar(gtype_to_geometry_internal, gt, "geometry"));

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
 *  @example symdifference {@link #symdifference} - SELECT ST_SymDifference(
 *      'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))',
 *      'POLYGON((5 5, 15 5, 15 7, 5 7, 5 5))');
 */
PG_FUNCTION_INFO_V1(ST_SymDifference);
PG_FUNCTION_INFO_V1(gtype_st_symdifference);
Datum
gtype_st_symdifference(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");

    Datum d;
    if (PG_NARGS() == 3) {
        gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);

        Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");

	d = DirectFunctionCall3(ST_SymDifference, d1, d2, d3);
    } else {
        d = DirectFunctionCall2(ST_SymDifference, d1, d2);
    }

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
 *  @brief Compute the Hausdorff distance thanks to the corresponding GEOS function
 *  @example ST_HausdorffDistance {@link #hausdorffdistance} - SELECT ST_HausdorffDistance(
 *      'POLYGON((0 0, 0 2, 1 2, 2 2, 2 0, 0 0))'::geometry,
 *      'POLYGON((0.5 0.5, 0.5 2.5, 1.5 2.5, 2.5 2.5, 2.5 0.5, 0.5 0.5))'::geometry);
 */
PG_FUNCTION_INFO_V1(hausdorffdistance);
PG_FUNCTION_INFO_V1(gtype_hausdorffdistance);
Datum
gtype_hausdorffdistance(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(hausdorffdistance, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
 *  @brief Compute the Hausdorff distance with densification thanks to the corresponding GEOS function
 *  @example hausdorffdistancedensify {@link #hausdorffdistancedensify} - SELECT ST_HausdorffDistance(
 *      'POLYGON((0 0, 0 2, 1 2, 2 2, 2 0, 0 0))'::geometry,
 *      'POLYGON((0.5 0.5, 0.5 2.5, 1.5 2.5, 2.5 2.5, 2.5 0.5, 0.5 0.5))'::geometry,
 *       0.5);
 */
PG_FUNCTION_INFO_V1(hausdorffdistancedensify);
PG_FUNCTION_INFO_V1(gtype_hausdorffdistancedensify);
Datum
gtype_hausdorffdistancedensify(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");

    bool is_null;

    Datum d = PostGraphDirectFunctionCall3(hausdorffdistancedensify, 100, &is_null, d1, d2, d3);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
 *  @brief Compute the Frechet distance with optional densification thanks to the corresponding GEOS function
 *  @example ST_FrechetDistance {@link #frechetdistance} - SELECT ST_FrechetDistance(
 *      'LINESTRING (0 0, 50 200, 100 0, 150 200, 200 0)'::geometry,
 *      'LINESTRING (0 200, 200 150, 0 100, 200 50, 0 0)'::geometry, 0.5);
 */
PG_FUNCTION_INFO_V1(ST_FrechetDistance);
PG_FUNCTION_INFO_V1(gtype_st_frechet_distance);
Datum
gtype_st_frechet_distance(PG_FUNCTION_ARGS) {
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
    Datum d3 = convert_to_scalar(gtype_to_float8_internal, gt_3, "float");

    bool is_null;

    Datum d = PostGraphDirectFunctionCall3(ST_FrechetDistance, 100, &is_null, d1, d2, d3);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


