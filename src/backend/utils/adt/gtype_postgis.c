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
#include "liblwgeom/liblwgeom_internal.h"

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

#include <float.h>

/**
Returns boolean describing if
mininimum 3d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(gtype_dwithin3d);
Datum gtype_dwithin3d(PG_FUNCTION_ARGS)
{
	double mindist;
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
	GSERIALIZED *geom1 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	GSERIALIZED *geom2 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry"));
	double tolerance = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, gt_3, "float"));
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (tolerance < 0)
	{
		elog(ERROR, "Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	gserialized_error_if_srid_mismatch(geom1, geom2, __func__);

	mindist = lwgeom_mindistance3d_tolerance(lwgeom1, lwgeom2, tolerance);

	/*empty geometries cases should be right handled since return from underlying
	 functions should be FLT_MAX which causes false as answer*/
    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = (tolerance >= mindist) };
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


/**
Returns boolean describing if
maximum 3d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(gtype_dfullywithin3d);
Datum gtype_dfullywithin3d(PG_FUNCTION_ARGS)
{
	double maxdist;
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    gtype *gt_3 = AG_GET_ARG_GTYPE_P(2);
	GSERIALIZED *geom1 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	GSERIALIZED *geom2 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry"));
	double tolerance = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, gt_3, "float"));
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (tolerance < 0)
	{
		elog(ERROR, "Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	gserialized_error_if_srid_mismatch(geom1, geom2, __func__);
	maxdist = lwgeom_maxdistance3d_tolerance(lwgeom1, lwgeom2, tolerance);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = maxdist > -1 ? (tolerance >= maxdist) : LW_FALSE };
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

}

/**
 Maximum 3d distance between objects in geom1 and geom2.
 */
PG_FUNCTION_INFO_V1(LWGEOM_maxdistance3d);
PG_FUNCTION_INFO_V1(gtype_maxdistance3d);
Datum gtype_maxdistance3d(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
	GSERIALIZED *geom1 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	GSERIALIZED *geom2 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry"));
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);
	gserialized_error_if_srid_mismatch(geom1, geom2, __func__);

	double maxdist = lwgeom_maxdistance3d(lwgeom1, lwgeom2);

	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (maxdist > -1){
        gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = maxdist };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    }

    PG_RETURN_NULL();
}


/**
 Minimum 2d distance between objects in geom1 and geom2 in 3D
 */
PG_FUNCTION_INFO_V1(ST_3DDistance);
PG_FUNCTION_INFO_V1(gtype_3DDistance);
Datum gtype_3DDistance(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
	GSERIALIZED *geom1 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	GSERIALIZED *geom2 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry"));
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);
	gserialized_error_if_srid_mismatch(geom1, geom2, __func__);

	double mindist = lwgeom_mindistance3d(lwgeom1, lwgeom2);

	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (mindist < FLT_MAX){
        gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = mindist };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    }

    PG_RETURN_NULL();
}

/**
Returns the point in first input geometry that is closest to the second input geometry in 3D
*/
PG_FUNCTION_INFO_V1(LWGEOM_closestpoint3d);
PG_FUNCTION_INFO_V1(gtype_closestpoint3d);
Datum gtype_closestpoint3d(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    bool is_null;


    Datum d = PostGraphDirectFunctionCall2(LWGEOM_closestpoint3d, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d)  };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
Returns the point in first input geometry that is closest to the second input geometry in 3D
*/
PG_FUNCTION_INFO_V1(LWGEOM_shortestline3d);
PG_FUNCTION_INFO_V1(gtype_shortestline3d);
Datum gtype_shortestline3d(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(LWGEOM_shortestline3d, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d)  };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/**
Returns the longest line between two geometries in 3D
*/
PG_FUNCTION_INFO_V1(LWGEOM_longestline3d);
PG_FUNCTION_INFO_V1(gtype_longestline3d);
Datum gtype_longestline3d(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum d1 = convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry");
    Datum d2 = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
    bool is_null;

    Datum d = PostGraphDirectFunctionCall2(LWGEOM_longestline3d, 100, &is_null, d1, d2);

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = DatumGetPointer(d)  };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


/** convert LWGEOM to wkt (in TEXT format) */
PG_FUNCTION_INFO_V1(gtype_asText);
Datum gtype_asText(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);

	int dbl_dig_for_wkt = OUT_DEFAULT_DECIMAL_DIGITS;
	if (PG_NARGS() > 1) {
        gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
        dbl_dig_for_wkt = DatumGetInt32(convert_to_scalar(gtype_to_int4_internal, gt_2, "int"));
    }

    lwvarlena_t *lwv = lwgeom_to_wkt_varlena(lwgeom, WKT_ISO, dbl_dig_for_wkt);
    gtype_value gtv = { .type = AGTV_STRING, .val.string = {.len = VARSIZE_ANY_EXHDR(lwv), .val = VARDATA(lwv)}  };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}



/** @brief
* 		returns 0 for points, 1 for lines, 2 for polygons, 3 for volume.
* 		returns max dimension for a collection.
*/
PG_FUNCTION_INFO_V1(gtype_dimension);
Datum gtype_dimension(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	int dimension = -1;

	dimension = lwgeom_dimension(lwgeom);
	lwgeom_free(lwgeom);

	if ( dimension < 0 )
	{
		elog(NOTICE, "Could not compute geometry dimensions");
		PG_RETURN_NULL();
	}

    gtype_value gtv = { .type = AGTV_INTEGER, .val.int_value = dimension };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/* Matches lwutil.c::lwgeomTypeName */
static char *stTypeName[] = {"Unknown",
			     "ST_Point",
			     "ST_LineString",
			     "ST_Polygon",
			     "ST_MultiPoint",
			     "ST_MultiLineString",
			     "ST_MultiPolygon",
			     "ST_GeometryCollection",
			     "ST_CircularString",
			     "ST_CompoundCurve",
			     "ST_CurvePolygon",
			     "ST_MultiCurve",
			     "ST_MultiSurface",
			     "ST_PolyhedralSurface",
			     "ST_Triangle",
			     "ST_Tin"};

/* returns a string representation of this geometry's type */
PG_FUNCTION_INFO_V1(gtype_geometrytype);
Datum gtype_geometrytype(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *gser = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));

	text *type_text;

	/* Build a text type to store things in */
	char *str = stTypeName[gserialized_get_type(gser)];

    gtype_value gtv = { .type = AGTV_STRING, .val.string = {.len = strlen(str), .val = str}  };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}



PG_FUNCTION_INFO_V1(gtype_numgeometries_collection);
Datum gtype_numgeometries_collection(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    GSERIALIZED *geom = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	LWGEOM *lwgeom;
	int32 ret = 0;

	lwgeom = lwgeom_from_gserialized(geom);
	if (lwgeom_is_empty(lwgeom))
	{
		ret = 0;
	}
	else if (lwgeom_is_unitary(lwgeom))
	{
		ret = 1;
	}
	else
	{
		LWCOLLECTION *col = lwgeom_as_lwcollection(lwgeom);
		ret = col->ngeoms;
	}
	lwgeom_free(lwgeom);

    gtype_value gtv = { .type = AGTV_INTEGER, .val.int_value = ret };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/** 1-based offset */
PG_FUNCTION_INFO_V1(gtype_geometryn_collection);
Datum gtype_geometryn_collection(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
	GSERIALIZED *geom =  DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	GSERIALIZED *result = NULL;
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);	
    int32 idx = DatumGetInt32(convert_to_scalar(gtype_to_int4_internal, gt_2, "int"));
	LWCOLLECTION *coll = NULL;
	LWGEOM *subgeom = NULL;

	/* Empty returns NULL */
	if (lwgeom_is_empty(lwgeom))
		PG_RETURN_NULL();

	/* Unitary geometries just reflect back */
	if (lwgeom_is_unitary(lwgeom))
	{
		if ( idx == 1 )
			PG_RETURN_POINTER(gt_1);
		else
			PG_RETURN_NULL();
	}

	coll = lwgeom_as_lwcollection(lwgeom);
	if (!coll)
		elog(ERROR, "Unable to handle type %d in ST_GeometryN", lwgeom->type);

	/* Handle out-of-range index value */
	idx -= 1;
	if ( idx < 0 || idx >= (int32) coll->ngeoms )
		PG_RETURN_NULL();

	subgeom = coll->geoms[idx];
	subgeom->srid = coll->srid;

	/* COMPUTE_BBOX==TAINTING */
	if ( coll->bbox ) lwgeom_add_bbox(subgeom);

	result = geometry_serialize(subgeom);
	lwgeom_free(lwgeom);


    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = result };
	AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/** Reverse vertex order of geometry */
PG_FUNCTION_INFO_V1(gtype_st_reverse);
Datum gtype_st_reverse(PG_FUNCTION_ARGS)
{
	gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
	GSERIALIZED *geom =  PG_DETOAST_DATUM_COPY(DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry")));
	LWGEOM *lwgeom;

	POSTGIS_DEBUG(2, "LWGEOM_reverse called");

	lwgeom = lwgeom_from_gserialized(geom);
	lwgeom_reverse_in_place(lwgeom);

	geom = geometry_serialize(lwgeom);

	//PG_RETURN_POINTER(geom);
    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geom };
	AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/*
 * Error message parsing functions
 *
 * Produces nicely formatted messages for parser/unparser errors with optional HINT
 */

void
pg_parser_errhint(LWGEOM_PARSER_RESULT *lwg_parser_result)
{
	char *hintbuffer;

	/* Only display the parser position if the location is > 0; this provides a nicer output when the first token
	   within the input stream cannot be matched */
	if (lwg_parser_result->errlocation > 0)
	{
		/* Return a copy of the input string start truncated
		 * at the error location */
		hintbuffer = lwmessage_truncate(
			(char *)lwg_parser_result->wkinput, 0,
			lwg_parser_result->errlocation - 1, 40, 0);

		ereport(ERROR,
		        (errmsg("%s", lwg_parser_result->message),
		         errhint("\"%s\" <-- parse error at position %d within geometry", hintbuffer, lwg_parser_result->errlocation))
		       );
	}
	else
	{
		ereport(ERROR,
		        (errmsg("%s", lwg_parser_result->message),
		         errhint("You must specify a valid OGC WKT geometry type such as POINT, LINESTRING or POLYGON"))
		       );
	}
}
/**
 * @brief Returns a geometry Given an OGC WKT (and optionally a SRID)
 * @return a geometry.
 * @note Note that this is a a stricter version
 * 		of geometry_in, where we refuse to
 * 		accept (HEX)WKB or EWKT.
 */
PG_FUNCTION_INFO_V1(gtype_from_text);
Datum gtype_from_text(PG_FUNCTION_ARGS)
{
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    text *wkttext = DatumGetTextP(convert_to_scalar(gtype_to_text_internal, gt_1, "text"));
	char *wkt = text_to_cstring(wkttext);
	LWGEOM_PARSER_RESULT lwg_parser_result;
	GSERIALIZED *geom_result = NULL;
	LWGEOM *lwgeom;

	POSTGIS_DEBUG(2, "LWGEOM_from_text");
	POSTGIS_DEBUGF(3, "wkt: [%s]", wkt);

	if (lwgeom_parse_wkt(&lwg_parser_result, wkt, LW_PARSER_CHECK_ALL) == LW_FAILURE )
		PG_PARSER_ERROR(lwg_parser_result);

	lwgeom = lwg_parser_result.geom;

	if ( lwgeom->srid != SRID_UNKNOWN )
	{
		elog(WARNING, "OGC WKT expected, EWKT provided - use GeomFromEWKT() for this");
	}

	/* read user-requested SRID if any */
	if ( PG_NARGS() > 1 )
		lwgeom_set_srid(lwgeom, PG_GETARG_INT32(1));

	geom_result = geometry_serialize(lwgeom);
	lwgeom_parser_result_free(&lwg_parser_result);

    gtype_value gtv = { .type = AGTV_GSERIALIZED, .val.gserialized = geom_result };
	AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}
