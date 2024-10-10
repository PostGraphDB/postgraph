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
#include "libpgcommon/gserialized_gist.h"
#include "libpgcommon/lwgeom_pg.h"
#include "liblwgeom/liblwgeom_internal.h"

//PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"

/* When is a node split not so good? Prefer to not split 2 pages into more than 3 pages in index. */
#define LIMIT_RATIO 0.3333333333333333


#define FLAGS_NDIMS_GIDX(f) ( FLAGS_GET_GEODETIC(f) ? 3 : \
                              FLAGS_GET_M(f) ? 4 : \
                              FLAGS_GET_Z(f) ? 3 : 2 )



/*
** Function to pack floats of different realms.
** This function serves to pack bit flags inside float type.
** Result value represent can be from two different "realms".
** Every value from realm 1 is greater than any value from realm 0.
** Values from the same realm loose one bit of precision.
**
** This technique is possible due to floating point numbers specification
** according to IEEE 754: exponent bits are highest
** (excluding sign bits, but here penalty is always positive). If float a is
** greater than float b, integer A with same bit representation as a is greater
** than integer B with same bits as b.
*/
static inline float
pack_float(const float value, const uint8_t realm)
{
	union {
		float f;
		struct {
			unsigned value : 31, sign : 1;
		} vbits;
		struct {
			unsigned value : 30, realm : 1, sign : 1;
		} rbits;
	} a;

	a.f = value;
	a.rbits.value = a.vbits.value >> 1;
	a.rbits.realm = realm;

	return a.f;
}

/* Convert a double-based GBOX into a float-based GIDX,
   ensuring the float box is larger than the double box */
static int gidx_from_gbox_p(GBOX box, GIDX *a)
{
	int ndims;

	ndims = FLAGS_NDIMS_GIDX(box.flags);
	SET_VARSIZE(a, VARHDRSZ + ndims * 2 * sizeof(float));

	GIDX_SET_MIN(a,0,next_float_down(box.xmin));
	GIDX_SET_MAX(a,0,next_float_up(box.xmax));
	GIDX_SET_MIN(a,1,next_float_down(box.ymin));
	GIDX_SET_MAX(a,1,next_float_up(box.ymax));

	/* Geodetic indexes are always 3d, geocentric x/y/z */
	if ( FLAGS_GET_GEODETIC(box.flags) )
	{
		GIDX_SET_MIN(a,2,next_float_down(box.zmin));
		GIDX_SET_MAX(a,2,next_float_up(box.zmax));
	}
	else
	{
		/* Cartesian with Z implies Z is third dimension */
		if ( FLAGS_GET_Z(box.flags) )
		{
			GIDX_SET_MIN(a,2,next_float_down(box.zmin));
			GIDX_SET_MAX(a,2,next_float_up(box.zmax));
		}
		/* M is always fourth dimension, we pad if needed */
		if ( FLAGS_GET_M(box.flags) )
		{
			if ( ! FLAGS_GET_Z(box.flags) )
			{
				GIDX_SET_MIN(a,2,-1*FLT_MAX);
				GIDX_SET_MAX(a,2,FLT_MAX);
			}
			GIDX_SET_MIN(a,3,next_float_down(box.mmin));
			GIDX_SET_MAX(a,3,next_float_up(box.mmax));
		}
	}

	POSTGIS_DEBUGF(5, "converted %s to %s", gbox_to_string(&box), gidx_to_string(a));

	return LW_SUCCESS;
}

/*
** GiST support function. Called from gserialized_gist_consistent below.
*/
static inline bool
gserialized_gist_consistent_leaf(GIDX *key, GIDX *query, StrategyNumber strategy)
{
	bool retval;

	POSTGIS_DEBUGF(4, "[GIST] leaf consistent, strategy [%d], count[%i]", strategy, geog_counter_leaf++);

	switch (strategy)
	{
	case RTOverlapStrategyNumber:
		retval = (bool)gidx_overlaps(key, query);
		break;
	case RTSameStrategyNumber:
		retval = (bool)gidx_equals(key, query);
		break;
	case RTContainsStrategyNumber:
	case RTOldContainsStrategyNumber:
		retval = (bool)gidx_contains(key, query);
		break;
	case RTContainedByStrategyNumber:
	case RTOldContainedByStrategyNumber:
		retval = (bool)gidx_contains(query, key);
		break;
	default:
		retval = false;
	}

	return retval;
}

/* Allocates a new GIDX on the heap of the requested dimensionality */
GIDX* gidx_new(int ndims)
{
	size_t size = GIDX_SIZE(ndims);
	GIDX *g = (GIDX*)palloc(size);
	Assert( (ndims <= GIDX_MAX_DIM) && (size <= GIDX_MAX_SIZE) );
	POSTGIS_DEBUGF(5,"created new gidx of %d dimensions, size %d", ndims, (int)size);
	SET_VARSIZE(g, size);
	return g;
}

/**
* Peak into a #GSERIALIZED datum to find the bounding box. If the
* box is there, copy it out and return it. If not, calculate the box from the
* full object and return the box based on that. If no box is available,
* return #LW_FAILURE, otherwise #LW_SUCCESS.
*/
int
gserialized_datum_get_gidx_p(Datum gsdatum, GIDX *gidx)
{
	GSERIALIZED *gpart = NULL;
	int need_detoast = PG_GSERIALIZED_DATUM_NEEDS_DETOAST((struct varlena *)gsdatum);
	if (need_detoast)
	{
		gpart = (GSERIALIZED *)PG_DETOAST_DATUM_SLICE(gsdatum, 0, gserialized_max_header_size());
	}
	else
	{
		gpart = (GSERIALIZED *)gsdatum;
	}

	/* Do we even have a serialized bounding box? */
	if (gserialized_has_bbox(gpart))
	{
		/* Yes! Copy it out into the GIDX! */
		lwflags_t lwflags = gserialized_get_lwflags(gpart);
		size_t size = gbox_serialized_size(lwflags);
		size_t ndims, dim;
		const float *f = gserialized_get_float_box_p(gpart, &ndims);
		if (!f) return LW_FAILURE;
		for (dim = 0; dim < ndims; dim++)
		{
			GIDX_SET_MIN(gidx, dim, f[2*dim]);
			GIDX_SET_MAX(gidx, dim, f[2*dim+1]);
		}
		/* if M is present but Z is not, pad Z and shift M */
		if (gserialized_has_m(gpart) && ! gserialized_has_z(gpart))
		{
			size += 2 * sizeof(float);
			GIDX_SET_MIN(gidx,3,GIDX_GET_MIN(gidx,2));
			GIDX_SET_MAX(gidx,3,GIDX_GET_MAX(gidx,2));
			GIDX_SET_MIN(gidx,2,-1*FLT_MAX);
			GIDX_SET_MAX(gidx,2,FLT_MAX);
		}
		SET_VARSIZE(gidx, VARHDRSZ + size);
	}
	else
	{
		/* No, we need to calculate it from the full object. */
		LWGEOM *lwgeom;
		GBOX gbox;
		if (need_detoast && LWSIZE_GET(gpart->size) >= gserialized_max_header_size())
		{
			/* If we haven't, read the whole gserialized object */
			POSTGIS_FREE_IF_COPY_P(gpart, gsdatum);
			gpart = (GSERIALIZED *)PG_DETOAST_DATUM(gsdatum);
		}

		lwgeom = lwgeom_from_gserialized(gpart);
		if (lwgeom_calculate_gbox(lwgeom, &gbox) == LW_FAILURE)
		{
			POSTGIS_DEBUG(4, "could not calculate bbox, returning failure");
			lwgeom_free(lwgeom);
			POSTGIS_FREE_IF_COPY_P(gpart, gsdatum);
			return LW_FAILURE;
		}
		lwgeom_free(lwgeom);
		gidx_from_gbox_p(gbox, gidx);
	}
	POSTGIS_FREE_IF_COPY_P(gpart, gsdatum);
	POSTGIS_DEBUGF(4, "got gidx %s", gidx_to_string(gidx));
	return LW_SUCCESS;
}



/*
** GIDX true/false test function type
*/
typedef bool (*gidx_predicate)(GIDX *a, GIDX *b);

PG_FUNCTION_INFO_V1(gserialized_gist_penalty);

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



/* Calculate the volume (in n-d units) of the GIDX */
static float
gidx_volume(GIDX *a)
{
	float result;
	uint32_t i;
	if (!a || gidx_is_unknown(a))
		return 0.0;
	result = GIDX_GET_MAX(a, 0) - GIDX_GET_MIN(a, 0);
	for (i = 1; i < GIDX_NDIMS(a); i++)
		result *= (GIDX_GET_MAX(a, i) - GIDX_GET_MIN(a, i));
	POSTGIS_DEBUGF(5, "calculated volume of %s as %.8g", gidx_to_string(a), result);
	return result;
}

/* Calculate the edge of the GIDX */
static float
gidx_edge(GIDX *a)
{
	float result;
	uint32_t i;
	if (!a || gidx_is_unknown(a))
		return 0.0;
	result = GIDX_GET_MAX(a, 0) - GIDX_GET_MIN(a, 0);
	for (i = 1; i < GIDX_NDIMS(a); i++)
		result += (GIDX_GET_MAX(a, i) - GIDX_GET_MIN(a, i));
	POSTGIS_DEBUGF(5, "calculated edge of %s as %.8g", gidx_to_string(a), result);
	return result;
}

/* Ensure the first argument has the higher dimensionality. */
static void
gidx_dimensionality_check(GIDX **a, GIDX **b)
{
	if (GIDX_NDIMS(*a) < GIDX_NDIMS(*b))
	{
		GIDX *tmp = *b;
		*b = *a;
		*a = tmp;
	}
}

/* Calculate the volume of the union of the boxes. Avoids creating an intermediate box. */
static float
gidx_union_volume(GIDX *a, GIDX *b)
{
	float result;
	int i;
	int ndims_a, ndims_b;

	POSTGIS_DEBUG(5, "entered function");

	if (!a && !b)
	{
		elog(ERROR, "gidx_union_volume received two null arguments");
		return 0.0;
	}

	if (!a || gidx_is_unknown(a))
		return gidx_volume(b);

	if (!b || gidx_is_unknown(b))
		return gidx_volume(a);

	if (gidx_is_unknown(a) && gidx_is_unknown(b))
		return 0.0;

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	ndims_a = GIDX_NDIMS(a);
	ndims_b = GIDX_NDIMS(b);

	/* Initialize with maximal length of first dimension. */
	result = Max(GIDX_GET_MAX(a, 0), GIDX_GET_MAX(b, 0)) - Min(GIDX_GET_MIN(a, 0), GIDX_GET_MIN(b, 0));

	/* Multiply by maximal length of remaining dimensions. */
	for (i = 1; i < ndims_b; i++)
		result *= (Max(GIDX_GET_MAX(a, i), GIDX_GET_MAX(b, i)) - Min(GIDX_GET_MIN(a, i), GIDX_GET_MIN(b, i)));

	/* Add in dimensions of higher dimensional box. */
	for (i = ndims_b; i < ndims_a; i++)
		result *= (GIDX_GET_MAX(a, i) - GIDX_GET_MIN(a, i));

	POSTGIS_DEBUGF(5, "volume( %s union %s ) = %.12g", gidx_to_string(a), gidx_to_string(b), result);

	return result;
}

/* Calculate the edge of the union of the boxes. Avoids creating an intermediate box. */
static float
gidx_union_edge(GIDX *a, GIDX *b)
{
	float result;
	int i;
	int ndims_a, ndims_b;

	POSTGIS_DEBUG(5, "entered function");

	if (!a && !b)
	{
		elog(ERROR, "gidx_union_edge received two null arguments");
		return 0.0;
	}

	if (!a || gidx_is_unknown(a))
		return gidx_volume(b);

	if (!b || gidx_is_unknown(b))
		return gidx_volume(a);

	if (gidx_is_unknown(a) && gidx_is_unknown(b))
		return 0.0;

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	ndims_a = GIDX_NDIMS(a);
	ndims_b = GIDX_NDIMS(b);

	/* Initialize with maximal length of first dimension. */
	result = Max(GIDX_GET_MAX(a, 0), GIDX_GET_MAX(b, 0)) - Min(GIDX_GET_MIN(a, 0), GIDX_GET_MIN(b, 0));

	/* Add maximal length of remaining dimensions. */
	for (i = 1; i < ndims_b; i++)
		result += (Max(GIDX_GET_MAX(a, i), GIDX_GET_MAX(b, i)) - Min(GIDX_GET_MIN(a, i), GIDX_GET_MIN(b, i)));

	/* Add in dimensions of higher dimensional box. */
	for (i = ndims_b; i < ndims_a; i++)
		result += (GIDX_GET_MAX(a, i) - GIDX_GET_MIN(a, i));

	POSTGIS_DEBUGF(5, "edge( %s union %s ) = %.12g", gidx_to_string(a), gidx_to_string(b), result);

	return result;
}

/* Calculate the volume of the intersection of the boxes. */
static float
gidx_inter_volume(GIDX *a, GIDX *b)
{
	uint32_t i;
	float result;

	POSTGIS_DEBUG(5, "entered function");

	if (!a || !b)
	{
		elog(ERROR, "gidx_inter_volume received a null argument");
		return 0.0;
	}

	if (gidx_is_unknown(a) || gidx_is_unknown(b))
		return 0.0;

	/* Ensure 'a' has the most dimensions. */
	gidx_dimensionality_check(&a, &b);

	/* Initialize with minimal length of first dimension. */
	result = Min(GIDX_GET_MAX(a, 0), GIDX_GET_MAX(b, 0)) - Max(GIDX_GET_MIN(a, 0), GIDX_GET_MIN(b, 0));

	/* If they are disjoint (max < min) then return zero. */
	if (result < 0.0)
		return 0.0;

	/* Continue for remaining dimensions. */
	for (i = 1; i < GIDX_NDIMS(b); i++)
	{
		float width = Min(GIDX_GET_MAX(a, i), GIDX_GET_MAX(b, i)) - Max(GIDX_GET_MIN(a, i), GIDX_GET_MIN(b, i));
		if (width < 0.0)
			return 0.0;
		/* Multiply by minimal length of remaining dimensions. */
		result *= width;
	}
	POSTGIS_DEBUGF(5, "volume( %s intersection %s ) = %.12g", gidx_to_string(a), gidx_to_string(b), result);
	return result;
}

/*
** Overlapping GIDX box test.
**
** Box(A) Overlaps Box(B) IFF for every dimension d:
**   min(A,d) <= max(B,d) && max(A,d) => min(B,d)
**
** Any missing dimension is assumed by convention to
** overlap whatever finite range available on the
** other operand. See
** http://lists.osgeo.org/pipermail/postgis-devel/2015-February/024757.html
**
** Empty boxes never overlap.
*/
bool
gidx_overlaps(GIDX *a, GIDX *b)
{
	int i, dims_a, dims_b;

	POSTGIS_DEBUG(5, "entered function");

	if (!a || !b)
		return false;

	if (gidx_is_unknown(a) || gidx_is_unknown(b))
		return false;

	dims_a = GIDX_NDIMS(a);
	dims_b = GIDX_NDIMS(b);

	/* For all shared dimensions min(a) > max(b) and min(b) > max(a)
	   Unshared dimensions do not matter */
	for (i = 0; i < Min(dims_a, dims_b); i++)
	{
		/* If the missing dimension was not padded with -+FLT_MAX */
		if (GIDX_GET_MAX(a, i) != FLT_MAX && GIDX_GET_MAX(b, i) != FLT_MAX)
		{
			if (GIDX_GET_MIN(a, i) > GIDX_GET_MAX(b, i))
				return false;
			if (GIDX_GET_MIN(b, i) > GIDX_GET_MAX(a, i))
				return false;
		}
	}

	return true;
}

/*
** Containment GIDX test.
**
** Box(A) CONTAINS Box(B) IFF (pt(A)LL < pt(B)LL) && (pt(A)UR > pt(B)UR)
*/
bool
gidx_contains(GIDX *a, GIDX *b)
{
	uint32_t i, dims_a, dims_b;

	if (!a || !b)
		return false;

	if (gidx_is_unknown(a) || gidx_is_unknown(b))
		return false;

	dims_a = GIDX_NDIMS(a);
	dims_b = GIDX_NDIMS(b);

	/* For all shared dimensions min(a) > min(b) and max(a) < max(b)
	   Unshared dimensions do not matter */
	for (i = 0; i < Min(dims_a, dims_b); i++)
	{
		/* If the missing dimension was not padded with -+FLT_MAX */
		if (GIDX_GET_MAX(a, i) != FLT_MAX && GIDX_GET_MAX(b, i) != FLT_MAX)
		{
			if (GIDX_GET_MIN(a, i) > GIDX_GET_MIN(b, i))
				return false;
			if (GIDX_GET_MAX(a, i) < GIDX_GET_MAX(b, i))
				return false;
		}
	}

	return true;
}

/*
** Equality GIDX test.
**
** Box(A) EQUALS Box(B) IFF (pt(A)LL == pt(B)LL) && (pt(A)UR == pt(B)UR)
*/
bool
gidx_equals(GIDX *a, GIDX *b)
{
	uint32_t i, dims_a, dims_b;

	if (!a && !b)
		return true;
	if (!a || !b)
		return false;

	if (gidx_is_unknown(a) && gidx_is_unknown(b))
		return true;

	if (gidx_is_unknown(a) || gidx_is_unknown(b))
		return false;

	dims_a = GIDX_NDIMS(a);
	dims_b = GIDX_NDIMS(b);

	/* For all shared dimensions min(a) == min(b), max(a) == max(b)
	   Unshared dimensions do not matter */
	for (i = 0; i < Min(dims_a, dims_b); i++)
	{
		/* If the missing dimension was not padded with -+FLT_MAX */
		if (GIDX_GET_MAX(a, i) != FLT_MAX && GIDX_GET_MAX(b, i) != FLT_MAX)
		{
			if (GIDX_GET_MIN(a, i) != GIDX_GET_MIN(b, i))
				return false;
			if (GIDX_GET_MAX(a, i) != GIDX_GET_MAX(b, i))
				return false;
		}
	}
	return true;
}

/**
 * Support function. Based on two datums return true if
 * they satisfy the predicate and false otherwise.
 */
static int
gserialized_datum_predicate(Datum gs1, Datum gs2, gidx_predicate predicate)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char boxmem1[GIDX_MAX_SIZE];
	char boxmem2[GIDX_MAX_SIZE];
	GIDX *gidx1 = (GIDX *)boxmem1;
	GIDX *gidx2 = (GIDX *)boxmem2;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for each argument (ie, not empty geometry)
	   and predicate function to return true. */
	if ((gserialized_datum_get_gidx_p(gs1, gidx1) == LW_SUCCESS) &&
	    (gserialized_datum_get_gidx_p(gs2, gidx2) == LW_SUCCESS) && predicate(gidx1, gidx2))
	{
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(gidx1), gidx_to_string(gidx2));
		return LW_TRUE;
	}
	return LW_FALSE;
}

static int
gserialized_datum_predicate_gidx_geom(GIDX *gidx1, Datum gs2, gidx_predicate predicate)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char boxmem2[GIDX_MAX_SIZE];
	GIDX *gidx2 = (GIDX *)boxmem2;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for gs2 argument (ie, not empty geometry)
	   and predicate function to return true. */
	if ((gserialized_datum_get_gidx_p(gs2, gidx2) == LW_SUCCESS) && predicate(gidx1, gidx2))
	{
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(gidx1), gidx_to_string(gidx2));
		return LW_TRUE;
	}
	return LW_FALSE;
}

static int
gserialized_datum_predicate_geom_gidx(Datum gs1, GIDX *gidx2, gidx_predicate predicate)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char boxmem2[GIDX_MAX_SIZE];
	GIDX *gidx1 = (GIDX *)boxmem2;

	POSTGIS_DEBUG(3, "entered function");

	/* Must be able to build box for gs2 argument (ie, not empty geometry)
	   and predicate function to return true. */
	if ((gserialized_datum_get_gidx_p(gs1, gidx1) == LW_SUCCESS) && predicate(gidx1, gidx2))
	{
		POSTGIS_DEBUGF(3, "got boxes %s and %s", gidx_to_string(gidx1), gidx_to_string(gidx2));
		return LW_TRUE;
	}
	return LW_FALSE;
}

/**
 * Calculate the box->box distance.
 */
static double
gidx_distance(const GIDX *a, const GIDX *b, int m_is_time)
{
	int ndims, i;
	double sum = 0;

	/* Base computation on least available dimensions */
	ndims = Min(GIDX_NDIMS(b), GIDX_NDIMS(a));
	for (i = 0; i < ndims; ++i)
	{
		double d;
		double amin = GIDX_GET_MIN(a, i);
		double amax = GIDX_GET_MAX(a, i);
		double bmin = GIDX_GET_MIN(b, i);
		double bmax = GIDX_GET_MAX(b, i);
		POSTGIS_DEBUGF(3, "A %g - %g", amin, amax);
		POSTGIS_DEBUGF(3, "B %g - %g", bmin, bmax);

		if ((amin <= bmax && amax >= bmin))
		{
			/* overlaps */
			d = 0;
		}
		else if (i == 4 && m_is_time)
		{
			return FLT_MAX;
		}
		else if (bmax < amin)
		{
			/* is "left" */
			d = amin - bmax;
		}
		else
		{
			/* is "right" */
			assert(bmin > amax);
			d = bmin - amax;
		}
		if (!isfinite(d))
		{
			/* Can happen if coordinates are corrupted/NaN */
			continue;
		}
		sum += d * d;
		POSTGIS_DEBUGF(3, "dist %g, squared %g, grows sum to %g", d, d * d, sum);
	}
	return sqrt(sum);
}



/***********************************************************************
 * GiST N-D Index Operator Functions
 */

/*
 * do "real" n-d distance
 */
PG_FUNCTION_INFO_V1(gtype_distance_nd);
Datum gtype_distance_nd(PG_FUNCTION_ARGS)
{
	/* Feature-to-feature distance */
    gtype *gt_1 = AG_GET_ARG_GTYPE_P(0);
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    GSERIALIZED *geom1 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_1, "geometry"));
    GSERIALIZED *geom2 = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry"));
	LWGEOM *lw1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lw2 = lwgeom_from_gserialized(geom2);
	LWGEOM *closest;
	double distance;

	/* Find an exact shortest line w/ the dimensions we support */
	if (lwgeom_has_z(lw1) && lwgeom_has_z(lw2))
	{
		closest = lwgeom_closest_line_3d(lw1, lw2);
		distance = lwgeom_length(closest);
	}
	else
	{
		closest = lwgeom_closest_line(lw1, lw2);
		distance = lwgeom_length_2d(closest);
	}

	/* Can only add the M term if both objects have M */
	if (lwgeom_has_m(lw1) && lwgeom_has_m(lw2))
	{
		double m1 = 0, m2 = 0;
		int usebox = false;
		/* Un-sqrt the distance so we can add extra terms */
		distance = distance * distance;

		if (lwgeom_get_type(lw1) == POINTTYPE)
		{
			POINT4D p;
			lwpoint_getPoint4d_p((LWPOINT *)lw1, &p);
			m1 = p.m;
		}
		else if (lwgeom_get_type(lw1) == LINETYPE)
		{
			LWPOINT *lwp1 = lwline_get_lwpoint(lwgeom_as_lwline(closest), 0);
			m1 = lwgeom_interpolate_point(lw1, lwp1);
			lwpoint_free(lwp1);
		}
		else
			usebox = true;

		if (lwgeom_get_type(lw2) == POINTTYPE)
		{
			POINT4D p;
			lwpoint_getPoint4d_p((LWPOINT *)lw2, &p);
			m2 = p.m;
		}
		else if (lwgeom_get_type(lw2) == LINETYPE)
		{
			LWPOINT *lwp2 = lwline_get_lwpoint(lwgeom_as_lwline(closest), 1);
			m2 = lwgeom_interpolate_point(lw2, lwp2);
			lwpoint_free(lwp2);
		}
		else
			usebox = true;

		if (usebox)
		{
			GBOX b1, b2;
			if (gserialized_get_gbox_p(geom1, &b1) && gserialized_get_gbox_p(geom2, &b2))
			{
				double d;
				/* Disjoint left */
				if (b1.mmin > b2.mmax)
					d = b1.mmin - b2.mmax;
				/* Disjoint right */
				else if (b2.mmin > b1.mmax)
					d = b2.mmin - b1.mmax;
				/* Not Disjoint */
				else
					d = 0;
				distance += d * d;
			}
		}
		else
			distance += (m2 - m1) * (m2 - m1);

		distance = sqrt(distance);
	}

	lwgeom_free(closest);

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = distance };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}



/*
** GiST support function.
** Take in a query and an entry and return the "distance" between them.
**
** Given an index entry p and a query value q, this function determines the
** index entry's "distance" from the query value. This function must be
** supplied if the operator class contains any ordering operators. A query
** using the ordering operator will be implemented by returning index entries
** with the smallest "distance" values first, so the results must be consistent
** with the operator's semantics. For a leaf index entry the result just
** represents the distance to the index entry; for an internal tree node, the
** result must be the smallest distance that any child entry could have.
**
** Strategy 13 is centroid-based distance tests <<->>
** Strategy 20 is cpa based distance tests |=|
*/
PG_FUNCTION_INFO_V1(gtype_gist_distance);
Datum gtype_gist_distance(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
	StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
	char query_box_mem[GIDX_MAX_SIZE];
	GIDX *query_box = (GIDX *)query_box_mem;
	GIDX *entry_box;
	bool *recheck = (bool *)PG_GETARG_POINTER(4);

	double distance;

	POSTGIS_DEBUG(4, "[GIST] 'distance' function called");

	/* Strategy 13 is <<->> */
	/* Strategy 20 is |=| */
	if (strategy != 13 && strategy != 20)
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		PG_RETURN_FLOAT8(FLT_MAX);
	}

	/* Null box should never make this far. */
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum geom = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
	if (gserialized_datum_get_gidx_p(geom, query_box) == LW_FAILURE)
	{
		POSTGIS_DEBUG(4, "[GIST] null query_gbox_index!");
		PG_RETURN_FLOAT8(FLT_MAX);
	}

	/* Get the entry box */
	entry_box = (GIDX *)PG_DETOAST_DATUM(entry->key);

	/* Strategy 20 is |=| */
	distance = gidx_distance(entry_box, query_box, strategy == 20);

	/* Treat leaf node tests different from internal nodes */
	if (GIST_LEAF(entry))
		*recheck = true;


    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = distance };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

	//PG_RETURN_FLOAT8(distance);
}

/*
** GiST support function. Called from gserialized_gist_consistent below.
*/
static inline bool
gserialized_gist_consistent_internal(GIDX *key, GIDX *query, StrategyNumber strategy)
{
	bool retval;

	POSTGIS_DEBUGF(4,
		       "[GIST] internal consistent, strategy [%d], count[%i], query[%s], key[%s]",
		       strategy,
		       geog_counter_internal++,
		       gidx_to_string(query),
		       gidx_to_string(key));

	switch (strategy)
	{
	case RTOverlapStrategyNumber:
	case RTContainedByStrategyNumber:
	case RTOldContainedByStrategyNumber:
		retval = (bool)gidx_overlaps(key, query);
		break;
	case RTSameStrategyNumber:
	case RTContainsStrategyNumber:
	case RTOldContainsStrategyNumber:
		retval = (bool)gidx_contains(key, query);
		break;
	default:
		retval = false;
	}

	return retval;
}

/*
** GiST support function. Take in a query and an entry and see what the
** relationship is, based on the query strategy.
*/
PG_FUNCTION_INFO_V1(gtype_gist_consistent);
Datum gtype_gist_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
	StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
	bool result;
	char gidxmem[GIDX_MAX_SIZE];
	GIDX *query_gbox_index = (GIDX *)gidxmem;

	/* PostgreSQL 8.4 and later require the RECHECK flag to be set here,
	   rather than being supplied as part of the operator class definition */
	bool *recheck = (bool *)PG_GETARG_POINTER(4);

	/* We set recheck to false to avoid repeatedly pulling every "possibly matched" geometry
	   out during index scans. For cases when the geometries are large, rechecking
	   can make things twice as slow. */
	*recheck = false;

	POSTGIS_DEBUG(4, "[GIST] 'consistent' function called");

	/* Quick sanity check on query argument. */
	if (!DatumGetPointer(PG_GETARG_DATUM(1)))
	{
		POSTGIS_DEBUG(4, "[GIST] null query pointer (!?!), returning false");
		PG_RETURN_BOOL(false); /* NULL query! This is screwy! */
	}

	/* Quick sanity check on entry key. */
	if (!DatumGetPointer(entry->key))
	{
		POSTGIS_DEBUG(4, "[GIST] null index entry, returning false");
		PG_RETURN_BOOL(false); /* NULL entry! */
	}

	/* Null box should never make this far. */
    gtype *gt_2 = AG_GET_ARG_GTYPE_P(1);
    Datum geom = convert_to_scalar(gtype_to_geometry_internal, gt_2, "geometry");
	if (gserialized_datum_get_gidx_p(geom, query_gbox_index) == LW_FAILURE)
	{
		POSTGIS_DEBUG(4, "[GIST] null query_gbox_index!");
		PG_RETURN_BOOL(false);
	}

	/* Treat leaf node tests different from internal nodes */
	if (GIST_LEAF(entry))
	{
		result =
		    gserialized_gist_consistent_leaf((GIDX *)PG_DETOAST_DATUM(entry->key), query_gbox_index, strategy);
	}
	else
	{
		result = gserialized_gist_consistent_internal(
		    (GIDX *)PG_DETOAST_DATUM(entry->key), query_gbox_index, strategy);
	}

	PG_RETURN_BOOL(result);
}

/***********************************************************************
 * GiST Index  Support Functions
 */

/*
** GiST support function. Given a geography, return a "compressed"
** version. In this case, we convert the geography into a geocentric
** bounding box. If the geography already has the box embedded in it
** we pull that out and hand it back.
*/
PG_FUNCTION_INFO_V1(gtype_gist_compress);
Datum gtype_gist_compress(PG_FUNCTION_ARGS)
{
	GISTENTRY *entry_in = (GISTENTRY *)PG_GETARG_POINTER(0);
	GISTENTRY *entry_out = NULL;
	char gidxmem[GIDX_MAX_SIZE];
	GIDX *bbox_out = (GIDX *)gidxmem;
	int result = LW_SUCCESS;
	uint32_t i;

	POSTGIS_DEBUG(4, "[GIST] 'compress' function called");

	/*
	** Not a leaf key? There's nothing to do.
	** Return the input unchanged.
	*/
	if (!entry_in->leafkey)
	{
		POSTGIS_DEBUG(4, "[GIST] non-leafkey entry, returning input unaltered");
		PG_RETURN_POINTER(entry_in);
	}

	POSTGIS_DEBUG(4, "[GIST] processing leafkey input");
	entry_out = palloc(sizeof(GISTENTRY));

	/*
	** Null key? Make a copy of the input entry and
	** return.
	*/
	if (!DatumGetPointer(entry_in->key))
	{
		POSTGIS_DEBUG(4, "[GIST] leafkey is null");
		gistentryinit(*entry_out, (Datum)0, entry_in->rel, entry_in->page, entry_in->offset, false);
		POSTGIS_DEBUG(4, "[GIST] returning copy of input");
		PG_RETURN_POINTER(entry_out);
	}

	/* Extract our index key from the GiST entry. */
	result = gserialized_datum_get_gidx_p(convert_to_scalar(gtype_to_geometry_internal, entry_in->key, "geometry"), bbox_out);

	/* Is the bounding box valid (non-empty, non-infinite) ?
	 * If not, use the "unknown" GIDX. */
	if (result == LW_FAILURE)
	{
		POSTGIS_DEBUG(4, "[GIST] empty geometry!");
		gidx_set_unknown(bbox_out);
		gistentryinit(*entry_out,
			      PointerGetDatum(gidx_copy(bbox_out)),
			      entry_in->rel,
			      entry_in->page,
			      entry_in->offset,
			      false);
		PG_RETURN_POINTER(entry_out);
	}

	POSTGIS_DEBUGF(4, "[GIST] got entry_in->key: %s", gidx_to_string(bbox_out));

	/* ORIGINAL VERSION */
	/* Check all the dimensions for finite values.
	 * If not, use the "unknown" GIDX as a key */
	for (i = 0; i < GIDX_NDIMS(bbox_out); i++)
	{
		if (!isfinite(GIDX_GET_MAX(bbox_out, i)) || !isfinite(GIDX_GET_MIN(bbox_out, i)))
		{
			gidx_set_unknown(bbox_out);
			gistentryinit(*entry_out,
				      PointerGetDatum(gidx_copy(bbox_out)),
				      entry_in->rel,
				      entry_in->page,
				      entry_in->offset,
				      false);
			PG_RETURN_POINTER(entry_out);
		}
	}

	/* Ensure bounding box has minimums below maximums. */
	gidx_validate(bbox_out);

	/* Prepare GISTENTRY for return. */
	gistentryinit(
	    *entry_out, PointerGetDatum(gidx_copy(bbox_out)), entry_in->rel, entry_in->page, entry_in->offset, false);

	/* Return GISTENTRY. */
	POSTGIS_DEBUG(4, "[GIST] 'compress' function complete");
	PG_RETURN_POINTER(entry_out);
}


/*
** GiST support function. Calculate the "penalty" cost of adding this entry into an existing entry.
** Calculate the change in volume of the old entry once the new entry is added.
*/
PG_FUNCTION_INFO_V1(gtype_gist_penalty);
Datum gtype_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY *origentry = (GISTENTRY *)PG_GETARG_POINTER(0);
	GISTENTRY *newentry = (GISTENTRY *)PG_GETARG_POINTER(1);
	float *result = (float *)PG_GETARG_POINTER(2);
	GIDX *gbox_index_orig, *gbox_index_new;

	gbox_index_orig = (GIDX *)DatumGetPointer(origentry->key);
	gbox_index_new = (GIDX *)DatumGetPointer(newentry->key);

	/* Penalty value of 0 has special code path in Postgres's gistchoose.
	 * It is used as an early exit condition in subtree loop, allowing faster tree descend.
	 * For multi-column index, it lets next column break the tie, possibly more confidently.
	 */
	*result = 0.0;

	/* Drop out if we're dealing with null inputs. Shouldn't happen. */
	if (gbox_index_orig && gbox_index_new)
	{
		/* Calculate the size difference of the boxes (volume difference in this case). */
		float size_union = gidx_union_volume(gbox_index_orig, gbox_index_new);
		float size_orig = gidx_volume(gbox_index_orig);
		float volume_extension = size_union - size_orig;

		gbox_index_orig = (GIDX *)PG_DETOAST_DATUM(origentry->key);
		gbox_index_new = (GIDX *)PG_DETOAST_DATUM(newentry->key);

		/* REALM 1: Area extension is nonzero, return it */
		if (volume_extension > FLT_EPSILON)
			*result = pack_float(volume_extension, 1);
		else
		{
			/* REALM 0: Area extension is zero, return nonzero edge extension */
			float edge_union = gidx_union_edge(gbox_index_orig, gbox_index_new);
			float edge_orig = gidx_edge(gbox_index_orig);
			float edge_extension = edge_union - edge_orig;
			if (edge_extension > FLT_EPSILON)
				*result = pack_float(edge_extension, 0);
		}
	}
	PG_RETURN_POINTER(result);
}


/*
** Utility function to add entries to the axis partition lists in the
** picksplit function.
*/
static void
gserialized_gist_picksplit_addlist(OffsetNumber *list, GIDX **box_union, GIDX *box_current, int *pos, int num)
{
	if (*pos)
		gidx_merge(box_union, box_current);
	else
	{
		pfree(*box_union);
		*box_union = gidx_copy(box_current);
	}

	list[*pos] = num;
	(*pos)++;
}

/*
** Utility function check whether the number of entries two halves of the
** space constitute a "bad ratio" (poor balance).
*/
static int
gserialized_gist_picksplit_badratio(int x, int y)
{
	POSTGIS_DEBUGF(4, "[GIST] checking split ratio (%d, %d)", x, y);
	if ((y == 0) || (((float)x / (float)y) < LIMIT_RATIO) || (x == 0) || (((float)y / (float)x) < LIMIT_RATIO))
		return true;

	return false;
}

static bool
gserialized_gist_picksplit_badratios(int *pos, int dims)
{
	int i;
	for (i = 0; i < dims; i++)
	{
		if (gserialized_gist_picksplit_badratio(pos[2 * i], pos[2 * i + 1]) == false)
			return false;
	}
	return true;
}

/*
** Where the picksplit algorithm cannot find any basis for splitting one way
** or another, we simply split the overflowing node in half.
*/
static void
gserialized_gist_picksplit_fallback(GistEntryVector *entryvec, GIST_SPLITVEC *v)
{
	OffsetNumber i, maxoff;
	GIDX *unionL = NULL;
	GIDX *unionR = NULL;
	int nbytes;

	POSTGIS_DEBUG(4, "[GIST] in fallback picksplit function");

	maxoff = entryvec->n - 1;

	nbytes = (maxoff + 2) * sizeof(OffsetNumber);
	v->spl_left = (OffsetNumber *)palloc(nbytes);
	v->spl_right = (OffsetNumber *)palloc(nbytes);
	v->spl_nleft = v->spl_nright = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		GIDX *cur = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);

		if (i <= (maxoff - FirstOffsetNumber + 1) / 2)
		{
			v->spl_left[v->spl_nleft] = i;
			if (!unionL)
				unionL = gidx_copy(cur);
			else
				gidx_merge(&unionL, cur);
			v->spl_nleft++;
		}
		else
		{
			v->spl_right[v->spl_nright] = i;
			if (!unionR)
				unionR = gidx_copy(cur);
			else
				gidx_merge(&unionR, cur);
			v->spl_nright++;
		}
	}

	if (v->spl_ldatum_exists)
		gidx_merge(&unionL, (GIDX *)PG_DETOAST_DATUM(v->spl_ldatum));

	v->spl_ldatum = PointerGetDatum(unionL);

	if (v->spl_rdatum_exists)
		gidx_merge(&unionR, (GIDX *)PG_DETOAST_DATUM(v->spl_rdatum));

	v->spl_rdatum = PointerGetDatum(unionR);
	v->spl_ldatum_exists = v->spl_rdatum_exists = false;
}

static void
gserialized_gist_picksplit_constructsplit(GIST_SPLITVEC *v,
					  OffsetNumber *list1,
					  int nlist1,
					  GIDX **union1,
					  OffsetNumber *list2,
					  int nlist2,
					  GIDX **union2)
{
	bool firstToLeft = true;

	POSTGIS_DEBUG(4, "[GIST] picksplit in constructsplit function");

	if (v->spl_ldatum_exists || v->spl_rdatum_exists)
	{
		if (v->spl_ldatum_exists && v->spl_rdatum_exists)
		{
			GIDX *LRl = gidx_copy(*union1);
			GIDX *LRr = gidx_copy(*union2);
			GIDX *RLl = gidx_copy(*union2);
			GIDX *RLr = gidx_copy(*union1);
			double sizeLR, sizeRL;

			gidx_merge(&LRl, (GIDX *)PG_DETOAST_DATUM(v->spl_ldatum));
			gidx_merge(&LRr, (GIDX *)PG_DETOAST_DATUM(v->spl_rdatum));
			gidx_merge(&RLl, (GIDX *)PG_DETOAST_DATUM(v->spl_ldatum));
			gidx_merge(&RLr, (GIDX *)PG_DETOAST_DATUM(v->spl_rdatum));

			sizeLR = gidx_inter_volume(LRl, LRr);
			sizeRL = gidx_inter_volume(RLl, RLr);

			POSTGIS_DEBUGF(4, "[GIST] sizeLR / sizeRL == %.12g / %.12g", sizeLR, sizeRL);

			if (sizeLR > sizeRL)
				firstToLeft = false;
		}
		else
		{
			float p1 = 0.0, p2 = 0.0;
			GISTENTRY oldUnion, addon;

			gistentryinit(oldUnion,
				      (v->spl_ldatum_exists) ? v->spl_ldatum : v->spl_rdatum,
				      NULL,
				      NULL,
				      InvalidOffsetNumber,
				      false);

			gistentryinit(addon, PointerGetDatum(*union1), NULL, NULL, InvalidOffsetNumber, false);
			DirectFunctionCall3(gserialized_gist_penalty,
					    PointerGetDatum(&oldUnion),
					    PointerGetDatum(&addon),
					    PointerGetDatum(&p1));
			gistentryinit(addon, PointerGetDatum(*union2), NULL, NULL, InvalidOffsetNumber, false);
			DirectFunctionCall3(gserialized_gist_penalty,
					    PointerGetDatum(&oldUnion),
					    PointerGetDatum(&addon),
					    PointerGetDatum(&p2));

			POSTGIS_DEBUGF(4, "[GIST] p1 / p2 == %.12g / %.12g", p1, p2);

			if ((v->spl_ldatum_exists && p1 > p2) || (v->spl_rdatum_exists && p1 < p2))
				firstToLeft = false;
		}
	}

	POSTGIS_DEBUGF(4, "[GIST] firstToLeft == %d", firstToLeft);

	if (firstToLeft)
	{
		v->spl_left = list1;
		v->spl_right = list2;
		v->spl_nleft = nlist1;
		v->spl_nright = nlist2;
		if (v->spl_ldatum_exists)
			gidx_merge(union1, (GIDX *)PG_DETOAST_DATUM(v->spl_ldatum));
		v->spl_ldatum = PointerGetDatum(*union1);
		if (v->spl_rdatum_exists)
			gidx_merge(union2, (GIDX *)PG_DETOAST_DATUM(v->spl_rdatum));
		v->spl_rdatum = PointerGetDatum(*union2);
	}
	else
	{
		v->spl_left = list2;
		v->spl_right = list1;
		v->spl_nleft = nlist2;
		v->spl_nright = nlist1;
		if (v->spl_ldatum_exists)
			gidx_merge(union2, (GIDX *)PG_DETOAST_DATUM(v->spl_ldatum));
		v->spl_ldatum = PointerGetDatum(*union2);
		if (v->spl_rdatum_exists)
			gidx_merge(union1, (GIDX *)PG_DETOAST_DATUM(v->spl_rdatum));
		v->spl_rdatum = PointerGetDatum(*union1);
	}

	v->spl_ldatum_exists = v->spl_rdatum_exists = false;
}

#define BELOW(d) (2 * (d))
#define ABOVE(d) ((2 * (d)) + 1)

/*
** GiST support function. Split an overflowing node into two new nodes.
** Uses linear algorithm from Ang & Tan [2], dividing node extent into
** four quadrants, and splitting along the axis that most evenly distributes
** entries between the new nodes.
** TODO: Re-evaluate this in light of R*Tree picksplit approaches.
*/
PG_FUNCTION_INFO_V1(gtype_gist_picksplit);
Datum gtype_gist_picksplit(PG_FUNCTION_ARGS)
{

	GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);

	GIST_SPLITVEC *v = (GIST_SPLITVEC *)PG_GETARG_POINTER(1);
	OffsetNumber i;
	/* One union box for each half of the space. */
	GIDX **box_union;
	/* One offset number list for each half of the space. */
	OffsetNumber **list;
	/* One position index for each half of the space. */
	int *pos;
	GIDX *box_pageunion;
	GIDX *box_current;
	int direction = -1;
	bool all_entries_equal = true;
	OffsetNumber max_offset;
	int nbytes, ndims_pageunion, d;
	int posmin = entryvec->n;

	POSTGIS_DEBUG(4, "[GIST] 'picksplit' function called");

	/*
	** First calculate the bounding box and maximum number of dimensions in this page.
	*/

	max_offset = entryvec->n - 1;
	box_current = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[FirstOffsetNumber].key);
	box_pageunion = gidx_copy(box_current);

	/* Calculate the containing box (box_pageunion) for the whole page we are going to split. */
	for (i = OffsetNumberNext(FirstOffsetNumber); i <= max_offset; i = OffsetNumberNext(i))
	{
		box_current = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);

		if (all_entries_equal && !gidx_equals(box_pageunion, box_current))
			all_entries_equal = false;

		gidx_merge(&box_pageunion, box_current);
	}

	POSTGIS_DEBUGF(3, "[GIST] box_pageunion: %s", gidx_to_string(box_pageunion));

	/* Every box in the page is the same! So, we split and just put half the boxes in each child. */
	if (all_entries_equal)
	{
		POSTGIS_DEBUG(4, "[GIST] picksplit finds all entries equal!");
		gserialized_gist_picksplit_fallback(entryvec, v);
		PG_RETURN_POINTER(v);
	}

	/* Initialize memory structures. */
	nbytes = (max_offset + 2) * sizeof(OffsetNumber);
	ndims_pageunion = GIDX_NDIMS(box_pageunion);
	POSTGIS_DEBUGF(4, "[GIST] ndims_pageunion == %d", ndims_pageunion);
	pos = palloc(2 * ndims_pageunion * sizeof(int));
	list = palloc(2 * ndims_pageunion * sizeof(OffsetNumber *));
	box_union = palloc(2 * ndims_pageunion * sizeof(GIDX *));
	for (d = 0; d < ndims_pageunion; d++)
	{
		list[BELOW(d)] = (OffsetNumber *)palloc(nbytes);
		list[ABOVE(d)] = (OffsetNumber *)palloc(nbytes);
		box_union[BELOW(d)] = gidx_new(ndims_pageunion);
		box_union[ABOVE(d)] = gidx_new(ndims_pageunion);
		pos[BELOW(d)] = 0;
		pos[ABOVE(d)] = 0;
	}

	/*
	** Assign each entry in the node to the volume partitions it belongs to,
	** such as "above the x/y plane, left of the y/z plane, below the x/z plane".
	** Each entry thereby ends up in three of the six partitions.
	*/
	POSTGIS_DEBUG(4, "[GIST] 'picksplit' calculating best split axis");
	for (i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i))
	{
		box_current = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);

		for (d = 0; d < ndims_pageunion; d++)
		{
			if (GIDX_GET_MIN(box_current, d) - GIDX_GET_MIN(box_pageunion, d) <
			    GIDX_GET_MAX(box_pageunion, d) - GIDX_GET_MAX(box_current, d))
				gserialized_gist_picksplit_addlist(
				    list[BELOW(d)], &(box_union[BELOW(d)]), box_current, &(pos[BELOW(d)]), i);
			else
				gserialized_gist_picksplit_addlist(
				    list[ABOVE(d)], &(box_union[ABOVE(d)]), box_current, &(pos[ABOVE(d)]), i);
		}
	}

	/*
	** "Bad disposition", too many entries fell into one octant of the space, so no matter which
	** plane we choose to split on, we're going to end up with a mostly full node. Where the
	** data is pretty homogeneous (lots of duplicates) entries that are equidistant from the
	** sides of the page union box can occasionally all end up in one place, leading
	** to this condition.
	*/
	if (gserialized_gist_picksplit_badratios(pos, ndims_pageunion))
	{
		/*
		** Instead we split on center points and see if we do better.
		** First calculate the average center point for each axis.
		*/
		double *avgCenter = palloc(ndims_pageunion * sizeof(double));

		for (d = 0; d < ndims_pageunion; d++)
			avgCenter[d] = 0.0;

		POSTGIS_DEBUG(4, "[GIST] picksplit can't find good split axis, trying center point method");

		for (i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i))
		{
			box_current = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);
			for (d = 0; d < ndims_pageunion; d++)
				avgCenter[d] += (GIDX_GET_MAX(box_current, d) + GIDX_GET_MIN(box_current, d)) / 2.0;
		}
		for (d = 0; d < ndims_pageunion; d++)
		{
			avgCenter[d] /= max_offset;
			pos[BELOW(d)] = pos[ABOVE(d)] = 0; /* Re-initialize our counters. */
			POSTGIS_DEBUGF(4, "[GIST] picksplit average center point[%d] = %.12g", d, avgCenter[d]);
		}

		/* For each of our entries... */
		for (i = FirstOffsetNumber; i <= max_offset; i = OffsetNumberNext(i))
		{
			double center;
			box_current = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);

			for (d = 0; d < ndims_pageunion; d++)
			{
				center = (GIDX_GET_MIN(box_current, d) + GIDX_GET_MAX(box_current, d)) / 2.0;
				if (center < avgCenter[d])
					gserialized_gist_picksplit_addlist(
					    list[BELOW(d)], &(box_union[BELOW(d)]), box_current, &(pos[BELOW(d)]), i);
				else if (FPeq(center, avgCenter[d]))
					if (pos[BELOW(d)] > pos[ABOVE(d)])
						gserialized_gist_picksplit_addlist(list[ABOVE(d)],
										   &(box_union[ABOVE(d)]),
										   box_current,
										   &(pos[ABOVE(d)]),
										   i);
					else
						gserialized_gist_picksplit_addlist(list[BELOW(d)],
										   &(box_union[BELOW(d)]),
										   box_current,
										   &(pos[BELOW(d)]),
										   i);
				else
					gserialized_gist_picksplit_addlist(
					    list[ABOVE(d)], &(box_union[ABOVE(d)]), box_current, &(pos[ABOVE(d)]), i);
			}
		}

		/* Do we have a good disposition now? If not, screw it, just cut the node in half. */
		if (gserialized_gist_picksplit_badratios(pos, ndims_pageunion))
		{
			POSTGIS_DEBUG(4,
				      "[GIST] picksplit still cannot find a good split! just cutting the node in half");
			gserialized_gist_picksplit_fallback(entryvec, v);
			PG_RETURN_POINTER(v);
		}
	}

	/*
	** Now, what splitting plane gives us the most even ratio of
	** entries in our child pages? Since each split region has been apportioned entries
	** against the same number of total entries, the axis that has the smallest maximum
	** number of entries in its regions is the most evenly distributed.
	** TODO: what if the distributions are equal in two or more axes?
	*/
	for (d = 0; d < ndims_pageunion; d++)
	{
		int posd = Max(pos[ABOVE(d)], pos[BELOW(d)]);
		if (posd < posmin)
		{
			direction = d;
			posmin = posd;
		}
	}
	if (direction == -1 || posmin == entryvec->n)
		elog(ERROR, "Error in building split, unable to determine split direction.");

	POSTGIS_DEBUGF(3, "[GIST] 'picksplit' splitting on axis %d", direction);

	gserialized_gist_picksplit_constructsplit(v,
						  list[BELOW(direction)],
						  pos[BELOW(direction)],
						  &(box_union[BELOW(direction)]),
						  list[ABOVE(direction)],
						  pos[ABOVE(direction)],
						  &(box_union[ABOVE(direction)]));

	POSTGIS_DEBUGF(4, "[GIST] spl_ldatum: %s", gidx_to_string((GIDX *)v->spl_ldatum));
	POSTGIS_DEBUGF(4, "[GIST] spl_rdatum: %s", gidx_to_string((GIDX *)v->spl_rdatum));

	POSTGIS_DEBUGF(
	    4,
	    "[GIST] axis %d: parent range (%.12g, %.12g) left range (%.12g, %.12g), right range (%.12g, %.12g)",
	    direction,
	    GIDX_GET_MIN(box_pageunion, direction),
	    GIDX_GET_MAX(box_pageunion, direction),
	    GIDX_GET_MIN((GIDX *)v->spl_ldatum, direction),
	    GIDX_GET_MAX((GIDX *)v->spl_ldatum, direction),
	    GIDX_GET_MIN((GIDX *)v->spl_rdatum, direction),
	    GIDX_GET_MAX((GIDX *)v->spl_rdatum, direction));

	PG_RETURN_POINTER(v);
}


/*
** GiST support function. Merge all the boxes in a page into one master box.
*/
PG_FUNCTION_INFO_V1(gtype_gist_union);
Datum gtype_gist_union(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
	int *sizep = (int *)PG_GETARG_POINTER(1); /* Size of the return value */
	int numranges, i;
	GIDX *box_cur, *box_union;

	POSTGIS_DEBUG(4, "[GIST] 'union' function called");

	numranges = entryvec->n;

	box_cur = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[0].key);

	box_union = gidx_copy(box_cur);

	for (i = 1; i < numranges; i++)
	{
		box_cur = (GIDX *)PG_DETOAST_DATUM(entryvec->vector[i].key);
		gidx_merge(&box_union, box_cur);
	}

	*sizep = VARSIZE(box_union);

	POSTGIS_DEBUGF(4, "[GIST] union called, numranges(%i), pageunion %s", numranges, gidx_to_string(box_union));

	PG_RETURN_POINTER(box_union);
}

/*
** GiST support function. Test equality of keys.
*/
PG_FUNCTION_INFO_V1(gtype_gist_same);
Datum gtype_gist_same(PG_FUNCTION_ARGS)
{
	GIDX *b1 = (GIDX *)PG_GETARG_POINTER(0);
	GIDX *b2 = (GIDX *)PG_GETARG_POINTER(1);
	bool *result = (bool *)PG_GETARG_POINTER(2);

	POSTGIS_DEBUG(4, "[GIST] 'same' function called");

	*result = gidx_equals(b1, b2);

	PG_RETURN_POINTER(result);
}


/*
** GiST support function.
** Decompress an entry. Unused for geography, so we return.
*/
PG_FUNCTION_INFO_V1(gtype_gist_decompress);
Datum gtype_gist_decompress(PG_FUNCTION_ARGS)
{
	POSTGIS_DEBUG(5, "[GIST] 'decompress' function called");
	/* We don't decompress. Just return the input. */
	PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}
