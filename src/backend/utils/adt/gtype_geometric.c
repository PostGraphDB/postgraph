/*
 * PostGraph
 * Copyright (C) 2023 PostGraphDB
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "postgraph.h"

// Postgres
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/geo_decls.h"

// PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"


Datum
PostGraphDirectFunctionCall2(PGFunction func, Oid collation, bool *is_null, Datum arg1, Datum arg2)
{
        LOCAL_FCINFO(fcinfo, 3);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
        
        Datum           result;

        InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;

        result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull) {
            *is_null = true;
	    return NULL;
	}

	*is_null = false;

        return result;
}


PG_FUNCTION_INFO_V1(gtype_intersection_point);
Datum
gtype_intersection_point(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    Datum d;
    bool is_null;
    if (GT_IS_LSEG(lhs) || GT_IS_LSEG(rhs)) {
        d = PostGraphDirectFunctionCall2(lseg_interpt, 100, &is_null, GT_TO_LSEG_DATUM(lhs), GT_TO_LSEG_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();

        gtype_value gtv = { .type = AGTV_POINT, .val.box=DatumGetPointP(d)};

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

    } else if (GT_IS_LINE(lhs) || GT_IS_LINE(rhs)) {
        d = PostGraphDirectFunctionCall2(line_interpt, 100, &is_null, GT_TO_LINE_DATUM(lhs), GT_TO_LINE_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();

        gtype_value gtv = { .type = AGTV_POINT, .val.box=DatumGetPointP(d)};

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

    } else if (GT_IS_BOX(lhs) && GT_IS_BOX(rhs)) {
        d = PostGraphDirectFunctionCall2(box_intersect, 100, &is_null, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();

        gtype_value gtv = { .type = AGTV_BOX, .val.box=DatumGetBoxP(d)};

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("invalid type for gtype # gtype")));
    }

    PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(gtype_closest_point);
Datum
gtype_closest_point(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    Datum d;
    bool is_null = false;
    if (GT_IS_POINT(lhs) && GT_IS_BOX(rhs)) {
        d = PostGraphDirectFunctionCall2(close_pb, 100, &is_null, GT_TO_POINT_DATUM(lhs), GT_TO_BOX_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();
    } else if (GT_IS_POINT(lhs) && GT_IS_LSEG(rhs)) {
        d = PostGraphDirectFunctionCall2(close_ps, 100, &is_null, GT_TO_POINT_DATUM(lhs), GT_TO_LSEG_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();
    } else if (GT_IS_POINT(lhs) && GT_IS_LINE(rhs)) {
        d = PostGraphDirectFunctionCall2(close_pl, 100, &is_null, GT_TO_POINT_DATUM(lhs), GT_TO_LINE_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();
    } else if (GT_IS_LSEG(lhs) && GT_IS_BOX(rhs)) {
        d = PostGraphDirectFunctionCall2(close_sb, 100, &is_null, GT_TO_LSEG_DATUM(lhs), GT_TO_BOX_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();
    } else if (GT_IS_LSEG(lhs) && GT_IS_LSEG(rhs)) {
        d = PostGraphDirectFunctionCall2(close_lseg, 100, &is_null, GT_TO_LSEG_DATUM(lhs), GT_TO_LSEG_DATUM(rhs));

        if (is_null)
            PG_RETURN_NULL();
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("invalid type for gtype # gtype")));
    }

    gtype_value gtv = { .type = AGTV_POINT, .val.box=DatumGetPointP(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_vertical);
Datum
gtype_vertical(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d;
    if (GT_IS_LSEG(gt))
        d = DirectFunctionCall1(lseg_vertical, GT_TO_LSEG_DATUM(gt));
    else
        d = DirectFunctionCall1(line_vertical, GT_TO_LINE_DATUM(gt));

    PG_RETURN_BOOL(DatumGetBool(d));
}

PG_FUNCTION_INFO_V1(gtype_perp);
Datum
gtype_perp(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    Datum d;
    if (GT_IS_LSEG(lhs) || GT_IS_LSEG(rhs))
        d = DirectFunctionCall2(lseg_perp, GT_TO_LSEG_DATUM(lhs), GT_TO_LSEG_DATUM(rhs));
    else
        d = DirectFunctionCall2(line_perp, GT_TO_LINE_DATUM(lhs), GT_TO_LINE_DATUM(rhs));

    PG_RETURN_BOOL(DatumGetBool(d));
}

PG_FUNCTION_INFO_V1(gtype_parallel);
Datum
gtype_parallel(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    Datum d;
    if (GT_IS_LSEG(lhs) || GT_IS_LSEG(rhs))
        d = DirectFunctionCall2(lseg_parallel, GT_TO_LSEG_DATUM(lhs), GT_TO_LSEG_DATUM(rhs));
    else
        d = DirectFunctionCall2(line_parallel, GT_TO_LINE_DATUM(lhs), GT_TO_LINE_DATUM(rhs));

    PG_RETURN_BOOL(DatumGetBool(d));
}

PG_FUNCTION_INFO_V1(gtype_bound_box);
Datum
gtype_bound_box(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    bool is_null;
    Datum d = PostGraphDirectFunctionCall2(boxes_bound_box, 100, &is_null, GT_TO_BOX_DATUM(lhs), GT_TO_BOX_DATUM(rhs));

    if (is_null)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_BOX, .val.box=DatumGetBoxP(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_height);
Datum
gtype_height(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(box_height, GT_TO_BOX_DATUM(AG_GET_ARG_GTYPE_P(0)));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value=DatumGetFloat8(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_width);
Datum
gtype_width(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(box_width, GT_TO_BOX_DATUM(AG_GET_ARG_GTYPE_P(0)));

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value=DatumGetFloat8(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

