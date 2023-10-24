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


