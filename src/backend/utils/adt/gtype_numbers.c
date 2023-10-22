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
 *
 */

#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_operator_d.h"
#include "executor/nodeAgg.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "portability/instr_time.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "utils/gtype.h"
#include "utils/edge.h"
#include "utils/variable_edge.h"
#include "utils/vector.h"
#include "utils/vertex.h"
#include "utils/gtype_parser.h"
#include "utils/gtype_typecasting.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"
#include "utils/numeric.h"



#define GTYPETRIGFUNC( name)                                                               \
PG_FUNCTION_INFO_V1(gtype_##name);                                                         \
Datum                                                                                      \
gtype_##name(PG_FUNCTION_ARGS)                                                             \
{                                                                                          \
    gtype *gt= AG_GET_ARG_GTYPE_P(0);                                                      \
    gtype_value gtv = { \
        .type =AGTV_FLOAT, \
	.val.float_value = DatumGetFloat8(DirectFunctionCall1(d##name, GT_ARG_TO_FLOAT8_DATUM(0))) \
    }; \
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));                                         \
}                                                                                          \
/* keep compiler quiet - no extra ; */                                                     \
extern int no_such_variable

GTYPETRIGFUNC(sin);
GTYPETRIGFUNC(cos);
GTYPETRIGFUNC(tan);
GTYPETRIGFUNC(sinh);
GTYPETRIGFUNC(cosh);
GTYPETRIGFUNC(tanh);
GTYPETRIGFUNC(cot);
GTYPETRIGFUNC(asin);
GTYPETRIGFUNC(acos);
GTYPETRIGFUNC(atan);
GTYPETRIGFUNC(asinh);
GTYPETRIGFUNC(acosh);
GTYPETRIGFUNC(atanh);

PG_FUNCTION_INFO_V1(gtype_atan2);
Datum
gtype_atan2(PG_FUNCTION_ARGS) {
    Datum x = convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(0), "float");
    Datum y = convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float");

    gtype_value gtv_result;
    gtv_result.type = AGTV_FLOAT;
    gtv_result.val.float_value = DatumGetFloat8(DirectFunctionCall2(datan2, y, x));

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv_result));
}

PG_FUNCTION_INFO_V1(gtype_degrees);

Datum gtype_degrees(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    gtype_value gtv_result;
    gtv_result.type = AGTV_FLOAT;
    gtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(degrees, convert_to_scalar(gtype_to_float8_internal, gt, "float")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv_result));
}

PG_FUNCTION_INFO_V1(gtype_radians);

Datum gtype_radians(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    gtype_value gtv_result;
    gtv_result.type = AGTV_FLOAT;
    gtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(radians, convert_to_scalar(gtype_to_float8_internal, gt, "float")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv_result));
}

PG_FUNCTION_INFO_V1(gtype_gcd);
Datum
gtype_gcd(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (GTYPE_IS_FLOAT(lhs) || GTE_IS_NUMERIC(lhs->root.children[0]) || GTYPE_IS_FLOAT(rhs) || GTE_IS_NUMERIC(rhs->root.children[0])) {
        Datum x = convert_to_scalar(gtype_to_numeric_internal, lhs, "numeric");
        Datum y = convert_to_scalar(gtype_to_numeric_internal, rhs, "numeric");

        gtype_value gtv_result;
        gtv_result.type = AGTV_NUMERIC;
        gtv_result.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_gcd, y, x));

        PG_RETURN_POINTER(gtype_value_to_gtype(&gtv_result));
    } else {
        Datum x = convert_to_scalar(gtype_to_int8_internal, lhs, "int8");
        Datum y = convert_to_scalar(gtype_to_int8_internal, rhs, "int8");

        gtype_value gtv_result;
        gtv_result.type = AGTV_INTEGER;
        gtv_result.val.int_value = DatumGetInt64(DirectFunctionCall2(int8gcd, y, x));

        PG_RETURN_POINTER(gtype_value_to_gtype(&gtv_result));
    }
}

PG_FUNCTION_INFO_V1(gtype_lcm);
Datum
gtype_lcm(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (GTYPE_IS_FLOAT(lhs) || GTE_IS_NUMERIC(lhs->root.children[0]) || GTYPE_IS_FLOAT(rhs) || GTE_IS_NUMERIC(rhs->root.children[0])) {
        Datum x = convert_to_scalar(gtype_to_numeric_internal, lhs, "numeric");
        Datum y = convert_to_scalar(gtype_to_numeric_internal, rhs, "numeric");

        gtype_value gtv_result;
        gtv_result.type = AGTV_NUMERIC;
        gtv_result.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_lcm, y, x));

        PG_RETURN_POINTER(gtype_value_to_gtype(&gtv_result));
    } else {
        Datum x = convert_to_scalar(gtype_to_int8_internal, lhs, "int8");
        Datum y = convert_to_scalar(gtype_to_int8_internal, rhs, "int8");

        gtype_value gtv_result;
        gtv_result.type = AGTV_INTEGER;
        gtv_result.val.int_value = DatumGetInt64(DirectFunctionCall2(int8lcm, y, x));

        PG_RETURN_POINTER(gtype_value_to_gtype(&gtv_result));
    }
}

PG_FUNCTION_INFO_V1(gtype_round_w_precision);

Datum gtype_round_w_precision(PG_FUNCTION_ARGS)
{
     gtype *gt = AG_GET_ARG_GTYPE_P(0);
     gtype *prec = AG_GET_ARG_GTYPE_P(1);

     Datum x = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");
     Datum y = convert_to_scalar(gtype_to_int4_internal, prec, "int4");

     gtype_value gtv;
     gtv.type = AGTV_NUMERIC;
     gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_round, x, y));

     AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_round);

Datum gtype_round(PG_FUNCTION_ARGS)
{
     gtype *gt = AG_GET_ARG_GTYPE_P(0);

     if (is_gtype_numeric(gt)) {
         Datum x = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");

         gtype_value gtv;
         gtv.type = AGTV_NUMERIC;
         gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_round, x, Int32GetDatum(0)));

         AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
     } else {
         Datum x = convert_to_scalar(gtype_to_float8_internal, gt, "float");

         gtype_value gtv;
         gtv.type = AGTV_FLOAT;
         gtv.val.float_value = DatumGetFloat8(DirectFunctionCall1(dround, x));

         AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
     }
}

PG_FUNCTION_INFO_V1(gtype_ceil);

Datum gtype_ceil(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    
    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");
    
        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_ceil, arg));
    
        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };
            
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");
    
        float8 result = DatumGetFloat8(DirectFunctionCall1(dceil, arg));
        
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(gtype_floor);

Datum gtype_floor(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_floor, arg));
        
        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };
            
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");

        float8 result = DatumGetFloat8(DirectFunctionCall1(dfloor, arg));
        
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };
        
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(gtype_abs);

Datum gtype_abs(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum x;
    gtype_value gtv;
    if (is_gtype_numeric(gt)) {
        gtv.type = AGTV_NUMERIC;
        x = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");
        gtv.val.numeric = DatumGetNumeric(DirectFunctionCall1(numeric_abs, x));
    }
    else if (is_gtype_float(gt)) {
        gtv.type = AGTV_FLOAT;
        x = convert_to_scalar(gtype_to_float8_internal, gt, "float");
        gtv.val.float_value = DatumGetFloat8(DirectFunctionCall1(float8abs, x));
    }
    else {
        gtv.type = AGTV_INTEGER;
        x = convert_to_scalar(gtype_to_int8_internal, gt, "int");
        gtv.val.int_value = DatumGetInt64(DirectFunctionCall1(int8abs, x));
    }

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_sign);
Datum gtype_sign(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum x;
    gtype_value gtv;
    if (is_gtype_numeric(gt)) {
        gtv.type = AGTV_NUMERIC;
        x = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");
        gtv.val.numeric = DatumGetNumeric(DirectFunctionCall1(numeric_sign, x));
    } else {
        gtv.type = AGTV_FLOAT;
        x = convert_to_scalar(gtype_to_float8_internal, gt, "float");
        gtv.val.float_value = DatumGetFloat8(DirectFunctionCall1(dsign, x));
    }

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_log);
Datum
gtype_log(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    
    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");
        
        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_ln, arg));
        
        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };
            
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");
        
        float8 result = DatumGetFloat8(DirectFunctionCall1(dlog1, arg));
        
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };
 
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(gtype_log10);
Datum
gtype_log10(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_log, arg));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");

        float8 result = DatumGetFloat8(DirectFunctionCall1(dlog10, arg));

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(gtype_e);
Datum
gtype_e(PG_FUNCTION_ARGS) {
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(DirectFunctionCall1(dexp, Float8GetDatum(1)))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_pi);
Datum
gtype_pi(PG_FUNCTION_ARGS) {   
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = M_PI
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}   

PG_FUNCTION_INFO_V1(gtype_rand);
Datum
gtype_rand(PG_FUNCTION_ARGS) {
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(DirectFunctionCall1(random, Float8GetDatum(1)))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_exp);
Datum
gtype_exp(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_exp, arg));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");
        
        float8 result = DatumGetFloat8(DirectFunctionCall1(dexp, arg));
        
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }

}

PG_FUNCTION_INFO_V1(gtype_sqrt);
Datum
gtype_sqrt(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_numeric(gt)) {
        Datum arg = convert_to_scalar(gtype_to_numeric_internal, gt, "numeric");

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_sqrt, arg));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        Datum arg = convert_to_scalar(gtype_to_float8_internal, gt, "float");

        float8 result = DatumGetFloat8(DirectFunctionCall1(dsqrt, arg));                                                       

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(gtype_cbrt);
Datum
gtype_cbrt(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    gtype_value gtv_result;
    gtv_result.type = AGTV_FLOAT;
    gtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dcbrt, convert_to_scalar(gtype_to_float8_internal, gt, "float")));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv_result));
}
