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


PG_FUNCTION_INFO_V1(gtype_toupper);

Datum gtype_toupper(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toUpper() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    char *string;
    int string_len;
    if (agtv_value->type == AGTV_NULL) {
        PG_RETURN_NULL();
    } else if (agtv_value->type == AGTV_STRING) {
        string = agtv_value->val.string.val;
        string_len = agtv_value->val.string.len;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toUpper() unsupported argument gtype %d", agtv_value->type)));
    }

    char *result = palloc(string_len);

    for (int i = 0; i < string_len; i++)
        result[i] = pg_toupper(string[i]);

    gtype_value agtv_result = { .type = AGTV_STRING, .val.string = {string_len, result}};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_tolower);

Datum gtype_tolower(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toLower() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    char *string;
    int string_len;
    if (agtv_value->type == AGTV_NULL) {
        PG_RETURN_NULL();
    } else if (agtv_value->type == AGTV_STRING) {
        string = agtv_value->val.string.val;
        string_len = agtv_value->val.string.len;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toLower() unsupported argument gtype %d", agtv_value->type)));
    }

    char *result = palloc(string_len);

    for (int i = 0; i < string_len; i++)
        result[i] = pg_tolower(string[i]);

    gtype_value agtv_result = { .type = AGTV_STRING, .val.string = {string_len, result}};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_rtrim);

Datum gtype_rtrim(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");

    Datum d = DirectFunctionCall1(rtrim1, x);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ltrim);

Datum gtype_ltrim(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");

    Datum d = DirectFunctionCall1(ltrim1, x);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_trim);

Datum gtype_trim(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");

    Datum d = DirectFunctionCall1(btrim1, x);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_right);

Datum gtype_right(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(1), "int");

    Datum d = DirectFunctionCall2(text_right, x, y);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_left);

Datum gtype_left(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(1), "int");

    Datum d = DirectFunctionCall2(text_left, x, y);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_substring_w_len);
Datum
gtype_substring_w_len(PG_FUNCTION_ARGS) {
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_int4_internal, AG_GET_ARG_GTYPE_P(1), "int4");
    Datum z = convert_to_scalar(gtype_to_int4_internal, AG_GET_ARG_GTYPE_P(2), "int4");

    Datum d = DirectFunctionCall3(text_substr, x, y, z);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_substring);

Datum gtype_substring(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_int4_internal, AG_GET_ARG_GTYPE_P(1), "int4");

    Datum d = DirectFunctionCall2(text_substr_no_len, x, y);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_split);

Datum gtype_split(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(1), "text");

    Datum d = DirectFunctionCall2(text_right, x, y);

    Datum text_array = DirectFunctionCall2Coll(regexp_split_to_array, DEFAULT_COLLATION_OID, x, y);

    gtype_in_state in_state;
    memset(&in_state, 0, sizeof(gtype_in_state));

    array_to_gtype_internal(text_array, &in_state);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(in_state.res));
}

PG_FUNCTION_INFO_V1(gtype_replace);

Datum gtype_replace(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(1), "text");
    Datum z = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(2), "text");

    Datum d = DirectFunctionCall3Coll(replace_text, DEFAULT_COLLATION_OID, x, y, z);

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_eq_tilde);
/*
 * function for =~ aka regular expression comparisons
 */
Datum gtype_eq_tilde(PG_FUNCTION_ARGS)
{
    gtype *agt_string = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_pattern = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(agt_string) && AGT_ROOT_IS_SCALAR(agt_pattern)) {
        gtype_value *agtv_string;
        gtype_value *agtv_pattern;

        agtv_string = get_ith_gtype_value_from_container(&agt_string->root, 0);
        agtv_pattern = get_ith_gtype_value_from_container(&agt_pattern->root, 0);

        if (agtv_string->type == AGTV_NULL || agtv_pattern->type == AGTV_NULL)
            PG_RETURN_NULL();

        /* only strings can be compared, all others are errors */
        if (agtv_string->type == AGTV_STRING && agtv_pattern->type == AGTV_STRING) {
            text *string = cstring_to_text_with_len(agtv_string->val.string.val, agtv_string->val.string.len);
            text *pattern = cstring_to_text_with_len(agtv_pattern->val.string.val, agtv_pattern->val.string.len);

            Datum result = (DirectFunctionCall2Coll(textregexeq, C_COLLATION_OID, PointerGetDatum(string), PointerGetDatum(pattern)));
            PG_RETURN_BOOL(DatumGetBool(result));
        }
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}

PG_FUNCTION_INFO_V1(gtype_match_case_insensitive);
/*
 * ~* Operator
 */
Datum gtype_match_case_insensitive(PG_FUNCTION_ARGS) 
{   
    gtype *agt_string = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_pattern = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(agt_string) && AGT_ROOT_IS_SCALAR(agt_pattern)) {
        gtype_value *agtv_string;
        gtype_value *agtv_pattern;
    
        agtv_string = get_ith_gtype_value_from_container(&agt_string->root, 0);
        agtv_pattern = get_ith_gtype_value_from_container(&agt_pattern->root, 0);

        if (agtv_string->type == AGTV_NULL || agtv_pattern->type == AGTV_NULL)
            PG_RETURN_NULL();

        /* only strings can be compared, all others are errors */
        if (agtv_string->type == AGTV_STRING && agtv_pattern->type == AGTV_STRING) {
            text *string = cstring_to_text_with_len(agtv_string->val.string.val, agtv_string->val.string.len);
            text *pattern = cstring_to_text_with_len(agtv_pattern->val.string.val, agtv_pattern->val.string.len);

            Datum result = (DirectFunctionCall2Coll(texticregexeq, C_COLLATION_OID, PointerGetDatum(string), PointerGetDatum(pattern)));
            PG_RETURN_BOOL(DatumGetBool(result));
	}
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}

PG_FUNCTION_INFO_V1(gtype_regex_not_cs);
/*
 * !~ Operator
 */
Datum gtype_regex_not_cs(PG_FUNCTION_ARGS)
{
    gtype *agt_string = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_pattern = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(agt_string) && AGT_ROOT_IS_SCALAR(agt_pattern)) {
        gtype_value *agtv_string;
        gtype_value *agtv_pattern;

        agtv_string = get_ith_gtype_value_from_container(&agt_string->root, 0);
        agtv_pattern = get_ith_gtype_value_from_container(&agt_pattern->root, 0);

        if (agtv_string->type == AGTV_NULL || agtv_pattern->type == AGTV_NULL)
            PG_RETURN_NULL();

        /* only strings can be compared, all others are errors */
        if (agtv_string->type == AGTV_STRING && agtv_pattern->type == AGTV_STRING) {
            text *string = cstring_to_text_with_len(agtv_string->val.string.val, agtv_string->val.string.len);
            text *pattern = cstring_to_text_with_len(agtv_pattern->val.string.val, agtv_pattern->val.string.len);

            Datum result = (DirectFunctionCall2Coll(textregexne, C_COLLATION_OID, PointerGetDatum(string), PointerGetDatum(pattern)));
            PG_RETURN_BOOL(DatumGetBool(result));
        }
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}


PG_FUNCTION_INFO_V1(gtype_regex_not_ci);
/*
 * !~ Operator
 */
Datum gtype_regex_not_ci(PG_FUNCTION_ARGS)
{
    gtype *agt_string = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_pattern = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(agt_string) && AGT_ROOT_IS_SCALAR(agt_pattern)) {
        gtype_value *agtv_string;
        gtype_value *agtv_pattern;

        agtv_string = get_ith_gtype_value_from_container(&agt_string->root, 0);
        agtv_pattern = get_ith_gtype_value_from_container(&agt_pattern->root, 0);

        if (agtv_string->type == AGTV_NULL || agtv_pattern->type == AGTV_NULL)
            PG_RETURN_NULL();

        /* only strings can be compared, all others are errors */
        if (agtv_string->type == AGTV_STRING && agtv_pattern->type == AGTV_STRING) {
            text *string = cstring_to_text_with_len(agtv_string->val.string.val, agtv_string->val.string.len);
            text *pattern = cstring_to_text_with_len(agtv_pattern->val.string.val, agtv_pattern->val.string.len);

            Datum result = (DirectFunctionCall2Coll(texticregexne, C_COLLATION_OID, PointerGetDatum(string), PointerGetDatum(pattern)));
            PG_RETURN_BOOL(DatumGetBool(result));
        }
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}
