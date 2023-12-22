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

#include "postgraph.h"

#include "catalog/pg_collation_d.h"
#include "utils/builtins.h"

#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"


PG_FUNCTION_INFO_V1(gtype_toupper);
// toUpper(gtype)
Datum gtype_toupper(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(upper, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tolower);
// toLower(gtype)
Datum gtype_tolower(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(lower, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_rtrim);
// rtrim(gtype)
Datum gtype_rtrim(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1(rtrim1, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ltrim);
//ltrim(gtype)
Datum gtype_ltrim(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1(ltrim1, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};
 
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_trim);
// trim(gtype)
Datum gtype_trim(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1(btrim1, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_right);
// gtype(gtype, gtype)
Datum gtype_right(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall2(text_right, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_INT8_DATUM(1));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_left);
// left(gtype, gtype)
Datum gtype_left(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall2(text_left, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_INT8_DATUM(1));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_initcap);
// initCap(gtype)
Datum gtype_initcap(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(initcap, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_substring_w_len);
// substring(gtype, gtype, gtype)
Datum
gtype_substring_w_len(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall3(text_substr, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_INT4_DATUM(1), GT_ARG_TO_INT4_DATUM(2));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_substring);
// substring(gtype, gtype)
Datum gtype_substring(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall2(text_substr_no_len, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_INT4_DATUM(1));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_split);
// split(gtype, gtype)
Datum gtype_split(PG_FUNCTION_ARGS)
{
    Datum text_array = DirectFunctionCall2Coll(regexp_split_to_array, DEFAULT_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1));

    gtype_in_state in_state;
    memset(&in_state, 0, sizeof(gtype_in_state));

    array_to_gtype_internal(text_array, &in_state);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(in_state.res));
}

PG_FUNCTION_INFO_V1(gtype_replace);
// replace(gtype, gtype, gtype)
Datum gtype_replace(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall3Coll(replace_text, DEFAULT_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1), GT_ARG_TO_TEXT_DATUM(2));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_like);
// gtype ~~ gtype
Datum gtype_like(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2Coll(textlike, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1))));
}

PG_FUNCTION_INFO_V1(gtype_eq_tilde);
// gtype ~ gtype
Datum gtype_eq_tilde(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2Coll(textregexeq, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1))));
}

PG_FUNCTION_INFO_V1(gtype_match_case_insensitive);
// gtype ~* gtype
Datum gtype_match_case_insensitive(PG_FUNCTION_ARGS) 
{   
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2Coll(texticregexeq, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1))));
}

PG_FUNCTION_INFO_V1(gtype_regex_not_cs);
// gtype !~ gtype
Datum gtype_regex_not_cs(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2Coll(textregexne, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1))));
}


PG_FUNCTION_INFO_V1(gtype_regex_not_ci);
// gtype !~ gtype
Datum gtype_regex_not_ci(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetBool(DirectFunctionCall2Coll(texticregexne, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0), GT_ARG_TO_TEXT_DATUM(1))));
}

PG_FUNCTION_INFO_V1(gtype_sha224);
// sha224(gtype)
Datum gtype_sha224(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(sha224_bytea, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_BYTEA, .val.bytea = DatumGetPointer(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_sha256);
// sha256(gtype)
Datum gtype_sha256(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(sha256_bytea, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_BYTEA, .val.bytea = DatumGetPointer(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_sha384);
// sha384(gtype)
Datum gtype_sha384(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(sha384_bytea, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_BYTEA, .val.bytea = DatumGetPointer(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_sha512);
// sha512(gtype)
Datum gtype_sha512(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(sha512_bytea, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_BYTEA, .val.bytea = DatumGetPointer(d)};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_md5);
// md5(gtype)
Datum gtype_md5(PG_FUNCTION_ARGS)
{
    Datum d = DirectFunctionCall1Coll(md5_text, C_COLLATION_OID, GT_ARG_TO_TEXT_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


