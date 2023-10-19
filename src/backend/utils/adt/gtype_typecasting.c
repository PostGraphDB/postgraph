/*
 * PostGraph
 * Copyright (C) 2023 by PostGraph
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"

#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"

#define int8_to_int4 int84
#define int8_to_int2 int82
#define int8_to_numeric int8_numeric
#define int8_to_string int8out

#define float8_to_int8 dtoi8
#define float8_to_int4 dtoi4
#define float8_to_int2 dtoi2
#define float8_to_float4 dtof
#define float8_to_numeric float8_numeric
#define float8_to_string float8out

#define numeric_to_int8 numeric_int8
#define numeric_to_int4 numeric_int4
#define numeric_to_int2 numeric_int2
#define numeric_to_float4 numeric_float4
#define numeric_to_string numeric_out

#define string_to_int8 int8in
#define string_to_int4 int4in
#define string_to_int2 int2in
#define string_to_numeric numeric_in

static ArrayType *gtype_to_array(coearce_function func, gtype *agt, char *type, Oid type_oid, int type_len, bool elembyval); 


// PostGIS
PG_FUNCTION_INFO_V1(BOX2D_in);
PG_FUNCTION_INFO_V1(BOX3D_in);
PG_FUNCTION_INFO_V1(ellipsoid_in);

static void cannot_cast_gtype_value(enum gtype_value_type type, const char *sqltype);

Datum convert_to_scalar(coearce_function func, gtype *agt, char *type) {
    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot cast non-scalar gtype to %s", type)));

    gtype_value *agtv = get_ith_gtype_value_from_container(&agt->root, 0);

    Datum d = func(agtv);

    return d;
}

Datum integer_to_gtype(int64 i) {
    gtype_value agtv;
    gtype *agt;

    agtv.type = AGTV_INTEGER;
    agtv.val.int_value = i;
    agt = gtype_value_to_gtype(&agtv);

    return GTYPE_P_GET_DATUM(agt);
}

Datum float_to_gtype(float8 f) {
    gtype_value agtv;
    gtype *agt;

    agtv.type = AGTV_FLOAT;
    agtv.val.float_value = f;
    agt = gtype_value_to_gtype(&agtv);

    return GTYPE_P_GET_DATUM(agt);
}

/*
 * s must be a UTF-8 encoded, unescaped, and null-terminated string which is
 * a valid string for internal storage of gtype.
 */
Datum string_to_gtype(char *s) {
    gtype_value agtv;
    gtype *agt;

    agtv.type = AGTV_STRING;
    agtv.val.string.len = check_string_length(strlen(s));
    agtv.val.string.val = s;
    agt = gtype_value_to_gtype(&agtv);

    return GTYPE_P_GET_DATUM(agt);
}

Datum boolean_to_gtype(bool b) {
    gtype_value agtv;
    gtype *agt;

    agtv.type = AGTV_BOOL;
    agtv.val.boolean = b;
    agt = gtype_value_to_gtype(&agtv);

    return GTYPE_P_GET_DATUM(agt);
}

/*
 * Extract scalar value from raw-scalar pseudo-array gtype.
 */
static bool gtype_extract_scalar(gtype_container *agtc, gtype_value *res)
{
    gtype_iterator *it;
    gtype_iterator_token tok PG_USED_FOR_ASSERTS_ONLY;
    gtype_value tmp;

    if (!GTYPE_CONTAINER_IS_ARRAY(agtc) || !GTYPE_CONTAINER_IS_SCALAR(agtc))
    {
        // inform caller about actual type of container 
        res->type = GTYPE_CONTAINER_IS_ARRAY(agtc) ? AGTV_ARRAY : AGTV_OBJECT;
        return false;
    }

    /*
     * A root scalar is stored as an array of one element, so we get the array
     * and then its first (and only) member.
     */
    it = gtype_iterator_init(agtc);

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WGT_BEGIN_ARRAY);
    Assert(tmp.val.array.num_elems == 1 && tmp.val.array.raw_scalar);

    tok = gtype_iterator_next(&it, res, true);
    Assert(tok == WGT_ELEM);
    Assert(IS_A_GTYPE_SCALAR(res));

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WGT_END_ARRAY);

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WGT_DONE);

    return true;
}

/*
 * gtype to and from graphid
 */
PG_FUNCTION_INFO_V1(graphid_to_gtype);

Datum graphid_to_gtype(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(integer_to_gtype(AG_GETARG_GRAPHID(0)));
}

PG_FUNCTION_INFO_V1(gtype_to_graphid);

Datum gtype_to_graphid(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT16(DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(0), "integer")));
}



/*
 * gtype to other gtype functions
 */

PG_FUNCTION_INFO_V1(gtype_tointeger);
Datum
gtype_tointeger(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    gtype_value agtv = {
        .type = AGTV_INTEGER,
        .val.int_value = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, agt, "gtype integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tofloat);
Datum
gtype_tofloat(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    gtype_value agtv = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, agt, "gtype float"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}


PG_FUNCTION_INFO_V1(gtype_tonumeric);
// gtype -> gtype numeric
Datum
gtype_tonumeric(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();


    gtype_value agtv = {
        .type = AGTV_NUMERIC,
        .val.numeric = DatumGetNumeric(convert_to_scalar(gtype_to_numeric_internal, agt, "gtype numeric"))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tostring);
Datum
gtype_tostring(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    char *string = DatumGetCString(convert_to_scalar(gtype_to_string_internal, agt, "string"));
    
    gtype_value agtv; 
    agtv.type = AGTV_STRING;
    agtv.val.string.val = string;
    agtv.val.string.len = strlen(string);

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_toboolean);
Datum
gtype_toboolean(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toBoolean() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    bool result;
    if (agtv_value->type == AGTV_BOOL) {
            result = agtv_value->val.boolean;
    } else if (agtv_value->type == AGTV_STRING) {
        int len = agtv_value->val.string.len;

        char *string = agtv_value->val.string.val;

        if (len == 4 && !pg_strncasecmp(string, "true", len))
            result = true;
        else if (len == 5 && !pg_strncasecmp(string, "false", len))
            result = false;
        else
            PG_RETURN_NULL();
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("toBoolean() unsupported argument gtype %d",
                               agtv_value->type)));
    }

    gtype_value agtv_result = { .type = AGTV_BOOL, .val.boolean = result };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_totimestamp);
Datum
gtype_totimestamp(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    int64 ts = DatumGetInt64(convert_to_scalar(gtype_to_timestamp_internal, agt, "timestamp"));

    gtype_value agtv;
    agtv.type = AGTV_TIMESTAMP;
    agtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(totimestamptz);
/*                  
 * Execute function to typecast an agtype to an agtype timestamp
 */                 
Datum totimestamptz(PG_FUNCTION_ARGS)
{               
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    int64 ts = DatumGetInt64(convert_to_scalar(gtype_to_timestamptz_internal, agt, "timestamptz"));

    gtype_value agtv;
    agtv.type = AGTV_TIMESTAMPTZ;
    agtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}           

PG_FUNCTION_INFO_V1(totime);
/*                  
 * Execute function to typecast an agtype to an agtype timestamp
 */                 
Datum totime(PG_FUNCTION_ARGS)
{  
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    Timestamp t;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    t = DatumGetInt64(convert_to_scalar(gtype_to_time_internal, agt, "time"));

    gtype_value agtv;
    agtv.type = AGTV_TIME;
    agtv.val.int_value = (int64)t;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}   

PG_FUNCTION_INFO_V1(totimetz);
/*                  
 * Execute function to typecast an agtype to an agtype timetz
 */                 
Datum totimetz(PG_FUNCTION_ARGS)
{   
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    TimeTzADT* t;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    t = DatumGetInt64(convert_to_scalar(gtype_to_timetz_internal, agt, "time"));

    gtype_value agtv;
    agtv.type = AGTV_TIMETZ;
    agtv.val.timetz.time = t->time;
    agtv.val.timetz.zone = t->zone;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
} 

PG_FUNCTION_INFO_V1(tointerval);
Datum tointerval(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *agtv = get_ith_gtype_value_from_container(&agt->root, 0);
    Interval *i;

    if (agtv->type == AGTV_NULL)
        PG_RETURN_NULL();

    if (agtv->type == AGTV_INTERVAL)
        AG_RETURN_GTYPE_P(agt);

    if (agtv->type != AGTV_STRING)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("typecastint to interval must be a string")));

    i = DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum(agtv->val.string.val),
                                                           ObjectIdGetDatum(InvalidOid),
                                                           Int32GetDatum(-1)));
    agtv->type = AGTV_INTERVAL;
    agtv->val.interval.time = i->time;
    agtv->val.interval.day = i->day;
    agtv->val.interval.month = i->month;

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv));
}

PG_FUNCTION_INFO_V1(todate);
/*                  
 * Execute function to typecast an agtype to an agtype interval
 */
Datum todate(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    DateADT dte = DatumGetInt64(convert_to_scalar(gtype_to_date_internal, agt, "date"));

    gtype_value agtv;
    agtv.type = AGTV_DATE;
    agtv.val.date = dte;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}  

PG_FUNCTION_INFO_V1(tovector);
/*                  
 * Execute function to typecast an agtype to an agtype timestamp
 */                 
Datum tovector(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *agtv = get_ith_gtype_value_from_container(&agt->root, 0);

    if (agtv->type == NULL)
        PG_RETURN_NULL();


    if (agtv->type != AGTV_STRING)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("typecastint to vector must be a string")));

    agtv = gtype_vector_in(agtv->val.string.val, -1);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv));
}

Datum _gtype_toinet(Datum arg){
    gtype *agt = DATUM_GET_GTYPE_P(arg);

    if (is_gtype_null(agt))
        return NULL;

    inet *i = DatumGetInetPP(convert_to_scalar(gtype_to_inet_internal, agt, "inet"));

    gtype_value agtv;
    agtv.type = AGTV_INET;
    agtv.val.inet.vl_len_[0] = i->vl_len_[0];
    agtv.val.inet.vl_len_[1] = i->vl_len_[1];
    agtv.val.inet.vl_len_[2] = i->vl_len_[2];
    agtv.val.inet.vl_len_[3] = i->vl_len_[3];
    agtv.val.inet.inet_data.family = i->inet_data.family;
    agtv.val.inet.inet_data.bits = i->inet_data.bits;
    agtv.val.inet.inet_data.ipaddr[0] = i->inet_data.ipaddr[0];
    agtv.val.inet.inet_data.ipaddr[1] = i->inet_data.ipaddr[1];
    agtv.val.inet.inet_data.ipaddr[2] = i->inet_data.ipaddr[2];
    agtv.val.inet.inet_data.ipaddr[3] = i->inet_data.ipaddr[3];
    agtv.val.inet.inet_data.ipaddr[4] = i->inet_data.ipaddr[4];
    agtv.val.inet.inet_data.ipaddr[5] = i->inet_data.ipaddr[5];
    agtv.val.inet.inet_data.ipaddr[6] = i->inet_data.ipaddr[6];
    agtv.val.inet.inet_data.ipaddr[7] = i->inet_data.ipaddr[7];
    agtv.val.inet.inet_data.ipaddr[8] = i->inet_data.ipaddr[8];
    agtv.val.inet.inet_data.ipaddr[9] = i->inet_data.ipaddr[9];
    agtv.val.inet.inet_data.ipaddr[10] = i->inet_data.ipaddr[10];
    agtv.val.inet.inet_data.ipaddr[11] = i->inet_data.ipaddr[11];
    agtv.val.inet.inet_data.ipaddr[12] = i->inet_data.ipaddr[12];
    agtv.val.inet.inet_data.ipaddr[13] = i->inet_data.ipaddr[13];
    agtv.val.inet.inet_data.ipaddr[14] = i->inet_data.ipaddr[14];
    agtv.val.inet.inet_data.ipaddr[15] = i->inet_data.ipaddr[15];

    return GTYPE_P_GET_DATUM(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_toinet);
/*                  
 * Execute function to typecast an agtype to an agtype timestamp
 */                 
Datum gtype_toinet(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    inet *i = DatumGetInetPP(convert_to_scalar(gtype_to_inet_internal, agt, "inet"));

    gtype_value agtv;
    agtv.type = AGTV_INET;
    agtv.val.inet.vl_len_[0] = i->vl_len_[0];
    agtv.val.inet.vl_len_[1] = i->vl_len_[1];
    agtv.val.inet.vl_len_[2] = i->vl_len_[2];
    agtv.val.inet.vl_len_[3] = i->vl_len_[3];
    agtv.val.inet.inet_data.family = i->inet_data.family;
    agtv.val.inet.inet_data.bits = i->inet_data.bits;
    agtv.val.inet.inet_data.ipaddr[0] = i->inet_data.ipaddr[0];
    agtv.val.inet.inet_data.ipaddr[1] = i->inet_data.ipaddr[1];
    agtv.val.inet.inet_data.ipaddr[2] = i->inet_data.ipaddr[2];
    agtv.val.inet.inet_data.ipaddr[3] = i->inet_data.ipaddr[3];
    agtv.val.inet.inet_data.ipaddr[4] = i->inet_data.ipaddr[4];
    agtv.val.inet.inet_data.ipaddr[5] = i->inet_data.ipaddr[5];
    agtv.val.inet.inet_data.ipaddr[6] = i->inet_data.ipaddr[6];
    agtv.val.inet.inet_data.ipaddr[7] = i->inet_data.ipaddr[7];
    agtv.val.inet.inet_data.ipaddr[8] = i->inet_data.ipaddr[8];
    agtv.val.inet.inet_data.ipaddr[9] = i->inet_data.ipaddr[9];
    agtv.val.inet.inet_data.ipaddr[10] = i->inet_data.ipaddr[10];
    agtv.val.inet.inet_data.ipaddr[11] = i->inet_data.ipaddr[11];
    agtv.val.inet.inet_data.ipaddr[12] = i->inet_data.ipaddr[12];
    agtv.val.inet.inet_data.ipaddr[13] = i->inet_data.ipaddr[13];
    agtv.val.inet.inet_data.ipaddr[14] = i->inet_data.ipaddr[14];
    agtv.val.inet.inet_data.ipaddr[15] = i->inet_data.ipaddr[15];

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tocidr);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tocidr(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    inet *i = DatumGetInetPP(convert_to_scalar(gtype_to_cidr_internal, agt, "cidr"));

    gtype_value agtv;
    agtv.type = AGTV_CIDR;
    agtv.val.inet.vl_len_[0] = i->vl_len_[0];
    agtv.val.inet.vl_len_[1] = i->vl_len_[1];
    agtv.val.inet.vl_len_[2] = i->vl_len_[2];
    agtv.val.inet.vl_len_[3] = i->vl_len_[3];
    agtv.val.inet.inet_data.family = i->inet_data.family;
    agtv.val.inet.inet_data.bits = i->inet_data.bits;
    agtv.val.inet.inet_data.ipaddr[0] = i->inet_data.ipaddr[0];
    agtv.val.inet.inet_data.ipaddr[1] = i->inet_data.ipaddr[1];
    agtv.val.inet.inet_data.ipaddr[2] = i->inet_data.ipaddr[2];
    agtv.val.inet.inet_data.ipaddr[3] = i->inet_data.ipaddr[3];
    agtv.val.inet.inet_data.ipaddr[4] = i->inet_data.ipaddr[4];
    agtv.val.inet.inet_data.ipaddr[5] = i->inet_data.ipaddr[5];
    agtv.val.inet.inet_data.ipaddr[6] = i->inet_data.ipaddr[6];
    agtv.val.inet.inet_data.ipaddr[7] = i->inet_data.ipaddr[7];
    agtv.val.inet.inet_data.ipaddr[8] = i->inet_data.ipaddr[8];
    agtv.val.inet.inet_data.ipaddr[9] = i->inet_data.ipaddr[9];
    agtv.val.inet.inet_data.ipaddr[10] = i->inet_data.ipaddr[10];
    agtv.val.inet.inet_data.ipaddr[11] = i->inet_data.ipaddr[11];
    agtv.val.inet.inet_data.ipaddr[12] = i->inet_data.ipaddr[12];
    agtv.val.inet.inet_data.ipaddr[13] = i->inet_data.ipaddr[13];
    agtv.val.inet.inet_data.ipaddr[14] = i->inet_data.ipaddr[14];
    agtv.val.inet.inet_data.ipaddr[15] = i->inet_data.ipaddr[15];

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tomacaddr);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tomacaddr(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    macaddr *mac = DatumGetMacaddrP(convert_to_scalar(gtype_to_macaddr_internal, agt, "cidr"));

    gtype_value agtv;
    agtv.type = AGTV_MAC;
    agtv.val.mac.a = mac->a;
    agtv.val.mac.b = mac->b;
    agtv.val.mac.c = mac->c;
    agtv.val.mac.d = mac->d;
    agtv.val.mac.e = mac->e;
    agtv.val.mac.f = mac->f;


    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tomacaddr8);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tomacaddr8(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    macaddr8 *mac = DatumGetMacaddrP(convert_to_scalar(gtype_to_macaddr8_internal, agt, "macaddr8"));

    gtype_value agtv;
    agtv.type = AGTV_MAC8;
    agtv.val.mac8.a = mac->a;
    agtv.val.mac8.b = mac->b;
    agtv.val.mac8.c = mac->c;
    agtv.val.mac8.d = mac->d;
    agtv.val.mac8.e = mac->e;
    agtv.val.mac8.f = mac->f;
    agtv.val.mac8.g = mac->g;
    agtv.val.mac8.h = mac->h;


    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tobox2d);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tobox2d(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    GBOX *box = convert_to_scalar(gtype_to_box2d_internal, agt, "box2d");

    gtype_value agtv;
    agtv.type = AGTV_BOX2D;

    agtv.val.gbox.flags = box->flags;
    agtv.val.gbox.xmin = box->xmin;
    agtv.val.gbox.xmax = box->xmax;
    agtv.val.gbox.ymin = box->ymin;
    agtv.val.gbox.ymax = box->ymax;
    agtv.val.gbox.zmin = box->zmin;
    agtv.val.gbox.zmax = box->zmax;
    agtv.val.gbox.mmin = box->mmin;
    agtv.val.gbox.mmax = box->mmax;


    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_tobox3d);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tobox3d(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    BOX3D *box = convert_to_scalar(gtype_to_box3d_internal, agt, "box3d");

    gtype_value agtv;
    agtv.type = AGTV_BOX3D;

    agtv.val.box3d.srid = box->srid;
    agtv.val.box3d.xmin = box->xmin;
    agtv.val.box3d.xmax = box->xmax;
    agtv.val.box3d.ymin = box->ymin;
    agtv.val.box3d.ymax = box->ymax;
    agtv.val.box3d.zmin = box->zmin;
    agtv.val.box3d.zmax = box->zmax;


    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}


PG_FUNCTION_INFO_V1(gtype_tospheroid);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tospheroid(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    SPHEROID *box = convert_to_scalar(gtype_to_spheroid_internal, agt, "spheroid");

    gtype_value agtv;
    agtv.type = AGTV_SPHEROID;

    memcpy(&agtv.val.spheroid, box, sizeof(SPHEROID));

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}


PG_FUNCTION_INFO_V1(gtype_togeometry);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_togeometry(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    void *gserial = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, agt, "geometry"));

    gtype_value agtv;
    agtv.type = AGTV_GSERIALIZED;
    agtv.val.gserialized = gserial;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}


PG_FUNCTION_INFO_V1(gtype_totsvector);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totsvector(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    TSVector tsvector = DatumGetPointer(DirectFunctionCall1(tsvectorin,
			                          convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value agtv;
    agtv.type = AGTV_TSVECTOR;
    agtv.val.gserialized = tsvector;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_totsquery);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totsquery(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    TSQuery tsquery = DatumGetPointer(DirectFunctionCall1(tsqueryin,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value agtv;
    agtv.type = AGTV_TSQUERY;
    agtv.val.tsquery = tsquery;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv));
}

/*
 * gtype to postgres functions
 */
PG_FUNCTION_INFO_V1(gtype_to_int8);
// gtype -> int8.
Datum
gtype_to_int8(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_int8_internal, agt, "int8");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_int4);
// gtype -> int4
Datum
gtype_to_int4(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_int4_internal, agt, "int4");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_int2);
// gtype -> int2
Datum
gtype_to_int2(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_int2_internal, agt, "int2");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}


PG_FUNCTION_INFO_V1(gtype_to_float8);
// gtype -> float8.
Datum
gtype_to_float8(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_float8_internal, agt, "float8");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_numeric);
// gtype -> numeric.
Datum
gtype_to_numeric(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_numeric_internal, agt, "numerifloat8c");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_float4);
// gtype -> float4.
Datum
gtype_to_float4(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_float4_internal, agt, "float4");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_text);
// gtype -> text
Datum
gtype_to_text(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_string_internal, agt, "text");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_bool);
/*
 * Cast gtype to boolean
 */
Datum gtype_to_bool(PG_FUNCTION_ARGS)
{
    gtype *gtype_in = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv;

    if (!gtype_extract_scalar(&gtype_in->root, &agtv) || agtv.type != AGTV_BOOL)
        cannot_cast_gtype_value(agtv.type, "boolean");

    PG_FREE_IF_COPY(gtype_in, 0);

    PG_RETURN_BOOL(agtv.val.boolean);
}

PG_FUNCTION_INFO_V1(gtype_to_inet);
// gtype -> inet
Datum
gtype_to_inet(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_inet_internal, agt, "inet");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_cidr);
// gtype -> cidr
Datum
gtype_to_cidr(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_cidr_internal, agt, "cidr");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_geometry);
// gtype -> geometry
Datum
gtype_to_geometry(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = DatumGetPointer(convert_to_scalar(gtype_to_geometry_internal, agt, "geometry"));

    PG_RETURN_DATUM(d);
}   

PG_FUNCTION_INFO_V1(gtype_to_box3d);
// gtype -> box3d
Datum
gtype_to_box3d(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = DatumGetPointer(convert_to_scalar(gtype_to_box3d_internal, agt, "box3d"));

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(gtype_to_box2d);
// gtype -> box2d
Datum
gtype_to_box2d(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = DatumGetPointer(convert_to_scalar(gtype_to_box2d_internal, agt, "box2d"));

    PG_RETURN_DATUM(d);
}

/*
 * Postgres types to gtype
 */

PG_FUNCTION_INFO_V1(bool_to_gtype);
// boolean -> gtype
Datum
bool_to_gtype(PG_FUNCTION_ARGS) {
    return boolean_to_gtype(PG_GETARG_BOOL(0));
}

PG_FUNCTION_INFO_V1(float8_to_gtype);
// float8 -> gtype
Datum
float8_to_gtype(PG_FUNCTION_ARGS) {
    return float_to_gtype(PG_GETARG_FLOAT8(0));
}

PG_FUNCTION_INFO_V1(int8_to_gtype);
// int8 -> gtype.
Datum
int8_to_gtype(PG_FUNCTION_ARGS) {
    return integer_to_gtype(PG_GETARG_INT64(0));
}

PG_FUNCTION_INFO_V1(text_to_gtype);
//text -> gtype
Datum
text_to_gtype(PG_FUNCTION_ARGS) {
    Datum txt = PG_GETARG_DATUM(0);

    return string_to_gtype(TextDatumGetCString(txt));
}

PG_FUNCTION_INFO_V1(timestamp_to_gtype);
//timestamp -> gtype
Datum
timestamp_to_gtype(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMP(0);

    gtype_value agtv;
    agtv.type = AGTV_TIMESTAMP;
    agtv.val.int_value = ts;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(timestamptz_to_gtype);
//timestamptz -> gtype
Datum
timestamptz_to_gtype(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMPTZ(0);
    
    gtype_value agtv;
    agtv.type = AGTV_TIMESTAMPTZ;
    agtv.val.int_value = ts;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(date_to_gtype);
//date -> gtype
Datum
date_to_gtype(PG_FUNCTION_ARGS) {

    gtype_value agtv;
    agtv.type = AGTV_DATE;
    agtv.val.date = PG_GETARG_DATEADT(0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(time_to_gtype);
//time -> gtype
Datum
time_to_gtype(PG_FUNCTION_ARGS) {
    gtype_value agtv;
    agtv.type = AGTV_TIME;
    agtv.val.int_value = PG_GETARG_TIMEADT(0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(timetz_to_gtype);
//timetz -> gtype
Datum
timetz_to_gtype(PG_FUNCTION_ARGS) {
    TimeTzADT *t = PG_GETARG_TIMETZADT_P(0);

    gtype_value agtv;
    agtv.type = AGTV_TIMETZ;
    agtv.val.timetz.time = t->time;
    agtv.val.timetz.zone = t->zone;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(inet_to_gtype);
//inet -> gtype
Datum
inet_to_gtype(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, ip, sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(cidr_to_gtype);
//cidr -> gtype
Datum
cidr_to_gtype(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, ip, sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

/*
 * gtype to postgres array functions
 */
PG_FUNCTION_INFO_V1(gtype_to_text_array);
// gtype -> text[]
Datum
gtype_to_text_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    ArrayType *result = gtype_to_array(gtype_to_text_internal, agt, "text[]", TEXTOID, -1, false);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_float8_array);
// gtype -> float8[]
Datum 
gtype_to_float8_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    ArrayType *result = gtype_to_array(gtype_to_float8_internal, agt, "float8[]", FLOAT8OID, 8, true);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_float4_array);
// gtype -> float4[]
Datum
gtype_to_float4_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    ArrayType *result = gtype_to_array(gtype_to_float4_internal, agt, "float4[]", FLOAT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_numeric_array);
// gtype -> numeric[]
Datum
gtype_to_numeric_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    ArrayType *result = gtype_to_array(gtype_to_numeric_internal, agt, "numeric[]", NUMERICOID, -1, false);

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_int8_array);
// gtype -> int8[]
Datum
gtype_to_int8_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    ArrayType *result = gtype_to_array(gtype_to_int8_internal, agt, "int8[]", INT8OID, 8, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_int4_array);
// gtype -> int4[]
Datum gtype_to_int4_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    
    ArrayType *result = gtype_to_array(gtype_to_int4_internal, agt, "int4[]", INT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(gtype_to_int2_array);
// gtype -> int2[]
Datum gtype_to_int2_array(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    
    ArrayType *result = gtype_to_array(gtype_to_int2_internal, agt, "int2[]", INT4OID, 4, true);

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

static ArrayType *
gtype_to_array(coearce_function func, gtype *agt, char *type, Oid type_oid, int type_len, bool elembyval) {
    gtype_value agtv;
    Datum *array_value;

    gtype_iterator *gtype_iterator = gtype_iterator_init(&agt->root);
    gtype_iterator_token agtv_token = gtype_iterator_next(&gtype_iterator, &agtv, false);

    if (agtv.type != AGTV_ARRAY && agtv.type != AGTV_VECTOR)
        cannot_cast_gtype_value(agtv.type, type);

    array_value = (Datum *) palloc(sizeof(Datum) * AGT_ROOT_COUNT(agt));

    int i = 0;
    while ((agtv_token = gtype_iterator_next(&gtype_iterator, &agtv, true)) < WGT_END_ARRAY)
        array_value[i++] = func(&agtv);

    ArrayType *result = construct_array(array_value, AGT_ROOT_COUNT(agt), type_oid, type_len, elembyval, 'i');

    return result;
}

/*
 * internal scalar conversion functions
 */
Datum
gtype_to_int8_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return Int64GetDatum(agtv->val.int_value);
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int8, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int8, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int8, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "int8");

    // cannot reach
    return 0;
}

Datum
gtype_to_int4_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_int4, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int4, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int4, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int4, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "int4");

    // cannot reach
    return 0;
}

Datum
gtype_to_int2_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_int2, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int2, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int2, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int2, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "int2");

    // cannot reach
    return 0;
}

Datum
gtype_to_float8_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_FLOAT)
        return Float8GetDatum(agtv->val.float_value);
    else if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(i8tod, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_float8, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(float8in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "float8");

    // unreachable
    return 0;
}


Datum
gtype_to_boolean_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_BOOL)
        return BoolGetDatum(agtv->val.boolean);
    else
        cannot_cast_gtype_value(agtv->type, "float8");

    // unreachable
    return 0;
}


Datum
gtype_to_float4_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(dtof, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(i8tof, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_float4, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(float4in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "float4");

    // unreachable
    return 0;
}

Datum
gtype_to_numeric_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_numeric, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_numeric, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return NumericGetDatum(agtv->val.numeric);
    else if (agtv->type == AGTV_STRING) {
        char *string = (char *) palloc(sizeof(char) * (agtv->val.string.len + 1));
        string = strncpy(string, agtv->val.string.val, agtv->val.string.len);
        string[agtv->val.string.len] = '\0';

        Datum numd = DirectFunctionCall3(string_to_numeric, CStringGetDatum(string), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

        pfree(string);

        return numd;
    } else
        cannot_cast_gtype_value(agtv->type, "numeric");

    // unreachable
    return 0;
}


Datum
gtype_to_text_internal(gtype_value *agtv) {
    return CStringGetTextDatum(DatumGetCString(gtype_to_string_internal(agtv)));
}

Datum
gtype_to_string_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_string, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_string, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_STRING)
        return CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_string, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_BOOL)
        return CStringGetDatum((agtv->val.boolean) ? "true" : "false");
    else if (agtv->type == AGTV_TIMESTAMP)
        return DirectFunctionCall1(timestamp_out, TimestampGetDatum(agtv->val.int_value));
    else
        cannot_cast_gtype_value(agtv->type, "string");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timestamp_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER || agtv->type == AGTV_TIMESTAMP)
        return TimestampGetDatum(agtv->val.int_value);
    else if (agtv->type == AGTV_STRING)
        return TimestampGetDatum(DirectFunctionCall3(timestamp_in,
				          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
					  ObjectIdGetDatum(InvalidOid),
					  Int32GetDatum(-1)));
    else if (agtv->type == AGTV_TIMESTAMPTZ)
        return DirectFunctionCall1(timestamptz_timestamp, TimestampTzGetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_DATE)
        return TimestampGetDatum(date2timestamp_opt_overflow(agtv->val.date, NULL));
    else
        cannot_cast_gtype_value(agtv->type, "timestamp");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timestamptz_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER || agtv->type == AGTV_TIMESTAMPTZ)
        return TimestampGetDatum(agtv->val.int_value);
    else if (agtv->type == AGTV_STRING)
        return TimestampTzGetDatum(DirectFunctionCall3(timestamptz_in,
                                          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (agtv->type == AGTV_TIMESTAMP)
        return DirectFunctionCall1(timestamp_timestamptz, TimestampGetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_DATE)
        return TimestampGetDatum(date2timestamptz_opt_overflow(agtv->val.date, NULL));
    else
        cannot_cast_gtype_value(agtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}


Datum
gtype_to_date_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_TIMESTAMPTZ)
        return DateADTGetDatum(DirectFunctionCall1(timestamptz_date, TimestampTzGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_STRING)
        return DateADTGetDatum(DirectFunctionCall3(date_in,
                                          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (agtv->type == AGTV_TIMESTAMP)
	return DateADTGetDatum(DirectFunctionCall1(timestamp_date, TimestampGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_DATE)
        return DateADTGetDatum(agtv->val.date);
    else
        cannot_cast_gtype_value(agtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_time_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_TIMESTAMPTZ)
        return TimeADTGetDatum(DirectFunctionCall1(timestamptz_time, TimestampTzGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_STRING)
        return TimeADTGetDatum(DirectFunctionCall3(time_in,
                                          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (agtv->type == AGTV_TIMESTAMP)
        return TimeADTGetDatum(DirectFunctionCall1(timestamp_time, TimestampGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_INTERVAL)
        return TimeADTGetDatum(DirectFunctionCall1(interval_time, IntervalPGetDatum(&agtv->val.interval)));
    else if (agtv->type == AGTV_TIMETZ)
	return TimeADTGetDatum(DirectFunctionCall1(timetz_time, TimeTzADTPGetDatum(&agtv->val.timetz)));
    else if (agtv->type == AGTV_TIME)
	    return TimeADTGetDatum(agtv->val.int_value);
    else
        cannot_cast_gtype_value(agtv->type, "time");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timetz_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_TIMESTAMPTZ)
        return TimeTzADTPGetDatum(DirectFunctionCall1(timestamptz_timetz, TimestampTzGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_STRING)
        return TimeTzADTPGetDatum(DirectFunctionCall3(timetz_in,
                                          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (agtv->type == AGTV_TIME)
        return TimeTzADTPGetDatum(DirectFunctionCall1(time_timetz, TimeADTGetDatum(agtv->val.int_value)));
    else if (agtv->type == AGTV_TIMETZ)
	return TimeTzADTPGetDatum(&agtv->val.timetz);
    else                 
        cannot_cast_gtype_value(agtv->type, "time");
    
    // unreachable
    return CStringGetDatum("");
}  

Datum
gtype_to_interval_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INTERVAL)
        return IntervalPGetDatum(&agtv->val.interval);
    else if (agtv->type == AGTV_STRING)
        return IntervalPGetDatum(DirectFunctionCall3(interval_in,
                                          CStringGetDatum(pnstrdup(agtv->val.string.val, agtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else                 
        cannot_cast_gtype_value(agtv->type, "interval");
    
    // unreachable
    return CStringGetDatum("");
}  


Datum
gtype_to_inet_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_INET)
        return InetPGetDatum(&agtv->val.inet);
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(inet_in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "inet");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_cidr_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_CIDR)
        return InetPGetDatum(&agtv->val.inet);
    else if (agtv->type == AGTV_INET)
	return DirectFunctionCall1(inet_to_cidr, InetPGetDatum(&agtv->val.inet));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(cidr_in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "cidr");
    
    // unreachable
    return CStringGetDatum("");
}   

Datum
gtype_to_macaddr_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_MAC)
	return MacaddrPGetDatum(&agtv->val.mac);
    else if (agtv->type == AGTV_MAC8)
        return DirectFunctionCall1(macaddr8tomacaddr, Macaddr8PGetDatum(&agtv->val.mac8));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(macaddr_in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_macaddr8_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_MAC8)
        return Macaddr8PGetDatum(&agtv->val.mac8);
    else if (agtv->type == AGTV_MAC)
        return DirectFunctionCall1(macaddrtomacaddr8, MacaddrPGetDatum(&agtv->val.mac));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(macaddr8_in, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_gtype_value(agtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(BOX3D_to_BOX2D);
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX2D);

Datum
gtype_to_box2d_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_BOX2D) {
        return PointerGetDatum(&agtv->val.gbox);
    } else if (agtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX3D_to_BOX2D, PointerGetDatum(&agtv->val.box3d));
    } else if (agtv->type == AGTV_GSERIALIZED) {
        return DirectFunctionCall1(LWGEOM_to_BOX2D, PointerGetDatum(agtv->val.gserialized));
    } else if (agtv->type == AGTV_STRING) {
        return DirectFunctionCall1(BOX2D_in, CStringGetDatum(agtv->val.string.val));
    }  else
        cannot_cast_gtype_value(agtv->type, "box2d");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(BOX2D_to_BOX3D);
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX3D);

Datum
gtype_to_box3d_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_BOX3D) {
        return PointerGetDatum(&agtv->val.box3d);	    
    } else if (agtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX2D_to_BOX3D, PointerGetDatum(&agtv->val.gbox));
    } else if (agtv->type == AGTV_GSERIALIZED) {
        return DirectFunctionCall1(LWGEOM_to_BOX3D, PointerGetDatum(agtv->val.gserialized));
    } else if (agtv->type == AGTV_STRING){
        return DirectFunctionCall1(BOX3D_in, CStringGetDatum(agtv->val.string.val));
    }  else
        cannot_cast_gtype_value(agtv->type, "box3d");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_spheroid_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_STRING){
        return DirectFunctionCall1(ellipsoid_in, CStringGetDatum(agtv->val.string.val));
    }  else
        cannot_cast_gtype_value(agtv->type, "spheroid");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(LWGEOM_in);
PG_FUNCTION_INFO_V1(BOX3D_to_LWGEOM);
PG_FUNCTION_INFO_V1(BOX2D_to_LWGEOM);

Datum
gtype_to_geometry_internal(gtype_value *agtv) {
    if (agtv->type == AGTV_GSERIALIZED) {
	return PointerGetDatum(agtv->val.gserialized);
    } else if (agtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX3D_to_LWGEOM, PointerGetDatum(&agtv->val.box3d));
    } else if (agtv->type == AGTV_BOX2D) {
        return DirectFunctionCall1(BOX2D_to_LWGEOM, PointerGetDatum(&agtv->val.gbox));
    } else if (agtv->type == AGTV_STRING){
        return DirectFunctionCall1(LWGEOM_in, CStringGetDatum(agtv->val.string.val));
    }  else
        cannot_cast_gtype_value(agtv->type, "geography");

    // unreachable
    return CStringGetDatum("");
}


/*
 * Emit correct, translatable cast error message
 */
static void
cannot_cast_gtype_value(enum gtype_value_type type, const char *sqltype) {
    static const struct {
        enum gtype_value_type type;
        const char *msg;
    } messages[] = {
        {AGTV_NULL, gettext_noop("cannot cast gtype null to type %s")},
        {AGTV_STRING, gettext_noop("cannot cast gtype string to type %s")},
        {AGTV_NUMERIC, gettext_noop("cannot cast gtype numeric to type %s")},
        {AGTV_INTEGER, gettext_noop("cannot cast gtype integer to type %s")},
        {AGTV_FLOAT, gettext_noop("cannot cast gtype float to type %s")},
        {AGTV_BOOL, gettext_noop("cannot cast gtype boolean to type %s")},
	{AGTV_TIMESTAMP, gettext_noop("cannot cast gtype timestamp to type %s")},
	{AGTV_TIMESTAMPTZ, gettext_noop("cannot cast gtype timestamptz to type %s")},
	{AGTV_DATE, gettext_noop("cannot cast gtype date to type %s")},
	{AGTV_TIME, gettext_noop("cannot cast gtype time to type %s")},
	{AGTV_TIMETZ, gettext_noop("cannot cast gtype timetz to type %s")},
        {AGTV_INTERVAL, gettext_noop("cannot cast gtype interval to type %s")},
	{AGTV_ARRAY, gettext_noop("cannot cast gtype array to type %s")},
        {AGTV_OBJECT, gettext_noop("cannot cast gtype object to type %s")},
        {AGTV_BINARY,
         gettext_noop("cannot cast gtype array or object to type %s")}};

    for (int i = 0; i < lengthof(messages); i++) {
        if (messages[i].type == type) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(messages[i].msg, sqltype)));
        }
    }

    elog(ERROR, "unknown gtype type: %d", (int)type);
}
