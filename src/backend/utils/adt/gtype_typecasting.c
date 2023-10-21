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

#include "postgraph.h"

// Postgres
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
#include "utils/palloc.h"

// PostGraph
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

    gtype_value *gtv = get_ith_gtype_value_from_container(&agt->root, 0);

    Datum d = func(gtv);

    return d;
}

Datum integer_to_gtype(int64 i) {
    gtype_value gtv;
    gtype *agt;

    gtv.type = AGTV_INTEGER;
    gtv.val.int_value = i;
    agt = gtype_value_to_gtype(&gtv);

    return GTYPE_P_GET_DATUM(agt);
}

Datum float_to_gtype(float8 f) {
    gtype_value gtv;
    gtype *agt;

    gtv.type = AGTV_FLOAT;
    gtv.val.float_value = f;
    agt = gtype_value_to_gtype(&gtv);

    return GTYPE_P_GET_DATUM(agt);
}

/*
 * s must be a UTF-8 encoded, unescaped, and null-terminated string which is
 * a valid string for internal storage of gtype.
 */
Datum string_to_gtype(char *s) {
    gtype_value gtv;
    gtype *agt;

    gtv.type = AGTV_STRING;
    gtv.val.string.len = check_string_length(strlen(s));
    gtv.val.string.val = s;
    agt = gtype_value_to_gtype(&gtv);

    return GTYPE_P_GET_DATUM(agt);
}

Datum boolean_to_gtype(bool b) {
    gtype_value gtv;
    gtype *agt;

    gtv.type = AGTV_BOOL;
    gtv.val.boolean = b;
    agt = gtype_value_to_gtype(&gtv);

    return GTYPE_P_GET_DATUM(agt);
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

    gtype_value gtv = {
        .type = AGTV_INTEGER,
        .val.int_value = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, agt, "gtype integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tofloat);
Datum
gtype_tofloat(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    gtype_value gtv = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(convert_to_scalar(gtype_to_float8_internal, agt, "gtype float"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_tonumeric);
// gtype -> gtype numeric
Datum
gtype_tonumeric(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();


    gtype_value gtv = {
        .type = AGTV_NUMERIC,
        .val.numeric = DatumGetNumeric(convert_to_scalar(gtype_to_numeric_internal, agt, "gtype numeric"))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tostring);
Datum
gtype_tostring(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    char *string = DatumGetCString(convert_to_scalar(gtype_to_string_internal, agt, "string"));
    
    gtype_value gtv; 
    gtv.type = AGTV_STRING;
    gtv.val.string.val = string;
    gtv.val.string.len = strlen(string);

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_tobytea);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tobytea(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    bytea *bytea = DatumGetPointer(DirectFunctionCall1(byteain,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_BYTEA;
    gtv.val.bytea = bytea;
    
    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_toboolean);
Datum
gtype_toboolean(PG_FUNCTION_ARGS) {

    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_boolean_internal, agt, "bool");

    PG_FREE_IF_COPY(agt, 0);

    gtype_value gtv = { .type = AGTV_BOOL, .val.boolean = BoolGetDatum(d) };

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_totimestamp);
Datum
gtype_totimestamp(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    int64 ts = DatumGetInt64(convert_to_scalar(gtype_to_timestamp_internal, agt, "timestamp"));

    gtype_value gtv;
    gtv.type = AGTV_TIMESTAMP;
    gtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_TIMESTAMPTZ;
    gtv.val.int_value = ts;

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_TIME;
    gtv.val.int_value = (int64)t;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_TIMETZ;
    gtv.val.timetz.time = t->time;
    gtv.val.timetz.zone = t->zone;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
} 

PG_FUNCTION_INFO_V1(tointerval);
Datum tointerval(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *gtv = get_ith_gtype_value_from_container(&agt->root, 0);
    Interval *i;

    if (gtv->type == AGTV_NULL)
        PG_RETURN_NULL();

    if (gtv->type == AGTV_INTERVAL)
        AG_RETURN_GTYPE_P(agt);

    if (gtv->type != AGTV_STRING)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("typecastint to interval must be a string")));

    i = DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum(gtv->val.string.val),
                                                           ObjectIdGetDatum(InvalidOid),
                                                           Int32GetDatum(-1)));
    gtv->type = AGTV_INTERVAL;
    gtv->val.interval.time = i->time;
    gtv->val.interval.day = i->day;
    gtv->val.interval.month = i->month;

    PG_RETURN_POINTER(gtype_value_to_gtype(gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_DATE;
    gtv.val.date = dte;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}  

PG_FUNCTION_INFO_V1(tovector);
/*                  
 * Execute function to typecast an agtype to an agtype timestamp
 */                 
Datum tovector(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *gtv = get_ith_gtype_value_from_container(&agt->root, 0);

    if (gtv->type == NULL)
        PG_RETURN_NULL();


    if (gtv->type != AGTV_STRING)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("typecastint to vector must be a string")));

    gtv = gtype_vector_in(gtv->val.string.val, -1);

    PG_RETURN_POINTER(gtype_value_to_gtype(gtv));
}

Datum _gtype_toinet(Datum arg){
    gtype *agt = DATUM_GET_GTYPE_P(arg);

    if (is_gtype_null(agt))
        return NULL;

    inet *i = DatumGetInetPP(convert_to_scalar(gtype_to_inet_internal, agt, "inet"));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    return GTYPE_P_GET_DATUM(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_MAC;
    memcpy(&gtv.val.mac, mac, sizeof(char) * 6);


    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_MAC8;
    memcpy(&gtv.val.mac, mac, sizeof(char) * 8);

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_topoint);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_topoint(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Point *point = DatumGetPointer(DirectFunctionCall1(point_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_POINT;
    gtv.val.point = point;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tolseg);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tolseg(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    LSEG *lseg = DatumGetPointer(DirectFunctionCall1(lseg_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_LSEG;
    gtv.val.lseg = lseg;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_topath);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_topath(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    PATH *path = DatumGetPointer(DirectFunctionCall1(path_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_PATH;
    gtv.val.path = path;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_toline);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_toline(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    LINE *line = DatumGetPointer(DirectFunctionCall1(line_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_LINE;
    gtv.val.path = line;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_topolygon);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_topolygon(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    POLYGON *polygon = DatumGetPointer(DirectFunctionCall1(poly_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_POLYGON;
    gtv.val.polygon = polygon;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tocircle);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tocircle(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    CIRCLE *circle = DatumGetPointer(DirectFunctionCall1(circle_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_CIRCLE;
    gtv.val.circle = circle;
    
    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}



PG_FUNCTION_INFO_V1(gtype_tobox);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tobox(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    BOX *box = DatumGetPointer(DirectFunctionCall1(box_in,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string")));

    gtype_value gtv;
    gtv.type = AGTV_BOX;
    gtv.val.box = box;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_BOX2D;
    memcpy(&gtv.val.gbox, box, sizeof(GBOX));

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_BOX3D;
    memcpy(&gtv.val.box3d, box, sizeof(BOX3D));

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    SPHEROID *spheroid = convert_to_scalar(gtype_to_spheroid_internal, agt, "spheroid");

    gtype_value gtv;
    gtv.type = AGTV_SPHEROID;
    memcpy(&gtv.val.spheroid, spheroid, sizeof(SPHEROID));

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_GSERIALIZED;
    gtv.val.gserialized = gserial;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_TSVECTOR;
    gtv.val.gserialized = tsvector;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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

    gtype_value gtv;
    gtv.type = AGTV_TSQUERY;
    gtv.val.tsquery = tsquery;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


Datum
PostGraphDirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2,
                                                Datum arg3)
{
        LOCAL_FCINFO(fcinfo, 3);
	fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
	
        Datum           result;

        InitFunctionCallInfoData(*fcinfo, NULL, 3, collation, NULL, NULL);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
        fcinfo->flinfo->fn_addr = func;
        fcinfo->flinfo->fn_oid = InvalidOid;
        fcinfo->flinfo->fn_strict = false;
        fcinfo->flinfo->fn_retset = false;
        fcinfo->flinfo->fn_extra = NULL;
        fcinfo->flinfo->fn_mcxt = CurrentMemoryContext;
        fcinfo->flinfo->fn_expr = NULL;

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].value = arg2;
        fcinfo->args[1].isnull = false;
        fcinfo->args[2].value = arg3;
        fcinfo->args[2].isnull = false;

        result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull)
                elog(ERROR, "function %p returned NULL", (void *) func);

        return result;
}

PG_FUNCTION_INFO_V1(gtype_tointrange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tointrange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();
						  
    RangeType *range = DatumGetPointer(convert_to_scalar(gtype_to_int_range_internal, agt, "int_range"));
    gtype_value gtv;
    gtv.type = AGTV_RANGE_INT;
    gtv.val.range = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tointmultirange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tointmultirange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(INT8MULTIRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_INT_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_tonumrange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tonumrange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(NUMRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_NUM;
    gtv.val.range = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tonummultirange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_tonummultirange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(NUMMULTIRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_NUM_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_totsrange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totsrange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_TS;
    gtv.val.range = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_totsmultirange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totsmultirange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSMULTIRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_TS_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_totstzrange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totstzrange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSTZRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_TS;
    gtv.val.range = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_totstzmultirange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_totstzmultirange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(TSTZMULTIRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_TSTZ_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_todaterange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_todaterange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    RangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(DATERANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_DATE;
    gtv.val.range = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_todatemultirange);
/*
 * Execute function to typecast an agtype to an agtype timestamp
 */
Datum gtype_todatemultirange(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    MultirangeType *range = DatumGetPointer(PostGraphDirectFunctionCall3Coll(multirange_in, DEFAULT_COLLATION_OID,
                                                  convert_to_scalar(gtype_to_string_internal, agt, "string"),
                                                  ObjectIdGetDatum(DATEMULTIRANGEOID), Int32GetDatum(1)));

    gtype_value gtv;
    gtv.type = AGTV_RANGE_DATE_MULTI;
    gtv.val.multirange = range;

    PG_RETURN_POINTER(gtype_value_to_gtype(&gtv));
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
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(gtype_to_boolean_internal, agt, "bool");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
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
    AG_RETURN_GTYPE_P(boolean_to_gtype(PG_GETARG_BOOL(0)));
}

PG_FUNCTION_INFO_V1(float8_to_gtype);
// float8 -> gtype
Datum
float8_to_gtype(PG_FUNCTION_ARGS) {
    AG_RETURN_GTYPE_P(float_to_gtype(PG_GETARG_FLOAT8(0)));
}

PG_FUNCTION_INFO_V1(int8_to_gtype);
// int8 -> gtype.
Datum
int8_to_gtype(PG_FUNCTION_ARGS) {
    AG_RETURN_GTYPE_P(integer_to_gtype(PG_GETARG_INT64(0)));
}

PG_FUNCTION_INFO_V1(text_to_gtype);
//text -> gtype
Datum
text_to_gtype(PG_FUNCTION_ARGS) {
    Datum txt = PG_GETARG_DATUM(0);

    AG_RETURN_GTYPE_P(string_to_gtype(TextDatumGetCString(txt)));
}

PG_FUNCTION_INFO_V1(timestamp_to_gtype);
//timestamp -> gtype
Datum
timestamp_to_gtype(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMP(0);

    gtype_value gtv;
    gtv.type = AGTV_TIMESTAMP;
    gtv.val.int_value = ts;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(timestamptz_to_gtype);
//timestamptz -> gtype
Datum
timestamptz_to_gtype(PG_FUNCTION_ARGS) {
    int64 ts = PG_GETARG_TIMESTAMPTZ(0);
    
    gtype_value gtv;
    gtv.type = AGTV_TIMESTAMPTZ;
    gtv.val.int_value = ts;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(date_to_gtype);
//date -> gtype
Datum
date_to_gtype(PG_FUNCTION_ARGS) {

    gtype_value gtv;
    gtv.type = AGTV_DATE;
    gtv.val.date = PG_GETARG_DATEADT(0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(time_to_gtype);
//time -> gtype
Datum
time_to_gtype(PG_FUNCTION_ARGS) {
    gtype_value gtv;
    gtv.type = AGTV_TIME;
    gtv.val.int_value = PG_GETARG_TIMEADT(0);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(timetz_to_gtype);
//timetz -> gtype
Datum
timetz_to_gtype(PG_FUNCTION_ARGS) {
    TimeTzADT *t = PG_GETARG_TIMETZADT_P(0);

    gtype_value gtv;
    gtv.type = AGTV_TIMETZ;
    gtv.val.timetz.time = t->time;
    gtv.val.timetz.zone = t->zone;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
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
    gtype_value gtv;
    Datum *array_value;

    gtype_iterator *gtype_iterator = gtype_iterator_init(&agt->root);
    gtype_iterator_token gtoken = gtype_iterator_next(&gtype_iterator, &gtv, false);

    if (gtv.type != AGTV_ARRAY && gtv.type != AGTV_VECTOR)
        cannot_cast_gtype_value(gtv.type, type);

    array_value = (Datum *) palloc(sizeof(Datum) * AGT_ROOT_COUNT(agt));

    int i = 0;
    while ((gtoken = gtype_iterator_next(&gtype_iterator, &gtv, true)) < WGT_END_ARRAY)
        array_value[i++] = func(&gtv);

    ArrayType *result = construct_array(array_value, AGT_ROOT_COUNT(agt), type_oid, type_len, elembyval, 'i');

    return result;
}

/*
 * internal scalar conversion functions
 */
Datum
gtype_to_int8_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER)
        return Int64GetDatum(gtv->val.int_value);
    else if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int8, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int8, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int8, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "int8");

    // cannot reach
    return 0;
}

Datum
gtype_to_int_range_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_RANGE_INT)
        return PointerGetDatum(gtv->val.range);
    else if (gtv->type == AGTV_STRING)
        return DatumGetPointer(PostGraphDirectFunctionCall3Coll(range_in, DEFAULT_COLLATION_OID,
                                                  CStringGetDatum(gtv->val.string.val),
                                                  ObjectIdGetDatum(INT8RANGEOID), Int32GetDatum(1)));
    else
        cannot_cast_gtype_value(gtv->type, "int8");

    // cannot reach
    return 0;
}

Datum
gtype_to_int4_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_int4, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int4, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int4, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int4, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "int4");

    // cannot reach
    return 0;
}

Datum
gtype_to_int2_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_int2, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int2, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int2, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int2, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "int2");

    // cannot reach
    return 0;
}

Datum
gtype_to_float8_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_FLOAT)
        return Float8GetDatum(gtv->val.float_value);
    else if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(i8tod, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_float8, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(float8in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "float8");

    // unreachable
    return 0;
}


Datum
gtype_to_boolean_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_BOOL) {
        return BoolGetDatum(gtv->val.boolean);
    } else if (gtv->type == AGTV_STRING) {
        char *string = gtv->val.string.val;
        int len = gtv->val.string.len;

        if (!pg_strncasecmp(string, "true", len))
            return BoolGetDatum(true);
        else if (!pg_strncasecmp(string, "false", len))
            return BoolGetDatum(false);
    }

    cannot_cast_gtype_value(gtv->type, "boolean");

    // unreachable
    return 0;
}


Datum
gtype_to_float4_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(dtof, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(i8tof, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_float4, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(float4in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "float4");

    // unreachable
    return 0;
}

Datum
gtype_to_numeric_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_numeric, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_numeric, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_NUMERIC)
        return NumericGetDatum(gtv->val.numeric);
    else if (gtv->type == AGTV_STRING) {
        char *string = (char *) palloc(sizeof(char) * (gtv->val.string.len + 1));
        string = strncpy(string, gtv->val.string.val, gtv->val.string.len);
        string[gtv->val.string.len] = '\0';

        Datum numd = DirectFunctionCall3(string_to_numeric, CStringGetDatum(string), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

        pfree(string);

        return numd;
    } else
        cannot_cast_gtype_value(gtv->type, "numeric");

    // unreachable
    return 0;
}


Datum
gtype_to_text_internal(gtype_value *gtv) {
    return CStringGetTextDatum(DatumGetCString(gtype_to_string_internal(gtv)));
}

Datum
gtype_to_string_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_string, Int64GetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_string, Float8GetDatum(gtv->val.float_value));
    else if (gtv->type == AGTV_STRING)
        return CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len));
    else if (gtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_string, NumericGetDatum(gtv->val.numeric));
    else if (gtv->type == AGTV_BOOL)
        return CStringGetDatum((gtv->val.boolean) ? "true" : "false");
    else if (gtv->type == AGTV_TIMESTAMP)
        return DirectFunctionCall1(timestamp_out, TimestampGetDatum(gtv->val.int_value));
    else
        cannot_cast_gtype_value(gtv->type, "string");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timestamp_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER || gtv->type == AGTV_TIMESTAMP)
        return TimestampGetDatum(gtv->val.int_value);
    else if (gtv->type == AGTV_STRING)
        return TimestampGetDatum(DirectFunctionCall3(timestamp_in,
				          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
					  ObjectIdGetDatum(InvalidOid),
					  Int32GetDatum(-1)));
    else if (gtv->type == AGTV_TIMESTAMPTZ)
        return DirectFunctionCall1(timestamptz_timestamp, TimestampTzGetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_DATE)
        return TimestampGetDatum(date2timestamp_opt_overflow(gtv->val.date, NULL));
    else
        cannot_cast_gtype_value(gtv->type, "timestamp");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timestamptz_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTEGER || gtv->type == AGTV_TIMESTAMPTZ)
        return TimestampGetDatum(gtv->val.int_value);
    else if (gtv->type == AGTV_STRING)
        return TimestampTzGetDatum(DirectFunctionCall3(timestamptz_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == AGTV_TIMESTAMP)
        return DirectFunctionCall1(timestamp_timestamptz, TimestampGetDatum(gtv->val.int_value));
    else if (gtv->type == AGTV_DATE)
        return TimestampGetDatum(date2timestamptz_opt_overflow(gtv->val.date, NULL));
    else
        cannot_cast_gtype_value(gtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}


Datum
gtype_to_date_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_TIMESTAMPTZ)
        return DateADTGetDatum(DirectFunctionCall1(timestamptz_date, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_STRING)
        return DateADTGetDatum(DirectFunctionCall3(date_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == AGTV_TIMESTAMP)
	return DateADTGetDatum(DirectFunctionCall1(timestamp_date, TimestampGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_DATE)
        return DateADTGetDatum(gtv->val.date);
    else
        cannot_cast_gtype_value(gtv->type, "timestamptz");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_time_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_TIMESTAMPTZ)
        return TimeADTGetDatum(DirectFunctionCall1(timestamptz_time, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_STRING)
        return TimeADTGetDatum(DirectFunctionCall3(time_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == AGTV_TIMESTAMP)
        return TimeADTGetDatum(DirectFunctionCall1(timestamp_time, TimestampGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_INTERVAL)
        return TimeADTGetDatum(DirectFunctionCall1(interval_time, IntervalPGetDatum(&gtv->val.interval)));
    else if (gtv->type == AGTV_TIMETZ)
	return TimeADTGetDatum(DirectFunctionCall1(timetz_time, TimeTzADTPGetDatum(&gtv->val.timetz)));
    else if (gtv->type == AGTV_TIME)
	    return TimeADTGetDatum(gtv->val.int_value);
    else
        cannot_cast_gtype_value(gtv->type, "time");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_timetz_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_TIMESTAMPTZ)
        return TimeTzADTPGetDatum(DirectFunctionCall1(timestamptz_timetz, TimestampTzGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_STRING)
        return TimeTzADTPGetDatum(DirectFunctionCall3(timetz_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else if (gtv->type == AGTV_TIME)
        return TimeTzADTPGetDatum(DirectFunctionCall1(time_timetz, TimeADTGetDatum(gtv->val.int_value)));
    else if (gtv->type == AGTV_TIMETZ)
	return TimeTzADTPGetDatum(&gtv->val.timetz);
    else                 
        cannot_cast_gtype_value(gtv->type, "time");
    
    // unreachable
    return CStringGetDatum("");
}  

Datum
gtype_to_interval_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INTERVAL)
        return IntervalPGetDatum(&gtv->val.interval);
    else if (gtv->type == AGTV_STRING)
        return IntervalPGetDatum(DirectFunctionCall3(interval_in,
                                          CStringGetDatum(pnstrdup(gtv->val.string.val, gtv->val.string.len)),
                                          ObjectIdGetDatum(InvalidOid),
                                          Int32GetDatum(-1)));
    else                 
        cannot_cast_gtype_value(gtv->type, "interval");
    
    // unreachable
    return CStringGetDatum("");
}  


Datum
gtype_to_inet_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_INET)
        return InetPGetDatum(&gtv->val.inet);
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(inet_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "inet");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_cidr_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_CIDR)
        return InetPGetDatum(&gtv->val.inet);
    else if (gtv->type == AGTV_INET)
	return DirectFunctionCall1(inet_to_cidr, InetPGetDatum(&gtv->val.inet));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(cidr_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "cidr");
    
    // unreachable
    return CStringGetDatum("");
}   

Datum
gtype_to_macaddr_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_MAC)
	return MacaddrPGetDatum(&gtv->val.mac);
    else if (gtv->type == AGTV_MAC8)
        return DirectFunctionCall1(macaddr8tomacaddr, Macaddr8PGetDatum(&gtv->val.mac8));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(macaddr_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_macaddr8_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_MAC8)
        return Macaddr8PGetDatum(&gtv->val.mac8);
    else if (gtv->type == AGTV_MAC)
        return DirectFunctionCall1(macaddrtomacaddr8, MacaddrPGetDatum(&gtv->val.mac));
    else if (gtv->type == AGTV_STRING)
        return DirectFunctionCall1(macaddr8_in, CStringGetDatum(gtv->val.string.val));
    else
        cannot_cast_gtype_value(gtv->type, "macaddr");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(BOX3D_to_BOX2D);
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX2D);

Datum
gtype_to_box2d_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_BOX2D) {
        return PointerGetDatum(&gtv->val.gbox);
    } else if (gtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX3D_to_BOX2D, PointerGetDatum(&gtv->val.box3d));
    } else if (gtv->type == AGTV_GSERIALIZED) {
        return DirectFunctionCall1(LWGEOM_to_BOX2D, PointerGetDatum(gtv->val.gserialized));
    } else if (gtv->type == AGTV_STRING) {
        return DirectFunctionCall1(BOX2D_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_gtype_value(gtv->type, "box2d");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(BOX2D_to_BOX3D);
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX3D);

Datum
gtype_to_box3d_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_BOX3D) {
        return PointerGetDatum(&gtv->val.box3d);	    
    } else if (gtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX2D_to_BOX3D, PointerGetDatum(&gtv->val.gbox));
    } else if (gtv->type == AGTV_GSERIALIZED) {
        return DirectFunctionCall1(LWGEOM_to_BOX3D, PointerGetDatum(gtv->val.gserialized));
    } else if (gtv->type == AGTV_STRING){
        return DirectFunctionCall1(BOX3D_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_gtype_value(gtv->type, "box3d");

    // unreachable
    return CStringGetDatum("");
}

Datum
gtype_to_spheroid_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_STRING){
        return DirectFunctionCall1(ellipsoid_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_gtype_value(gtv->type, "spheroid");

    // unreachable
    return CStringGetDatum("");
}

PG_FUNCTION_INFO_V1(LWGEOM_in);
PG_FUNCTION_INFO_V1(BOX3D_to_LWGEOM);
PG_FUNCTION_INFO_V1(BOX2D_to_LWGEOM);

Datum
gtype_to_geometry_internal(gtype_value *gtv) {
    if (gtv->type == AGTV_GSERIALIZED) {
	return PointerGetDatum(gtv->val.gserialized);
    } else if (gtv->type == AGTV_BOX3D) {
        return DirectFunctionCall1(BOX3D_to_LWGEOM, PointerGetDatum(&gtv->val.box3d));
    } else if (gtv->type == AGTV_BOX2D) {
        return DirectFunctionCall1(BOX2D_to_LWGEOM, PointerGetDatum(&gtv->val.gbox));
    } else if (gtv->type == AGTV_STRING){
        return DirectFunctionCall1(LWGEOM_in, CStringGetDatum(gtv->val.string.val));
    }  else
        cannot_cast_gtype_value(gtv->type, "geography");

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
