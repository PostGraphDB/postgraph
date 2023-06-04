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
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"

#include "utils/agtype.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"
#include "utils/numeric.h"

#define int8_to_int4 int84
#define int8_to_int2 int82

#define float8_to_int8 dtoi8
#define float8_to_int4 dtoi4
#define float8_to_int2 dtoi2

#define numeric_to_int8 numeric_int8
#define numeric_to_int4 numeric_int4
#define numeric_to_int2 numeric_int2

#define string_to_int8 int8in
#define string_to_int4 int4in
#define string_to_int2 int2in

typedef Datum (*coearce_function) (agtype_value *);
static Datum convert_to_scalar(coearce_function func, agtype *agt, char *type);
static ArrayType *agtype_to_array(coearce_function func, agtype *agt, char *type); 

static Datum agtype_to_int8_internal(agtype_value *agtv);
static Datum agtype_to_int4_internal(agtype_value *agtv);

static void cannot_cast_agtype_value(enum agtype_value_type type, const char *sqltype);

Datum convert_to_scalar(coearce_function func, agtype *agt, char *type) {
    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot cast non-scalar agtype to %s", type)));

    agtype_value *agtv = get_ith_agtype_value_from_container(&agt->root, 0);

    Datum d = func(agtv);

    return d;
}

/*
 * agtype to other agtype functions
 */

PG_FUNCTION_INFO_V1(agtype_tointeger);
Datum
agtype_tointeger(PG_FUNCTION_ARGS) {
    agtype *agt = AG_GET_ARG_AGTYPE_P(0);

    if (is_agtype_null(agt))
        PG_RETURN_NULL();

    agtype_value agtv = {
        .type = AGTV_INTEGER,
        .val.int_value = DatumGetInt64(convert_to_scalar(agtype_to_int8_internal, agt, "agtype integer"))
    };

    PG_FREE_IF_COPY(agt, 0);

    AG_RETURN_AGTYPE_P(agtype_value_to_agtype(&agtv));
}

/*
 * agtype to postgres functions
 */
PG_FUNCTION_INFO_V1(agtype_to_int8);
// agtype -> int8.
Datum
agtype_to_int8(PG_FUNCTION_ARGS) {
    agtype *agt = AG_GET_ARG_AGTYPE_P(0);

    if (is_agtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(agtype_to_int8_internal, agt, "int8");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

PG_FUNCTION_INFO_V1(agtype_to_int4);
// agtype -> int4
Datum
agtype_to_int4(PG_FUNCTION_ARGS) {
    agtype *agt = AG_GET_ARG_AGTYPE_P(0);

    if (is_agtype_null(agt))
        PG_RETURN_NULL();

    Datum d = convert_to_scalar(agtype_to_int4_internal, agt, "int4");

    PG_FREE_IF_COPY(agt, 0);

    PG_RETURN_DATUM(d);
}

/*
 * agtype to postgres array functions
 */

PG_FUNCTION_INFO_V1(agtype_to_int8_array);
// agtype -> int8[]
Datum
agtype_to_int8_array(PG_FUNCTION_ARGS) {
    agtype *agt = AG_GET_ARG_AGTYPE_P(0);

    ArrayType *result = agtype_to_array(agtype_to_int8_internal, agt, "int8[]");

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

PG_FUNCTION_INFO_V1(agtype_to_int4_array);
// agtype -> int4[]
Datum agtype_to_int4_array(PG_FUNCTION_ARGS) {
    agtype *agt = AG_GET_ARG_AGTYPE_P(0);
    
    ArrayType *result = agtype_to_array(agtype_to_int4_internal, agt, "int4[]");

    PG_FREE_IF_COPY(agt, 0);
    
    PG_RETURN_ARRAYTYPE_P(result);
}

static ArrayType *
agtype_to_array(coearce_function func, agtype *agt, char *type) {
    agtype_value agtv;
    Datum *array_value;

    agtype_iterator *agtype_iterator = agtype_iterator_init(&agt->root);
    agtype_iterator_token agtv_token = agtype_iterator_next(&agtype_iterator, &agtv, false);

    if (agtv.type != AGTV_ARRAY)
        cannot_cast_agtype_value(agtv.type, type);

    array_value = (Datum *) palloc(sizeof(Datum) * AGT_ROOT_COUNT(agt));

    int i = 0;
    while ((agtv_token = agtype_iterator_next(&agtype_iterator, &agtv, true)) != WAGT_END_ARRAY)
        array_value[i++] = func(&agtv);

    ArrayType *result = construct_array(array_value, AGT_ROOT_COUNT(agt), INT4OID, 4, true, 'i');

    return result;
}

/*
 * internal scalar conversion functions
 */
static Datum
agtype_to_int8_internal(agtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return Int64GetDatum(agtv->val.int_value);
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int8, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int8, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int8, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_agtype_value(agtv->type, "int8");

    // cannot reach
    return 0;
}

static Datum
agtype_to_int4_internal(agtype_value *agtv) {
    if (agtv->type == AGTV_INTEGER)
        return DirectFunctionCall1(int8_to_int4, Int64GetDatum(agtv->val.int_value));
    else if (agtv->type == AGTV_FLOAT)
        return DirectFunctionCall1(float8_to_int4, Float8GetDatum(agtv->val.float_value));
    else if (agtv->type == AGTV_NUMERIC)
        return DirectFunctionCall1(numeric_to_int4, NumericGetDatum(agtv->val.numeric));
    else if (agtv->type == AGTV_STRING)
        return DirectFunctionCall1(string_to_int4, CStringGetDatum(agtv->val.string.val));
    else
        cannot_cast_agtype_value(agtv->type, "int4");

    // cannot reach
    return 0;
}

/*
 * Emit correct, translatable cast error message
 */
static void
cannot_cast_agtype_value(enum agtype_value_type type, const char *sqltype) {
    static const struct {
        enum agtype_value_type type;
        const char *msg;
    } messages[] = {
        {AGTV_NULL, gettext_noop("cannot cast agtype null to type %s")},
        {AGTV_STRING, gettext_noop("cannot cast agtype string to type %s")},
        {AGTV_NUMERIC, gettext_noop("cannot cast agtype numeric to type %s")},
        {AGTV_INTEGER, gettext_noop("cannot cast agtype integer to type %s")},
        {AGTV_FLOAT, gettext_noop("cannot cast agtype float to type %s")},
        {AGTV_BOOL, gettext_noop("cannot cast agtype boolean to type %s")},
        {AGTV_ARRAY, gettext_noop("cannot cast agtype array to type %s")},
        {AGTV_OBJECT, gettext_noop("cannot cast agtype object to type %s")},
        {AGTV_VERTEX, gettext_noop("cannot cast agtype vertex to type %s")},
        {AGTV_EDGE, gettext_noop("cannot cast agtype edge to type %s")},
        {AGTV_PATH, gettext_noop("cannot cast agtype path to type %s")},
        {AGTV_BINARY,
         gettext_noop("cannot cast agtype array or object to type %s")}};

    for (int i = 0; i < lengthof(messages); i++) {
        if (messages[i].type == type) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(messages[i].msg, sqltype)));
        }
    }

    elog(ERROR, "unknown agtype type: %d", (int)type);
}
