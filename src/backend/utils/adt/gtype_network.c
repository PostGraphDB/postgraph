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
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/inet.h"

#include "utils/gtype.h"

PG_FUNCTION_INFO_V1(gtype_abbrev);
Datum
gtype_abbrev(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    char *str;
    if (val->type == AGTV_INET)
         str = TextDatumGetCString(DirectFunctionCall1(inet_abbrev, InetPGetDatum(&val->val.inet)));
    else if (val->type == AGTV_CIDR)
         str = TextDatumGetCString(DirectFunctionCall1(cidr_abbrev, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_STRING;
    gtv.val.string.val = str;
    gtv.val.string.len = strlen(str);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_broadcast);
Datum
gtype_broadcast(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    inet *i;
    if (val->type == AGTV_INET)
         i = DatumGetInetPP(DirectFunctionCall1(network_broadcast, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_family);
Datum
gtype_family(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    int *i;
    if (val->type == AGTV_INET)
         i = DatumGetInt32(DirectFunctionCall1(network_family, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INTEGER;
    gtv.val.int_value = i;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_host);
Datum
gtype_host(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    char *str;
    if (val->type == AGTV_INET)
         str = TextDatumGetCString(DirectFunctionCall1(network_host, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_STRING;
    gtv.val.string.val = str;
    gtv.val.string.len = strlen(str);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_hostmask);
Datum
gtype_hostmask(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    inet *i;
    if (val->type == AGTV_INET)
         i = DatumGetInetPP(DirectFunctionCall1(network_hostmask, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);


    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_inet_merge);
Datum
gtype_inet_merge(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    inet *i;
    if (lhs_value->type == AGTV_INET && rhs_value->type == AGTV_INET)
         i = DatumGetInetPP(DirectFunctionCall2(inet_merge,
                                    InetPGetDatum(&lhs_value->val.inet),
                                    InetPGetDatum(&rhs_value->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for  inet_merge(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_inet_same_family);
Datum
gtype_inet_same_family(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    bool b;
    if (lhs_value->type == AGTV_INET && rhs_value->type == AGTV_INET)
         b = DatumGetBool(DirectFunctionCall2(inet_same_family,
                                    InetPGetDatum(&lhs_value->val.inet),
                                    InetPGetDatum(&rhs_value->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for inet_same_family(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_BOOL;
    gtv.val.boolean = b;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_masklen);
Datum
gtype_masklen(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    int *i;
    if (val->type == AGTV_INET)
         i = DatumGetInt32(DirectFunctionCall1(network_family, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for masklen(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INTEGER;
    gtv.val.int_value = i;

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_netmask);
Datum
gtype_netmask(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    inet *i;
    if (val->type == AGTV_INET)
         i = DatumGetInetPP(DirectFunctionCall1(network_netmask, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);


    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_network);
Datum
gtype_network(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    inet *i;
    if (val->type == AGTV_INET)
         i = DatumGetInetPP(DirectFunctionCall1(network_network, InetPGetDatum(&val->val.inet)));
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid input for abbrev(gtype)"), errhint("You may have to use explicit casts.")));

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, i, sizeof(char) * 22);


    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_set_masklen);
Datum
gtype_set_masklen(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (is_gtype_null(lhs) || is_gtype_null(rhs))
        PG_RETURN_NULL();

    gtype_value *lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
    gtype_value *rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

    if (rhs_value->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for set_masklen arg 2 must be an integer"),
                        errhint("You may have to use explicit casts.")));

    Datum len = gtype_to_int4_internal(rhs_value);

    gtype_value gtv;
    inet *i;
    if (lhs_value->type == AGTV_INET) {
        i = DatumGetInetPP(DirectFunctionCall2(inet_set_masklen, InetPGetDatum(&lhs_value->val.inet), len));
        gtv.type = AGTV_INET;
        memcpy(&gtv.val.inet, i, sizeof(char) * 22);
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else if (lhs_value->type == AGTV_CIDR) {
        i = DatumGetInetPP(DirectFunctionCall2(cidr_set_masklen, InetPGetDatum(&lhs_value->val.inet), len));
        gtv.type = AGTV_CIDR;
        memcpy(&gtv.val.inet, i, sizeof(char) * 22);
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for set_masklen(gtype, gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    //unreachable
    PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(gtype_trunc);
Datum
gtype_trunc(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    gtype_value gtv;
    if (val->type == AGTV_MAC) {
        macaddr *m = DatumGetMacaddrP(DirectFunctionCall1(macaddr_trunc, MacaddrPGetDatum(&val->val.mac)));
        gtv.type = AGTV_MAC8;
        memcpy(&gtv.val.inet, m, sizeof(char) * 6);
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else if (val->type == AGTV_MAC8) {
        macaddr8 *m = DatumGetMacaddr8P(DirectFunctionCall1(macaddr8_trunc, Macaddr8PGetDatum(&val->val.mac8)));
        gtv.type = AGTV_MAC8;
        memcpy(&gtv.val.inet, m, sizeof(char) * 8);
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for trunc(gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    //unreachable
    PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(gtype_macaddr8_set7bit);
Datum
gtype_macaddr8_set7bit(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(gt))
        PG_RETURN_NULL();

    gtype_value *val = get_ith_gtype_value_from_container(&gt->root, 0);

    gtype_value gtv;
    if (val->type == AGTV_MAC8) {
        macaddr8 *m = DatumGetMacaddr8P(DirectFunctionCall1(macaddr8_set7bit, Macaddr8PGetDatum(&val->val.mac8)));
        gtv.type = AGTV_MAC8;
        memcpy(&gtv.val.inet, m, sizeof(char) * 8);
        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid input for trunc(gtype)"),
                        errhint("You may have to use explicit casts.")));
    }

    //unreachable
    PG_RETURN_NULL();
}


