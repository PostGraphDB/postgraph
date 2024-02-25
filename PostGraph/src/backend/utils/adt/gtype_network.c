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
#include "utils/gtype_typecasting.h"

PG_FUNCTION_INFO_V1(gtype_abbrev);
Datum
gtype_abbrev(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    Datum d;
    if (GT_IS_CIDR(gt))
        d = DirectFunctionCall1(cidr_abbrev, GT_TO_CIDR_DATUM(gt));
    else 
        d = DirectFunctionCall1(inet_abbrev, GT_TO_INET_DATUM(gt));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_broadcast);
Datum
gtype_broadcast(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_broadcast, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_family);
Datum
gtype_family(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_family, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv = { .type = AGTV_INTEGER, .val.int_value = DatumGetInt32(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
  
}

PG_FUNCTION_INFO_V1(gtype_host);
Datum
gtype_host(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_host, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(DatumGetTextP(d)) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_hostmask);
Datum
gtype_hostmask(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_hostmask, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_inet_merge);
Datum
gtype_inet_merge(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall2(inet_merge, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1));

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_inet_same_family);
Datum
gtype_inet_same_family(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall2(inet_same_family, GT_ARG_TO_INET_DATUM(0), GT_ARG_TO_INET_DATUM(1));

    gtype_value gtv = { .type = AGTV_BOOL, .val.int_value = DatumGetBool(d) };
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_masklen);
Datum
gtype_masklen(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_masklen, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv = { .type = AGTV_INTEGER, .val.int_value = DatumGetInt32(d) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_netmask);
Datum
gtype_netmask(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_netmask, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv;
    gtv.type = AGTV_INET;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_network);
Datum
gtype_network(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(network_network, GT_ARG_TO_INET_DATUM(0));

    gtype_value gtv;
    gtv.type = AGTV_CIDR;
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_set_masklen);
Datum
gtype_set_masklen(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);
    gtype_value gtv;
    Datum d;
    if (GT_IS_CIDR(gt)) {
        d = DirectFunctionCall2(cidr_set_masklen, GT_TO_CIDR_DATUM(gt), GT_ARG_TO_INT4_DATUM(1));
        gtv.type = AGTV_CIDR;
    } else {
        d = DirectFunctionCall2(inet_set_masklen, GT_TO_INET_DATUM(gt), GT_ARG_TO_INT4_DATUM(1));
       gtv.type = AGTV_INET;
    }
    
    memcpy(&gtv.val.inet, DatumGetInetPP(d), sizeof(char) * 22);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_trunc);
Datum
gtype_trunc(PG_FUNCTION_ARGS) {
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    gtype_value gtv;
    Datum d;
    if (GT_IS_MACADDR(gt)) {
        d = DirectFunctionCall1(macaddr_trunc, GT_TO_MAC_DATUM(gt));
        gtv.type = AGTV_MAC;
    } else {
        d = DirectFunctionCall1(macaddr8_trunc, GT_TO_MAC8_DATUM(gt));
       gtv.type = AGTV_MAC8;
    }
    
    memcpy(&gtv.val.inet, DatumGetMacaddrP(d), sizeof(char) * 6);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


PG_FUNCTION_INFO_V1(gtype_macaddr8_set7bit);
Datum
gtype_macaddr8_set7bit(PG_FUNCTION_ARGS) {
    Datum d = DirectFunctionCall1(macaddr8_set7bit, GT_ARG_TO_MAC8_DATUM(0));

    gtype_value gtv;
    gtv.type = AGTV_MAC8;
    memcpy(&gtv.val.inet, DatumGetMacaddr8P(d), sizeof(char) * 8);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}


