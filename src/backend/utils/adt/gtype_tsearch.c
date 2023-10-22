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
 */

#include "postgraph.h"
    
// Postgres
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "tsearch/ts_utils.h"

// PostGraph
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"


PG_FUNCTION_INFO_V1(gtype_ts_delete);
Datum gtype_ts_delete(PG_FUNCTION_ARGS) {
    TSVector tsvector = DatumGetPointer(DirectFunctionCall2(tsvector_delete_str,
			                                    GT_ARG_TO_TSVECTOR_DATUM(0),
							    GT_ARG_TO_TEXT_DATUM(1)));

    gtype_value gtv = { .type = AGTV_TSVECTOR, .val.tsvector = tsvector };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}
