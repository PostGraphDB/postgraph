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


PG_FUNCTION_INFO_V1(gtype_tsquery_or);
Datum gtype_tsquery_or(PG_FUNCTION_ARGS) {
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (GT_IS_TSQUERY(lhs) || GT_IS_TSQUERY(rhs)) {
        TSQuery tsquery = DatumGetPointer(DirectFunctionCall2(tsquery_or,
                                                              GT_ARG_TO_TSQUERY_DATUM(0),
	  	  	  	  	  	  	      GT_ARG_TO_TSQUERY_DATUM(1)));

        gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsquery = tsquery };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
    } else {
        TSVector tsvector = DatumGetPointer(DirectFunctionCall2(tsvector_concat,
                                                              GT_ARG_TO_TSVECTOR_DATUM(0),
                                                              GT_ARG_TO_TSVECTOR_DATUM(1)));

        gtype_value gtv = { .type = AGTV_TSVECTOR, .val.tsvector = tsvector };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

    }
}


/*
 * Text Search Functions
 */
PG_FUNCTION_INFO_V1(gtype_ts_delete);
Datum gtype_ts_delete(PG_FUNCTION_ARGS) {
    TSVector tsvector = DatumGetPointer(DirectFunctionCall2(tsvector_delete_str,
			                                    GT_ARG_TO_TSVECTOR_DATUM(0),
							    GT_ARG_TO_TEXT_DATUM(1)));

    gtype_value gtv = { .type = AGTV_TSVECTOR, .val.tsvector = tsvector };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_ts_strip);
Datum gtype_ts_strip(PG_FUNCTION_ARGS) {
    TSVector tsvector = DatumGetPointer(DirectFunctionCall1(tsvector_strip, GT_ARG_TO_TSVECTOR_DATUM(0)));

    gtype_value gtv = { .type = AGTV_TSVECTOR, .val.tsvector = tsvector };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tsquery_phrase);
Datum gtype_tsquery_phrase(PG_FUNCTION_ARGS) {
    TSQuery tsquery = DatumGetPointer(DirectFunctionCall3(tsquery_phrase_distance,
                                                            GT_ARG_TO_TSQUERY_DATUM(0),
                                                            GT_ARG_TO_TSQUERY_DATUM(1),
							    Int32GetDatum(1)));

    gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsvector = tsquery };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_tsquery_phrase_distance);
Datum gtype_tsquery_phrase_distance(PG_FUNCTION_ARGS) {
    TSQuery tsquery = DatumGetPointer(DirectFunctionCall3(tsquery_phrase_distance,
                                                            GT_ARG_TO_TSQUERY_DATUM(0),
                                                            GT_ARG_TO_TSQUERY_DATUM(1),
                                                            GT_ARG_TO_INT4_DATUM(2)));

    gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsvector = tsquery };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_plainto_tsquery);
Datum gtype_plainto_tsquery(PG_FUNCTION_ARGS) {
    TSQuery tsquery = DatumGetPointer(DirectFunctionCall1(plainto_tsquery, GT_ARG_TO_TEXT_DATUM(0)));

    gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsvector = tsquery };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_phraseto_tsquery);
Datum gtype_phraseto_tsquery(PG_FUNCTION_ARGS) {
    TSQuery tsquery = DatumGetPointer(DirectFunctionCall1(phraseto_tsquery, GT_ARG_TO_TEXT_DATUM(0)));

    gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsvector = tsquery };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_websearch_to_tsquery);
Datum gtype_websearch_to_tsquery(PG_FUNCTION_ARGS) {
    TSQuery tsquery = DatumGetPointer(DirectFunctionCall1(websearch_to_tsquery, GT_ARG_TO_TEXT_DATUM(0)));

    gtype_value gtv = { .type = AGTV_TSQUERY, .val.tsvector = tsquery };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

