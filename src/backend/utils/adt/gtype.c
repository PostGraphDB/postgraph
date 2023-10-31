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
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 */

/*
 * I/O routines for gtype type
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

// State structure for Percentile aggregate functions 
typedef struct PercentileGroupAggState
{
    // percentile value 
    float8 percentile;
    // Sort object we're accumulating data in: 
    Tuplesortstate *sortstate;
    // Number of normal rows inserted into sortstate: 
    int64 number_of_rows;
    // Have we already done tuplesort_performsort? 
    bool sort_done;
} PercentileGroupAggState;

typedef enum // type categories for datum_to_gtype 
{
    AGT_TYPE_NULL, // null, so we didn't bother to identify 
    AGT_TYPE_BOOL, // boolean (built-in types only) 
    AGT_TYPE_INTEGER, // Cypher Integer type 
    AGT_TYPE_FLOAT, // Cypher Float type 
    AGT_TYPE_NUMERIC, // numeric (ditto) 
    AGT_TYPE_DATE, // we use special formatting for datetimes 
    AGT_TYPE_TIMESTAMP, // we use special formatting for timestamp 
    AGT_TYPE_TIMESTAMPTZ, // ... and timestamptz 
    AGT_TYPE_TIME,
    AGT_TYPE_TIMETZ,
    AGT_TYPE_INTERVAL,
    AGT_TYPE_VECTOR,
    AGT_TYPE_INET,
    AGT_TYPE_CIDR,
    AGT_TYPE_MAC,
    AGT_TYPE_MAC8,
    AGT_TYPE_GTYPE, // GTYPE 
    AGT_TYPE_JSON, // JSON 
    AGT_TYPE_JSONB, // JSONB 
    AGT_TYPE_ARRAY, // array 
    AGT_TYPE_COMPOSITE, // composite 
    AGT_TYPE_JSONCAST, // something with an explicit cast to JSON 
    AGT_TYPE_VERTEX,
    AGT_TYPE_OTHER // all else 
} agt_type_category;

size_t check_string_length(size_t len);
static void gtype_in_object_start(void *pstate);
static void gtype_in_object_end(void *pstate);
static void gtype_in_array_start(void *pstate);
static void gtype_in_array_end(void *pstate);
static void gtype_in_object_field_start(void *pstate, char *fname, bool isnull);
static void gtype_put_array(StringInfo out, gtype_value *scalar_val);
static void gtype_put_object(StringInfo out, gtype_value *scalar_val);
static void escape_gtype(StringInfo buf, const char *str);
bool is_decimal_needed(char *numstr);
static void gtype_in_scalar(void *pstate, char *token, gtype_token_type tokentype, char *annotation);
static void gtype_categorize_type(Oid typoid, agt_type_category *tcategory, Oid *outfuncoid);
static void composite_to_gtype(Datum composite, gtype_in_state *result);
static void array_dim_to_gtype(gtype_in_state *result, int dim, int ndims, int *dims, Datum *vals, bool *nulls, int *valcount, agt_type_category tcategory, Oid outfuncoid);
static void datum_to_gtype(Datum val, bool is_null, gtype_in_state *result, agt_type_category tcategory, Oid outfuncoid, bool key_scalar);
static char *gtype_to_cstring_worker(StringInfo out, gtype_container *in, int estimated_len, bool indent);
static text *gtype_value_to_text(gtype_value *scalar_val, bool err_not_scalar);
static void add_indent(StringInfo out, bool indent, int level);
static gtype_value *execute_array_access_operator_internal(gtype *array, int64 array_index);
static gtype_iterator *get_next_object_key(gtype_iterator *it, gtype_container *agtc, gtype_value *key);
static Datum process_access_operator_result(FunctionCallInfo fcinfo, gtype_value *agtv, bool as_text);
static Datum process_access_operator_result(FunctionCallInfo fcinfo, gtype_value *agtv, bool as_text);
Datum gtype_array_element_impl(FunctionCallInfo fcinfo, gtype *gtype_in, int element, bool as_text);

// PostGIS
#include "liblwgeom/liblwgeom_internal.h"

PG_FUNCTION_INFO_V1(BOX2D_out);
PG_FUNCTION_INFO_V1(BOX3D_out);
PG_FUNCTION_INFO_V1(ellipsoid_out);
PG_FUNCTION_INFO_V1(LWGEOM_out);

PG_FUNCTION_INFO_V1(gtype_build_map_noargs);
/*              
 * degenerate case of gtype_build_map where it gets 0 arguments.
 */                 
Datum gtype_build_map_noargs(PG_FUNCTION_ARGS)
{   
    gtype_in_state result;
    
    memset(&result, 0, sizeof(gtype_in_state));
                                          
    push_gtype_value(&result.parse_state, WGT_BEGIN_OBJECT, NULL);
    result.res = push_gtype_value(&result.parse_state, WGT_END_OBJECT, NULL); 
                
    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}    

// fast helper function to test for AGTV_NULL in an gtype 
bool is_gtype_null(gtype *agt_arg)
{
    gtype_container *agtc = &agt_arg->root;

    if (GTYPE_CONTAINER_IS_SCALAR(agtc) && GTE_IS_NULL(agtc->children[0]))
        return true;

    return false;
}

bool is_gtype_integer(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && GTE_IS_GTYPE(agt->root.children[0]) && GT_IS_INTEGER(agt->root.children[1]);
}

bool is_gtype_float(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && GTE_IS_GTYPE(agt->root.children[0]) && GT_IS_FLOAT(agt->root.children[1]);
}

bool is_gtype_numeric(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && GTE_IS_NUMERIC(agt->root.children[0]);
}

bool is_gtype_string(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && GTE_IS_STRING(agt->root.children[0]);
}

/*
 * gtype recv function copied from PGs jsonb_recv as gtype is based
 * off of jsonb
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
PG_FUNCTION_INFO_V1(gtype_recv);

Datum gtype_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int version = pq_getmsgint(buf, 1);
    char *str = NULL;
    int nbytes = 0;

    if (version == 1)
        str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    else
        elog(ERROR, "unsupported gtype version number %d", version);

    return gtype_from_cstring(str, nbytes);
}

/*
 * gtype send function copied from PGs jsonb_send as gtype is based
 * off of jsonb
 *
 * Just send gtype as a version number, then a string of text
 */
PG_FUNCTION_INFO_V1(gtype_send);
Datum gtype_send(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    StringInfoData buf;
    StringInfo gtype_text = makeStringInfo();
    int version = 1;

    (void) gtype_to_cstring(gtype_text, &agt->root, VARSIZE(agt));

    pq_begintypsend(&buf);
    pq_sendint8(&buf, version);
    pq_sendtext(&buf, gtype_text->data, gtype_text->len);
    pfree(gtype_text->data);
    pfree(gtype_text);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(gtype_in);
/*
 * gtype type input function
 */
Datum gtype_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);

    return gtype_from_cstring(str, strlen(str));
}

PG_FUNCTION_INFO_V1(gtype_out);
/*
 * gtype type output function
 */
Datum gtype_out(PG_FUNCTION_ARGS) {
    gtype *agt = NULL;
    char *out = NULL;

    agt = AG_GET_ARG_GTYPE_P(0);

    out = gtype_to_cstring(NULL, &agt->root, VARSIZE(agt));

    PG_RETURN_CSTRING(out);
}

/*
 * gtype_from_cstring
 *
 * Turns gtype string into an gtype Datum.
 *
 * Uses the gtype parser (with hooks) to construct an gtype.
 */
Datum gtype_from_cstring(char *str, int len)
{
    gtype_lex_context *lex;
    gtype_in_state state;
    gtype_sem_action sem;

    memset(&state, 0, sizeof(state));
    memset(&sem, 0, sizeof(sem));
    lex = make_gtype_lex_context_cstring_len(str, len, true);

    sem.semstate = (void *)&state;

    sem.object_start = gtype_in_object_start;
    sem.array_start = gtype_in_array_start;
    sem.object_end = gtype_in_object_end;
    sem.array_end = gtype_in_array_end;
    sem.scalar = gtype_in_scalar;
    sem.object_field_start = gtype_in_object_field_start;

    parse_gtype(lex, &sem);

    // after parsing, the item member has the composed gtype structure 
    PG_RETURN_POINTER(gtype_value_to_gtype(state.res));
}

size_t check_string_length(size_t len) {
    if (len > GTENTRY_OFFLENMASK)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("string too long to represent as gtype string"),
                 errdetail("Due to an implementation restriction, gtype strings cannot exceed %d bytes.", GTENTRY_OFFLENMASK)));

    return len;
}

static void gtype_in_object_start(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WGT_BEGIN_OBJECT, NULL);
}

static void gtype_in_object_end(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WGT_END_OBJECT, NULL);
}

static void gtype_in_array_start(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WGT_BEGIN_ARRAY, NULL);
}

static void gtype_in_array_end(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WGT_END_ARRAY, NULL);
}

static void gtype_in_object_field_start(void *pstate, char *fname, bool isnull) {
    gtype_in_state *_state = (gtype_in_state *)pstate;
    gtype_value v;

    Assert(fname != NULL);
    v.type = AGTV_STRING;
    v.val.string.len = check_string_length(strlen(fname));
    v.val.string.val = fname;

    _state->res = push_gtype_value(&_state->parse_state, WGT_KEY, &v);
}

Datum
PostGraphDirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
        LOCAL_FCINFO(fcinfo, 3);
        fcinfo->flinfo = palloc0(sizeof(FmgrInfo));

        Datum           result;

        InitFunctionCallInfoData(*fcinfo, NULL, 1, collation, NULL, NULL);
fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
        fcinfo->flinfo->fn_addr = func;
    //    fcinfo->flinfo->fn_oid = get_pg_func_oid("range_out", 1, INT8RANGEOID);
        fcinfo->flinfo->fn_strict = false;
        fcinfo->flinfo->fn_retset = false;
        fcinfo->flinfo->fn_extra = NULL;
        fcinfo->flinfo->fn_mcxt = CurrentMemoryContext;
        fcinfo->flinfo->fn_expr = NULL;

        fcinfo->args[0].value = arg1;
        fcinfo->args[0].isnull = false;

        result = (*func) (fcinfo);

        /* Check for null result, since caller is clearly not expecting one */
        if (fcinfo->isnull)
                elog(ERROR, "function %p returned NULL", (void *) func);

        return result;
}



void gtype_put_escaped_value(StringInfo out, gtype_value *scalar_val)
{
    char *numstr;

    switch (scalar_val->type)
    {
    case AGTV_NULL:
        appendBinaryStringInfo(out, "null", 4);
        break;
    case AGTV_STRING:
        escape_gtype(out, pnstrdup(scalar_val->val.string.val, scalar_val->val.string.len));
        break;
    case AGTV_NUMERIC:
        appendStringInfoString(
            out, DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(scalar_val->val.numeric))));
        appendBinaryStringInfo(out, "::numeric", 9);
        break;
    case AGTV_INTEGER:
        appendStringInfoString(
            out, DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(scalar_val->val.int_value))));
        break;
    case AGTV_FLOAT:
        numstr = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(scalar_val->val.float_value)));
        appendStringInfoString(out, numstr);

        if (is_decimal_needed(numstr))
            appendBinaryStringInfo(out, ".0", 2);
        break;
    case AGTV_TIMESTAMP:
	numstr = DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(scalar_val->val.int_value)));
	appendStringInfoString(out, numstr);
	break;
    case AGTV_TIMESTAMPTZ:
        numstr = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampGetDatum(scalar_val->val.int_value)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_DATE:
        numstr = DatumGetCString(DirectFunctionCall1(date_out, DateADTGetDatum(scalar_val->val.date)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_TIME:
        numstr = DatumGetCString(DirectFunctionCall1(time_out, TimeADTGetDatum(scalar_val->val.int_value)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_TIMETZ:
        numstr = DatumGetCString(DirectFunctionCall1(timetz_out, DatumGetTimeTzADTP(&scalar_val->val.timetz)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_INTERVAL:
        numstr = DatumGetCString(DirectFunctionCall1(interval_out, IntervalPGetDatum(&scalar_val->val.interval)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_INET:
        numstr = DatumGetCString(DirectFunctionCall1(inet_out, InetPGetDatum(&scalar_val->val.inet)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_CIDR:
        numstr = DatumGetCString(DirectFunctionCall1(cidr_out, InetPGetDatum(&scalar_val->val.inet)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_MAC:
        numstr = DatumGetCString(DirectFunctionCall1(macaddr_out, MacaddrPGetDatum(&scalar_val->val.mac)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_MAC8:
        numstr = DatumGetCString(DirectFunctionCall1(macaddr8_out, MacaddrPGetDatum(&scalar_val->val.mac)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_POINT:
        numstr = DatumGetCString(DirectFunctionCall1(point_out, PointerGetDatum(scalar_val->val.point)));
        appendStringInfoString(out, numstr);
        break;	   
    case AGTV_LSEG:
        numstr = DatumGetCString(DirectFunctionCall1(lseg_out, PointerGetDatum(scalar_val->val.lseg)));
        appendStringInfoString(out, numstr);
        break;	   
    case AGTV_LINE:
        numstr = DatumGetCString(DirectFunctionCall1(line_out, PointerGetDatum(scalar_val->val.line)));
        appendStringInfoString(out, numstr);
        break;	   
    case AGTV_PATH:
        numstr = DatumGetCString(DirectFunctionCall1(path_out, PointerGetDatum(scalar_val->val.path)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_POLYGON:
        numstr = DatumGetCString(DirectFunctionCall1(poly_out, PointerGetDatum(scalar_val->val.polygon)));
        appendStringInfoString(out, numstr);
        break;	 
    case AGTV_CIRCLE:
        numstr = DatumGetCString(DirectFunctionCall1(circle_out, PointerGetDatum(scalar_val->val.circle)));
        appendStringInfoString(out, numstr);
        break;	    
    case AGTV_BOX:
        numstr = DatumGetCString(DirectFunctionCall1(box_out, PointerGetDatum(scalar_val->val.box)));
        appendStringInfoString(out, numstr);
        break;	     
    case AGTV_BOX2D:
        numstr = DatumGetCString(DirectFunctionCall1(BOX2D_out, PointerGetDatum(&scalar_val->val.gbox)));
        appendStringInfoString(out, numstr);
	break;
    case AGTV_BOX3D:
        numstr = DatumGetCString(DirectFunctionCall1(BOX3D_out, PointerGetDatum(&scalar_val->val.box3d)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_SPHEROID:
        numstr = DatumGetCString(DirectFunctionCall1(ellipsoid_out, PointerGetDatum(&scalar_val->val.spheroid)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_GSERIALIZED:
        numstr = DatumGetCString(DirectFunctionCall1(LWGEOM_out, PointerGetDatum(scalar_val->val.gserialized)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_TSVECTOR:
        numstr = DatumGetCString(DirectFunctionCall1(tsvectorout, PointerGetDatum(scalar_val->val.tsvector)));
        appendStringInfoString(out, numstr);
        break;  
    case AGTV_TSQUERY:
        numstr = DatumGetCString(DirectFunctionCall1(tsqueryout, PointerGetDatum(scalar_val->val.tsquery)));
        appendStringInfoString(out, numstr);
        break;	
    case AGTV_BYTEA:
        numstr = DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(scalar_val->val.bytea)));
        appendStringInfoString(out, numstr);
        break;	      
    case AGTV_RANGE_INT:
    case AGTV_RANGE_NUM:
    case AGTV_RANGE_TS:
    case AGTV_RANGE_TSTZ:
    case AGTV_RANGE_DATE:
	numstr = DatumGetCString(PostGraphDirectFunctionCall1Coll(range_out, DEFAULT_COLLATION_OID, PointerGetDatum(scalar_val->val.range)));
        appendStringInfoString(out, numstr);
        break;
    case AGTV_RANGE_INT_MULTI:
    case AGTV_RANGE_NUM_MULTI:
    case AGTV_RANGE_TS_MULTI:
    case AGTV_RANGE_TSTZ_MULTI:
    case AGTV_RANGE_DATE_MULTI:
        numstr = DatumGetCString(PostGraphDirectFunctionCall1Coll(multirange_out, DEFAULT_COLLATION_OID, PointerGetDatum(scalar_val->val.multirange)));
        appendStringInfoString(out, numstr);
	break; 
    case AGTV_BOOL:
        if (scalar_val->val.boolean)
            appendBinaryStringInfo(out, "true", 4);
        else
            appendBinaryStringInfo(out, "false", 5);
        break;
    default:
        elog(ERROR, "unknown gtype scalar type");
    }
}

/*
 * Produce an gtype string literal, properly escaping characters in the text.
 */
static void escape_gtype(StringInfo buf, const char *str) {
    const char *p;

    appendStringInfoCharMacro(buf, '"');
    for (p = str; *p; p++)
    {
        switch (*p)
        {
        case '\b':
            appendStringInfoString(buf, "\\b");
            break;
        case '\f':
            appendStringInfoString(buf, "\\f");
            break;
        case '\n':
            appendStringInfoString(buf, "\\n");
            break;
        case '\r':
            appendStringInfoString(buf, "\\r");
            break;
        case '\t':
            appendStringInfoString(buf, "\\t");
            break;
        case '"':
            appendStringInfoString(buf, "\\\"");
            break;
        case '\\':
            appendStringInfoString(buf, "\\\\");
            break;
        default:
            if ((unsigned char)*p < ' ')
                appendStringInfo(buf, "\\u%04x", (int)*p);
            else
                appendStringInfoCharMacro(buf, *p);
            break;
        }
    }
    appendStringInfoCharMacro(buf, '"');
}

bool is_decimal_needed(char *numstr) {
    int i;

    Assert(numstr);

    i = (numstr[0] == '-') ? 1 : 0;

    while (numstr[i] != '\0') {
        if (numstr[i] < '0' || numstr[i] > '9')
            return false;

        i++;
    }

    return true;
}

/*
 * For gtype we always want the de-escaped value - that's what's in token
 */
static void gtype_in_scalar(void *pstate, char *token, gtype_token_type tokentype, char *annotation) {
    gtype_in_state *_state = (gtype_in_state *)pstate;
    gtype_value v;
    Datum numd;

    /*
     * Process the scalar typecast annotations, if present, but not if the
     * argument is a null. Typecasting a null is a null.
     */
    if (annotation != NULL && tokentype != GTYPE_TOKEN_NULL) {
        int len = strlen(annotation);

        if (pg_strcasecmp(annotation, "numeric") == 0)
            tokentype = GTYPE_TOKEN_NUMERIC;
        else if (pg_strcasecmp(annotation, "integer") == 0)
            tokentype = GTYPE_TOKEN_INTEGER;
        else if (pg_strcasecmp(annotation, "float") == 0)
            tokentype = GTYPE_TOKEN_FLOAT;
        else if (pg_strcasecmp(annotation, "timestamp") == 0)
            tokentype = GTYPE_TOKEN_TIMESTAMP;
        else if (len == 11 && pg_strcasecmp(annotation, "timestamptz") == 0)
            tokentype = GTYPE_TOKEN_TIMESTAMPTZ;
	else if (len == 4 && pg_strcasecmp(annotation, "date") == 0)
            tokentype = GTYPE_TOKEN_DATE;
        else if (len == 4 && pg_strcasecmp(annotation, "time") == 0)
            tokentype = GTYPE_TOKEN_TIME;
        else if (len == 6 && pg_strcasecmp(annotation, "timetz") == 0)
            tokentype = GTYPE_TOKEN_TIMETZ;
	else if (len == 8 && pg_strcasecmp(annotation, "interval") == 0)
            tokentype = GTYPE_TOKEN_INTERVAL;
        else if (len == 4 && pg_strcasecmp(annotation, "inet") == 0)
            tokentype = GTYPE_TOKEN_INET;
        else if (len == 4 && pg_strcasecmp(annotation, "cidr") == 0)
            tokentype = GTYPE_TOKEN_CIDR;
        else if (len == 7 && pg_strcasecmp(annotation, "macaddr") == 0)
            tokentype = GTYPE_TOKEN_MACADDR;
        else if (len == 8 && pg_strcasecmp(annotation, "macaddr8") == 0)
            tokentype = GTYPE_TOKEN_MACADDR;
	else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid annotation value for scalar")));
    }

    switch (tokentype)
    {
    case GTYPE_TOKEN_STRING:
        Assert(token != NULL);
        v.type = AGTV_STRING;
        v.val.string.len = check_string_length(strlen(token));
        v.val.string.val = token;
        break;
    case GTYPE_TOKEN_INTEGER:
        Assert(token != NULL);
        v.type = AGTV_INTEGER;
        scanint8(token, false, &v.val.int_value);
        break;
    case GTYPE_TOKEN_FLOAT:
        Assert(token != NULL);
        v.type = AGTV_FLOAT;
        v.val.float_value = float8in_internal(token, NULL, "double precision", token);
        break;
    case GTYPE_TOKEN_NUMERIC:
        Assert(token != NULL);
        v.type = AGTV_NUMERIC;
        numd = DirectFunctionCall3(numeric_in,
			           CStringGetDatum(token),
				   ObjectIdGetDatum(InvalidOid),
				   Int32GetDatum(-1));
        v.val.numeric = DatumGetNumeric(numd);
        break;
    case GTYPE_TOKEN_TIMESTAMP:
        Assert(token != NULL);
        v.type = AGTV_TIMESTAMP;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(timestamp_in,
				                            CStringGetDatum(token),
							    ObjectIdGetDatum(InvalidOid),
							    Int32GetDatum(-1)));
        break;
    case GTYPE_TOKEN_TIMESTAMPTZ:
        v.type = AGTV_TIMESTAMPTZ;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(timestamptz_in,
                                                            CStringGetDatum(token),
                                                            ObjectIdGetDatum(InvalidOid),
                                                            Int32GetDatum(-1)));
        break;
    case GTYPE_TOKEN_DATE:
        v.type = AGTV_DATE;
        v.val.date = DatumGetInt32(DirectFunctionCall1(date_in, CStringGetDatum(token)));
        break;
    case GTYPE_TOKEN_TIME:
        v.type = AGTV_TIME;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(time_in,
                                                            CStringGetDatum(token),
                                                            ObjectIdGetDatum(InvalidOid),
                                                            Int32GetDatum(-1)));
	break;
    case GTYPE_TOKEN_TIMETZ:
        v.type = AGTV_TIMETZ;
        TimeTzADT * timetz= DatumGetTimeTzADTP(DirectFunctionCall3(timetz_in,
                                                                   CStringGetDatum(token),
                                                                   ObjectIdGetDatum(InvalidOid),
                                                                   Int32GetDatum(-1)));
        v.val.timetz.time = timetz->time;
        v.val.timetz.zone = timetz->zone;
        break;
    case GTYPE_TOKEN_INTERVAL:
        {
        Interval *interval;

        Assert(token != NULL);

        v.type = AGTV_INTERVAL;
        interval = DatumGetIntervalP(DirectFunctionCall3(interval_in,
				             CStringGetDatum(token),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1)));

        v.val.interval.time = interval->time;
        v.val.interval.day = interval->day;
        v.val.interval.month = interval->month;
        break;
        }
    case GTYPE_TOKEN_INET:
        {
        inet *i;

        Assert(token != NULL);

        v.type = AGTV_INET;
        i = DatumGetInetPP(DirectFunctionCall1(inet_in, CStringGetDatum(token)));

	memcpy(&v.val.inet, i, sizeof(char) * 22);
        break;
        }
    case GTYPE_TOKEN_CIDR:
        {
        inet *i;

        Assert(token != NULL);

        v.type = AGTV_CIDR;
        i = DatumGetInetPP(DirectFunctionCall1(cidr_in, CStringGetDatum(token)));

        memcpy(&v.val.inet, i, sizeof(char) * 22);
        break;
        }
    case GTYPE_TOKEN_MACADDR:
        {
        macaddr *mac;

        Assert(token != NULL);

        v.type = AGTV_MAC;
        mac = DatumGetMacaddrP(DirectFunctionCall1(macaddr_in, CStringGetDatum(token)));

        memcpy(&v.val.mac, mac, sizeof(char) * 6);
        break;
        }
    case GTYPE_TOKEN_MACADDR8:
        {
        macaddr8 *mac;

        Assert(token != NULL);

        v.type = AGTV_MAC8;
        mac = DatumGetMacaddrP(DirectFunctionCall1(macaddr8_in, CStringGetDatum(token)));

        memcpy(&v.val.mac, mac, sizeof(char) * 8);
        break;
        }
    case GTYPE_TOKEN_TRUE:
        v.type = AGTV_BOOL;
        v.val.boolean = true;
        break;
    case GTYPE_TOKEN_FALSE:
        v.type = AGTV_BOOL;
        v.val.boolean = false;
        break;
    case GTYPE_TOKEN_NULL:
        v.type = AGTV_NULL;
        break;
    default:
        // should not be possible 
        elog(ERROR, "invalid gtype token type");
        break;
    }

    if (_state->parse_state == NULL) {
        // single scalar 
        gtype_value va;

        va.type = AGTV_ARRAY;
        va.val.array.raw_scalar = true;
        va.val.array.num_elems = 1;

        _state->res = push_gtype_value(&_state->parse_state, WGT_BEGIN_ARRAY, &va);
        _state->res = push_gtype_value(&_state->parse_state, WGT_ELEM, &v);
        _state->res = push_gtype_value(&_state->parse_state, WGT_END_ARRAY, NULL);
    } else {
        gtype_value *o = &_state->parse_state->cont_val;

        switch (o->type)
        {
        case AGTV_ARRAY:
            _state->res = push_gtype_value(&_state->parse_state, WGT_ELEM, &v);
            break;
        case AGTV_OBJECT:
            _state->res = push_gtype_value(&_state->parse_state, WGT_VALUE, &v);
            break;
        default:
            elog(ERROR, "unexpected parent of nested structure");
        }
    }
}

/*
 * gtype_to_cstring
 *     Converts gtype value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 *
 * A typical case for passing the StringInfo in rather than NULL is where the
 * caller wants access to the len attribute without having to call strlen, e.g.
 * if they are converting it to a text* object.
 */
char *gtype_to_cstring(StringInfo out, gtype_container *in, int estimated_len) {
    return gtype_to_cstring_worker(out, in, estimated_len, false);
}

/*
 * same thing but with indentation turned on
 */
char *gtype_to_cstring_indent(StringInfo out, gtype_container *in, int estimated_len) {
    return gtype_to_cstring_worker(out, in, estimated_len, true);
}

/*
 * common worker for above two functions
 */
static char *gtype_to_cstring_worker(StringInfo out, gtype_container *in, int estimated_len, bool indent)
{
    bool first = true;
    gtype_iterator *it;
    gtype_value v;
    gtype_iterator_token type = WGT_DONE;
    int level = 0;
    bool redo_switch = false;

    // If we are indenting, don't add a space after a comma 
    int ispaces = indent ? 1 : 2;

    /*
     * Don't indent the very first item. This gets set to the indent flag at
     * the bottom of the loop.
     */
    bool use_indent = false;
    bool raw_scalar = false;
    bool last_was_key = false;

    if (out == NULL)
        out = makeStringInfo();

    enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

    it = gtype_iterator_init(in);

    while (redo_switch ||
           ((type = gtype_iterator_next(&it, &v, false)) != WGT_DONE))
    {
        redo_switch = false;
        switch (type)
        {
        case WGT_BEGIN_ARRAY:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            if (!v.val.array.raw_scalar) {
                add_indent(out, use_indent && !last_was_key, level);
                appendStringInfoCharMacro(out, '[');
            } else {
                raw_scalar = true;
            }

            first = true;
            level++;
            break;
        case WGT_BEGIN_VECTOR:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            add_indent(out, use_indent && !last_was_key, level);
            appendStringInfoCharMacro(out, '[');

            first = true;
            level++;
            break;

        case WGT_BEGIN_OBJECT:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            add_indent(out, use_indent && !last_was_key, level);
            appendStringInfoCharMacro(out, '{');

            first = true;
            level++;
            break;
        case WGT_KEY:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = true;

            add_indent(out, use_indent, level);

            // gtype rules guarantee this is a string 
            gtype_put_escaped_value(out, &v);
            appendBinaryStringInfo(out, ": ", 2);

            type = gtype_iterator_next(&it, &v, false);
            if (type == WGT_VALUE) {
                first = false;
                gtype_put_escaped_value(out, &v);
            } else {
                Assert(type == WGT_BEGIN_OBJECT || type == WGT_BEGIN_ARRAY || type == WGT_BEGIN_VECTOR);

                /*
                 * We need to rerun the current switch() since we need to
                 * output the object which we just got from the iterator
                 * before calling the iterator again.
                 */
                redo_switch = true;
            }
            break;
	case WGT_VECTOR_VALUE:
        case WGT_ELEM:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = false;

            if (!raw_scalar)
                add_indent(out, use_indent, level);
            gtype_put_escaped_value(out, &v);
            break;
        case WGT_END_ARRAY:
            level--;
            if (!raw_scalar) {
                add_indent(out, use_indent, level);
                appendStringInfoCharMacro(out, ']');
            }
            first = false;
            break;
        case WGT_END_VECTOR:
            level--;
            add_indent(out, use_indent, level);
            appendStringInfoCharMacro(out, ']');
            first = false;
            break;
        case WGT_END_OBJECT:
            level--;
            add_indent(out, use_indent, level);
            appendStringInfoCharMacro(out, '}');
            first = false;
            break;
        default:
            elog(ERROR, "unknown gtype iterator token type");
        }
        use_indent = indent;
        last_was_key = redo_switch;
    }

    Assert(level == 0);

    return out->data;
}

/*
 * Convert gtype_value(scalar) to text
 */
static text *gtype_value_to_text(gtype_value *scalar_val, bool err_not_scalar)
{
    text *result = NULL;
    switch (scalar_val->type)
    {
    case AGTV_INTEGER:
        result = cstring_to_text(DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(scalar_val->val.int_value))));
        break;
    case AGTV_FLOAT:
        result = cstring_to_text(DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(scalar_val->val.float_value))));
        break;
    case AGTV_STRING:
        result = cstring_to_text_with_len(scalar_val->val.string.val, scalar_val->val.string.len);
        break;
    case AGTV_NUMERIC:
        result = cstring_to_text(DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(scalar_val->val.numeric))));
        break;
    case AGTV_BOOL:
        result = cstring_to_text((scalar_val->val.boolean) ? "true" : "false");
        break;
    case AGTV_NULL:
        result = NULL;
        break;
    default:
        if (err_not_scalar) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("gtype_value_to_text: unsupported argument gtype %d", scalar_val->type)));
        }
    }
    return result;
}

static void add_indent(StringInfo out, bool indent, int level) {
    if (indent) {
        int i;

        appendStringInfoCharMacro(out, '\n');
        for (i = 0; i < level; i++)
            appendBinaryStringInfo(out, "    ", 4);
    }
}

/*
 * Determine how we want to render values of a given type in datum_to_gtype.
 *
 * Given the datatype OID, return its agt_type_category, as well as the type's
 * output function OID.  If the returned category is AGT_TYPE_JSONCAST,
 * we return the OID of the relevant cast function instead.
 */
static void gtype_categorize_type(Oid typoid, agt_type_category *tcategory, Oid *outfuncoid) {
    bool typisvarlena;

    // Look through any domain 
    typoid = getBaseType(typoid);

    *outfuncoid = InvalidOid;

    /*
     * We need to get the output function for everything except date and
     * timestamp types, booleans, array and composite types, json and jsonb,
     * and non-builtin types where there's a cast to json. In this last case
     * we return the oid of the cast function instead.
     */

    switch (typoid)
    {
    case BOOLOID:
        *tcategory = AGT_TYPE_BOOL;
        break;

    case INT2OID:
    case INT4OID:
    case INT8OID:
        getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
        *tcategory = AGT_TYPE_INTEGER;
        break;

    case FLOAT4OID:
    case FLOAT8OID:
        getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
        *tcategory = AGT_TYPE_FLOAT;
        break;

    case NUMERICOID:
        getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
        *tcategory = AGT_TYPE_NUMERIC;
        break;

    case DATEOID:
        *tcategory = AGT_TYPE_DATE;
        break;

    case TIMESTAMPOID:
        *tcategory = AGT_TYPE_TIMESTAMP;
        break;

    case TIMESTAMPTZOID:
        *tcategory = AGT_TYPE_TIMESTAMPTZ;
        break;

    case INTERVALOID:
        *tcategory = AGT_TYPE_INTERVAL;
        break;

    case INETOID:
        *tcategory = AGT_TYPE_INET;
        break;

    case CIDROID:
        *tcategory = AGT_TYPE_CIDR;
        break;

    case MACADDROID:
        *tcategory = AGT_TYPE_MAC;
        break;

    case MACADDR8OID:
        *tcategory = AGT_TYPE_MAC8;
        break;

    case JSONBOID:
        *tcategory = AGT_TYPE_JSONB;
        break;

    case JSONOID:
        *tcategory = AGT_TYPE_JSON;
        break;

    default:
        // Check for arrays and composites 
        if (typoid == GTYPEOID) {
            *tcategory = AGT_TYPE_GTYPE;
        } else if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID || typoid == RECORDARRAYOID) {
            *tcategory = AGT_TYPE_ARRAY;
        } else if (type_is_rowtype(typoid)) {// includes RECORDOID  {
            *tcategory = AGT_TYPE_COMPOSITE;
        } else if (typoid == GRAPHIDOID) {
            getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
            *tcategory = AGT_TYPE_INTEGER;
        } else {
            // It's probably the general case ... 
            *tcategory = AGT_TYPE_OTHER;

            /*
             * but first let's look for a cast to json (note: not to
             * jsonb) if it's not built-in.
             */
            if (typoid >= FirstNormalObjectId) {
                Oid castfunc;
                CoercionPathType ctype;

                ctype = find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT, &castfunc);
                if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc)) {
                    *tcategory = AGT_TYPE_JSONCAST;
                    *outfuncoid = castfunc;
                } else {
                    // not a cast type, so just get the usual output func 
                    getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
                }
            } else {
                // any other builtin type 
                getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
            }
            break;
        }
    }
}

/*
 * Turn a Datum into gtype, adding it to the result gtype_in_state.
 *
 * tcategory and outfuncoid are from a previous call to gtype_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is stored as a key, so insist
 * it's of an acceptable type, and force it to be a AGTV_STRING.
 */
static void datum_to_gtype(Datum val, bool is_null, gtype_in_state *result, agt_type_category tcategory, Oid outfuncoid, bool key_scalar) {
    char *outputstr;
    bool numeric_error;
    gtype_value agtv;
    bool scalar_gtype = false;

    check_stack_depth();

    // Convert val to an gtype_value in agtv (in most cases) 
    if (is_null) {
        Assert(!key_scalar);
        agtv.type = AGTV_NULL;
    } else if (key_scalar &&
             (tcategory == AGT_TYPE_ARRAY || tcategory == AGT_TYPE_COMPOSITE ||
              tcategory == AGT_TYPE_JSON || tcategory == AGT_TYPE_JSONB ||
              tcategory == AGT_TYPE_GTYPE || tcategory == AGT_TYPE_JSONCAST)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("key value must be scalar, not array, composite, or json")));
    } else {
        if (tcategory == AGT_TYPE_JSONCAST)
            val = OidFunctionCall1(outfuncoid, val);

        switch (tcategory)
        {
        case AGT_TYPE_ARRAY:
            array_to_gtype_internal(val, result);
            break;
        case AGT_TYPE_COMPOSITE:
            composite_to_gtype(val, result);
            break;
        case AGT_TYPE_BOOL:
            if (key_scalar) {
                outputstr = DatumGetBool(val) ? "true" : "false";
                agtv.type = AGTV_STRING;
                agtv.val.string.len = strlen(outputstr);
                agtv.val.string.val = outputstr;
            } else {
                agtv.type = AGTV_BOOL;
                agtv.val.boolean = DatumGetBool(val);
            }
            break;
        case AGT_TYPE_INTEGER:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            if (key_scalar) {
                agtv.type = AGTV_STRING;
                agtv.val.string.len = strlen(outputstr);
                agtv.val.string.val = outputstr;
            } else {
                Datum intd;

                intd = DirectFunctionCall1(int8in, CStringGetDatum(outputstr));
                agtv.type = AGTV_INTEGER;
                agtv.val.int_value = DatumGetInt64(intd);
                pfree(outputstr);
            }
            break;
        case AGT_TYPE_FLOAT:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            if (key_scalar) {
                agtv.type = AGTV_STRING;
                agtv.val.string.len = strlen(outputstr);
                agtv.val.string.val = outputstr;
            } else {
                Datum intd;

                intd = DirectFunctionCall1(float8in, CStringGetDatum(outputstr));
                agtv.type = AGTV_FLOAT;
                agtv.val.float_value = DatumGetFloat8(intd);
            }
            break;
        case AGT_TYPE_NUMERIC:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            if (key_scalar) {
                // always quote keys 
                agtv.type = AGTV_STRING;
                agtv.val.string.len = strlen(outputstr);
                agtv.val.string.val = outputstr;
            } else {
                /*
                 * Make it numeric if it's a valid gtype number, otherwise
                 * a string. Invalid numeric output will always have an
                 * 'N' or 'n' in it (I think).
                 */
                numeric_error = (strchr(outputstr, 'N') != NULL || strchr(outputstr, 'n') != NULL);
                if (!numeric_error) {
                    Datum numd;

                    agtv.type = AGTV_NUMERIC;
                    numd = DirectFunctionCall3(numeric_in, CStringGetDatum(outputstr), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                    agtv.val.numeric = DatumGetNumeric(numd);
                    pfree(outputstr);
                } else {
                    agtv.type = AGTV_STRING;
                    agtv.val.string.len = strlen(outputstr);
                    agtv.val.string.val = outputstr;
                }
            }
            break;
        case AGT_TYPE_DATE:
            agtv.type = AGTV_DATE;
            agtv.val.date = DatumGetInt32(val);
	    break;
        case AGT_TYPE_TIMESTAMP:
	    agtv.type = AGTV_TIMESTAMP;
            agtv.val.int_value = DatumGetInt64(val);
            break;
        case AGT_TYPE_TIMESTAMPTZ:
            agtv.type = AGTV_TIMESTAMPTZ;
            agtv.val.int_value = DatumGetInt64(val);
            break;
        case AGT_TYPE_TIME:
            agtv.type = AGTV_TIME;
            agtv.val.int_value = DatumGetInt64(val);
            break; 
        case AGT_TYPE_TIMETZ:
            agtv.type = AGTV_TIMETZ;
            TimeTzADT *timetz = DatumGetTimeTzADTP(val);
            agtv.val.timetz.time = timetz->time;
            agtv.val.timetz.zone = timetz->zone;
            break;
	case AGT_TYPE_INTERVAL:
        {
            Interval *i = DatumGetIntervalP(val);
            agtv.type = AGTV_INTERVAL;
            agtv.val.interval.time = i->time;
            agtv.val.interval.day = i->day;
            agtv.val.interval.month = i->month;
            break;
	}
	case AGT_TYPE_INET:
	{
           inet *i = DatumGetInetPP(val);
	   agtv.val.inet.inet_data.family = i->inet_data.family;
	   agtv.val.inet.inet_data.bits = i->inet_data.bits;
	   memcpy(&agtv.val.inet.inet_data.ipaddr, &i->inet_data.ipaddr, 16 * sizeof(char));
           memcpy(&agtv.val.inet.vl_len_, &i->vl_len_, 4 * sizeof(char));
	}
        case AGT_TYPE_JSONCAST:
        case AGT_TYPE_JSON:
        {
            /*
             * Parse the json right into the existing result object.
             * We can handle it as an gtype because gtype is currently an
             * extension of json.
             * Unlike AGT_TYPE_JSONB, numbers will be stored as either
             * an integer or a float, not a numeric.
             */
            gtype_lex_context *lex;
            gtype_sem_action sem;
            text *json = DatumGetTextPP(val);

            lex = make_gtype_lex_context(json, true);

            memset(&sem, 0, sizeof(sem));

            sem.semstate = (void *)result;

            sem.object_start = gtype_in_object_start;
            sem.array_start = gtype_in_array_start;
            sem.object_end = gtype_in_object_end;
            sem.array_end = gtype_in_array_end;
            sem.scalar = gtype_in_scalar;
            sem.object_field_start = gtype_in_object_field_start;

            parse_gtype(lex, &sem);
        }
        break;
        case AGT_TYPE_GTYPE:
        case AGT_TYPE_JSONB:
        {
            gtype *jsonb = DATUM_GET_GTYPE_P(val);
            gtype_iterator *it;

            /*
             * val is actually jsonb datum but we can handle it as an gtype
             * datum because gtype is currently an extension of jsonb.
             */

            it = gtype_iterator_init(&jsonb->root);
            if (AGT_ROOT_IS_SCALAR(jsonb)) {
                gtype_iterator_next(&it, &agtv, true);
                Assert(agtv.type == AGTV_ARRAY);
                gtype_iterator_next(&it, &agtv, true);
                scalar_gtype = true;
            } else {
                gtype_iterator_token type;

                while ((type = gtype_iterator_next(&it, &agtv, false)) != WGT_DONE) {
                    if (type == WGT_END_ARRAY || type == WGT_END_OBJECT ||
		        type == WGT_END_VECTOR || type == WGT_BEGIN_VECTOR ||
   	  	        type == WGT_BEGIN_ARRAY || type == WGT_BEGIN_OBJECT) {
                        result->res = push_gtype_value(&result->parse_state, type, NULL);
                    } else {
                        result->res = push_gtype_value(&result->parse_state, type, &agtv);
                    }
                }
            }
        }
        break;
        default:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            agtv.type = AGTV_STRING;
            agtv.val.string.len = check_string_length(strlen(outputstr));
            agtv.val.string.val = outputstr;
            break;
        }
    }

    // Now insert agtv into result, unless we did it recursively 
    if (!is_null && !scalar_gtype && tcategory >= AGT_TYPE_GTYPE && tcategory <= AGT_TYPE_JSONCAST) {
        // work has been done recursively 
        return;
    } else if (result->parse_state == NULL) {
        // single root scalar 
        gtype_value va;

        va.type = AGTV_ARRAY;
        va.val.array.raw_scalar = true;
        va.val.array.num_elems = 1;

        result->res = push_gtype_value(&result->parse_state, WGT_BEGIN_ARRAY, &va);
        result->res = push_gtype_value(&result->parse_state, WGT_ELEM, &agtv);
        result->res = push_gtype_value(&result->parse_state, WGT_END_ARRAY, NULL);
    } else {
        gtype_value *o = &result->parse_state->cont_val;

        switch (o->type) {
        case AGTV_ARRAY:
            result->res = push_gtype_value(&result->parse_state, WGT_ELEM, &agtv);
            break;
        case AGTV_OBJECT:
            result->res = push_gtype_value(&result->parse_state, key_scalar ? WGT_KEY : WGT_VALUE, &agtv);
            break;
        default:
            elog(ERROR, "unexpected parent of nested structure");
        }
    }
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void array_dim_to_gtype(gtype_in_state *result, int dim, int ndims,
                                int *dims, Datum *vals, bool *nulls,
                                int *valcount, agt_type_category tcategory,
                                Oid outfuncoid) {
    int i;

    Assert(dim < ndims);

    result->res = push_gtype_value(&result->parse_state, WGT_BEGIN_ARRAY, NULL);

    for (i = 1; i <= dims[dim]; i++)
    {
        if (dim + 1 == ndims) {
            datum_to_gtype(vals[*valcount], nulls[*valcount], result, tcategory, outfuncoid, false);
            (*valcount)++;
        } else {
            array_dim_to_gtype(result, dim + 1, ndims, dims, vals, nulls, valcount, tcategory, outfuncoid);
        }
    }

    result->res = push_gtype_value(&result->parse_state, WGT_END_ARRAY, NULL);
}

/*
 * Turn an array into gtype.
 */
void array_to_gtype_internal(Datum array, gtype_in_state *result)
{
    ArrayType *v = DatumGetArrayTypeP(array);
    Oid element_type = ARR_ELEMTYPE(v);
    int *dim;
    int ndim;
    int nitems;
    int count = 0;
    Datum *elements;
    bool *nulls;
    int16 typlen;
    bool typbyval;
    char typalign;
    agt_type_category tcategory;
    Oid outfuncoid;

    ndim = ARR_NDIM(v);
    dim = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    if (nitems <= 0) {
        result->res = push_gtype_value(&result->parse_state, WGT_BEGIN_ARRAY, NULL);
        result->res = push_gtype_value(&result->parse_state, WGT_END_ARRAY, NULL);
        return;
    }

    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    gtype_categorize_type(element_type, &tcategory, &outfuncoid);

    deconstruct_array(v, element_type, typlen, typbyval, typalign, &elements, &nulls, &nitems);

    array_dim_to_gtype(result, 0, ndim, dim, elements, nulls, &count, tcategory, outfuncoid);

    pfree(elements);
    pfree(nulls);
}

/*
 * Turn a composite / record into gtype.
 */
static void composite_to_gtype(Datum composite, gtype_in_state *result)
{
    HeapTupleHeader td;
    Oid tup_type;
    int32 tup_typmod;
    TupleDesc tupdesc;
    HeapTupleData tmptup, *tuple;
    int i;

    td = DatumGetHeapTupleHeader(composite);

    // Extract rowtype info and find a tupdesc 
    tup_type = HeapTupleHeaderGetTypeId(td);
    tup_typmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

    // Build a temporary HeapTuple control structure 
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &tmptup;

    result->res = push_gtype_value(&result->parse_state, WGT_BEGIN_OBJECT, NULL);

    for (i = 0; i < tupdesc->natts; i++)
    {
        Datum val;
        bool isnull;
        char *attname;
        agt_type_category tcategory;
        Oid outfuncoid;
        gtype_value v;
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (att->attisdropped)
            continue;

        attname = NameStr(att->attname);

        v.type = AGTV_STRING;
        /*
         * don't need check_string_length here - can't exceed maximum name length
         */
        v.val.string.len = strlen(attname);
        v.val.string.val = attname;

        result->res = push_gtype_value(&result->parse_state, WGT_KEY, &v);

        val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        if (isnull)
        {
            tcategory = AGT_TYPE_NULL;
            outfuncoid = InvalidOid;
        }
        else
        {
            gtype_categorize_type(att->atttypid, &tcategory, &outfuncoid);
        }

        datum_to_gtype(val, isnull, result, tcategory, outfuncoid, false);
    }

    result->res = push_gtype_value(&result->parse_state, WGT_END_OBJECT,
                                    NULL);
    ReleaseTupleDesc(tupdesc);
}

/*
 * Append gtype text for "val" to "result".
 *
 * This is just a thin wrapper around datum_to_gtype.  If the same type
 * will be printed many times, avoid using this; better to do the
 * gtype_categorize_type lookups only once.
 */
void add_gtype(Datum val, bool is_null, gtype_in_state *result,
                       Oid val_type, bool key_scalar)
{
    agt_type_category tcategory;
    Oid outfuncoid;

    if (val_type == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));

    if (is_null) {
        tcategory = AGT_TYPE_NULL;
        outfuncoid = InvalidOid;
    } else {
        gtype_categorize_type(val_type, &tcategory, &outfuncoid);
    }

    datum_to_gtype(val, is_null, result, tcategory, outfuncoid, key_scalar);
}

gtype_value *string_to_gtype_value(char *s)
{
    gtype_value *agtv = palloc0(sizeof(gtype_value));

    agtv->type = AGTV_STRING;
    agtv->val.string.len = check_string_length(strlen(s));
    agtv->val.string.val = s;

    return agtv;
}


PG_FUNCTION_INFO_V1(gtype_build_map);
/*
 * SQL function gtype_build_map(variadic "any")
 */
Datum gtype_build_map(PG_FUNCTION_ARGS)
{
    int nargs;
    int i;
    gtype_in_state result;
    Datum *args;
    bool *nulls;
    Oid *types;

    // build argument values to build the object 
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    if (nargs < 0)
        PG_RETURN_NULL();

    if (nargs % 2 != 0)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("argument list must have been even number of elements"),
             errhint("The arguments of gtype_build_map() must consist of alternating keys and values.")));
    }

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WGT_BEGIN_OBJECT,
                                   NULL);

    for (i = 0; i < nargs; i += 2)
    {
        // process key 
        if (nulls[i])
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %d: key must not be null", i + 1)));
        }

        add_gtype(args[i], false, &result, types[i], true);

        // process value 
        add_gtype(args[i + 1], nulls[i + 1], &result, types[i + 1], false);
    }

    result.res = push_gtype_value(&result.parse_state, WGT_END_OBJECT, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

PG_FUNCTION_INFO_V1(gtype_build_list);

/*
 * SQL function gtype_build_list(variadic "any")
 */
Datum gtype_build_list(PG_FUNCTION_ARGS)
{
    int nargs;
    int i;
    gtype_in_state result;
    Datum *args;
    bool *nulls;
    Oid *types;

    //build argument values to build the array 
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    if (nargs < 0)
        PG_RETURN_NULL();

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WGT_BEGIN_ARRAY,
                                   NULL);

    for (i = 0; i < nargs; i++)
        add_gtype(args[i], nulls[i], &result, types[i], false);

    result.res = push_gtype_value(&result.parse_state, WGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

PG_FUNCTION_INFO_V1(gtype_build_list_noargs);

/*
 * degenerate case of gtype_build_list where it gets 0 arguments.
 */
Datum gtype_build_list_noargs(PG_FUNCTION_ARGS)
{
    gtype_in_state result;

    memset(&result, 0, sizeof(gtype_in_state));

    push_gtype_value(&result.parse_state, WGT_BEGIN_ARRAY, NULL);
    result.res = push_gtype_value(&result.parse_state, WGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

static gtype_value *execute_array_access_operator_internal(gtype *array, int64 array_index)
{
    uint32 size;

    // get the size of the array, given the type of the input 
    if (AGT_ROOT_IS_ARRAY(array) && !AGT_ROOT_IS_SCALAR(array))
        size = AGT_ROOT_COUNT(array);
    else
        elog(ERROR, "execute_array_access_operator_internal: unexpected type");

    if (array_index < 0) {
        array_index = size + array_index;
        if (array_index < 0)
             return NULL;
    }
    else if (array_index > size) {
            return NULL;
    }

    return get_ith_gtype_value_from_container(&array->root, array_index);
}

PG_FUNCTION_INFO_V1(gtype_field_access);
Datum gtype_field_access(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype *key = AG_GET_ARG_GTYPE_P(1);
    gtype_value *key_value;

    if (!AGT_ROOT_IS_SCALAR(key))
        PG_RETURN_NULL();

    key_value = get_ith_gtype_value_from_container(&key->root, 0);

    if (key_value->type == AGTV_INTEGER)
        AG_RETURN_GTYPE_P(gtype_array_element_impl(fcinfo, agt, key_value->val.int_value, false));
    else if (key_value->type == AGTV_STRING)
        AG_RETURN_GTYPE_P(gtype_object_field_impl(fcinfo, agt, key_value->val.string.val,
                                                    key_value->val.string.len, false));
    else
        PG_RETURN_NULL();
}

static Datum process_access_operator_result(FunctionCallInfo fcinfo, gtype_value *agtv, bool as_text)
{       
    text *result;

    if (!agtv || agtv->type == AGTV_NULL)
        PG_RETURN_NULL();

    if (!as_text)
        return GTYPE_P_GET_DATUM(gtype_value_to_gtype(agtv));

    if (agtv->type == AGTV_BINARY) {
        StringInfo out = makeStringInfo();
        gtype_container *agtc = (gtype_container *)agtv->val.binary.data;
        char *str;

        str = gtype_to_cstring(out, agtc, agtv->val.binary.len);

        result = cstring_to_text(str);
     } else {
        result = gtype_value_to_text(agtv, false);
     }

    if (result)
        PG_RETURN_TEXT_P(result);
    else
        PG_RETURN_NULL();
}

Datum gtype_array_element_impl(FunctionCallInfo fcinfo, gtype *gtype_in, int element, bool as_text) {       
    gtype_value *v;

    if (!AGT_ROOT_IS_ARRAY(gtype_in))
        PG_RETURN_NULL();

    v = execute_array_access_operator_internal(gtype_in, element);
                                     
    return process_access_operator_result(fcinfo, v, as_text);
}       
   
Datum gtype_object_field_impl(FunctionCallInfo fcinfo, gtype *gtype_in, char *key, int key_len, bool as_text) {  
    gtype_value *v;
    gtype_container *agtc = &gtype_in->root;
    const gtype_value new_key_value = { .type = AGTV_STRING, .val.string = { key_len, key} };

    if (!AGT_ROOT_IS_OBJECT(gtype_in))
        PG_RETURN_NULL();

    v = find_gtype_value_from_container(agtc, GT_FOBJECT, &new_key_value);

    return process_access_operator_result(fcinfo, v, as_text);
}  
 
PG_FUNCTION_INFO_V1(gtype_object_field);
Datum gtype_object_field(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    text *key = PG_GETARG_TEXT_PP(1);

    AG_RETURN_GTYPE_P(gtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), false));
}

PG_FUNCTION_INFO_V1(gtype_object_field_text);
Datum gtype_object_field_text(PG_FUNCTION_ARGS)
{

    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    text *key = PG_GETARG_TEXT_PP(1);

    PG_RETURN_TEXT_P(gtype_object_field_impl(fcinfo, agt, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key), true));
}

PG_FUNCTION_INFO_V1(gtype_array_element);
Datum gtype_array_element(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    int elem = PG_GETARG_INT32(1);

    AG_RETURN_GTYPE_P(gtype_array_element_impl(fcinfo, agt, elem, false));
}

PG_FUNCTION_INFO_V1(gtype_array_element_text);
Datum gtype_array_element_text(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    int elem = PG_GETARG_INT32(1);

    PG_RETURN_TEXT_P(gtype_array_element_impl(fcinfo, agt, elem, true));
}

PG_FUNCTION_INFO_V1(gtype_access_slice);
/*
 * Execution function for list slices
 */
Datum gtype_access_slice(PG_FUNCTION_ARGS)
{
    gtype_value *lidx_value = NULL;
    gtype_value *uidx_value = NULL;
    gtype_in_state result;
    gtype *array = NULL;
    int64 upper_index = 0;
    int64 lower_index = 0;
    uint32 array_size = 0;
    int64 i = 0;

    // return null if the array to slice is null 
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    // return an error if both indices are NULL 
    if (PG_ARGISNULL(1) && PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("slice start and/or end is required")));

    // get the array parameter and verify that it is a list 
    array = AG_GET_ARG_GTYPE_P(0);
    if (!AGT_ROOT_IS_ARRAY(array) || AGT_ROOT_IS_SCALAR(array))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("slice must access a list")));
    }

    // get its size 
    array_size = AGT_ROOT_COUNT(array);

    // if we don't have a lower bound, make it 0 
    if (PG_ARGISNULL(1))
    {
        lower_index = 0;
    }
    else
    {
        lidx_value = get_ith_gtype_value_from_container(&(AG_GET_ARG_GTYPE_P(1))->root, 0);

        if (lidx_value->type == AGTV_NULL)
        {
            lower_index = 0;
            lidx_value = NULL;
        }
    }

    // if we don't have an upper bound, make it the size of the array 
    if (PG_ARGISNULL(2))
    {
        upper_index = array_size;
    }
    else
    {
        uidx_value = get_ith_gtype_value_from_container(
            &(AG_GET_ARG_GTYPE_P(2))->root, 0);
        // adjust for AGTV_NULL 
        if (uidx_value->type == AGTV_NULL)
        {
            upper_index = array_size;
            uidx_value = NULL;
        }
    }

    // if both indices are NULL (AGTV_NULL) return an error 
    if (lidx_value == NULL && uidx_value == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("slice start and/or end is required")));

    // key must be an integer or NULL 
    if ((lidx_value != NULL && lidx_value->type != AGTV_INTEGER) ||
        (uidx_value != NULL && uidx_value->type != AGTV_INTEGER))
        ereport(ERROR,
                (errmsg("array slices must resolve to an integer value")));

    // set indices if not already set 
    if (lidx_value)
    {
        lower_index = lidx_value->val.int_value;
    }
    if (uidx_value)
    {
        upper_index = uidx_value->val.int_value;
    }

    // adjust for negative and out of bounds index values 
    if (lower_index < 0)
        lower_index = array_size + lower_index;
    if (lower_index < 0)
        lower_index = 0;
    if (lower_index > array_size)
        lower_index = array_size;
    if (upper_index < 0)
        upper_index = array_size + upper_index;
    if (upper_index < 0)
        upper_index = 0;
    if (upper_index > array_size)
        upper_index = array_size;

    // build our result array 
    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WGT_BEGIN_ARRAY, NULL);

    // get array elements 
    for (i = lower_index; i < upper_index; i++)
    {
        result.res = push_gtype_value(&result.parse_state, WGT_ELEM,
            get_ith_gtype_value_from_container(&array->root, i));
    }

    result.res = push_gtype_value(&result.parse_state, WGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

PG_FUNCTION_INFO_V1(gtype_in_operator);
/*
 * Execute function for IN operator
 */
Datum gtype_in_operator(PG_FUNCTION_ARGS)
{
    gtype *agt_array, *agt_item;
    gtype_iterator *it_array, *it_item;
    gtype_value agtv_item, agtv_elem;
    uint32 array_size = 0;
    bool result = false;
    uint32 i = 0;

    // return null if the array is null 
    if (PG_ARGISNULL(1))
        PG_RETURN_NULL();

    // get the array parameter and verify that it is a list 
    agt_array = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_ARRAY(agt_array))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("object of IN must be a list")));

    // init array iterator 
    it_array = gtype_iterator_init(&agt_array->root);
    // open array container 
    gtype_iterator_next(&it_array, &agtv_elem, false);
    // check for an array scalar value 
    if (agtv_elem.type == AGTV_ARRAY && agtv_elem.val.array.raw_scalar)
    {
        gtype_iterator_next(&it_array, &agtv_elem, false);
        // check for GTYPE NULL 
        if (agtv_elem.type == AGTV_NULL)
            PG_RETURN_NULL();
        // if it is a scalar, but not AGTV_NULL, error out 
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("object of IN must be a list")));
    }

    array_size = AGT_ROOT_COUNT(agt_array);

    // return null if the item to find is null 
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    // get the item to search for 
    agt_item = AG_GET_ARG_GTYPE_P(0);

    // init item iterator 
    it_item = gtype_iterator_init(&agt_item->root);

    // get value of item 
    gtype_iterator_next(&it_item, &agtv_item, false);
    if (agtv_item.type == AGTV_ARRAY && agtv_item.val.array.raw_scalar)
    {
        gtype_iterator_next(&it_item, &agtv_item, false);
        // check for GTYPE NULL 
        if (agtv_item.type == AGTV_NULL)
            PG_RETURN_NULL();
    }

    // iterate through the array, but stop if we find it 
    for (i = 0; i < array_size && !result; i++)
    {
        // get next element 
        gtype_iterator_next(&it_array, &agtv_elem, true);
        // if both are containers, compare containers 
        if (!IS_A_GTYPE_SCALAR(&agtv_item) && !IS_A_GTYPE_SCALAR(&agtv_elem))
            result = (compare_gtype_containers_orderability( &agt_item->root, agtv_elem.val.binary.data) == 0);
        // if both are scalars and of the same type, compare scalars 
        else if (IS_A_GTYPE_SCALAR(&agtv_item) && IS_A_GTYPE_SCALAR(&agtv_elem) && agtv_item.type == agtv_elem.type)
            result = (compare_gtype_scalar_values(&agtv_item, &agtv_elem) == 0);
    }
    return result;
}

PG_FUNCTION_INFO_V1(gtype_string_match_starts_with);
// STARTS WITH
Datum gtype_string_match_starts_with(PG_FUNCTION_ARGS)
{
    Datum x = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(0), "text");
    Datum y = convert_to_scalar(gtype_to_text_internal, AG_GET_ARG_GTYPE_P(1), "text");

    Datum d = DirectFunctionCall2Coll(text_starts_with, DEFAULT_COLLATION_OID, x, y);

    AG_RETURN_GTYPE_P(boolean_to_gtype(DatumGetBool(d)));
}

PG_FUNCTION_INFO_V1(gtype_string_match_ends_with);
/*
 * Execution function for ENDS WITH
 */
Datum gtype_string_match_ends_with(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(lhs) && AGT_ROOT_IS_SCALAR(rhs))
    {
        gtype_value *lhs_value;
        gtype_value *rhs_value;

        lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
        rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

        if (lhs_value->type == AGTV_STRING && rhs_value->type == AGTV_STRING)
        {
            if (lhs_value->val.string.len < rhs_value->val.string.len)
                return boolean_to_gtype(false);

            if (strncmp(lhs_value->val.string.val + lhs_value->val.string.len - rhs_value->val.string.len,
                        rhs_value->val.string.val, rhs_value->val.string.len) == 0)
                return boolean_to_gtype(true);
            else
                return boolean_to_gtype(false);
        }
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}

PG_FUNCTION_INFO_V1(gtype_string_match_contains);
/*
 * Execution function for CONTAINS
 */
Datum gtype_string_match_contains(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    if (AGT_ROOT_IS_SCALAR(lhs) && AGT_ROOT_IS_SCALAR(rhs))
    {
        gtype_value *lhs_value;
        gtype_value *rhs_value;

        lhs_value = get_ith_gtype_value_from_container(&lhs->root, 0);
        rhs_value = get_ith_gtype_value_from_container(&rhs->root, 0);

        if (lhs_value->type == AGTV_STRING && rhs_value->type == AGTV_STRING)
        {
            char *l;
            char *r;

            if (lhs_value->val.string.len < rhs_value->val.string.len)
                return boolean_to_gtype(false);

            l = pnstrdup(lhs_value->val.string.val, lhs_value->val.string.len);
            r = pnstrdup(rhs_value->val.string.val, rhs_value->val.string.len);

            if (strstr(l, r) == NULL)
                return boolean_to_gtype(false);
            else
                return boolean_to_gtype(true);
        }
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}

#define LEFT_ROTATE(n, i) ((n << i) | (n >> (64 - i)))
#define RIGHT_ROTATE(n, i)  ((n >> i) | (n << (64 - i)))

//Hashing Function for Hash Indexes
PG_FUNCTION_INFO_V1(gtype_hash_cmp);

Datum gtype_hash_cmp(PG_FUNCTION_ARGS)
{
    uint64 hash = 0;
    gtype *agt;
    gtype_iterator *it;
    gtype_iterator_token tok;
    gtype_value *r;
    uint64 seed = 0xF0F0F0F0;

    if (PG_ARGISNULL(0))
        PG_RETURN_INT16(0);

    agt = AG_GET_ARG_GTYPE_P(0);

    r = palloc0(sizeof(gtype_value));

    it = gtype_iterator_init(&agt->root);
    while ((tok = gtype_iterator_next(&it, r, false)) != WGT_DONE)
    {
        if (IS_A_GTYPE_SCALAR(r) && GTYPE_ITERATOR_TOKEN_IS_HASHABLE(tok))
            gtype_hash_scalar_value_extended(r, &hash, seed);
        else if (tok == WGT_BEGIN_ARRAY && !r->val.array.raw_scalar)
            seed = LEFT_ROTATE(seed, 4);
        else if (tok == WGT_BEGIN_OBJECT)
            seed = LEFT_ROTATE(seed, 6);
        else if (tok == WGT_END_ARRAY && !r->val.array.raw_scalar)
            seed = RIGHT_ROTATE(seed, 4);
        else if (tok == WGT_END_OBJECT)
            seed = RIGHT_ROTATE(seed, 4);

        seed = LEFT_ROTATE(seed, 1);
    }

    PG_RETURN_INT16(hash);
}

// Comparision function for btree Indexes
PG_FUNCTION_INFO_V1(gtype_btree_cmp);

Datum gtype_btree_cmp(PG_FUNCTION_ARGS)
{
    gtype *gtype_lhs;
    gtype *gtype_rhs;

    if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
        PG_RETURN_INT16(0);
    else if (PG_ARGISNULL(0))
        PG_RETURN_INT16(1);
    else if (PG_ARGISNULL(1))
        PG_RETURN_INT16(-1);

    gtype_lhs = AG_GET_ARG_GTYPE_P(0);
    gtype_rhs = AG_GET_ARG_GTYPE_P(1);

    PG_RETURN_INT16(compare_gtype_containers_orderability(&gtype_lhs->root, &gtype_rhs->root));
}

/*
 * Helper function to return the Datum value of a column (attribute) in a heap
 * tuple (row) given the column number (starting from 0), attribute name, typid,
 * and whether it can be null. The function is designed to extract and validate
 * that the data (attribute) is what is expected. The function will error on any
 * issues.
 */
Datum column_get_datum(TupleDesc tupdesc, HeapTuple tuple, int column,
                       const char *attname, Oid typid, bool isnull)
{
    Form_pg_attribute att;
    HeapTupleHeader hth;
    HeapTupleData tmptup, *htd;
    Datum result;
    bool _isnull = true;

    // build the heap tuple data 
    hth = tuple->t_data;
    tmptup.t_len = HeapTupleHeaderGetDatumLength(hth);
    tmptup.t_data = hth;
    htd = &tmptup;

    // get the description for the column from the tuple descriptor 
    att = TupleDescAttr(tupdesc, column);
    // get the datum (attribute) for that column
    result = heap_getattr(htd, column + 1, tupdesc, &_isnull);
    // verify that the attribute typid is as expected 
    if (att->atttypid != typid)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid attribute typid. Expected %d, found %d", typid,
                        att->atttypid)));
    // verify that the attribute name is as expected 
    if (strcmp(att->attname.data, attname) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid attribute name. Expected %s, found %s",
                        attname, att->attname.data)));
    // verify that if it is null, it is allowed to be null 
    if (isnull == false && _isnull == true)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Attribute was found to be null when null is not allowed.")));

    return result;
}

PG_FUNCTION_INFO_V1(gtype_head);

Datum gtype_head(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_ARRAY(agt) || AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("head() argument must resolve to a list")));

    if (AGT_ROOT_COUNT(agt) == 0)
        PG_RETURN_NULL();

    gtype_value *agtv_result = get_ith_gtype_value_from_container(&agt->root, 0);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_last);

Datum gtype_last(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_ARRAY(gt) || AGT_ROOT_IS_SCALAR(gt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("last() argument must resolve to a list or null")));

    if (0 == AGT_ROOT_COUNT(gt))
        PG_RETURN_NULL();

    gtype_value *gtv = get_ith_gtype_value_from_container(&gt->root, AGT_ROOT_COUNT(gt) - 1);

    if (gtv->type == AGTV_NULL)
        PG_RETURN_NULL();

    PG_RETURN_POINTER(gtype_value_to_gtype(gtv));
}

PG_FUNCTION_INFO_V1(gtype_size);
Datum gtype_size(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    int64 result;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if ((AGT_ROOT_IS_SCALAR(agt) && !is_gtype_string(agt)) || (!AGT_ROOT_IS_SCALAR(agt) && !AGT_ROOT_IS_ARRAY(agt)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("size() unsupported argument")));

    if (AGT_ROOT_IS_SCALAR(agt)) {
        gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

        result = agtv_value->val.string.len;
    } else {
        result = AGT_ROOT_COUNT(agt);
    }

    gtype_value agtv = { .type = AGTV_INTEGER, .val.int_value = result };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
}

PG_FUNCTION_INFO_V1(gtype_reverse);

Datum gtype_reverse(PG_FUNCTION_ARGS)
{
    gtype *gt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(gt) && AGT_ROOT_IS_ARRAY(gt))
    {
        gtype_parse_state *parse_state = NULL;
        gtype_value *agtv_value = push_gtype_value(&parse_state, WGT_BEGIN_ARRAY, NULL);

	for (int i = AGT_ROOT_COUNT(gt) - 1; i >= 0; i--) {
            gtype_value *gtv = get_ith_gtype_value_from_container(&gt->root, i);
    
            agtv_value = push_gtype_value(&parse_state, WGT_ELEM, gtv);
	}

        agtv_value = push_gtype_value(&parse_state, WGT_END_ARRAY, NULL);

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(agtv_value));
   }
   else
   {
        Datum x = convert_to_scalar(gtype_to_text_internal, gt, "text");

        Datum d = DirectFunctionCall1(text_reverse, x);

        gtype_value gtv = { .type = AGTV_STRING, .val.string = { VARSIZE(d), text_to_cstring(d) }};

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));

   }
}

/*
 * For the given properties, update the property with the key equal
 * to var_name with the value defined in new_v. If the remove_property
 * flag is set, simply remove the property with the given property
 * name instead.
 */
gtype_value *alter_property_value(gtype *properties, char *var_name,
                                   gtype *new_v, bool remove_property)
{
    gtype_iterator *it;
    gtype_iterator_token tok = WGT_DONE;
    gtype_parse_state *parse_state = NULL;
    gtype_value *r;
    gtype *prop_gtype;
    gtype_value *parsed_gtype_value = NULL;
    bool found;

    // if no properties, return NULL
    if (properties == NULL)
        return NULL;

    // if properties is not an object, throw an error
    if (!GTYPE_CONTAINER_IS_OBJECT(&properties->root))//->type != AGTV_OBJECT)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("can only update objects")));

    r = palloc0(sizeof(gtype_value));

    prop_gtype = properties;
    it = gtype_iterator_init(&prop_gtype->root);
    tok = gtype_iterator_next(&it, r, true);

    parsed_gtype_value = push_gtype_value(&parse_state, tok, tok < WGT_BEGIN_ARRAY ? r : NULL);

    /*
     * If the new value is NULL, this is equivalent to the remove_property
     * flag set to true.
     */
    if (new_v == NULL)
        remove_property = true;

    found = false;
    while (true)
    {
        char *str;

        tok = gtype_iterator_next(&it, r, true);

        if (tok == WGT_DONE || tok == WGT_END_OBJECT)
            break;

        str = pnstrdup(r->val.string.val, r->val.string.len);

        /*
         * Check the key value, if it is equal to the passed in
         * var_name, replace the value for this key with the passed
         * in gtype. Otherwise pass the existing value to the
         * new properties gtype_value.
         */
        if (strcmp(str, var_name))
        {
            // push the key
            parsed_gtype_value = push_gtype_value(&parse_state, tok, tok < WGT_BEGIN_ARRAY ? r : NULL);

            // get the value and push the value
            tok = gtype_iterator_next(&it, r, true);
            parsed_gtype_value = push_gtype_value(&parse_state, tok, r);
        }
        else
        {
            gtype_value *new_gtype_value_v;

            // if the remove flag is set, don't push the key or any value
            if(remove_property)
            {
                // skip the value
                tok = gtype_iterator_next(&it, r, true);
                continue;
            }

            // push the key
            parsed_gtype_value = push_gtype_value(
                &parse_state, tok, tok < WGT_BEGIN_ARRAY ? r : NULL);

            // skip the existing value for the key
            tok = gtype_iterator_next(&it, r, true);

            /*
             * If the the new gtype is scalar, push the gtype_value to the
             * parse state. If the gtype is an object or array convert the
             * gtype to a binary gtype_value to pass to the parse_state.
             * This will save uncessary deserialization and serialization
             * logic from running.
             */
            if (GTYPE_CONTAINER_IS_SCALAR(&new_v->root))
            {
                //get the scalar value and push as the value
                new_gtype_value_v = get_ith_gtype_value_from_container(&new_v->root, 0);

                parsed_gtype_value = push_gtype_value(&parse_state, WGT_VALUE, new_gtype_value_v);
            }
            else
            {
                gtype_value *result = palloc(sizeof(gtype_value)); 

                result->type = AGTV_BINARY;
                result->val.binary.len = GTYPE_CONTAINER_SIZE(&new_v->root);
                result->val.binary.data = &new_v->root;
                parsed_gtype_value = push_gtype_value(&parse_state, WGT_VALUE, result);
            }

            found = true;
        }
    }

    /*
     * If we have not found the property and we aren't trying to remove it,
     * add the key/value pair now.
     */
    if (!found && !remove_property)
    {
        gtype_value *new_gtype_value_v;
        gtype_value *key = string_to_gtype_value(var_name);

        // push the new key
        parsed_gtype_value = push_gtype_value(&parse_state, WGT_KEY, key);

        /*
         * If the the new gtype is scalar, push the gtype_value to the
         * parse state. If the gtype is an object or array convert the
         * gtype to a binary gtype_value to pass to the parse_state.
         * This will save uncessary deserialization and serialization
         * logic from running.
         */
        if (GTYPE_CONTAINER_IS_SCALAR(&new_v->root))
        {
            new_gtype_value_v = get_ith_gtype_value_from_container(&new_v->root, 0);

            // convert the gtype array or object to a binary gtype_value
            parsed_gtype_value = push_gtype_value(&parse_state, WGT_VALUE, new_gtype_value_v);
        }
        else
        {
            gtype_value *result = palloc(sizeof(gtype_value)); 

            result->type = AGTV_BINARY;
            result->val.binary.len = GTYPE_CONTAINER_SIZE(&new_v->root);
            result->val.binary.data = &new_v->root;
            parsed_gtype_value = push_gtype_value(&parse_state, WGT_VALUE, result);
        }
    }

    // push the end object token to parse state
    parsed_gtype_value = push_gtype_value(&parse_state, WGT_END_OBJECT, NULL);

    return parsed_gtype_value;
}

/*
 * Transfer function for age_sum(gtype, gtype).
 *
 * Note: that the running sum will change type depending on the
 * precision of the input. The most precise value determines the
 * result type.
 */
PG_FUNCTION_INFO_V1(gtype_gtype_sum);

Datum gtype_gtype_sum(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    gtype_value gtv;
    if (is_gtype_numeric(lhs) || is_gtype_numeric(rhs))
        gtv.type = AGTV_NUMERIC;
    else if (is_gtype_float(lhs) || is_gtype_float(rhs))
        gtv.type = AGTV_FLOAT;
    else
        gtv.type = AGTV_INTEGER;

    Datum x, y;
    switch(gtv.type)
    {
        case AGTV_INTEGER:
            x = convert_to_scalar(gtype_to_int8_internal, lhs, "int");
	    y = convert_to_scalar(gtype_to_int8_internal, rhs, "int");
	    gtv.val.int_value = DatumGetInt64(DirectFunctionCall2(int8pl, x, y));
	    break;
        case AGTV_FLOAT:
            x = convert_to_scalar(gtype_to_float8_internal, lhs, "float");
            y = convert_to_scalar(gtype_to_float8_internal, rhs, "float");
	    gtv.val.float_value = DatumGetFloat8(DirectFunctionCall2(float8pl, x, y));
            break;
        case AGTV_NUMERIC:
            x = convert_to_scalar(gtype_to_numeric_internal, lhs, "numeric");
            y = convert_to_scalar(gtype_to_numeric_internal, rhs, "numeric");
	    gtv.val.numeric = DatumGetNumeric(DirectFunctionCall2(numeric_add, x, y));
            break;
    }

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_accum);

Datum gtype_accum(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(DirectFunctionCall2(float8_accum, PG_GETARG_DATUM(0),
		                        convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float")));
}

PG_FUNCTION_INFO_V1(gtype_regr_accum);

Datum gtype_regr_accum(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(DirectFunctionCall3(float8_regr_accum, PG_GETARG_DATUM(0), 
		                        convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(1), "float"),
				                convert_to_scalar(gtype_to_float8_internal, AG_GET_ARG_GTYPE_P(2), "float")));
}   


PG_FUNCTION_INFO_V1(gtype_corr_final);
Datum
gtype_corr_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_corr) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_covar_pop_final);
Datum
gtype_covar_pop_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_corr) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_covar_samp_final);
Datum
gtype_covar_samp_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_covar_samp) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_sxx_final);
Datum
gtype_regr_sxx_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_sxx) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_syy_final);
Datum
gtype_regr_syy_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_syy) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_sxy_final);
Datum
gtype_regr_sxy_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_syy) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_slope_final);
Datum
gtype_regr_slope_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_slope) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_intercept_final);
Datum
gtype_regr_intercept_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_intercept) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_avgx_final);
Datum
gtype_regr_avgx_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_avgx) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_avgy_final);
Datum
gtype_regr_avgy_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_avgy) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_regr_r2_final);
Datum
gtype_regr_r2_final(PG_FUNCTION_ARGS) {
    Datum result = (*float8_regr_r2) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv = { .type = AGTV_FLOAT, .val.float_value = DatumGetFloat8(result) };

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_stddev_samp_final);

Datum gtype_stddev_samp_final(PG_FUNCTION_ARGS)
{
    PGFunction func = float8_stddev_samp;
    Datum result = (*func) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv;
    gtv.type = AGTV_FLOAT;
    gtv.val.float_value = DatumGetFloat8(result);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_stddev_pop_final);

Datum gtype_stddev_pop_final(PG_FUNCTION_ARGS)
{
    PGFunction func = float8_stddev_pop;
    Datum result = (*func) (fcinfo);

    if (fcinfo->isnull)
        PG_RETURN_NULL();

    gtype_value gtv;
    gtv.type = AGTV_FLOAT;
    gtv.val.float_value = DatumGetFloat8(result);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&gtv));
}

PG_FUNCTION_INFO_V1(gtype_max_trans);

Datum gtype_max_trans(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P((compare_gtype_containers_orderability(&lhs->root, &rhs->root) >= 0) ? lhs : rhs);
}

PG_FUNCTION_INFO_V1(gtype_min_trans);

Datum gtype_min_trans(PG_FUNCTION_ARGS)
{
    gtype *lhs = AG_GET_ARG_GTYPE_P(0);
    gtype *rhs = AG_GET_ARG_GTYPE_P(1);

    AG_RETURN_GTYPE_P((compare_gtype_containers_orderability(&lhs->root, &rhs->root) <= 0) ? lhs : rhs);
}

// borrowed from PGs float8 routines for percentile_cont 
static Datum float8_lerp(Datum lo, Datum hi, double pct)
{
    double loval = DatumGetFloat8(lo);
    double hival = DatumGetFloat8(hi);

    return Float8GetDatum(loval + (pct * (hival - loval)));
}

// Code borrowed and adjusted from PG's ordered_set_transition function 
PG_FUNCTION_INFO_V1(gtype_percentile_aggtransfn);

Datum gtype_percentile_aggtransfn(PG_FUNCTION_ARGS)
{
    PercentileGroupAggState *pgastate;

    // verify we are in an aggregate context 
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    // if this is the first invocation, create the state 
    if (PG_ARGISNULL(0))
    {
        MemoryContext old_mcxt;
        float8 percentile;

        // validate the percentile 
        if (PG_ARGISNULL(2))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("percentile value NULL is not a valid numeric value")));

        percentile = DatumGetFloat8(DirectFunctionCall1(gtype_to_float8,
                         PG_GETARG_DATUM(2)));

        if (percentile < 0 || percentile > 1 || isnan(percentile))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("percentile value %g is not between 0 and 1",
                               percentile)));

        // switch to the correct aggregate context 
        old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        // create and initialize the state 
        pgastate = palloc0(sizeof(PercentileGroupAggState));
        pgastate->percentile = percentile;
        /*
         * Percentiles need to be calculated from a sorted set. We are only
         * using float8 values, using the less than operator, and flagging
         * randomAccess to true - as we can potentially be reusing this
         * sort multiple times in the same query.
         */
        pgastate->sortstate = tuplesort_begin_datum(FLOAT8OID,
                                                    Float8LessOperator,
                                                    InvalidOid, false, work_mem,
                                                    NULL, true);
        pgastate->number_of_rows = 0;
        pgastate->sort_done = false;

        // restore the old context 
        MemoryContextSwitchTo(old_mcxt);
    }
    // otherwise, retrieve the state 
    else
        pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);

    // Load the datum into the tuplesort object, but only if it's not null 
    if (!PG_ARGISNULL(1))
    {
        Datum dfloat = DirectFunctionCall1(gtype_to_float8, PG_GETARG_DATUM(1));

        tuplesort_putdatum(pgastate->sortstate, dfloat, false);
        pgastate->number_of_rows++;
    }
    // return the state 
    PG_RETURN_POINTER(pgastate);
}

// Code borrowed and adjusted from PG's percentile_cont_final function 
PG_FUNCTION_INFO_V1(gtype_percentile_cont_aggfinalfn);

Datum gtype_percentile_cont_aggfinalfn(PG_FUNCTION_ARGS)
{
    PercentileGroupAggState *pgastate;
    float8 percentile;
    int64 first_row = 0;
    int64 second_row = 0;
    Datum val;
    Datum first_val;
    Datum second_val;
    double proportion;
    bool isnull;
    gtype_value agtv_float;

    // verify we are in an aggregate context 
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    // If there were no regular rows, the result is NULL 
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    // retrieve the state and percentile 
    pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);
    percentile = pgastate->percentile;

    // number_of_rows could be zero if we only saw NULL input values 
    if (pgastate->number_of_rows == 0)
        PG_RETURN_NULL();

    // Finish the sort, or rescan if we already did 
    if (!pgastate->sort_done)
    {
        tuplesort_performsort(pgastate->sortstate);
        pgastate->sort_done = true;
    }
    else
        tuplesort_rescan(pgastate->sortstate);

    // calculate the percentile cont
    first_row = floor(percentile * (pgastate->number_of_rows - 1));
    second_row = ceil(percentile * (pgastate->number_of_rows - 1));

    Assert(first_row < pgastate->number_of_rows);

    if (!tuplesort_skiptuples(pgastate->sortstate, first_row, true))
        elog(ERROR, "missing row in percentile_cont");

    if (!tuplesort_getdatum(pgastate->sortstate, true, &first_val, &isnull, NULL))
        elog(ERROR, "missing row in percentile_cont");
    if (isnull)
        PG_RETURN_NULL();

    if (first_row == second_row)
    {
        val = first_val;
    }
    else
    {
        if (!tuplesort_getdatum(pgastate->sortstate, true, &second_val, &isnull, NULL))
            elog(ERROR, "missing row in percentile_cont");

        if (isnull)
            PG_RETURN_NULL();

        proportion = (percentile * (pgastate->number_of_rows - 1)) - first_row;
        val = float8_lerp(first_val, second_val, proportion);
    }

    // convert to an gtype float and return the result 
    agtv_float.type = AGTV_FLOAT;
    agtv_float.val.float_value = DatumGetFloat8(val);

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

// Code borrowed and adjusted from PG's percentile_disc_final function 
PG_FUNCTION_INFO_V1(gtype_percentile_disc_aggfinalfn);

Datum gtype_percentile_disc_aggfinalfn(PG_FUNCTION_ARGS)
{
    PercentileGroupAggState *pgastate;
    double percentile;
    Datum val;
    bool isnull;
    int64 rownum;
    gtype_value agtv_float;

    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    // If there were no regular rows, the result is NULL 
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);
    percentile = pgastate->percentile;

    // number_of_rows could be zero if we only saw NULL input values 
    if (pgastate->number_of_rows == 0)
        PG_RETURN_NULL();

    // Finish the sort, or rescan if we already did 
    if (!pgastate->sort_done)
    {
        tuplesort_performsort(pgastate->sortstate);
        pgastate->sort_done = true;
    }
    else
        tuplesort_rescan(pgastate->sortstate);

    /*----------
     * We need the smallest K such that (K/N) >= percentile.
     * N>0, therefore K >= N*percentile, therefore K = ceil(N*percentile).
     * So we skip K-1 rows (if K>0) and return the next row fetched.
     *----------
     */
    rownum = (int64) ceil(percentile * pgastate->number_of_rows);
    Assert(rownum <= pgastate->number_of_rows);

    if (rownum > 1)
    {
        if (!tuplesort_skiptuples(pgastate->sortstate, rownum - 1, true))
            elog(ERROR, "missing row in percentile_disc");
    }

    if (!tuplesort_getdatum(pgastate->sortstate, true, &val, &isnull, NULL))
        elog(ERROR, "missing row in percentile_disc");

    // We shouldn't have stored any nulls, but do the right thing anyway 
    if (isnull)
        PG_RETURN_NULL();

    // convert to an gtype float and return the result 
    agtv_float.type = AGTV_FLOAT;
    agtv_float.val.float_value = DatumGetFloat8(val);

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

// functions to support the aggregate function COLLECT() 
PG_FUNCTION_INFO_V1(gtype_collect_aggtransfn);

Datum gtype_collect_aggtransfn(PG_FUNCTION_ARGS)
{
    // verify we are in an aggregate context 
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /*
     * Switch to the correct aggregate context. Otherwise, the data added to the
     * array will be lost.
     */
    MemoryContext old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);


    // if this is the first invocation, create the state 
    gtype_in_state *castate;
    if (PG_ARGISNULL(0)) {
        castate = palloc0(sizeof(gtype_in_state));

        castate->res = push_gtype_value(&castate->parse_state, WGT_BEGIN_ARRAY, NULL);
    } else {
        castate = (gtype_in_state *) PG_GETARG_POINTER(0);
    }

    if (!PG_ARGISNULL(1)) {
        gtype *gt = AG_GET_ARG_GTYPE_P(1);

            
        add_gtype(gt, false, castate, GTYPEOID, false);
    }

    MemoryContextSwitchTo(old_mcxt);

    PG_RETURN_POINTER(castate);
}

PG_FUNCTION_INFO_V1(gtype_collect_aggfinalfn);

Datum gtype_collect_aggfinalfn(PG_FUNCTION_ARGS) {
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    gtype_in_state *castate = (gtype_in_state *) PG_GETARG_POINTER(0);

    MemoryContext old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

    castate->res = push_gtype_value(&castate->parse_state, WGT_END_ARRAY, NULL);

    MemoryContextSwitchTo(old_mcxt);

    PG_RETURN_POINTER(gtype_value_to_gtype(castate->res));
}

/*
 * Extract an gtype_value from an gtype and optionally verify that it is of
 * the correct type. It will always complain if the passed argument is not a
 * scalar.
 *
 * Optionally, the function will throw an error, stating the calling function
 * name, for invalid values - including AGTV_NULL
 *
 * Note: This only works for scalars wrapped in an array container, not
 * in objects.
 */
gtype_value *get_gtype_value(char *funcname, gtype *agt_arg,
                               enum gtype_value_type type, bool error)
{
    gtype_value *agtv_value = NULL;

    // we need these 
    Assert(funcname != NULL);
    Assert(agt_arg != NULL);

    // error if the argument is not a scalar 
    if (!GTYPE_CONTAINER_IS_SCALAR(&agt_arg->root))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument must be a scalar", funcname)));

    // is it AGTV_NULL? 
    if (error && is_gtype_null(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument must not be AGTV_NULL", funcname)));

    // get the gtype value 
    agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    // is it the correct type? 
    if (error && agtv_value->type != type)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument of wrong type", funcname)));

    return agtv_value;
}

/*
 * Helper function to step through and retrieve keys from an object.
 * borrowed and modified from get_next_object_pair() in gtype_vle.c
 */
static gtype_iterator *get_next_object_key(gtype_iterator *it, gtype_container *agtc, gtype_value *key) {
    gtype_iterator_token itok;
    gtype_value tmp;

    // verify input params 
    Assert(agtc != NULL);
    Assert(key != NULL);

    // check to see if the container is empty 
    if (GTYPE_CONTAINER_SIZE(agtc) == 0)
        return NULL;

    // if the passed iterator is NULL, this is the first time, create it 
    if (it == NULL) {
        // initial the iterator 
        it = gtype_iterator_init(agtc);
        // get the first token 
        itok = gtype_iterator_next(&it, &tmp, false);
        // it should be WGT_BEGIN_OBJECT 
        Assert(itok == WGT_BEGIN_OBJECT);
    }

    // the next token should be a key or the end of the object 
    itok = gtype_iterator_next(&it, &tmp, false);
    Assert(itok == WGT_KEY || WGT_END_OBJECT);
    // if this is the end of the object return NULL 
    if (itok == WGT_END_OBJECT)
        return NULL;

    // this should be the key, copy it 
    if (itok == WGT_KEY)
        memcpy(key, &tmp, sizeof(gtype_value));

    /*
     * The next token should be a value but, it could be a begin tokens for
     * arrays or objects. For those we just return NULL to ignore them.
     */
    itok = gtype_iterator_next(&it, &tmp, true);
    Assert(itok == WGT_VALUE);

    // return the iterator 
    return it;
}

PG_FUNCTION_INFO_V1(vertex_keys);
Datum vertex_keys(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    vertex *v = AG_GET_ARG_VERTEX(0);
    	agt_arg = extract_vertex_properties(v);

    agtv_result = push_gtype_value(&parse_state, WGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WGT_ELEM, &obj_key);

    // push the end of the array
    agtv_result = push_gtype_value(&parse_state, WGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}


PG_FUNCTION_INFO_V1(edge_keys);
Datum edge_keys(PG_FUNCTION_ARGS)
{   
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    edge *v = AG_GET_ARG_EDGE(0);
        agt_arg = extract_edge_properties(v);

    agtv_result = push_gtype_value(&parse_state, WGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WGT_ELEM, &obj_key);

    // push the end of the array
    agtv_result = push_gtype_value(&parse_state, WGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}



PG_FUNCTION_INFO_V1(gtype_keys);
Datum gtype_keys(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    agt_arg = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt_arg))
        PG_RETURN_NULL();

    if (!AGT_ROOT_IS_OBJECT(agt_arg)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("keys() argument must be an object")));
    }

    agtv_result = push_gtype_value(&parse_state, WGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WGT_ELEM, &obj_key);

    // push the end of the array
    agtv_result = push_gtype_value(&parse_state, WGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}

PG_FUNCTION_INFO_V1(gtype_range_w_skip);
// range function
Datum gtype_range_w_skip(PG_FUNCTION_ARGS)
{
    int64 start = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(0), "int"));
    int64 end = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(1), "int"));
    int64 step = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(2), "int"));

    if (step == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(gtype, gtype, gtype): arg 3 cannot be 0")));

    gtype_in_state in_state;
    MemSet(&in_state, 0, sizeof(gtype_in_state));

    in_state.res = push_gtype_value(&in_state.parse_state, WGT_BEGIN_ARRAY, NULL);

    for (int64 i = start; (step > 0 && i <= end) || (step < 0 && i >= end); i += step) {
        gtype_value gtv;

        gtv.type = AGTV_INTEGER;
        gtv.val.int_value = i;

        in_state.res = push_gtype_value(&in_state.parse_state, WGT_ELEM, &gtv);
    }

    in_state.res = push_gtype_value(&in_state.parse_state, WGT_END_ARRAY, NULL);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(in_state.res));
}

PG_FUNCTION_INFO_V1(gtype_range);
// range function
Datum gtype_range(PG_FUNCTION_ARGS)
{
    int64 start = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(0), "int"));
    int64 end = DatumGetInt64(convert_to_scalar(gtype_to_int8_internal, AG_GET_ARG_GTYPE_P(1), "int"));

    gtype_in_state in_state;
    MemSet(&in_state, 0, sizeof(gtype_in_state));

    in_state.res = push_gtype_value(&in_state.parse_state, WGT_BEGIN_ARRAY, NULL);

    for (int64 i = start; i <= end; i++) {
        gtype_value gtv;

        gtv.type = AGTV_INTEGER;
        gtv.val.int_value = i;
        
        in_state.res = push_gtype_value(&in_state.parse_state, WGT_ELEM, &gtv);
    }

    in_state.res = push_gtype_value(&in_state.parse_state, WGT_END_ARRAY, NULL);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(in_state.res));
}

PG_FUNCTION_INFO_V1(gtype_unnest);
/*
 * Function to convert the Array type of Agtype into each row. It is used for
 * Cypher `UNWIND` clause, but considering the situation in which the user can
 * directly use this function in vanilla PGSQL, put a second parameter related
 * to this.
 */
Datum gtype_unnest(PG_FUNCTION_ARGS)
{
    gtype *gtype_arg = AG_GET_ARG_GTYPE_P(0);
    bool block_types = PG_GETARG_BOOL(1);
    ReturnSetInfo *rsi;
    Tuplestorestate *tuple_store;
    TupleDesc tupdesc;
    TupleDesc ret_tdesc;
    MemoryContext old_cxt, tmp_cxt;
    bool skipNested = false;
    gtype_iterator *it;
    gtype_value v;
    gtype_iterator_token r;

    if (!AGT_ROOT_IS_ARRAY(gtype_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("cannot extract elements from an object")));

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    rsi->returnMode = SFRM_Materialize;

    // it's a simple type, so don't use get_call_result_type() 
    tupdesc = rsi->expectedDesc;

    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

    ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(ret_tdesc);
    tuple_store = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, work_mem);

    MemoryContextSwitchTo(old_cxt);

    tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "age_unnest temporary cxt", ALLOCSET_DEFAULT_SIZES);

    it = gtype_iterator_init(&gtype_arg->root);

    while ((r = gtype_iterator_next(&it, &v, skipNested)) != WGT_DONE)
    {
        skipNested = true;

        if (r == WGT_ELEM)
        {
            HeapTuple tuple;
            Datum values[1];
            bool nulls[1] = {false};
            gtype *val = gtype_value_to_gtype(&v);

            // use the tmp context so we can clean up after each tuple is done 
            old_cxt = MemoryContextSwitchTo(tmp_cxt);

            values[0] = PointerGetDatum(val);

            tuple = heap_form_tuple(ret_tdesc, values, nulls);

            tuplestore_puttuple(tuple_store, tuple);

            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }

    MemoryContextDelete(tmp_cxt);

    rsi->setResult = tuple_store;
    rsi->setDesc = ret_tdesc;

    PG_RETURN_NULL();
}


