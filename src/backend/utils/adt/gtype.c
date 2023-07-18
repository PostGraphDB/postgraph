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
#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
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
#include "utils/typcache.h"

#include "utils/age_vle.h"
#include "utils/gtype.h"
#include "utils/edge.h"
#include "utils/variable_edge.h"
#include "utils/vertex.h"
#include "utils/gtype_parser.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"
#include "utils/numeric.h"

/* State structure for Percentile aggregate functions */
typedef struct PercentileGroupAggState
{
    /* percentile value */
    float8 percentile;
    /* Sort object we're accumulating data in: */
    Tuplesortstate *sortstate;
    /* Number of normal rows inserted into sortstate: */
    int64 number_of_rows;
    /* Have we already done tuplesort_performsort? */
    bool sort_done;
} PercentileGroupAggState;

typedef enum /* type categories for datum_to_gtype */
{
    AGT_TYPE_NULL, /* null, so we didn't bother to identify */
    AGT_TYPE_BOOL, /* boolean (built-in types only) */
    AGT_TYPE_INTEGER, /* Cypher Integer type */
    AGT_TYPE_FLOAT, /* Cypher Float type */
    AGT_TYPE_NUMERIC, /* numeric (ditto) */
    AGT_TYPE_DATE, /* we use special formatting for datetimes */
    AGT_TYPE_TIMESTAMP, /* we use special formatting for timestamp */
    AGT_TYPE_TIMESTAMPTZ, /* ... and timestamptz */
    AGT_TYPE_GTYPE, /* GTYPE */
    AGT_TYPE_JSON, /* JSON */
    AGT_TYPE_JSONB, /* JSONB */
    AGT_TYPE_ARRAY, /* array */
    AGT_TYPE_COMPOSITE, /* composite */
    AGT_TYPE_JSONCAST, /* something with an explicit cast to JSON */
    AGT_TYPE_VERTEX,
    AGT_TYPE_OTHER /* all else */
} agt_type_category;

size_t check_string_length(size_t len);
static void gtype_in_gtype_annotation(void *pstate, char *annotation);
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
static void array_to_gtype_internal(Datum array, gtype_in_state *result);
static void datum_to_gtype(Datum val, bool is_null, gtype_in_state *result, agt_type_category tcategory, Oid outfuncoid, bool key_scalar);
static char *gtype_to_cstring_worker(StringInfo out, gtype_container *in, int estimated_len, bool indent);
static text *gtype_value_to_text(gtype_value *scalar_val, bool err_not_scalar);
static void add_indent(StringInfo out, bool indent, int level);
static void cannot_cast_gtype_value(enum gtype_value_type type, const char *sqltype);
static bool gtype_extract_scalar(gtype_container *agtc, gtype_value *res);
static gtype_value *execute_array_access_operator_internal(gtype *array, int64 array_index);
/* typecast functions */
static void gtype_typecast_object(gtype_in_state *state, char *annotation);
/* validation functions */
static bool is_object_vertex(gtype_value *agtv);
static bool is_object_edge(gtype_value *agtv);
/* graph entity retrieval */
static Datum get_vertex(const char *graph, const char *vertex_label, int64 graphid);
static char *get_label_name(const char *graph_name, int64 label_id);
static float8 get_float_compatible_arg(Datum arg, Oid type, char *funcname, bool *is_null);
static Numeric get_numeric_compatible_arg(Datum arg, Oid type, char *funcname, bool *is_null, enum gtype_value_type *ag_type);
gtype *get_one_gtype_from_variadic_args(FunctionCallInfo fcinfo, int variadic_offset, int expected_nargs);
static int64 get_int64_from_int_datums(Datum d, Oid type, char *funcname, bool *is_agnull);
static gtype_iterator *get_next_object_key(gtype_iterator *it, gtype_container *agtc, gtype_value *key);
static gtype_iterator *get_next_list_element(gtype_iterator *it, gtype_container *agtc, gtype_value *elem);
gtype_value *gtype_composite_to_gtype_value_binary(gtype *a);
static Datum process_access_operator_result(FunctionCallInfo fcinfo, gtype_value *agtv, bool as_text);
static Datum process_access_operator_result(FunctionCallInfo fcinfo, gtype_value *agtv, bool as_text);
Datum gtype_array_element_impl(FunctionCallInfo fcinfo, gtype *gtype_in, int element, bool as_text);

// Used to extact properties field from vertices and edges quickly
static const gtype_value id_key = {
    .type = AGTV_STRING,
    .val.string = {2, "id"}
};
static const gtype_value start_key = {
    .type = AGTV_STRING,
    .val.string = {8, "start_id"}
};
static const gtype_value end_key = {
    .type = AGTV_STRING,
    .val.string = {6, "end_id"}
};
static const gtype_value prop_key = {
    .type = AGTV_STRING,
    .val.string = {10, "properties"}
};

PG_FUNCTION_INFO_V1(gtype_build_map_noargs);

/*              
 * degenerate case of gtype_build_map where it gets 0 arguments.
 */                 
Datum gtype_build_map_noargs(PG_FUNCTION_ARGS)
{   
    gtype_in_state result;
    
    memset(&result, 0, sizeof(gtype_in_state));
                                          
    push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);
    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL); 
                
    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}    

/* fast helper function to test for AGTV_NULL in an gtype */
bool is_gtype_null(gtype *agt_arg)
{
    gtype_container *agtc = &agt_arg->root;

    if (GTYPE_CONTAINER_IS_SCALAR(agtc) && AGTE_IS_NULL(agtc->children[0]))
        return true;

    return false;
}

bool is_gtype_integer(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && AGTE_IS_GTYPE(agt->root.children[0]) && AGT_IS_INTEGER(agt->root.children[1]);
}

bool is_gtype_float(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && AGTE_IS_GTYPE(agt->root.children[0]) && AGT_IS_FLOAT(agt->root.children[1]);
}

bool is_gtype_numeric(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && AGTE_IS_NUMERIC(agt->root.children[0]);
}

bool is_gtype_string(gtype *agt) {
    return AGT_ROOT_IS_SCALAR(agt) && AGTE_IS_STRING(agt->root.children[0]);
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
    /* callback for annotation (typecasts) */
    sem.gtype_annotation = gtype_in_gtype_annotation;

    parse_gtype(lex, &sem);

    /* after parsing, the item member has the composed gtype structure */
    PG_RETURN_POINTER(gtype_value_to_gtype(state.res));
}

size_t check_string_length(size_t len) {
    if (len > AGTENTRY_OFFLENMASK)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("string too long to represent as gtype string"),
                 errdetail("Due to an implementation restriction, gtype strings cannot exceed %d bytes.", AGTENTRY_OFFLENMASK)));

    return len;
}

static void gtype_in_object_start(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WAGT_BEGIN_OBJECT, NULL);
}

static void gtype_in_object_end(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WAGT_END_OBJECT, NULL);
}

static void gtype_in_array_start(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WAGT_BEGIN_ARRAY, NULL);
}

static void gtype_in_array_end(void *pstate) {
    gtype_in_state *_state = (gtype_in_state *)pstate;

    _state->res = push_gtype_value(&_state->parse_state, WAGT_END_ARRAY, NULL);
}

static void gtype_in_object_field_start(void *pstate, char *fname, bool isnull) {
    gtype_in_state *_state = (gtype_in_state *)pstate;
    gtype_value v;

    Assert(fname != NULL);
    v.type = AGTV_STRING;
    v.val.string.len = check_string_length(strlen(fname));
    v.val.string.val = fname;

    _state->res = push_gtype_value(&_state->parse_state, WAGT_KEY, &v);
}

/* main in function to process annotations */
static void gtype_in_gtype_annotation(void *pstate, char *annotation)
{
    gtype_in_state *_state = (gtype_in_state *)pstate;

    /* verify that our required params are not null */
    Assert(pstate != NULL);
    Assert(annotation != NULL);

    /* pass to the appropriate typecast routine */
    switch (_state->res->type)
    {
    case AGTV_OBJECT:
        gtype_typecast_object(_state, annotation);
        break;

    /*
     * Maybe we need to eventually move scalar annotations here. However,
     * we need to think about how an actual scalar value may be incorporated
     * into another object. Remember, the scalar is copied in on close, before
     * we would apply the annotation.
     */
    default:
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unsupported type to annotate")));
        break;
    }
}

/* function to handle object typecasts */
static void gtype_typecast_object(gtype_in_state *state, char *annotation) {
    gtype_value *agtv = NULL;
    gtype_value *last_updated_value = NULL;
    int len;
    bool top = true;

    /* verify that our required params are not null */
    Assert(annotation != NULL);
    Assert(state != NULL);

    len = strlen(annotation);
    agtv = state->res;

    /*
     * If the parse_state is not NULL, then we are not at the top level
     * and the following must be valid for a nested object with a typecast
     * at the end.
     */
    if (state->parse_state != NULL) {
        top = false;
        last_updated_value = state->parse_state->last_updated_value;
        Assert(last_updated_value != NULL);
        Assert(last_updated_value->type == AGTV_OBJECT);
    }

    /* check for a cast to a vertex */
    if (len == 6 && pg_strncasecmp(annotation, "vertex", len) == 0) {
        if (is_object_vertex(agtv)) {
            agtv->type = AGTV_VERTEX;
            if (!top)
                last_updated_value->type = AGTV_VERTEX;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("object is not a vertex")));
        }

    }
    else if (len == 4 && pg_strncasecmp(annotation, "edge", len) == 0) {
        if (is_object_edge(agtv)) {
            agtv->type = AGTV_EDGE;
            if (!top)
                last_updated_value->type = AGTV_EDGE;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("object is not a edge")));
        }
    }
    /* otherwise this isn't a supported typecast */
    else
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid annotation value for object")));
    }
}

/* helper function to check if an object fits a vertex */
static bool is_object_vertex(gtype_value *agtv) {
    bool has_id = false;
    bool has_label = false;
    bool has_properties = false;
    int i;

    /* we require a valid object */
    Assert(agtv != NULL);
    Assert(agtv->type == AGTV_OBJECT);

    /* we need 3 pairs for a vertex */
    if (agtv->val.object.num_pairs != 3)
        return false;

    /* iterate through all pairs */
    for (i = 0; i < agtv->val.object.num_pairs; i++) {
        gtype_value *key = &agtv->val.object.pairs[i].key;
        gtype_value *value = &agtv->val.object.pairs[i].value;

        char *key_val = key->val.string.val;
        int key_len = key->val.string.len;

        Assert(key->type == AGTV_STRING);

        /* check for an id of type integer */
        if (key_len == 2 && pg_strncasecmp(key_val, "id", key_len) == 0 && value->type == AGTV_INTEGER) {
            has_id = true;
        } else if (key_len == 5 && pg_strncasecmp(key_val, "label", key_len) == 0 && value->type == AGTV_STRING) {
            has_label = true;
        } else if (key_len == 10 && pg_strncasecmp(key_val, "properties", key_len) == 0 && value->type == AGTV_OBJECT) {
            has_properties = true;
        } else {
            return false;
        }
    }
    return (has_id && has_label && has_properties);
}

/* helper function to check if an object fits an edge */
static bool is_object_edge(gtype_value *agtv)
{
    bool has_id = false;
    bool has_label = false;
    bool has_properties = false;
    bool has_start_id = false;
    bool has_end_id = false;
    int i;

    /* we require a valid object */
    Assert(agtv != NULL);
    Assert(agtv->type == AGTV_OBJECT);

    /* we need 5 pairs for an edge */
    if (agtv->val.object.num_pairs != 5)
        return false;

    /* iterate through the pairs */
    for (i = 0; i < agtv->val.object.num_pairs; i++)
    {
        gtype_value *key = &agtv->val.object.pairs[i].key;
        gtype_value *value = &agtv->val.object.pairs[i].value;

        char *key_val = key->val.string.val;
        int key_len = key->val.string.len;

        Assert(key->type == AGTV_STRING);

        if (key_len == 2 && pg_strncasecmp(key_val, "id", key_len) == 0 && value->type == AGTV_INTEGER) {
            has_id = true;
        } else if (key_len == 5 && pg_strncasecmp(key_val, "label", key_len) == 0 && value->type == AGTV_STRING) {
            has_label = true;
        } else if (key_len == 10 && pg_strncasecmp(key_val, "properties", key_len) == 0 && value->type == AGTV_OBJECT) {
            has_properties = true;
        } else if (key_len == 8 && pg_strncasecmp(key_val, "start_id", key_len) == 0 && value->type == AGTV_INTEGER) {
            has_start_id = true;
        } else if (key_len == 6 && pg_strncasecmp(key_val, "end_id", key_len) == 0 && value->type == AGTV_INTEGER) {
            has_end_id = true;
        } else {
            return false;
        }
    }
    return (has_id && has_label && has_properties && has_start_id && has_end_id);
}

static void gtype_put_array(StringInfo out, gtype_value *scalar_val) {
    int i = 0;

    appendBinaryStringInfo(out, "[", 1);

    for (i = 0; i < scalar_val->val.array.num_elems; i++) {
        gtype_value *agtv = &scalar_val->val.array.elems[i];

        if (agtv->type == AGTV_BINARY)
            gtype_to_cstring_worker(out, (gtype_container *)agtv->val.binary.data, agtv->val.binary.len, false);
        else if (agtv->type == AGTV_ARRAY)
	    gtype_put_array(out, agtv);
	else if (agtv->type == AGTV_OBJECT)
	    gtype_put_object(out, agtv);
        else
            gtype_put_escaped_value(out, agtv);

        if (i < scalar_val->val.object.num_pairs -1)
           appendBinaryStringInfo(out, ", ", 2);
    }

    appendBinaryStringInfo(out, "]", 1);
}


static void gtype_put_object(StringInfo out, gtype_value *scalar_val) {
    int i = 0;

    appendBinaryStringInfo(out, "{", 1);	

    for (i = 0; i < scalar_val->val.object.num_pairs; i++) {
	gtype_pair *pairs = &scalar_val->val.object.pairs[i];

	gtype_value *agtv = &pairs->key;
	gtype_put_escaped_value(out, agtv);		

        appendBinaryStringInfo(out, ": ", 2);


	agtv = &pairs->value;

	if (agtv->type == AGTV_BINARY)
	    gtype_to_cstring_worker(out, (gtype_container *)agtv->val.binary.data, agtv->val.binary.len, false);
        else if (agtv->type == AGTV_ARRAY)
            gtype_put_array(out, agtv);
        else if (agtv->type == AGTV_OBJECT)
            gtype_put_object(out, agtv);
	else
	    gtype_put_escaped_value(out, agtv);

	if (i < scalar_val->val.object.num_pairs -1)
	    appendBinaryStringInfo(out, ", ", 2);
    }

    appendBinaryStringInfo(out, "}", 1);
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
    case AGTV_BOOL:
        if (scalar_val->val.boolean)
            appendBinaryStringInfo(out, "true", 4);
        else
            appendBinaryStringInfo(out, "false", 5);
        break;
    case AGTV_VERTEX:
        gtype_put_object(out, scalar_val);  
        appendBinaryStringInfo(out, "::vertex", 8);
        break;
    case AGTV_EDGE:
        gtype_put_object(out, scalar_val);  
        appendBinaryStringInfo(out, "::edge", 6);
        break;
    case AGTV_PATH:
        gtype_put_array(out, scalar_val);
        appendBinaryStringInfo(out, "::path", 6);
        break;
    case AGTV_PARTIAL_PATH:
        gtype_put_array(out, scalar_val);
        appendBinaryStringInfo(out, "::partial_path", 6);
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
        numd = DirectFunctionCall3(numeric_in, CStringGetDatum(token), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        v.val.numeric = DatumGetNumeric(numd);
        break;
    case GTYPE_TOKEN_TIMESTAMP:
        Assert(token != NULL);
        v.type = AGTV_TIMESTAMP;
        v.val.int_value = DatumGetInt64(DirectFunctionCall3(timestamp_in, CStringGetDatum(token), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
        break;

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
        /* should not be possible */
        elog(ERROR, "invalid gtype token type");
        break;
    }

    if (_state->parse_state == NULL) {
        /* single scalar */
        gtype_value va;

        va.type = AGTV_ARRAY;
        va.val.array.raw_scalar = true;
        va.val.array.num_elems = 1;

        _state->res = push_gtype_value(&_state->parse_state, WAGT_BEGIN_ARRAY, &va);
        _state->res = push_gtype_value(&_state->parse_state, WAGT_ELEM, &v);
        _state->res = push_gtype_value(&_state->parse_state, WAGT_END_ARRAY, NULL);
    } else {
        gtype_value *o = &_state->parse_state->cont_val;

        switch (o->type)
        {
        case AGTV_ARRAY:
            _state->res = push_gtype_value(&_state->parse_state, WAGT_ELEM, &v);
            break;
        case AGTV_OBJECT:
            _state->res = push_gtype_value(&_state->parse_state, WAGT_VALUE, &v);
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
    gtype_iterator_token type = WAGT_DONE;
    int level = 0;
    bool redo_switch = false;

    /* If we are indenting, don't add a space after a comma */
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
           ((type = gtype_iterator_next(&it, &v, false)) != WAGT_DONE))
    {
        redo_switch = false;
        switch (type)
        {
        case WAGT_BEGIN_ARRAY:
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
        case WAGT_BEGIN_OBJECT:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);

            add_indent(out, use_indent && !last_was_key, level);
            appendStringInfoCharMacro(out, '{');

            first = true;
            level++;
            break;
        case WAGT_KEY:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = true;

            add_indent(out, use_indent, level);

            /* gtype rules guarantee this is a string */
            gtype_put_escaped_value(out, &v);
            appendBinaryStringInfo(out, ": ", 2);

            type = gtype_iterator_next(&it, &v, false);
            if (type == WAGT_VALUE) {
                first = false;
                gtype_put_escaped_value(out, &v);
            } else {
                Assert(type == WAGT_BEGIN_OBJECT || type == WAGT_BEGIN_ARRAY);

                /*
                 * We need to rerun the current switch() since we need to
                 * output the object which we just got from the iterator
                 * before calling the iterator again.
                 */
                redo_switch = true;
            }
            break;
        case WAGT_ELEM:
            if (!first)
                appendBinaryStringInfo(out, ", ", ispaces);
            first = false;

            if (!raw_scalar)
                add_indent(out, use_indent, level);
            gtype_put_escaped_value(out, &v);
            break;
        case WAGT_END_ARRAY:
            level--;
            if (!raw_scalar) {
                add_indent(out, use_indent, level);
                appendStringInfoCharMacro(out, ']');
            }
            first = false;
            break;
        case WAGT_END_OBJECT:
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
 * Determine how we want to render values of a given type in datum_to_gtype.
 *
 * Given the datatype OID, return its agt_type_category, as well as the type's
 * output function OID.  If the returned category is AGT_TYPE_JSONCAST,
 * we return the OID of the relevant cast function instead.
 */
static void gtype_categorize_type(Oid typoid, agt_type_category *tcategory, Oid *outfuncoid) {
    bool typisvarlena;

    /* Look through any domain */
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

    case FLOAT8OID:
        getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
        *tcategory = AGT_TYPE_FLOAT;
        break;

    case FLOAT4OID:
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

    case JSONBOID:
        *tcategory = AGT_TYPE_JSONB;
        break;

    case JSONOID:
        *tcategory = AGT_TYPE_JSON;
        break;

    default:
        /* Check for arrays and composites */
        if (typoid == GTYPEOID) {
            *tcategory = AGT_TYPE_GTYPE;
        } else if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID || typoid == RECORDARRAYOID) {
            *tcategory = AGT_TYPE_ARRAY;
        } else if (type_is_rowtype(typoid)) /* includes RECORDOID */ {
            *tcategory = AGT_TYPE_COMPOSITE;
        } else if (typoid == GRAPHIDOID) {
            getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
            *tcategory = AGT_TYPE_INTEGER;
        } else {
            /* It's probably the general case ... */
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
                    /* not a cast type, so just get the usual output func */
                    getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
                }
            } else {
                /* any other builtin type */
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

    /* Convert val to an gtype_value in agtv (in most cases) */
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
                agtv.type = AGTV_FLOAT;
                agtv.val.float_value = DatumGetFloat8(val);
            }
            break;
        case AGT_TYPE_NUMERIC:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            if (key_scalar) {
                /* always quote keys */
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
            agtv.type = AGTV_STRING;
            agtv.val.string.val = gtype_encode_date_time(NULL, val, DATEOID);
            agtv.val.string.len = strlen(agtv.val.string.val);
            break;
        case AGT_TYPE_TIMESTAMP:
	    agtv.type = AGTV_TIMESTAMP;
            agtv.val.int_value = DatumGetInt64(val);
            break;
        case AGT_TYPE_TIMESTAMPTZ:
            agtv.type = AGTV_STRING;
            agtv.val.string.val = gtype_encode_date_time(NULL, val, TIMESTAMPTZOID);
            agtv.val.string.len = strlen(agtv.val.string.val);
            break;
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

                while ((type = gtype_iterator_next(&it, &agtv, false)) != WAGT_DONE) {
                    if (type == WAGT_END_ARRAY || type == WAGT_END_OBJECT || type == WAGT_BEGIN_ARRAY || type == WAGT_BEGIN_OBJECT) {
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

    /* Now insert agtv into result, unless we did it recursively */
    if (!is_null && !scalar_gtype && tcategory >= AGT_TYPE_GTYPE && tcategory <= AGT_TYPE_JSONCAST) {
        /* work has been done recursively */
        return;
    } else if (result->parse_state == NULL) {
        /* single root scalar */
        gtype_value va;

        va.type = AGTV_ARRAY;
        va.val.array.raw_scalar = true;
        va.val.array.num_elems = 1;

        result->res = push_gtype_value(&result->parse_state, WAGT_BEGIN_ARRAY, &va);
        result->res = push_gtype_value(&result->parse_state, WAGT_ELEM, &agtv);
        result->res = push_gtype_value(&result->parse_state, WAGT_END_ARRAY, NULL);
    } else {
        gtype_value *o = &result->parse_state->cont_val;

        switch (o->type) {
        case AGTV_ARRAY:
            result->res = push_gtype_value(&result->parse_state, WAGT_ELEM, &agtv);
            break;
        case AGTV_OBJECT:
            result->res = push_gtype_value(&result->parse_state, key_scalar ? WAGT_KEY : WAGT_VALUE, &agtv);
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

    result->res = push_gtype_value(&result->parse_state, WAGT_BEGIN_ARRAY, NULL);

    for (i = 1; i <= dims[dim]; i++)
    {
        if (dim + 1 == ndims) {
            datum_to_gtype(vals[*valcount], nulls[*valcount], result, tcategory, outfuncoid, false);
            (*valcount)++;
        } else {
            array_dim_to_gtype(result, dim + 1, ndims, dims, vals, nulls, valcount, tcategory, outfuncoid);
        }
    }

    result->res = push_gtype_value(&result->parse_state, WAGT_END_ARRAY, NULL);
}

/*
 * Turn an array into gtype.
 */
static void array_to_gtype_internal(Datum array, gtype_in_state *result)
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
        result->res = push_gtype_value(&result->parse_state, WAGT_BEGIN_ARRAY, NULL);
        result->res = push_gtype_value(&result->parse_state, WAGT_END_ARRAY, NULL);
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

    /* Extract rowtype info and find a tupdesc */
    tup_type = HeapTupleHeaderGetTypeId(td);
    tup_typmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tup_type, tup_typmod);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &tmptup;

    result->res = push_gtype_value(&result->parse_state, WAGT_BEGIN_OBJECT, NULL);

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

        result->res = push_gtype_value(&result->parse_state, WAGT_KEY, &v);

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

    result->res = push_gtype_value(&result->parse_state, WAGT_END_OBJECT,
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
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine input data type")));
    }

    if (is_null)
    {
        tcategory = AGT_TYPE_NULL;
        outfuncoid = InvalidOid;
    }
    else
    {
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

/* helper function to create an gtype_value integer from an integer */
gtype_value *integer_to_gtype_value(int64 int_value)
{
    gtype_value *agtv = palloc0(sizeof(gtype_value));

    agtv->type = AGTV_INTEGER;
    agtv->val.int_value = int_value;

    return agtv;
}

PG_FUNCTION_INFO_V1(_gtype_build_path);
Datum _gtype_build_path(PG_FUNCTION_ARGS)
{
    gtype_in_state result;
    Datum *args = NULL;
    bool *nulls = NULL;
    Oid *types = NULL;
    int nargs = 0;
    int i = 0, j = 0;
    bool is_zero_boundary_case = false;

    /* build argument values to build the object */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    if (nargs < 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("paths require at least 1 vertex")));

    if (nargs % 2 == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("a path is of the form: [vertex, (edge, vertex)*i] where i >= 0")));

    /* initialize the result */
    memset(&result, 0, sizeof(gtype_in_state));

    /* push in the begining of the gtype array */
    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    /* loop through the path components */
    for (i = 0; i < nargs; i++)
    {
        gtype *agt = NULL;

        if (nulls[i])
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %d must not be null", i + 1)));

        else if (types[i] == VARIABLEEDGEOID) {
           VariableEdge *ve = DATUM_GET_VARIABLE_EDGE(args[i]);

           char *ptr = &ve->children[1];

           for (j = 0; j < ve->children[0]; j++, ptr = ptr + VARSIZE(ptr)) {
                if (j % 2 == 1) { //vertex
		    vertex *v = (vertex *)ptr;
                    graphid id = (int64)v->children[0];
		    char *label = extract_vertex_label(v);
		    gtype *prop = extract_vertex_properties(v);
                    gtype *gt = make_vertex(Int64GetDatum(id), CStringGetDatum(label), DirectFunctionCall1(gtype_build_map_noargs, NULL));

		    add_gtype(GTYPE_P_GET_DATUM(gt), false, &result, GTYPEOID, false);
		} else { //edge
	            edge *e = (edge *)ptr;
                    graphid id = (int64)e->children[0];
                    graphid startid = (int64)e->children[2];
                    graphid endid = (int64)e->children[4];
		    char *label = extract_edge_label(e);
                    gtype *prop = extract_edge_properties(e);
                    gtype *gt = make_edge(Int64GetDatum(id), Int64GetDatum(startid), Int64GetDatum(endid),
		   	 	          CStringGetDatum(label), DirectFunctionCall1(gtype_build_map_noargs, NULL));

		    add_gtype(GTYPE_P_GET_DATUM(gt), false, &result, GTYPEOID, false);
		}
	   }
           continue; 
	}
	else if (types[i] == GTYPEOID)
	{
        agt = DATUM_GET_GTYPE_P(args[i]);

        if (i % 2 == 1 && (AGT_ROOT_IS_BINARY(agt) && AGT_ROOT_BINARY_FLAGS(agt) == AGT_FBINARY_TYPE_VLE_PATH)) {
            gtype_value *agtv_path = agtv_materialize_vle_path(agt);
            /*
             * Add in the interior path - excluding the start and end vertices.
             * The other iterations of the for loop has handled start and will
             * handle end.
             */
            for (int j = 1; j <= agtv_path->val.array.num_elems - 2; j++) {
                result.res = push_gtype_value(&result.parse_state, WAGT_ELEM, &agtv_path->val.array.elems[j]);
            }
        }
        if (i % 2 == 1 && (AGT_IS_PARTIAL_PATH(agt))) {
            gtype *agtv_path = agt;

            gtype_iterator *it = NULL;
            gtype_iterator_token tok;
            gtype_parse_state *parse_state = NULL;
            gtype_value *r = NULL;
            //offset container by the extended type header
            char *container_base = &agt->root.children[2];

            r = palloc(sizeof(gtype_value));

            it = gtype_iterator_init((gtype_container *)container_base);
            tok = gtype_iterator_next(&it, r, true);
            while ((tok = gtype_iterator_next(&it, r, true)) != WAGT_END_ARRAY) {
                result.res = push_gtype_value(&result.parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);
            }


        }
        else if (i % 2 == 1 && (!AGTE_IS_GTYPE(agt->root.children[0]) || agt->root.children[1] != AGT_HEADER_EDGE))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("paths consist of alternating vertices and edges"),
                     errhint("argument %d must be an edge", i + 1)));
        }
        else if (i % 2 == 0 && (!AGTE_IS_GTYPE(agt->root.children[0]) || agt->root.children[1] != AGT_HEADER_VERTEX))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("paths consist of alternating vertices and edges"),
                     errhint("argument %d must be an vertex", i + 1)));
        }
        else 
        {
            add_gtype(GTYPE_P_GET_DATUM(agt), false, &result, types[i], false);
        }
	}
	else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %d must be an gtype", i + 1)));

    }

    /* push the end of the array */
    result.res = push_gtype_value(&result.parse_state, WAGT_END_ARRAY, NULL);

    /* set it to a path type */
    result.res->type = AGTV_PATH;

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

Datum make_path(List *path)
{
    ListCell *lc;
    gtype_in_state result;
    int i = 1;

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    if (list_length(path) < 1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("paths require at least 1 vertex")));
    }

    if (list_length(path) % 2 != 1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("a path is of the form: [vertex, (edge, vertex)*i] where i >= 0")));
    }


    foreach (lc, path)
    {
        gtype *agt= DATUM_GET_GTYPE_P(PointerGetDatum(lfirst(lc)));
        gtype_value *elem;
        elem = get_ith_gtype_value_from_container(&agt->root, 0);

        if (!agt)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument must not be null")));
        }
        else if (i % 2 == 1 && elem->type != AGTV_VERTEX)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %i must be a vertex", i)));
        }
        else if (i % 2 == 0 && elem->type != AGTV_EDGE)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %i must be an edge", i)));
        }

        add_gtype((Datum)agt, false, &result, GTYPEOID, false);

        i++;
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_ARRAY, NULL);

    result.res->type = AGTV_PATH;

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

PG_FUNCTION_INFO_V1(_gtype_build_vertex);

/*
 * SQL function gtype_build_vertex(graphid, cstring, gtype)
 */
Datum _gtype_build_vertex(PG_FUNCTION_ARGS)
{
    gtype_in_state result;
    graphid id;

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);

    /* process graphid */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("id"));

    if (fcinfo->args[0].isnull)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_vertex() graphid cannot be NULL")));

    id = AG_GETARG_GRAPHID(0);
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, integer_to_gtype_value(id));

    /* process label */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("label"));

    if (fcinfo->args[1].isnull)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("_gtype_build_vertex() label cannot be NULL")));

    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, string_to_gtype_value(PG_GETARG_CSTRING(1)));

    /* process properties */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("properties"));

    //if the properties object is null, push an empty object
    if (fcinfo->args[2].isnull)
    {
        result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);
        result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);
    }
    else
    {
        gtype *properties = AG_GET_ARG_GTYPE_P(2);

        if (!AGT_ROOT_IS_OBJECT(properties))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_vertex() properties argument must be an object")));

        add_gtype((Datum)properties, false, &result, GTYPEOID, false);
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    result.res->type = AGTV_VERTEX;

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

Datum make_vertex(Datum id, Datum label, Datum properties)
{
    return DirectFunctionCall3(_gtype_build_vertex, id, label, properties);

}

PG_FUNCTION_INFO_V1(_gtype_build_edge);
/*
 * SQL function gtype_build_edge(graphid, graphid, graphid, cstring, gtype)
 */
Datum _gtype_build_edge(PG_FUNCTION_ARGS)
{
    gtype_in_state result;
    graphid id, start_id, end_id;

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);

    /* process graph id */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY,
                                   string_to_gtype_value("id"));

    if (fcinfo->args[0].isnull)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_edge() graphid cannot be NULL")));

    id = AG_GETARG_GRAPHID(0);
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE,
                                   integer_to_gtype_value(id));

    /* process label */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY,
                                   string_to_gtype_value("label"));

    if (fcinfo->args[3].isnull)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("_gtype_build_vertex() label cannot be NULL")));

    result.res =
        push_gtype_value(&result.parse_state, WAGT_VALUE,
                          string_to_gtype_value(PG_GETARG_CSTRING(3)));

    /* process end_id */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY,
                                   string_to_gtype_value("end_id"));

    if (fcinfo->args[2].isnull)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_edge() endid cannot be NULL")));

    end_id = AG_GETARG_GRAPHID(2);
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE,
                                   integer_to_gtype_value(end_id));

    /* process start_id */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY,
                                   string_to_gtype_value("start_id"));

    if (fcinfo->args[1].isnull)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_edge() startid cannot be NULL")));

    start_id = AG_GETARG_GRAPHID(1);
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE,
                                   integer_to_gtype_value(start_id));

    /* process properties */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY,
                                   string_to_gtype_value("properties"));

    /* if the properties object is null, push an empty object */
    if (fcinfo->args[4].isnull)
    {
        result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);
        result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);
    }
    else
    {
        gtype *properties = AG_GET_ARG_GTYPE_P(4);

        if (!AGT_ROOT_IS_OBJECT(properties))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("_gtype_build_edge() properties argument must be an object")));

        add_gtype((Datum)properties, false, &result, GTYPEOID, false);
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    result.res->type = AGTV_EDGE;

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

Datum make_edge(Datum id, Datum startid, Datum endid, Datum label, Datum properties) {
    return DirectFunctionCall5(_gtype_build_edge, id, startid, endid, label, properties);
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

    /* build argument values to build the object */
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

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);

    for (i = 0; i < nargs; i += 2)
    {
        /* process key */
        if (nulls[i])
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %d: key must not be null", i + 1)));
        }

        add_gtype(args[i], false, &result, types[i], true);

        /* process value */
        add_gtype(args[i + 1], nulls[i + 1], &result, types[i + 1], false);
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

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

    /*build argument values to build the array */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    if (nargs < 0)
        PG_RETURN_NULL();

    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_ARRAY,
                                   NULL);

    for (i = 0; i < nargs; i++)
        add_gtype(args[i], nulls[i], &result, types[i], false);

    result.res = push_gtype_value(&result.parse_state, WAGT_END_ARRAY, NULL);

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

    push_gtype_value(&result.parse_state, WAGT_BEGIN_ARRAY, NULL);
    result.res = push_gtype_value(&result.parse_state, WAGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
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
        /* inform caller about actual type of container */
        res->type = GTYPE_CONTAINER_IS_ARRAY(agtc) ? AGTV_ARRAY : AGTV_OBJECT;
        return false;
    }

    /*
     * A root scalar is stored as an array of one element, so we get the array
     * and then its first (and only) member.
     */
    it = gtype_iterator_init(agtc);

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WAGT_BEGIN_ARRAY);
    Assert(tmp.val.array.num_elems == 1 && tmp.val.array.raw_scalar);

    tok = gtype_iterator_next(&it, res, true);
    Assert(tok == WAGT_ELEM);
    Assert(IS_A_GTYPE_SCALAR(res));

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WAGT_END_ARRAY);

    tok = gtype_iterator_next(&it, &tmp, true);
    Assert(tok == WAGT_DONE);

    return true;
}

/*
 * Emit correct, translatable cast error message
 */
static void cannot_cast_gtype_value(enum gtype_value_type type, const char *sqltype)
{
    static const struct
    {
        enum gtype_value_type type;
        const char *msg;
    } messages[] = {
        {AGTV_NULL, gettext_noop("cannot cast gtype null to type %s")},
        {AGTV_STRING, gettext_noop("cannot cast gtype string to type %s")},
        {AGTV_NUMERIC, gettext_noop("cannot cast gtype numeric to type %s")},
        {AGTV_INTEGER, gettext_noop("cannot cast gtype integer to type %s")},
        {AGTV_FLOAT, gettext_noop("cannot cast gtype float to type %s")},
        {AGTV_BOOL, gettext_noop("cannot cast gtype boolean to type %s")},
        {AGTV_ARRAY, gettext_noop("cannot cast gtype array to type %s")},
        {AGTV_OBJECT, gettext_noop("cannot cast gtype object to type %s")},
        {AGTV_VERTEX, gettext_noop("cannot cast gtype vertex to type %s")},
        {AGTV_EDGE, gettext_noop("cannot cast gtype edge to type %s")},
        {AGTV_PATH, gettext_noop("cannot cast gtype path to type %s")},
        {AGTV_BINARY,
         gettext_noop("cannot cast gtype array or object to type %s")}};
    int i;

    for (i = 0; i < lengthof(messages); i++)
    {
        if (messages[i].type == type)
        {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(messages[i].msg, sqltype)));
        }
    }

    // unreachable
    elog(ERROR, "unknown gtype type: %d", (int)type);
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

static gtype_value *execute_array_access_operator_internal(gtype *array, int64 array_index)
{
    uint32 size;

    /* get the size of the array, given the type of the input */
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

/*
 * Helper function to do a binary search through an object's key/value pairs,
 * looking for a specific key. It will return the key or NULL if not found.
 */
gtype_value *get_gtype_value_object_value(const gtype_value *agtv_object,
                                            char *search_key,
                                            int search_key_length)
{
    gtype_value *agtv_key = NULL;
    int current_key_length = 0;
    int middle = 0;
    int num_pairs = 0;
    int left = 0;
    int right = 0;
    int result = 0;

    if (agtv_object == NULL || search_key == NULL || search_key_length <= 0)
    {
        return NULL;
    }

    /* get the number of object pairs */
    num_pairs = agtv_object->val.object.num_pairs;

    /* do a binary search through the pairs */
    right = num_pairs - 1;
    middle = num_pairs / 2;

    /* while middle is within the constraints */
    while (middle >= left && middle <= right)
    {
        /* get the current key length */
        agtv_key = &agtv_object->val.object.pairs[middle].key;
        current_key_length = agtv_key->val.string.len;

        /* if not the same length, divide the search space and continue */
        if (current_key_length != search_key_length)
        {
            /* if we need to search in the lower half */
            if (search_key_length < current_key_length)
            {
                middle -= 1;
                right = middle;
                middle = ((middle - left) / 2) + left;
            }
            /* else we need to search in the upper half */
            else
            {
                middle += 1;
                left = middle;
                middle = ((right - middle) / 2) + left;
            }
            continue;
        }

        /* they are the same length so compare the keys */
        result = strncmp(search_key, agtv_key->val.string.val,
                         search_key_length);

        /* if they don't match */
        if (result != 0)
        {
            /* if smaller */
            if (result < 0)
            {
                middle -= 1;
                right = middle;
                middle = ((middle - left) / 2) + left;
            }
            /* if larger */
            else
            {
                middle += 1;
                left = middle;
                middle = ((right - middle) / 2) + left;
            }
            continue;
        }

        /* they match */
        return (&agtv_object->val.object.pairs[middle].value);
    }

    /* they don't match */
    return NULL;
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

    if (AGT_IS_VERTEX(gtype_in) || AGT_IS_EDGE(gtype_in))
    {
        gtype_value *agtv =
            find_gtype_value_from_container((gtype_container *)&gtype_in->root.children[2],
                                             AGT_FOBJECT, &prop_key);

        agtc = (gtype_container *)agtv->val.binary.data;
    }
    else if (!AGT_ROOT_IS_OBJECT(gtype_in))
    {
        PG_RETURN_NULL();
    }

    v = find_gtype_value_from_container(agtc, AGT_FOBJECT,
                                         &new_key_value);

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

    /* return null if the array to slice is null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    /* return an error if both indices are NULL */
    if (PG_ARGISNULL(1) && PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("slice start and/or end is required")));

    /* get the array parameter and verify that it is a list */
    array = AG_GET_ARG_GTYPE_P(0);
    if (!AGT_ROOT_IS_ARRAY(array) || AGT_ROOT_IS_SCALAR(array))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("slice must access a list")));
    }

    /* get its size */
    array_size = AGT_ROOT_COUNT(array);

    /* if we don't have a lower bound, make it 0 */
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

    /* if we don't have an upper bound, make it the size of the array */
    if (PG_ARGISNULL(2))
    {
        upper_index = array_size;
    }
    else
    {
        uidx_value = get_ith_gtype_value_from_container(
            &(AG_GET_ARG_GTYPE_P(2))->root, 0);
        /* adjust for AGTV_NULL */
        if (uidx_value->type == AGTV_NULL)
        {
            upper_index = array_size;
            uidx_value = NULL;
        }
    }

    /* if both indices are NULL (AGTV_NULL) return an error */
    if (lidx_value == NULL && uidx_value == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("slice start and/or end is required")));

    /* key must be an integer or NULL */
    if ((lidx_value != NULL && lidx_value->type != AGTV_INTEGER) ||
        (uidx_value != NULL && uidx_value->type != AGTV_INTEGER))
        ereport(ERROR,
                (errmsg("array slices must resolve to an integer value")));

    /* set indices if not already set */
    if (lidx_value)
    {
        lower_index = lidx_value->val.int_value;
    }
    if (uidx_value)
    {
        upper_index = uidx_value->val.int_value;
    }

    /* adjust for negative and out of bounds index values */
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

    /* build our result array */
    memset(&result, 0, sizeof(gtype_in_state));

    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_ARRAY,
                                   NULL);

    /* get array elements */
    for (i = lower_index; i < upper_index; i++)
    {
        result.res = push_gtype_value(&result.parse_state, WAGT_ELEM,
            get_ith_gtype_value_from_container(&array->root, i));
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_ARRAY, NULL);

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

    /* return null if the array is null */
    if (PG_ARGISNULL(1))
        PG_RETURN_NULL();

    /* get the array parameter and verify that it is a list */
    agt_array = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_ARRAY(agt_array))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("object of IN must be a list")));

    /* init array iterator */
    it_array = gtype_iterator_init(&agt_array->root);
    /* open array container */
    gtype_iterator_next(&it_array, &agtv_elem, false);
    /* check for an array scalar value */
    if (agtv_elem.type == AGTV_ARRAY && agtv_elem.val.array.raw_scalar)
    {
        gtype_iterator_next(&it_array, &agtv_elem, false);
        /* check for GTYPE NULL */
        if (agtv_elem.type == AGTV_NULL)
            PG_RETURN_NULL();
        /* if it is a scalar, but not AGTV_NULL, error out */
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("object of IN must be a list")));
    }

    array_size = AGT_ROOT_COUNT(agt_array);

    /* return null if the item to find is null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    /* get the item to search for */
    agt_item = AG_GET_ARG_GTYPE_P(0);

    /* init item iterator */
    it_item = gtype_iterator_init(&agt_item->root);

    /* get value of item */
    gtype_iterator_next(&it_item, &agtv_item, false);
    if (agtv_item.type == AGTV_ARRAY && agtv_item.val.array.raw_scalar)
    {
        gtype_iterator_next(&it_item, &agtv_item, false);
        /* check for GTYPE NULL */
        if (agtv_item.type == AGTV_NULL)
            PG_RETURN_NULL();
    }

    /* iterate through the array, but stop if we find it */
    for (i = 0; i < array_size && !result; i++)
    {
        /* get next element */
        gtype_iterator_next(&it_array, &agtv_elem, true);
        /* if both are containers, compare containers */
        if (!IS_A_GTYPE_SCALAR(&agtv_item) && !IS_A_GTYPE_SCALAR(&agtv_elem))
        {
            result = (compare_gtype_containers_orderability(
                          &agt_item->root, agtv_elem.val.binary.data) == 0);
        }
        /* if both are scalars and of the same type, compare scalars */
        else if (IS_A_GTYPE_SCALAR(&agtv_item) &&
                 IS_A_GTYPE_SCALAR(&agtv_elem) &&
                 agtv_item.type == agtv_elem.type)
            result = (compare_gtype_scalar_values(&agtv_item, &agtv_elem) ==
                      0);
    }
    return result;
}

PG_FUNCTION_INFO_V1(gtype_string_match_starts_with);
/*
 * Execution function for STARTS WITH
 */
Datum gtype_string_match_starts_with(PG_FUNCTION_ARGS)
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

            if (strncmp(lhs_value->val.string.val, rhs_value->val.string.val,
                        rhs_value->val.string.len) == 0)
                return boolean_to_gtype(true);
            else
                return boolean_to_gtype(false);
        }
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
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

            if (strncmp(lhs_value->val.string.val + lhs_value->val.string.len -
                            rhs_value->val.string.len,
                        rhs_value->val.string.val,
                        rhs_value->val.string.len) == 0)
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
    while ((tok = gtype_iterator_next(&it, r, false)) != WAGT_DONE)
    {
        if (IS_A_GTYPE_SCALAR(r) && GTYPE_ITERATOR_TOKEN_IS_HASHABLE(tok))
            gtype_hash_scalar_value_extended(r, &hash, seed);
        else if (tok == WAGT_BEGIN_ARRAY && !r->val.array.raw_scalar)
            seed = LEFT_ROTATE(seed, 4);
        else if (tok == WAGT_BEGIN_OBJECT)
            seed = LEFT_ROTATE(seed, 6);
        else if (tok == WAGT_END_ARRAY && !r->val.array.raw_scalar)
            seed = RIGHT_ROTATE(seed, 4);
        else if (tok == WAGT_END_OBJECT)
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

PG_FUNCTION_INFO_V1(age_id);
Datum
age_id(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    
    if (is_gtype_null(agt))
        PG_RETURN_NULL();
    
    if (!AGT_IS_EDGE(agt) && !AGT_IS_VERTEX(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("properties() argument must be a vertex or edge")));
    
    gtype_value *agtv = 
        find_gtype_value_from_container((gtype_container *)&agt->root.children[2],
                                         AGT_FOBJECT, &id_key);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(agtv));
}

PG_FUNCTION_INFO_V1(age_start_id);
Datum
age_start_id(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (!AGT_IS_EDGE(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("start_id() argument must be an edge")));

    gtype_value *agtv =
        find_gtype_value_from_container((gtype_container *)&agt->root.children[2],
                                         AGT_FOBJECT, &start_key);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(agtv));
}

PG_FUNCTION_INFO_V1(age_end_id);
Datum 
age_end_id(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (!AGT_IS_EDGE(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("end_id() argument must be an edge")));

    gtype_value *agtv =
        find_gtype_value_from_container((gtype_container *)&agt->root.children[2],
                                         AGT_FOBJECT, &end_key);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(agtv));
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

    /* build the heap tuple data */
    hth = tuple->t_data;
    tmptup.t_len = HeapTupleHeaderGetDatumLength(hth);
    tmptup.t_data = hth;
    htd = &tmptup;

    /* get the description for the column from the tuple descriptor */
    att = TupleDescAttr(tupdesc, column);
    /* get the datum (attribute) for that column*/
    result = heap_getattr(htd, column + 1, tupdesc, &_isnull);
    /* verify that the attribute typid is as expected */
    if (att->atttypid != typid)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid attribute typid. Expected %d, found %d", typid,
                        att->atttypid)));
    /* verify that the attribute name is as expected */
    if (strcmp(att->attname.data, attname) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid attribute name. Expected %s, found %s",
                        attname, att->attname.data)));
    /* verify that if it is null, it is allowed to be null */
    if (isnull == false && _isnull == true)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Attribute was found to be null when null is not allowed.")));

    return result;
}

/*
 * Function to retrieve a label name, given the graph name and graphid. The
 * function returns a pointer to a duplicated string that needs to be freed
 * when you are finished using it.
 */
static char *get_label_name(const char *graph_name, int64 label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    char *result = NULL;
    bool column_is_null;

    Oid graph_id = get_graph_oid(graph_name);

    ScanKeyInit(&scan_keys[0], Anum_ag_label_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_id));
    ScanKeyInit(&scan_keys[1], Anum_ag_label_id, BTEqualStrategyNumber,
                F_INT42EQ, Int32GetDatum(get_graphid_label_id(label_id)));

    ag_label = table_open(ag_label_relation_id(), ShareLock);
    scan_desc = systable_beginscan(ag_label, ag_label_graph_oid_index_id(), true,
                                   NULL, 2, scan_keys);

    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("graphid abc %lu does not exist", label_id)));
    }

    tupdesc = RelationGetDescr(ag_label);

    if (tupdesc->natts != Natts_ag_label)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid number of attributes for postgraph.ag_label")));

    result = NameStr(*DatumGetName(
        heap_getattr(tuple, Anum_ag_label_name, tupdesc, &column_is_null)));
    result = strdup(result);

    systable_endscan(scan_desc);
    table_close(ag_label, ShareLock);

    return result;
}

static Datum get_vertex(const char *graph, const char *vertex_label, int64 graphid)
{
    ScanKeyData scan_keys[1];
    Relation graph_vertex_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum id, properties, result;

    Oid graph_namespace_oid = get_namespace_oid(graph, false);
    Oid vertex_label_table_oid = get_relname_relid(vertex_label, graph_namespace_oid);
    Snapshot snapshot = GetActiveSnapshot();

    ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(graphid));

    graph_vertex_label = table_open(vertex_label_table_oid, ShareLock);
    scan_desc = table_beginscan(graph_vertex_label, snapshot, 1, scan_keys);
    tuple = heap_getnext(scan_desc, ForwardScanDirection);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("graphid cde %lu does not exist", graphid)));

    tupdesc = RelationGetDescr(graph_vertex_label);
    if (tupdesc->natts != 2)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid number of attributes for %s.%s", graph,
                        vertex_label )));

    id = column_get_datum(tupdesc, tuple, 0, "id", GRAPHIDOID, true);
    properties = column_get_datum(tupdesc, tuple, 1, "properties",
                                  GTYPEOID, true);
    /* reconstruct the vertex */
    result = DirectFunctionCall3(_gtype_build_vertex, id,
                                 CStringGetDatum(vertex_label), properties);

    /* end the scan and close the relation */
    table_endscan(scan_desc);
    table_close(graph_vertex_label, ShareLock);
    /* return the vertex datum */
    return result;
}

static Datum get_vertex_1(const char *graph, const char *vertex_label, int64 graphid)
{
    ScanKeyData scan_keys[1];
    Relation graph_vertex_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum id, properties, result;

    Oid graph_namespace_oid = get_namespace_oid(graph, false);
    Oid vertex_label_table_oid = get_relname_relid(vertex_label, graph_namespace_oid);
    Snapshot snapshot = GetActiveSnapshot();

    ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, Int64GetDatum(graphid));

    graph_vertex_label = table_open(vertex_label_table_oid, ShareLock);
    scan_desc = table_beginscan(graph_vertex_label, snapshot, 1, scan_keys);
    tuple = heap_getnext(scan_desc, ForwardScanDirection);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("graphid cde %lu does not exist", graphid)));

    tupdesc = RelationGetDescr(graph_vertex_label);
    if (tupdesc->natts != 2)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("Invalid number of attributes for %s.%s", graph,
                        vertex_label )));

    id = column_get_datum(tupdesc, tuple, 0, "id", GRAPHIDOID, true);
    properties = column_get_datum(tupdesc, tuple, 1, "properties", GTYPEOID, true);
    /* reconstruct the vertex */
    result = VERTEX_GET_DATUM(create_vertex(DATUM_GET_GRAPHID(id), vertex_label, DATUM_GET_GTYPE_P(properties)));
    /* end the scan and close the relation */
    table_endscan(scan_desc);
    table_close(graph_vertex_label, ShareLock);
    /* return the vertex datum */
    return result;
}

PG_FUNCTION_INFO_V1(age_edge_startnode);
Datum age_edge_startnode(PG_FUNCTION_ARGS) {
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val, agtv_object->val.string.len);

    edge *e = AG_GET_ARG_EDGE(1);
    graph_oid = *((int64 *)(&e->children[2])); 

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);
    result = get_vertex_1(graph_name, label_name, graph_oid);

    return result;
}

PG_FUNCTION_INFO_V1(age_edge_endnode);
Datum age_edge_endnode(PG_FUNCTION_ARGS) {
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val, agtv_object->val.string.len);

    edge *e = AG_GET_ARG_EDGE(1);
    graph_oid = *((int64 *)(&e->children[4]));

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);
    result = get_vertex_1(graph_name, label_name, graph_oid);

    return result;
}


/*
PG_FUNCTION_INFO_V1(age_startnode);
Datum age_startnode(PG_FUNCTION_ARGS) {
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;
Assert(PG_ARGISNULL(0) == false);
if (PG_ARGISNULL(1))
        PG_RETURN_NULL();

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val, agtv_object->val.string.len);
agt_arg = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("startNode() argument must resolve to a scalar value")));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    if (agtv_object->type == AGTV_NULL)
            PG_RETURN_NULL();

    if (agtv_object->type != AGTV_EDGE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("startNode() argument must be an edge or null")));

    agtv_value = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_object, "start_id");
    Assert(agtv_value != NULL);
    Assert(agtv_value->type = AGTV_INTEGER);
    graph_oid = agtv_value->val.int_value;

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);
    result = get_vertex(graph_name, label_name, graph_oid);

    return result;
}
*/
/*
PG_FUNCTION_INFO_V1(age_endnode);

Datum age_endnode(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_object = NULL;
    gtype_value *agtv_value = NULL;
    char *graph_name = NULL;
    char *label_name = NULL;
    graphid graph_oid;
    Datum result;

    Assert(PG_ARGISNULL(0) == false);

    if (PG_ARGISNULL(1))
        PG_RETURN_NULL();
    agt_arg = AG_GET_ARG_GTYPE_P(0);
    Assert(AGT_ROOT_IS_SCALAR(agt_arg));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);
    Assert(agtv_object->type == AGTV_STRING);
    graph_name = strndup(agtv_object->val.string.val,
                         agtv_object->val.string.len);

    agt_arg = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("endNode() argument must resolve to a scalar value")));
    agtv_object = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    if (agtv_object->type == AGTV_NULL)
            PG_RETURN_NULL();

    if (agtv_object->type != AGTV_EDGE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("endNode() argument must be an edge")));

    agtv_value = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_object, "end_id");
    Assert(agtv_value != NULL);
    Assert(agtv_value->type = AGTV_INTEGER);
    graph_oid = agtv_value->val.int_value;

    label_name = get_label_name(graph_name, graph_oid);
    Assert(label_name != NULL);

    result = get_vertex(graph_name, label_name, graph_oid);

    free(label_name);

    return result;
}
*/
PG_FUNCTION_INFO_V1(age_head);

Datum age_head(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_ARRAY(agt) || AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("head() argument must resolve to a list")));

    int count = AGT_ROOT_COUNT(agt);

    if (count == 0)
        PG_RETURN_NULL();

    gtype_value *agtv_result = get_ith_gtype_value_from_container(&agt->root, 0);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}

PG_FUNCTION_INFO_V1(age_last);

Datum age_last(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    int count;

    /* check for null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    /* check for an array */
    if (!AGT_ROOT_IS_ARRAY(agt_arg) || AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("last() argument must resolve to a list or null")));

    count = AGT_ROOT_COUNT(agt_arg);

    /* if we have an empty list, return null */
    if (count == 0)
        PG_RETURN_NULL();

    /* get the last element of the array */
    agtv_result = get_ith_gtype_value_from_container(&agt_arg->root, count -1);

    /* if it is AGTV_NULL, return null */
    if (agtv_result->type == AGTV_NULL)
        PG_RETURN_NULL();

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}

PG_FUNCTION_INFO_V1(age_properties);

Datum age_properties(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (!AGT_IS_EDGE(agt) && !AGT_IS_VERTEX(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("properties() argument must be a vertex or edge")));

    gtype_value *agtv =
        find_gtype_value_from_container((gtype_container *)&agt->root.children[2],
                                         AGT_FOBJECT, &prop_key);

    Assert(agtv != NULL);
    Assert(agtv->type == AGTV_OBJECT || agtv->type == AGTV_BINARY);

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(agtv));
}

PG_FUNCTION_INFO_V1(age_length);

Datum age_length(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_path = NULL;
    gtype_value agtv_result;

    /* check for null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    /* check for a scalar */
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("length() argument must resolve to a scalar")));

    /* get the path array */
    agtv_path = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    /* if it is AGTV_NULL, return null */
    if (agtv_path->type == AGTV_NULL)
        PG_RETURN_NULL();

    /* check for a path */
    if (agtv_path ->type != AGTV_PATH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("length() argument must resolve to a path or null")));

    agtv_result.type = AGTV_INTEGER;
    agtv_result.val.int_value = (agtv_path->val.array.num_elems - 1) /2;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_toboolean);
Datum
age_toboolean(PG_FUNCTION_ARGS) {
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

PG_FUNCTION_INFO_V1(age_size);
Datum age_size(PG_FUNCTION_ARGS) {
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

PG_FUNCTION_INFO_V1(graphid_to_gtype);

Datum graphid_to_gtype(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(integer_to_gtype(AG_GETARG_GRAPHID(0)));
}

PG_FUNCTION_INFO_V1(gtype_to_graphid);

Datum gtype_to_graphid(PG_FUNCTION_ARGS)
{
    gtype *gtype_in = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv;

    if (!gtype_extract_scalar(&gtype_in->root, &agtv) ||
        agtv.type != AGTV_INTEGER)
        cannot_cast_gtype_value(agtv.type, "graphid");

    PG_FREE_IF_COPY(gtype_in, 0);

    PG_RETURN_INT16(agtv.val.int_value);
}

PG_FUNCTION_INFO_V1(age_label);
Datum age_label(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value *agtv_value;
    gtype_value *label;

    if (AGTE_IS_NULL(agt->root.children[0]))
            PG_RETURN_NULL();

    // edges and vertices are considered scalars
    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("label() argument must resolve to an edge or vertex")));

    agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    // fail if gtype value isn't an edge or vertex
    if (agtv_value->type != AGTV_VERTEX && agtv_value->type != AGTV_EDGE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("label() argument must resolve to an edge or vertex")));


    // extract the label gtype value from the vertex or edge
    label = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_value, "label");

    PG_RETURN_POINTER(gtype_value_to_gtype(label));
}

static gtype_iterator *get_next_list_element(gtype_iterator *it,
                           gtype_container *agtc, gtype_value *elem)
{
    gtype_iterator_token itok;
    gtype_value tmp;

    /* verify input params */
    Assert(agtc != NULL);
    Assert(elem != NULL);

    /* check to see if the container is empty */
    if (GTYPE_CONTAINER_SIZE(agtc) == 0)
    {
       return NULL;
    }

    /* if the passed iterator is NULL, this is the first time, create it */
    if (it == NULL)
    {
        /* initial the iterator */
        it = gtype_iterator_init(agtc);
        /* get the first token */
        itok = gtype_iterator_next(&it, &tmp, true);
        /* it should be WAGT_BEGIN_ARRAY */
        Assert(itok == WAGT_BEGIN_ARRAY);
    }

    /* the next token should be an element or the end of the array */
    itok = gtype_iterator_next(&it, &tmp, true);
    Assert(itok == WAGT_ELEM || WAGT_END_ARRAY);

    /* if this is the end of the array return NULL */
    if (itok == WAGT_END_ARRAY) {
        return NULL;
    }

    /* this should be the element, copy it */
    if (itok == WAGT_ELEM) {
        memcpy(elem, &tmp, sizeof(gtype_value));
    }

    return it;
}

PG_FUNCTION_INFO_V1(age_reverse);

Datum age_reverse(PG_FUNCTION_ARGS)
{
    int nargs;
    Datum *args;
    Datum arg;
    bool *nulls;
    Oid *types;
    gtype_value agtv_result;
    text *text_string = NULL;
    char *string = NULL;
    int string_len;
    Oid type;

    /* extract argument values */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    /* check number of args */
    if (nargs > 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("reverse() only supports one argument")));

    /* check for null */
    if (nargs < 0 || nulls[0])
        PG_RETURN_NULL();

    /* reverse() supports text, cstring, or the gtype string input */
    arg = args[0];
    type = types[0];

        gtype *agt_arg = NULL;
        gtype_value *agtv_value = NULL;
        gtype_parse_state *parse_state = NULL;
        gtype_value elem = {0};
        gtype_iterator *it = NULL;
        gtype_value tmp;
        gtype_value *elems = NULL;
        int num_elems;
        int i;

        /* get the gtype argument */
        agt_arg = DATUM_GET_GTYPE_P(arg);

        if (!AGT_ROOT_IS_SCALAR(agt_arg))
        {
            agtv_value = push_gtype_value(&parse_state, WAGT_BEGIN_ARRAY, NULL);

            while ((it = get_next_list_element(it, &agt_arg->root, &elem)))
            {
                agtv_value = push_gtype_value(&parse_state, WAGT_ELEM, &elem);
            }

            /* now reverse the list */
            elems = parse_state->cont_val.val.array.elems;
            num_elems = parse_state->cont_val.val.array.num_elems;

            for(i = 0; i < num_elems/2; i++)
            {
                tmp = elems[i];
                elems[i] = elems[num_elems - 1 - i];
                elems[num_elems - 1 - i] = tmp;
            }
            /* reverse done*/

            elems = NULL;

            agtv_value = push_gtype_value(&parse_state, WAGT_END_ARRAY, NULL);

            Assert(agtv_value != NULL);
            Assert(agtv_value->type = AGTV_ARRAY);

            PG_RETURN_POINTER(gtype_value_to_gtype(agtv_value));

        }

        agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        /* check for gtype null */
        if (agtv_value->type == AGTV_NULL)
            PG_RETURN_NULL();
        if (agtv_value->type == AGTV_STRING)
            text_string = cstring_to_text_with_len(agtv_value->val.string.val,
                                                   agtv_value->val.string.len);
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("reverse() unsupported argument gtype %d",
                                   agtv_value->type)));

    /*
     * We need the string as a text string so that we can let PG deal with
     * multibyte characters in reversing the string.
     */
    text_string = DatumGetTextPP(DirectFunctionCall1(text_reverse,
                                                     PointerGetDatum(text_string)));

    /* convert it back to a cstring */
    string = text_to_cstring(text_string);
    string_len = strlen(string);

    /* if we have an empty string, return null */
    if (string_len == 0)
        PG_RETURN_NULL();

    /* build the result */
    agtv_result.type = AGTV_STRING;
    agtv_result.val.string.val = string;
    agtv_result.val.string.len = string_len;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_toupper);

Datum age_toupper(PG_FUNCTION_ARGS)
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

PG_FUNCTION_INFO_V1(age_tolower);

Datum age_tolower(PG_FUNCTION_ARGS)
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

PG_FUNCTION_INFO_V1(age_rtrim);

Datum age_rtrim(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("rTrim() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    text *text_string;
    if (agtv_value->type == AGTV_NULL)
        PG_RETURN_NULL();
    else if (agtv_value->type == AGTV_STRING)
        text_string = cstring_to_text_with_len(agtv_value->val.string.val, agtv_value->val.string.len);
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("rTrim() unsupported argument gtype %d", agtv_value->type)));

    text_string = DatumGetTextPP(DirectFunctionCall1(rtrim1, PointerGetDatum(text_string)));

    gtype_value agtv_result = { .type = AGTV_STRING, 
                                 .val.string = { VARSIZE(text_string), text_to_cstring(text_string) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_ltrim);

Datum age_ltrim(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("lTrim() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    text *text_string;
    if (agtv_value->type == AGTV_NULL)
        PG_RETURN_NULL();
    else if (agtv_value->type == AGTV_STRING)
        text_string = cstring_to_text_with_len(agtv_value->val.string.val, agtv_value->val.string.len);
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("lTrim() unsupported argument gtype %d", agtv_value->type)));

    text_string = DatumGetTextPP(DirectFunctionCall1(ltrim1, PointerGetDatum(text_string)));

    gtype_value agtv_result = { .type = AGTV_STRING, 
                                 .val.string = { VARSIZE(text_string), text_to_cstring(text_string) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_trim);

Datum age_trim(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Trim() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    text *text_string;
    if (agtv_value->type == AGTV_NULL)
        PG_RETURN_NULL();
    else if (agtv_value->type == AGTV_STRING)
        text_string = cstring_to_text_with_len(agtv_value->val.string.val, agtv_value->val.string.len);
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Trim() unsupported argument gtype %d", agtv_value->type)));

    text_string = DatumGetTextPP(DirectFunctionCall1(btrim1, PointerGetDatum(text_string)));

    gtype_value agtv_result = { .type = AGTV_STRING,
                                 .val.string = { VARSIZE(text_string), text_to_cstring(text_string) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_right);

Datum age_right(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("right() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    text *text_string;
    if (agtv_value->type == AGTV_NULL)
        PG_RETURN_NULL();
    else if (agtv_value->type == AGTV_STRING)
        text_string = cstring_to_text_with_len(agtv_value->val.string.val, agtv_value->val.string.len);
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("right() unsupported argument gtype %d", agtv_value->type)));

    agt = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("right() only supports scalar arguments")));

    agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    if (agtv_value->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("right() unsupported argument gtype %d", agtv_value->type)));

    text_string = DatumGetTextPP(DirectFunctionCall2(text_right,
                                                     PointerGetDatum(text_string),
                                                     Int64GetDatum(agtv_value->val.int_value)));

    gtype_value agtv_result = { .type = AGTV_STRING,
                                 .val.string = { VARSIZE(text_string), text_to_cstring(text_string) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_left);

Datum age_left(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);

    if (!AGT_ROOT_IS_SCALAR(agt))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("left() only supports scalar arguments")));

    gtype_value *agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

    text *text_string;
    if (agtv_value->type == AGTV_NULL)
        PG_RETURN_NULL();
    else if (agtv_value->type == AGTV_STRING)
        text_string = cstring_to_text_with_len(agtv_value->val.string.val, agtv_value->val.string.len);
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("left() unsupported argument gtype %d", agtv_value->type)));

    agt = AG_GET_ARG_GTYPE_P(1);
    if (!AGT_ROOT_IS_SCALAR(agt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("left() only supports scalar arguments")));

    agtv_value = get_ith_gtype_value_from_container(&agt->root, 0); 

    if (agtv_value->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("left() unsupported argument gtype %d", agtv_value->type)));

    text_string = DatumGetTextPP(DirectFunctionCall2(text_left,
                                                     PointerGetDatum(text_string),
                                                     Int64GetDatum(agtv_value->val.int_value)));

    gtype_value agtv_result = { .type = AGTV_STRING,
                                 .val.string = { VARSIZE(text_string), text_to_cstring(text_string) }};

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_substring);

Datum age_substring(PG_FUNCTION_ARGS)
{
    int nargs;
    Datum *args;
    Datum arg;
    bool *nulls;
    Oid *types;
    gtype_value agtv_result;
    text *text_string = NULL;
    char *string = NULL;
    int param;
    int string_start = 0;
    int string_len = 0;
    int i;
    Oid type;

    /* extract argument values */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    /* check number of args */
    if (nargs < 2 || nargs > 3)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("substring() invalid number of arguments")));

    /* check for null */
    if (nargs < 0 || nulls[0])
        PG_RETURN_NULL();

    /* neither offset or length can be null if there is a valid string */
    if ((nargs == 2 && nulls[1]) ||
        (nargs == 3 && nulls[2]))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("substring() offset or length cannot be null")));

    /* substring() supports text, cstring, or the gtype string input */
    arg = args[0];
    type = types[0];

    if (type != GTYPEOID)
    {
        if (type == CSTRINGOID)
            text_string = cstring_to_text(DatumGetCString(arg));
        else if (type == TEXTOID)
            text_string = DatumGetTextPP(arg);
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("substring() unsupported argument type %d",
                                   type)));
    }
    else
    {
        gtype *agt_arg;
        gtype_value *agtv_value;

        /* get the gtype argument */
        agt_arg = DATUM_GET_GTYPE_P(arg);

        if (!AGT_ROOT_IS_SCALAR(agt_arg))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("substring() only supports scalar arguments")));

        agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        /* check for gtype null */
        if (agtv_value->type == AGTV_NULL)
            PG_RETURN_NULL();
        if (agtv_value->type == AGTV_STRING)
            text_string = cstring_to_text_with_len(agtv_value->val.string.val,
                                                   agtv_value->val.string.len);
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("substring() unsupported argument gtype %d",
                                   agtv_value->type)));
    }

    /*
     * substring() only supports integer and gtype integer for the second and
     * third parameters values.
     */
    for (i = 1; i < nargs; i++)
    {
        arg = args[i];
        type = types[i];

        if (type != GTYPEOID)
        {
            if (type == INT2OID)
                param = (int64) DatumGetInt16(arg);
            else if (type == INT4OID)
                param = (int64) DatumGetInt32(arg);
            else if (type == INT8OID)
                param = (int64) DatumGetInt64(arg);
            else
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("substring() unsupported argument type %d",
                                       type)));
        }
        else
        {
            gtype *agt_arg;
            gtype_value *agtv_value;

            /* get the gtype argument */
            agt_arg = DATUM_GET_GTYPE_P(arg);

            if (!AGT_ROOT_IS_SCALAR(agt_arg))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("substring() only supports scalar arguments")));

            agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

            /* no need to check for gtype null because it is an error if found */
            if (agtv_value->type != AGTV_INTEGER)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("substring() unsupported argument gtype %d",
                                       agtv_value->type)));

            param = agtv_value->val.int_value;
        }

        if (i == 1)
            string_start = param;
        if (i == 2)
            string_len = param;
    }

    /* negative values are not supported in the opencypher spec */
    if (string_start < 0 || string_len < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("substring() negative values are not supported for offset or length")));

    /* cypher substring is 0 based while PG's is 1 based */
    string_start += 1;

    /*
     * We need the string as a text string so that we can let PG deal with
     * multibyte characters in the string.
     */

    /* if optional length is left out */
    if (nargs == 2)
         text_string = DatumGetTextPP(DirectFunctionCall2(text_substr_no_len,
                                                          PointerGetDatum(text_string),
                                                          Int64GetDatum(string_start)));
    /* if length is given */
    else
        text_string = DatumGetTextPP(DirectFunctionCall3(text_substr,
                                                         PointerGetDatum(text_string),
                                                         Int64GetDatum(string_start),
                                                         Int64GetDatum(string_len)));

    /* convert it back to a cstring */
    string = text_to_cstring(text_string);
    string_len = strlen(string);

    /* if we have an empty string, return null */
    if (string_len == 0)
        PG_RETURN_NULL();

    /* build the result */
    agtv_result.type = AGTV_STRING;
    agtv_result.val.string.val = string;
    agtv_result.val.string.len = string_len;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_split);

Datum age_split(PG_FUNCTION_ARGS)
{
    text *text_string;
    text *text_delimiter;

    for (int i = 0; i < 2; i++) {
        gtype *agt = AG_GET_ARG_GTYPE_P(i);
        gtype_value *agtv_value;

        if (!AGT_ROOT_IS_SCALAR(agt))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("split() only supports scalar arguments")));

        agtv_value = get_ith_gtype_value_from_container(&agt->root, 0);

        /* check for gtype null */
        if (agtv_value->type == AGTV_NULL) {
            PG_RETURN_NULL();
        } else if (agtv_value->type == AGTV_STRING) {
            text *param = cstring_to_text_with_len(agtv_value->val.string.val,
                                                   agtv_value->val.string.len);

            if (i == 0)
                text_string = param;
            else
                text_delimiter = param;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("split() unsupported argument gtype %d",
                                       agtv_value->type)));
        }
    }

    Datum text_array = DirectFunctionCall2Coll(regexp_split_to_array,
                                               DEFAULT_COLLATION_OID,
                                               PointerGetDatum(text_string),
                                               PointerGetDatum(text_delimiter));

    gtype_in_state result;
    memset(&result, 0, sizeof(gtype_in_state));
    array_to_gtype_internal(text_array, &result);
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(result.res));
}

PG_FUNCTION_INFO_V1(age_replace);

Datum age_replace(PG_FUNCTION_ARGS)
{
    int nargs;
    Datum *args;
    Datum arg;
    bool *nulls;
    Oid *types;
    gtype_value agtv_result;
    text *param = NULL;
    text *text_string = NULL;
    text *text_search = NULL;
    text *text_replace = NULL;
    text *text_result = NULL;
    char *string = NULL;
    int string_len;
    Oid type;
    int i;

    /* extract argument values */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    /* check number of args */
    if (nargs != 3)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("replace() invalid number of arguments")));

    /* check for a null string, search, and replace */
    if (nargs < 0 || nulls[0] || nulls[1] || nulls[2])
        PG_RETURN_NULL();

    /*
     * replace() supports text, cstring, or the gtype string input for the
     * string and delimiter values
     */

    for (i = 0; i < 3; i++)
    {
        arg = args[i];
        type = types[i];

        if (type != GTYPEOID)
        {
            if (type == CSTRINGOID)
                param = cstring_to_text(DatumGetCString(arg));
            else if (type == TEXTOID)
                param = DatumGetTextPP(arg);
            else
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("replace() unsupported argument type %d",
                                       type)));
        }
        else
        {
            gtype *agt_arg;
            gtype_value *agtv_value;

            /* get the gtype argument */
            agt_arg = DATUM_GET_GTYPE_P(arg);

            if (!AGT_ROOT_IS_SCALAR(agt_arg))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("replace() only supports scalar arguments")));

            agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

            /* check for gtype null */
            if (agtv_value->type == AGTV_NULL)
                PG_RETURN_NULL();
            if (agtv_value->type == AGTV_STRING)
                param = cstring_to_text_with_len(agtv_value->val.string.val,
                                                 agtv_value->val.string.len);
            else
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("replace() unsupported argument gtype %d",
                                       agtv_value->type)));
        }
        if (i == 0)
            text_string = param;
        if (i == 1)
            text_search = param;
        if (i == 2)
            text_replace = param;
    }

    /*
     * We need the strings as a text strings so that we can let PG deal with
     * multibyte characters in the string.
     */
    text_result = DatumGetTextPP(DirectFunctionCall3Coll(
        replace_text, C_COLLATION_OID, PointerGetDatum(text_string),
        PointerGetDatum(text_search), PointerGetDatum(text_replace)));

    /* convert it back to a cstring */
    string = text_to_cstring(text_result);
    string_len = strlen(string);

    /* if we have an empty string, return null */
    if (string_len == 0)
        PG_RETURN_NULL();

    /* build the result */
    agtv_result.type = AGTV_STRING;
    agtv_result.val.string.val = string;
    agtv_result.val.string.len = string_len;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

/*
 * Helper function to extract one float8 compatible value from a variadic any.
 * It supports integer2/4/8, float4/8, and numeric or the gtype integer, float,
 * and numeric for the argument. It does not support a character based float,
 * otherwise we would just use tofloat. It returns a float on success or fails
 * with a message stating the funcname that called it and a specific message
 * stating the error.
 */
static float8 get_float_compatible_arg(Datum arg, Oid type, char *funcname,
                                       bool *is_null)
{
    float8 result;

    /* Assume the value is null. Although, this is only necessary for gtypes */
    *is_null = true;

    if (type != GTYPEOID)
    {
        if (type == INT2OID)
            result = (float8) DatumGetInt16(arg);
        else if (type == INT4OID)
            result = (float8) DatumGetInt32(arg);
        else if (type == INT8OID)
            result = DatumGetFloat8(DirectFunctionCall1(i8tod, Int64GetDatum(arg)));
        else if (type == FLOAT4OID)
            result = (float8) DatumGetFloat4(arg);
        else if (type == FLOAT8OID)
            result = DatumGetFloat8(arg);
        else if (type == NUMERICOID)
            result = DatumGetFloat8(DirectFunctionCall1(
                numeric_float8_no_overflow, arg));
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() unsupported argument type %d", funcname,
                                   type)));
    }
    else
    {
        gtype *agt_arg;
        gtype_value *agtv_value;

        /* get the gtype argument */
        agt_arg = DATUM_GET_GTYPE_P(arg);

        if (!AGT_ROOT_IS_SCALAR(agt_arg))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() only supports scalar arguments",
                                   funcname)));

        agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        /* check for gtype null */
        if (agtv_value->type == AGTV_NULL)
            return 0;

        if (agtv_value->type == AGTV_INTEGER)
            result = DatumGetFloat8(DirectFunctionCall1(i8tod, Int64GetDatum(agtv_value->val.int_value)));
        else if (agtv_value->type == AGTV_FLOAT)
            result = agtv_value->val.float_value;
        else if (agtv_value->type == AGTV_NUMERIC)
            result = DatumGetFloat8(DirectFunctionCall1(
                numeric_float8_no_overflow,
                NumericGetDatum(agtv_value->val.numeric)));
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() unsupported argument gtype %d",
                                   funcname, agtv_value->type)));
    }

    /* there is a valid non null value */
    *is_null = false;

    return result;
}

/*
 * Helper function to extract one numeric compatible value from a variadic any.
 * It supports integer2/4/8, float4/8, and numeric or the gtype integer, float,
 * and numeric for the argument. It does not support a character based numeric,
 * otherwise we would just cast it to numeric. It returns a numeric on success
 * or fails with a message stating the funcname that called it and a specific
 * message stating the error.
 */
static Numeric get_numeric_compatible_arg(Datum arg, Oid type, char *funcname,
                                          bool *is_null,
                                          enum gtype_value_type *ag_type)
{
    Numeric result;

    /* Assume the value is null. Although, this is only necessary for gtypes */
    *is_null = true;

    if (ag_type != NULL)
        *ag_type = AGTV_NULL;

    if (type != GTYPEOID)
    {
        if (type == INT2OID)
            result = DatumGetNumeric(DirectFunctionCall1(int2_numeric, arg));
        else if (type == INT4OID)
            result = DatumGetNumeric(DirectFunctionCall1(int4_numeric, arg));
        else if (type == INT8OID)
            result = DatumGetNumeric(DirectFunctionCall1(int8_numeric, arg));
        else if (type == FLOAT4OID)
            result = DatumGetNumeric(DirectFunctionCall1(float4_numeric, arg));
        else if (type == FLOAT8OID)
            result = DatumGetNumeric(DirectFunctionCall1(float8_numeric, arg));
        else if (type == NUMERICOID)
            result = DatumGetNumeric(arg);
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() unsupported argument type %d", funcname, type)));
    }
    else
    {
        gtype *agt_arg;
        gtype_value *agtv_value;

        /* get the gtype argument */
        agt_arg = DATUM_GET_GTYPE_P(arg);

        if (!AGT_ROOT_IS_SCALAR(agt_arg))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() only supports scalar arguments",
                                   funcname)));

        agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        /* check for gtype null */
        if (agtv_value->type == AGTV_NULL)
            return 0;

        if (agtv_value->type == AGTV_INTEGER)
        {
            result = DatumGetNumeric(DirectFunctionCall1(
                int8_numeric, Int64GetDatum(agtv_value->val.int_value)));
            if (ag_type != NULL)
                *ag_type = AGTV_INTEGER;
        }
        else if (agtv_value->type == AGTV_FLOAT)
        {
            result = DatumGetNumeric(DirectFunctionCall1(
                float8_numeric, Float8GetDatum(agtv_value->val.float_value)));
            if (ag_type != NULL)
                *ag_type = AGTV_FLOAT;
        }
        else if (agtv_value->type == AGTV_NUMERIC)
        {
            result = agtv_value->val.numeric;
            if (ag_type != NULL)
                *ag_type = AGTV_NUMERIC;
        }
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("%s() unsupported argument gtype %d",
                                   funcname, agtv_value->type)));
    }

    /* there is a valid non null value */
    *is_null = false;

    return result;
}

PG_FUNCTION_INFO_V1(age_sin);

Datum age_sin(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;

    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "sin", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dsin, Float8GetDatum(angle)));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_cos);

Datum age_cos(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;

    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "cos", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dcos, Float8GetDatum(angle)));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_tan);

Datum age_tan(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;

    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "tan", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dtan, Float8GetDatum(angle)));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_cot);

Datum age_cot(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;
    
    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "cot", &is_null);
    
    if (is_null)
        PG_RETURN_NULL();
    
    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dcot, Float8GetDatum(angle)));
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_asin);

Datum age_asin(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;

    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "asin", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dasin, Float8GetDatum(angle)));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_acos);

Datum age_acos(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;
    
    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "acos", &is_null);

    if (is_null)
        PG_RETURN_NULL();
    
    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(dacos, Float8GetDatum(angle)));
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_atan);

Datum age_atan(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;
    
    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "atan", &is_null);
    
    if (is_null)
        PG_RETURN_NULL();
    
    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(datan, Float8GetDatum(angle)));
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_atan2);

Datum age_atan2(PG_FUNCTION_ARGS)
{
    gtype *x_agt = AG_GET_ARG_GTYPE_P(0);
    gtype *y_agt = AG_GET_ARG_GTYPE_P(1);
    gtype_value agtv_result;
    bool is_null;
    float8 x, y;
    float8 angle;

    y = get_float_compatible_arg(GTYPE_P_GET_DATUM(y_agt), GTYPEOID, "atan2", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    x = get_float_compatible_arg(GTYPE_P_GET_DATUM(x_agt), GTYPEOID, "atan2", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    angle = DatumGetFloat8(DirectFunctionCall2(datan2,
                                               Float8GetDatum(y),
                                               Float8GetDatum(x)));

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value = angle;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_degrees);

Datum age_degrees(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;

    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "degrees", &is_null);

    if (is_null)
        PG_RETURN_NULL();

    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(degrees, Float8GetDatum(angle)));

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_radians);

Datum age_radians(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    gtype_value agtv_result;
    float8 angle;
    bool is_null;
    
    angle = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "radians", &is_null);
    
    if (is_null)
        PG_RETURN_NULL();
    
    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value =
        DatumGetFloat8(DirectFunctionCall1(radians, Float8GetDatum(angle)));
    
    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_round);

Datum age_round(PG_FUNCTION_ARGS)
{
    Datum *args = NULL;
    bool *nulls = NULL;
    Oid *types = NULL;
    int nargs = 0;
    gtype_value agtv_result;
    Numeric arg, arg_precision;
    Numeric numeric_result;
    float8 float_result;
    bool is_null = true;
    int precision = 0;

    /* extract argument values */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    /* check number of args */
    if (nargs != 1 && nargs != 2)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("round() invalid number of arguments")));
    }

    /* check for a null input */
    if (nargs < 0 || nulls[0])
        PG_RETURN_NULL();

    /*
     * round() supports integer, float, and numeric or the gtype integer,
     * float, and numeric for the input expression.
     */
    arg = get_numeric_compatible_arg(args[0], types[0], "round", &is_null, NULL);

    /* check for a gtype null input */
    if (is_null)
        PG_RETURN_NULL();

    /* We need the input as a numeric so that we can pass it off to PG */
    if (nargs == 2 && !nulls[1])
    {
        arg_precision = get_numeric_compatible_arg(args[1], types[1], "round",
                                                   &is_null, NULL);
        if (!is_null)
        {
            precision = DatumGetInt64(DirectFunctionCall1(numeric_int8,
                                      NumericGetDatum(arg_precision)));
            numeric_result = DatumGetNumeric(DirectFunctionCall2(numeric_round,
                                             NumericGetDatum(arg),
                                             Int32GetDatum(precision)));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("round() invalid NULL precision value")));
        }
    }
    else
    {
        numeric_result = DatumGetNumeric(DirectFunctionCall2(numeric_round,
                                         NumericGetDatum(arg),
                                         Int32GetDatum(0)));
    }

    float_result = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow,
                                               NumericGetDatum(numeric_result)));
    /* build the result */
    agtv_result.type = AGTV_FLOAT;
    agtv_result.val.float_value = float_result;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_ceil);

Datum age_ceil(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "ceil", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_ceil, NumericGetDatum(arg)));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "ceil", &is_null);
    
        float8 result = DatumGetFloat8(DirectFunctionCall1(dceil, Float8GetDatum(arg)));
    
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(age_floor);

Datum age_floor(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "floor", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_floor, NumericGetDatum(arg)));
                                          
        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "floor", &is_null);
    
        float8 result = DatumGetFloat8(DirectFunctionCall1(dfloor, Float8GetDatum(arg)));
    
        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(age_abs);

Datum age_abs(PG_FUNCTION_ARGS)
{
    gtype_value agtv_result;
    bool is_null;
    enum gtype_value_type type;

    Numeric arg = get_numeric_compatible_arg(AG_GET_ARG_GTYPE_P(0), GTYPEOID, "abs", &is_null, &type);

    if (is_null)
        PG_RETURN_NULL();

    Numeric numeric_result = DatumGetNumeric(DirectFunctionCall1(numeric_abs, NumericGetDatum(arg)));

    if (type == AGTV_INTEGER) {
        int64 int_result;

        int_result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
                                                       NumericGetDatum(numeric_result)));

        agtv_result.type = AGTV_INTEGER;
        agtv_result.val.int_value = int_result;
    } else if (type == AGTV_FLOAT) {
        float8 float_result;

        float_result = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow,
                                                          NumericGetDatum(numeric_result)));

        agtv_result.type = AGTV_FLOAT;
        agtv_result.val.float_value = float_result;
    } else if (type == AGTV_NUMERIC) {
        agtv_result.type = AGTV_NUMERIC;
        agtv_result.val.numeric = numeric_result;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("abs() invalid argument type")));
    }

    AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_sign);
Datum age_sign(PG_FUNCTION_ARGS) {
    int nargs;
    Datum *args;
    bool *nulls;
    Oid *types;
    gtype_value agtv_result;
    Numeric arg;
    Numeric numeric_result;
    int int_result;
    bool is_null = true;

    /* extract argument values */
    nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    /* check number of args */
    if (nargs != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("sign() invalid number of arguments")));

    /* check for a null input */
    if (nargs < 0 || nulls[0])
        PG_RETURN_NULL();

    /*
     * sign() supports integer, float, and numeric or the gtype integer,
     * float, and numeric for the input expression.
     */
    arg = get_numeric_compatible_arg(args[0], types[0], "sign", &is_null, NULL);

    /* check for a gtype null input */
    if (is_null)
        PG_RETURN_NULL();

    /* We need the input as a numeric so that we can pass it off to PG */
    numeric_result = DatumGetNumeric(DirectFunctionCall1(numeric_sign, NumericGetDatum(arg)));

    int_result = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(numeric_result)));

    /* build the result */
    agtv_result.type = AGTV_INTEGER;
    agtv_result.val.int_value = int_result;

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_log);
Datum age_log(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "log", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_ln, NumericGetDatum(arg)));
        
        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "log", &is_null);

        float8 result = DatumGetFloat8(DirectFunctionCall1(dlog1, Float8GetDatum(arg)));

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(age_log10);
Datum age_log10(PG_FUNCTION_ARGS) {
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "log10", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_log, NumericGetDatum(arg)));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "log10", &is_null);

        float8 result = DatumGetFloat8(DirectFunctionCall1(dlog10, Float8GetDatum(arg)));

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(age_e);

Datum age_e(PG_FUNCTION_ARGS)
{
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(DirectFunctionCall1(dexp, Float8GetDatum(1)))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_pi);
    
Datum age_pi(PG_FUNCTION_ARGS)
{   
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = M_PI
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}   

PG_FUNCTION_INFO_V1(age_rand);

Datum age_rand(PG_FUNCTION_ARGS)
{
    gtype_value agtv_result = {
        .type = AGTV_FLOAT,
        .val.float_value = DatumGetFloat8(DirectFunctionCall1(random, Float8GetDatum(1)))
    };

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

PG_FUNCTION_INFO_V1(age_exp);

Datum age_exp(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "exp", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_exp, NumericGetDatum(arg)));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "exp", &is_null);

        float8 result = DatumGetFloat8(DirectFunctionCall1(dexp, Float8GetDatum(arg)));

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    }
}

PG_FUNCTION_INFO_V1(age_sqrt);

Datum age_sqrt(PG_FUNCTION_ARGS)
{
    gtype *agt = AG_GET_ARG_GTYPE_P(0);
    bool is_null = true;

    if (is_gtype_null(agt))
        PG_RETURN_NULL();

    if (is_gtype_numeric(agt)) {
        Numeric arg = get_numeric_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "sqrt", &is_null, NULL);

        Numeric result = DatumGetNumeric(DirectFunctionCall1(numeric_sqrt, NumericGetDatum(arg)));

        gtype_value agtv = { .type = AGTV_NUMERIC, .val.numeric = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));
    } else {
        float8 arg = get_float_compatible_arg(GTYPE_P_GET_DATUM(agt), GTYPEOID, "sqrt", &is_null);

        float8 result = DatumGetFloat8(DirectFunctionCall1(dsqrt, Float8GetDatum(arg)));                                                       

        gtype_value agtv = { .type = AGTV_FLOAT, .val.float_value = result };

        AG_RETURN_GTYPE_P(gtype_value_to_gtype(&agtv));

    }
}

/*
 * Converts an gtype object or array to a binary gtype_value.
 */
gtype_value *gtype_composite_to_gtype_value_binary(gtype *a)
{
    gtype_value *result;

    if (GTYPE_CONTAINER_IS_SCALAR(&a->root))
    {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert gtype scalar objects to binary gtype_value objects")));
    }

    result = palloc(sizeof(gtype_value));

    // convert the gtype to a binary gtype_value
    result->type = AGTV_BINARY;
    result->val.binary.len = GTYPE_CONTAINER_SIZE(&a->root);
    result->val.binary.data = &a->root;

    return result;
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
    gtype_iterator_token tok = WAGT_DONE;
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

    parsed_gtype_value = push_gtype_value(&parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);

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

        if (tok == WAGT_DONE || tok == WAGT_END_OBJECT)
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
            parsed_gtype_value = push_gtype_value(&parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);

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
                &parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);

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

                parsed_gtype_value = push_gtype_value(&parse_state, WAGT_VALUE, new_gtype_value_v);
            }
            else
            {
                gtype_value *result = gtype_composite_to_gtype_value_binary(new_v);

                parsed_gtype_value = push_gtype_value(&parse_state, WAGT_VALUE, result);
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
        parsed_gtype_value = push_gtype_value(
            &parse_state, WAGT_KEY, key);

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
            parsed_gtype_value = push_gtype_value(&parse_state, WAGT_VALUE, new_gtype_value_v);
        }
        else
        {
            gtype_value *result = gtype_composite_to_gtype_value_binary(new_v);

            parsed_gtype_value = push_gtype_value(&parse_state, WAGT_VALUE, result);
        }
    }

    // push the end object token to parse state
    parsed_gtype_value = push_gtype_value(&parse_state, WAGT_END_OBJECT, NULL);

    return parsed_gtype_value;
}

/*
 * Helper function to extract 1 datum from a variadic "any" and convert, if
 * possible, to an gtype, if it isn't already.
 *
 * If the value is a NULL or gtype NULL, the function returns NULL.
 * If the datum cannot be converted, the function will error out in
 * extract_variadic_args.
 */
gtype *get_one_gtype_from_variadic_args(FunctionCallInfo fcinfo, int variadic_offset, int expected_nargs) {
    int nargs;
    Datum *args = NULL;
    bool *nulls = NULL;
    Oid *types = NULL;
    gtype *gtype_result = NULL;

    nargs = extract_variadic_args(fcinfo, variadic_offset, false, &args, &types, &nulls);
    /* throw an error if the number of args is not the expected number */
    if (nargs != expected_nargs)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("number of args %d does not match expected %d",
                               nargs, expected_nargs)));
    }
    /* if null, return null */
    if (nulls[0])
    {
        return NULL;
    }

    /* if type is GTYPEOID, we don't need to convert it */
    if (types[0] == GTYPEOID)
    {
        gtype_container *agtc;

        gtype_result = DATUM_GET_GTYPE_P(args[0]);
        agtc = &gtype_result->root;

        /*
         * Is this a scalar (scalars are stored as one element arrays)? If so,
         * test for gtype NULL.
         */
        if (GTYPE_CONTAINER_IS_SCALAR(agtc) &&
            AGTE_IS_NULL(agtc->children[0]))
        {
            return NULL;
        }
    }
    /* otherwise, try to convert it to an gtype */
    else
    {
        gtype_in_state state;
        agt_type_category tcategory;
        Oid outfuncoid;

        /* we need an empty state */
        state.parse_state = NULL;
        state.res = NULL;
        /* get the category for the datum */
        gtype_categorize_type(types[0], &tcategory, &outfuncoid);
        /* convert it to an gtype_value */
        datum_to_gtype(args[0], false, &state, tcategory, outfuncoid, false);
        /* convert it to an gtype */
        gtype_result = gtype_value_to_gtype(state.res);
    }
    return gtype_result;
}

/*
 * Transfer function for age_sum(gtype, gtype).
 *
 * Note: that the running sum will change type depending on the
 * precision of the input. The most precise value determines the
 * result type.
 *
 * Note: The sql definition is STRICT so no input NULLs need to
 * be dealt with except for gtype.
 */
PG_FUNCTION_INFO_V1(age_gtype_sum);

Datum age_gtype_sum(PG_FUNCTION_ARGS)
{
    gtype *agt_arg0 = AG_GET_ARG_GTYPE_P(0);
    gtype *agt_arg1 = AG_GET_ARG_GTYPE_P(1);
    gtype_value *agtv_lhs;
    gtype_value *agtv_rhs;
    gtype_value agtv_result;

    /* get our args */
    agt_arg0 = AG_GET_ARG_GTYPE_P(0);
    agt_arg1 = AG_GET_ARG_GTYPE_P(1);

    /* only scalars are allowed */
    if (!AGT_ROOT_IS_SCALAR(agt_arg0) || !AGT_ROOT_IS_SCALAR(agt_arg1))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("arguments must resolve to a scalar")));

    /* get the values */
    agtv_lhs = get_ith_gtype_value_from_container(&agt_arg0->root, 0);
    agtv_rhs = get_ith_gtype_value_from_container(&agt_arg1->root, 0);

    /* only numbers are allowed */
    if ((agtv_lhs->type != AGTV_INTEGER && agtv_lhs->type != AGTV_FLOAT &&
         agtv_lhs->type != AGTV_NUMERIC) || (agtv_rhs->type != AGTV_INTEGER &&
        agtv_rhs->type != AGTV_FLOAT && agtv_rhs->type != AGTV_NUMERIC))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("arguments must resolve to a number")));

    /* check for gtype null */
    if (agtv_lhs->type == AGTV_NULL)
        PG_RETURN_POINTER(agt_arg1);
    if (agtv_rhs->type == AGTV_NULL)
        PG_RETURN_POINTER(agt_arg0);

    /* we want to maintain the precision of the most precise input */
    if (agtv_lhs->type == AGTV_NUMERIC || agtv_rhs->type == AGTV_NUMERIC)
    {
        agtv_result.type = AGTV_NUMERIC;
    }
    else if (agtv_lhs->type == AGTV_FLOAT || agtv_rhs->type == AGTV_FLOAT)
    {
        agtv_result.type = AGTV_FLOAT;
    }
    else
    {
        agtv_result.type = AGTV_INTEGER;
    }

    /* switch on the type to perform the correct addition */
    switch(agtv_result.type)
    {
        /* if the type is integer, they are obviously both ints */
        case AGTV_INTEGER:
            agtv_result.val.int_value = DatumGetInt64(
                DirectFunctionCall2(int8pl,
                                    Int64GetDatum(agtv_lhs->val.int_value),
                                    Int64GetDatum(agtv_rhs->val.int_value)));
            break;
        /* for float it can be either, float + float or float + int */
        case AGTV_FLOAT:
        {
            Datum dfl;
            Datum dfr;
            Datum dresult;
            /* extract and convert the values as necessary */
            /* float + float */
            if (agtv_lhs->type == AGTV_FLOAT && agtv_rhs->type == AGTV_FLOAT)
            {
                dfl = Float8GetDatum(agtv_lhs->val.float_value);
                dfr = Float8GetDatum(agtv_rhs->val.float_value);
            }
            /* float + int */
            else
            {
                int64 ival;
                float8 fval;
                bool is_null;

                ival = (agtv_lhs->type == AGTV_INTEGER) ?
                    agtv_lhs->val.int_value : agtv_rhs->val.int_value;
                fval = (agtv_lhs->type == AGTV_FLOAT) ?
                    agtv_lhs->val.float_value : agtv_rhs->val.float_value;

                dfl = Float8GetDatum(get_float_compatible_arg(Int64GetDatum(ival),
                                                              INT8OID, "",
                                                              &is_null));
                dfr = Float8GetDatum(fval);
            }
            /* add the floats and set the result */
            dresult = DirectFunctionCall2(float8pl, dfl, dfr);
            agtv_result.val.float_value = DatumGetFloat8(dresult);
        }
            break;
        /*
         * For numeric it can be either, numeric + numeric or numeric + float or
         * numeric + int
         */
        case AGTV_NUMERIC:
        {
            Datum dnl;
            Datum dnr;
            Datum dresult;
            /* extract and convert the values as necessary */
            /* numeric + numeric */
            if (agtv_lhs->type == AGTV_NUMERIC && agtv_rhs->type == AGTV_NUMERIC)
            {
                dnl = NumericGetDatum(agtv_lhs->val.numeric);
                dnr = NumericGetDatum(agtv_rhs->val.numeric);
            }
            /* numeric + float */
            else if (agtv_lhs->type == AGTV_FLOAT || agtv_rhs->type == AGTV_FLOAT)
            {
                float8 fval;
                Numeric nval;

                fval = (agtv_lhs->type == AGTV_FLOAT) ?
                    agtv_lhs->val.float_value : agtv_rhs->val.float_value;
                nval = (agtv_lhs->type == AGTV_NUMERIC) ?
                    agtv_lhs->val.numeric : agtv_rhs->val.numeric;

                dnl = DirectFunctionCall1(float8_numeric, Float8GetDatum(fval));
                dnr = NumericGetDatum(nval);
            }
            /* numeric + int */
            else
            {
                int64 ival;
                Numeric nval;

                ival = (agtv_lhs->type == AGTV_INTEGER) ?
                    agtv_lhs->val.int_value : agtv_rhs->val.int_value;
                nval = (agtv_lhs->type == AGTV_NUMERIC) ?
                    agtv_lhs->val.numeric : agtv_rhs->val.numeric;

                dnl = DirectFunctionCall1(int8_numeric, Int64GetDatum(ival));
                dnr = NumericGetDatum(nval);
            }
            /* add the numerics and set the result */
            dresult = DirectFunctionCall2(numeric_add, dnl, dnr);
            agtv_result.val.numeric = DatumGetNumeric(dresult);
        }
            break;

        default:
            elog(ERROR, "unexpected gtype");
            break;
    }
    /* return the result */
    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_result));
}

/*
 * Wrapper function for float8_accum to take an gtype input.
 * This function is defined as STRICT so it does not need to check
 * for NULL input parameters
 */
PG_FUNCTION_INFO_V1(gtype_accum);

Datum gtype_accum(PG_FUNCTION_ARGS)
{
    Datum dfloat;
    Datum result;

    /* convert to a float8 datum, if possible */
    dfloat = DirectFunctionCall1(gtype_to_float8, PG_GETARG_DATUM(1));
    /* pass the arguments off to float8_accum */
    result = DirectFunctionCall2(float8_accum, PG_GETARG_DATUM(0), dfloat);

    PG_RETURN_DATUM(result);
}

/* Wrapper for stdDev function. */
PG_FUNCTION_INFO_V1(gtype_stddev_samp_final);

Datum gtype_stddev_samp_final(PG_FUNCTION_ARGS)
{
    Datum result;
    PGFunction func;
    gtype_value agtv_float;

    /* we can't use DirectFunctionCall1 as it errors for NULL values */
    func = float8_stddev_samp;
    result = (*func) (fcinfo);

    agtv_float.type = AGTV_FLOAT;

    /*
     * Check to see if float8_stddev_samp returned null. If so, we need to
     * return a gtype float 0.
     */
    if (fcinfo->isnull)
    {
        /* we need to clear the flag */
        fcinfo->isnull = false;
        agtv_float.val.float_value = 0.0;
    }
    else
    {
        agtv_float.val.float_value = DatumGetFloat8(result);
    }

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

PG_FUNCTION_INFO_V1(gtype_stddev_pop_final);

Datum gtype_stddev_pop_final(PG_FUNCTION_ARGS)
{
    Datum result;
    PGFunction func;
    gtype_value agtv_float;

    /* we can't use DirectFunctionCall1 as it errors for NULL values */
    func = float8_stddev_pop;
    result = (*func) (fcinfo);

    agtv_float.type = AGTV_FLOAT;

    /*
     * Check to see if float8_stddev_pop returned null. If so, we need to
     * return a gtype float 0.
     */
    if (fcinfo->isnull)
    {
        /* we need to clear the flag */
        fcinfo->isnull = false;
        agtv_float.val.float_value = 0.0;
    }
    else
    {
        agtv_float.val.float_value = DatumGetFloat8(result);
    }

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

PG_FUNCTION_INFO_V1(gtype_max_trans);

Datum gtype_max_trans(PG_FUNCTION_ARGS)
{
    gtype *gtype_arg1;
    gtype *gtype_arg2;
    gtype *gtype_larger;
    int test;

    /* for max we need to ignore NULL values */
    /* extract the args as gtype */
    gtype_arg1 = get_one_gtype_from_variadic_args(fcinfo, 0, 2);
    gtype_arg2 = get_one_gtype_from_variadic_args(fcinfo, 1, 1);

    if (gtype_arg1 == NULL && gtype_arg2 == NULL)
        PG_RETURN_NULL();
    else if (gtype_arg2 == NULL)
        PG_RETURN_POINTER(gtype_arg1);
    else if (gtype_arg1 == NULL)
        PG_RETURN_POINTER(gtype_arg2);

    /* test for max value */
    test = compare_gtype_containers_orderability(&gtype_arg1->root, &gtype_arg2->root);

    gtype_larger = (test >= 0) ? gtype_arg1 : gtype_arg2;

    PG_RETURN_POINTER(gtype_larger);
}

PG_FUNCTION_INFO_V1(gtype_min_trans);

Datum gtype_min_trans(PG_FUNCTION_ARGS)
{
    gtype *gtype_arg1 = NULL;
    gtype *gtype_arg2 = NULL;
    gtype *gtype_smaller;
    int test;

    /* for min we need to ignore NULL values */
    /* extract the args as gtype */
    gtype_arg1 = get_one_gtype_from_variadic_args(fcinfo, 0, 2);
    gtype_arg2 = get_one_gtype_from_variadic_args(fcinfo, 1, 1);

    /* return NULL if both are NULL */
    if (gtype_arg1 == NULL && gtype_arg2 == NULL)
        PG_RETURN_NULL();
    /* if one is NULL, return the other */
    if (gtype_arg1 != NULL && gtype_arg2 == NULL)
        PG_RETURN_POINTER(gtype_arg1);
    if (gtype_arg1 == NULL && gtype_arg2 != NULL)
        PG_RETURN_POINTER(gtype_arg2);

    /* test for min value */
    test = compare_gtype_containers_orderability(&gtype_arg1->root,
                                                  &gtype_arg2->root);

    gtype_smaller = (test <= 0) ? gtype_arg1 : gtype_arg2;

    PG_RETURN_POINTER(gtype_smaller);
}

/* borrowed from PGs float8 routines for percentile_cont */
static Datum float8_lerp(Datum lo, Datum hi, double pct)
{
    double loval = DatumGetFloat8(lo);
    double hival = DatumGetFloat8(hi);

    return Float8GetDatum(loval + (pct * (hival - loval)));
}

/* Code borrowed and adjusted from PG's ordered_set_transition function */
PG_FUNCTION_INFO_V1(age_percentile_aggtransfn);

Datum age_percentile_aggtransfn(PG_FUNCTION_ARGS)
{
    PercentileGroupAggState *pgastate;

    /* verify we are in an aggregate context */
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /* if this is the first invocation, create the state */
    if (PG_ARGISNULL(0))
    {
        MemoryContext old_mcxt;
        float8 percentile;

        /* validate the percentile */
        if (PG_ARGISNULL(2))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("percentile value NULL is not a valid numeric value")));

        percentile = DatumGetFloat8(DirectFunctionCall1(gtype_to_float8,
                         PG_GETARG_DATUM(2)));

        if (percentile < 0 || percentile > 1 || isnan(percentile))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("percentile value %g is not between 0 and 1",
                               percentile)));

        /* switch to the correct aggregate context */
        old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        /* create and initialize the state */
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

        /* restore the old context */
        MemoryContextSwitchTo(old_mcxt);
    }
    /* otherwise, retrieve the state */
    else
        pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);

    /* Load the datum into the tuplesort object, but only if it's not null */
    if (!PG_ARGISNULL(1))
    {
        Datum dfloat = DirectFunctionCall1(gtype_to_float8, PG_GETARG_DATUM(1));

        tuplesort_putdatum(pgastate->sortstate, dfloat, false);
        pgastate->number_of_rows++;
    }
    /* return the state */
    PG_RETURN_POINTER(pgastate);
}

/* Code borrowed and adjusted from PG's percentile_cont_final function */
PG_FUNCTION_INFO_V1(age_percentile_cont_aggfinalfn);

Datum age_percentile_cont_aggfinalfn(PG_FUNCTION_ARGS)
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

    /* verify we are in an aggregate context */
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    /* retrieve the state and percentile */
    pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);
    percentile = pgastate->percentile;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (pgastate->number_of_rows == 0)
        PG_RETURN_NULL();

    /* Finish the sort, or rescan if we already did */
    if (!pgastate->sort_done)
    {
        tuplesort_performsort(pgastate->sortstate);
        pgastate->sort_done = true;
    }
    else
        tuplesort_rescan(pgastate->sortstate);

    /* calculate the percentile cont*/
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

    /* convert to an gtype float and return the result */
    agtv_float.type = AGTV_FLOAT;
    agtv_float.val.float_value = DatumGetFloat8(val);

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

/* Code borrowed and adjusted from PG's percentile_disc_final function */
PG_FUNCTION_INFO_V1(age_percentile_disc_aggfinalfn);

Datum age_percentile_disc_aggfinalfn(PG_FUNCTION_ARGS)
{
    PercentileGroupAggState *pgastate;
    double percentile;
    Datum val;
    bool isnull;
    int64 rownum;
    gtype_value agtv_float;

    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    pgastate = (PercentileGroupAggState *) PG_GETARG_POINTER(0);
    percentile = pgastate->percentile;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (pgastate->number_of_rows == 0)
        PG_RETURN_NULL();

    /* Finish the sort, or rescan if we already did */
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

    /* We shouldn't have stored any nulls, but do the right thing anyway */
    if (isnull)
        PG_RETURN_NULL();

    /* convert to an gtype float and return the result */
    agtv_float.type = AGTV_FLOAT;
    agtv_float.val.float_value = DatumGetFloat8(val);

    PG_RETURN_POINTER(gtype_value_to_gtype(&agtv_float));
}

/* functions to support the aggregate function COLLECT() */
PG_FUNCTION_INFO_V1(age_collect_aggtransfn);

Datum age_collect_aggtransfn(PG_FUNCTION_ARGS)
{
    gtype_in_state *castate;
    int nargs;
    Datum *args;
    bool *nulls;
    Oid *types;
    MemoryContext old_mcxt;

    /* verify we are in an aggregate context */
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /*
     * Switch to the correct aggregate context. Otherwise, the data added to the
     * array will be lost.
     */
    old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

    /* if this is the first invocation, create the state */
    if (PG_ARGISNULL(0)) {
        castate = palloc0(sizeof(gtype_in_state));
        memset(castate, 0, sizeof(gtype_in_state));

        castate->res = push_gtype_value(&castate->parse_state, WAGT_BEGIN_ARRAY, NULL);
    } else {
        castate = (gtype_in_state *) PG_GETARG_POINTER(0);
    }

    /*
     * Extract the variadic args, of which there should only be one.
     * Insert the arg into the array, unless it is null. Nulls are
     * skipped over.
     */
    if (PG_ARGISNULL(1))
        nargs = 0;
    else
        nargs = extract_variadic_args(fcinfo, 1, true, &args, &types, &nulls);

    if (nargs == 1) {
        /* only add non null values */
        if (nulls[0] == false) {
            /* we need to check for gtype null and skip it, if found */
            if (types[0] == GTYPEOID) {
                gtype *agt_arg;
                gtype_value *agtv_value;

                /* get the gtype argument */
                agt_arg = DATUM_GET_GTYPE_P(args[0]);
                agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);
                /* add the arg if not gtype null */
                if (agtv_value->type != AGTV_NULL)
                    add_gtype(args[0], nulls[0], castate, types[0], false);
            } else {
                add_gtype(args[0], nulls[0], castate, types[0], false);
            }
        }
    } else if (nargs > 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("collect() invalid number of arguments")));
    }

    /* restore the old context */
    MemoryContextSwitchTo(old_mcxt);

    /* return the state */
    PG_RETURN_POINTER(castate);
}

PG_FUNCTION_INFO_V1(age_collect_aggfinalfn);

Datum age_collect_aggfinalfn(PG_FUNCTION_ARGS) {
    gtype_in_state *castate;
    MemoryContext old_mcxt;

    /* verify we are in an aggregate context */
    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /*
     * Get the state. There are cases where the age_collect_aggtransfn never
     * gets called. So, check to see if this is one.
     */
    if (PG_ARGISNULL(0)) {
        /* create and initialize the state */
        castate = palloc0(sizeof(gtype_in_state));
        memset(castate, 0, sizeof(gtype_in_state));
        /* start the array */
        castate->res = push_gtype_value(&castate->parse_state, WAGT_BEGIN_ARRAY, NULL);
    } else {
        castate = (gtype_in_state *) PG_GETARG_POINTER(0);
    }

    old_mcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

    castate->res = push_gtype_value(&castate->parse_state, WAGT_END_ARRAY, NULL);

    MemoryContextSwitchTo(old_mcxt);

    PG_RETURN_POINTER(gtype_value_to_gtype(castate->res));
}

/* helper function to quickly build an gtype_value vertex */
gtype_value *gtype_value_build_vertex(graphid id, char *label, Datum properties) {
    gtype_in_state result;

    /* the label can't be NULL */
    Assert(label != NULL);

    memset(&result, 0, sizeof(gtype_in_state));

    /* push in the object beginning */
    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);

    /* push the graph id key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, integer_to_gtype_value(id));

    /* push the label key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("label"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, string_to_gtype_value(label));

    /* push the properties key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("properties"));
    add_gtype((Datum)properties, false, &result, GTYPEOID, false);

    /* push in the object end */
    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    /* set it as an edge */
    result.res->type = AGTV_VERTEX;

    /* return the result that was build (allocated) inside the result */
    return result.res;
}

/* helper function to quickly build an gtype_value edge */
gtype_value *gtype_value_build_edge(graphid id, char *label, graphid end_id, graphid start_id, Datum properties) {
    gtype_in_state result;

    /* the label can't be NULL */
    Assert(label != NULL);

    memset(&result, 0, sizeof(gtype_in_state));

    /* push in the object beginning */
    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);
    /* push the graph id key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, integer_to_gtype_value(id));

    /* push the label key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("label"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, string_to_gtype_value(label));

    /* push the end_id key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("end_id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, integer_to_gtype_value(end_id));

    /* push the start_id key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("start_id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, integer_to_gtype_value(start_id));

    /* push the properties key/value pair */
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("properties"));
    add_gtype((Datum)properties, false, &result, GTYPEOID, false);

    /* push in the object end */
    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    /* set it as an edge */
    result.res->type = AGTV_EDGE;

    /* return the result that was build (allocated) inside the result */
    return result.res;
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

    /* we need these */
    Assert(funcname != NULL);
    Assert(agt_arg != NULL);

    /* error if the argument is not a scalar */
    if (!GTYPE_CONTAINER_IS_SCALAR(&agt_arg->root))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument must be a scalar", funcname)));

    /* is it AGTV_NULL? */
    if (error && is_gtype_null(agt_arg))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument must not be AGTV_NULL", funcname)));

    /* get the gtype value */
    agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    /* is it the correct type? */
    if (error && agtv_value->type != type)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s: gtype argument of wrong type", funcname)));

    return agtv_value;
}

PG_FUNCTION_INFO_V1(age_eq_tilde);
/*
 * function for =~ aka regular expression comparisons
 */
Datum age_eq_tilde(PG_FUNCTION_ARGS)
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
            text *string = cstring_to_text_with_len(agtv_string->val.string.val,
                                                    agtv_string->val.string.len);
            text *pattern = cstring_to_text_with_len(agtv_pattern->val.string.val,
                                                     agtv_pattern->val.string.len);

            Datum result = (DirectFunctionCall2Coll(textregexeq, C_COLLATION_OID,
                                                    PointerGetDatum(string),
                                                    PointerGetDatum(pattern)));
            return boolean_to_gtype(DatumGetBool(result));
        }
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("gtype string values expected")));
}

/*
 * Helper function to step through and retrieve keys from an object.
 * borrowed and modified from get_next_object_pair() in gtype_vle.c
 */
static gtype_iterator *get_next_object_key(gtype_iterator *it, gtype_container *agtc, gtype_value *key) {
    gtype_iterator_token itok;
    gtype_value tmp;

    /* verify input params */
    Assert(agtc != NULL);
    Assert(key != NULL);

    /* check to see if the container is empty */
    if (GTYPE_CONTAINER_SIZE(agtc) == 0)
        return NULL;

    /* if the passed iterator is NULL, this is the first time, create it */
    if (it == NULL) {
        /* initial the iterator */
        it = gtype_iterator_init(agtc);
        /* get the first token */
        itok = gtype_iterator_next(&it, &tmp, false);
        /* it should be WAGT_BEGIN_OBJECT */
        Assert(itok == WAGT_BEGIN_OBJECT);
    }

    /* the next token should be a key or the end of the object */
    itok = gtype_iterator_next(&it, &tmp, false);
    Assert(itok == WAGT_KEY || WAGT_END_OBJECT);
    /* if this is the end of the object return NULL */
    if (itok == WAGT_END_OBJECT)
        return NULL;

    /* this should be the key, copy it */
    if (itok == WAGT_KEY)
        memcpy(key, &tmp, sizeof(gtype_value));

    /*
     * The next token should be a value but, it could be a begin tokens for
     * arrays or objects. For those we just return NULL to ignore them.
     */
    itok = gtype_iterator_next(&it, &tmp, true);
    Assert(itok == WAGT_VALUE);

    /* return the iterator */
    return it;
}

PG_FUNCTION_INFO_V1(age_vertex_keys);
Datum age_vertex_keys(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    vertex *v = AG_GET_ARG_VERTEX(0);
    	agt_arg = extract_vertex_properties(v);

    agtv_result = push_gtype_value(&parse_state, WAGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WAGT_ELEM, &obj_key);

    /* push the end of the array*/
    agtv_result = push_gtype_value(&parse_state, WAGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}


PG_FUNCTION_INFO_V1(age_edge_keys);
Datum age_edge_keys(PG_FUNCTION_ARGS)
{   
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    edge *v = AG_GET_ARG_EDGE(0);
        agt_arg = extract_edge_properties(v);

    agtv_result = push_gtype_value(&parse_state, WAGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WAGT_ELEM, &obj_key);

    /* push the end of the array*/
    agtv_result = push_gtype_value(&parse_state, WAGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}



PG_FUNCTION_INFO_V1(age_keys);
Datum age_keys(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_result = NULL;
    gtype_value obj_key = {0};
    gtype_iterator *it = NULL;
    gtype_parse_state *parse_state = NULL;
    agt_arg = AG_GET_ARG_GTYPE_P(0);

    if (is_gtype_null(agt_arg))
        PG_RETURN_NULL();

    if (AGT_ROOT_IS_SCALAR(agt_arg)) {
        agtv_result = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        if (agtv_result->type == AGTV_EDGE || agtv_result->type == AGTV_VERTEX) {
            agtv_result = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_result, "properties");

            Assert(agtv_result != NULL);
            Assert(agtv_result->type = AGTV_OBJECT);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("keys() argument must be a vertex, edge, object or null")));
        }

        agt_arg = gtype_value_to_gtype(agtv_result);
        agtv_result = NULL;
    } else if (!AGT_ROOT_IS_OBJECT(agt_arg)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("keys() argument must be a vertex, edge, object or null")));
    }

    agtv_result = push_gtype_value(&parse_state, WAGT_BEGIN_ARRAY, NULL);

    while ((it = get_next_object_key(it, &agt_arg->root, &obj_key)))
        agtv_result = push_gtype_value(&parse_state, WAGT_ELEM, &obj_key);

    /* push the end of the array*/
    agtv_result = push_gtype_value(&parse_state, WAGT_END_ARRAY, NULL);

    Assert(agtv_result != NULL);
    Assert(agtv_result->type == AGTV_ARRAY);

    PG_RETURN_POINTER(gtype_value_to_gtype(agtv_result));
}

PG_FUNCTION_INFO_V1(age_nodes);
/*
 * Execution function to implement openCypher nodes() function
 */
Datum age_nodes(PG_FUNCTION_ARGS) {
    gtype *agt_arg = NULL;
    gtype_value *agtv_path = NULL;
    gtype_in_state agis_result;
    int i = 0;

    /* check for null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    /* check for a scalar object */
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("nodes() argument must resolve to a scalar value")));

    /* get the potential path out of the array */
    agtv_path = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    /* is it an gtype null? */
    if (agtv_path->type == AGTV_NULL)
        PG_RETURN_NULL();

    if (agtv_path->type != AGTV_PATH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("nodes() argument must be a path")));

    /* clear the result structure */
    MemSet(&agis_result, 0, sizeof(gtype_in_state));

    /* push the beginning of the array */
    agis_result.res = push_gtype_value(&agis_result.parse_state,
                                        WAGT_BEGIN_ARRAY, NULL);
    /* push in each vertex (every other entry) from the path */
    for (i = 0; i < agtv_path->val.array.num_elems; i += 2) {
        agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_ELEM, &agtv_path->val.array.elems[i]);
    }

    /* push the end of the array */
    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_END_ARRAY, NULL);

    /* convert the gtype_value to a datum to return to the caller */
    PG_RETURN_POINTER(gtype_value_to_gtype(agis_result.res));
}

PG_FUNCTION_INFO_V1(age_labels);
Datum age_labels(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_temp = NULL;
    gtype_value *agtv_label = NULL;
    gtype_in_state agis_result;

    /* get the vertex argument */
    agt_arg = AG_GET_ARG_GTYPE_P(0);

    /* verify it is a scalar */
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("labels() argument must resolve to a scalar value")));

    /* is it an gtype null? */
    if (GTYPE_CONTAINER_IS_SCALAR(&agt_arg->root) && AGTE_IS_NULL((&agt_arg->root)->children[0]))
        PG_RETURN_NULL();

    agtv_temp = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    if (agtv_temp->type != AGTV_VERTEX)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("labels() argument must be a vertex")));

    agtv_label = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_temp, "label");
    Assert(agtv_label != NULL);

    MemSet(&agis_result, 0, sizeof(gtype_in_state));

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_ELEM, agtv_label);

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(agis_result.res));
}

PG_FUNCTION_INFO_V1(age_relationships);
/*
 * Execution function to implement openCypher relationships() function
 */
Datum age_relationships(PG_FUNCTION_ARGS)
{
    gtype *agt_arg = NULL;
    gtype_value *agtv_path = NULL;
    gtype_in_state agis_result;
    int i = 0;

    /* check for null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    agt_arg = AG_GET_ARG_GTYPE_P(0);
    if (!AGT_ROOT_IS_SCALAR(agt_arg))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("relationships() argument must resolve to a scalar value")));

    agtv_path = get_ith_gtype_value_from_container(&agt_arg->root, 0);

    if (agtv_path->type == AGTV_NULL)
        PG_RETURN_NULL();

    if (agtv_path->type != AGTV_PATH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("relationships() argument must be a path")));

    MemSet(&agis_result, 0, sizeof(gtype_in_state));

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    for (i = 1; i < agtv_path->val.array.num_elems; i += 2) {
        agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_ELEM, &agtv_path->val.array.elems[i]);
    }

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(agis_result.res));
}

/*
 * Helper function to convert an integer type (PostgreSQL or gtype) datum into
 * an int64. The function will flag if an gtype null was found. The function
 * will error out on invalid information, printing out the funcname passed.
 */
static int64 get_int64_from_int_datums(Datum d, Oid type, char *funcname, bool *is_agnull) {
    int64 result = 0;

    if (type == INT2OID) {
        result = (int64) DatumGetInt16(d);
    } else if (type == INT4OID) {
        result = (int64) DatumGetInt32(d);
    } else if (type == INT8OID) {
        result = (int64) DatumGetInt64(d);
    } else if (type == GTYPEOID) {
        gtype *agt_arg = NULL;
        gtype_value *agtv_value = NULL;
        gtype_container *agtc = NULL;

        agt_arg = DATUM_GET_GTYPE_P(d);

        if (!AGT_ROOT_IS_SCALAR(agt_arg))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%s() only supports scalar arguments", funcname)));

        agtc = &agt_arg->root;
        if (AGTE_IS_NULL(agtc->children[0])) {
            *is_agnull = true;
            return 0;
        }

        agtv_value = get_ith_gtype_value_from_container(&agt_arg->root, 0);

        if (agtv_value->type == AGTV_INTEGER)
            result = agtv_value->val.int_value;
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("%s() unsupported argument type", funcname)));
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("%s() unsupported argument type", funcname)));
    }

    *is_agnull = false;
    return result;
}

PG_FUNCTION_INFO_V1(age_range);
/*
 * Execution function to implement openCypher range() function
 */
Datum age_range(PG_FUNCTION_ARGS)
{
    Datum *args = NULL;
    bool *nulls = NULL;
    Oid *types = NULL;
    int nargs;
    int64 start_idx = 0;
    int64 end_idx = 0;
    /* step defaults to 1 */
    int64 step = 1;
    bool is_agnull = false;
    gtype_in_state agis_result;
    int64 i = 0;

    /* get the arguments */
    nargs = extract_variadic_args(fcinfo, 0, false, &args, &types, &nulls);

    /* throw an error if the number of args is not the expected number */
    if (nargs != 2 && nargs != 3)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(): invalid number of input parameters")));

    /* check for NULL start and end input */
    if (nulls[0] || nulls[1])
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(): neither start or end can be NULL")));

    /* get the start index */
    start_idx = get_int64_from_int_datums(args[0], types[0], "range", &is_agnull);
    if (is_agnull)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(): start cannot be NULL")));

    /* get the end index */
    end_idx = get_int64_from_int_datums(args[1], types[1], "range", &is_agnull);
    if (is_agnull)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(): end cannot be NULL")));

    /* get the step */
    if (nargs == 3 && !nulls[2]) {
        step = get_int64_from_int_datums(args[2], types[2], "range", &is_agnull);
        if (is_agnull)
            step = 1;
    }

    if (step == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("range(): step cannot be zero")));

    MemSet(&agis_result, 0, sizeof(gtype_in_state));

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    for (i = start_idx; (step > 0 && i <= end_idx) || (step < 0 && i >= end_idx); i += step) {
        gtype_value agtv;

        agtv.type = AGTV_INTEGER;
        agtv.val.int_value = i;

        agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_ELEM, &agtv);
    }

    agis_result.res = push_gtype_value(&agis_result.parse_state, WAGT_END_ARRAY, NULL);

    PG_RETURN_POINTER(gtype_value_to_gtype(agis_result.res));
}

PG_FUNCTION_INFO_V1(age_unnest);
/*
 * Function to convert the Array type of Agtype into each row. It is used for
 * Cypher `UNWIND` clause, but considering the situation in which the user can
 * directly use this function in vanilla PGSQL, put a second parameter related
 * to this.
 */
Datum age_unnest(PG_FUNCTION_ARGS)
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

    /* it's a simple type, so don't use get_call_result_type() */
    tupdesc = rsi->expectedDesc;

    old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

    ret_tdesc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(ret_tdesc);
    tuple_store = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, work_mem);

    MemoryContextSwitchTo(old_cxt);

    tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "age_unnest temporary cxt", ALLOCSET_DEFAULT_SIZES);

    it = gtype_iterator_init(&gtype_arg->root);

    while ((r = gtype_iterator_next(&it, &v, skipNested)) != WAGT_DONE)
    {
        skipNested = true;

        if (r == WAGT_ELEM)
        {
            HeapTuple tuple;
            Datum values[1];
            bool nulls[1] = {false};
            gtype *val = gtype_value_to_gtype(&v);

            if (block_types && (v.type == AGTV_VERTEX || v.type == AGTV_EDGE || v.type == AGTV_PATH))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("UNWIND clause does not support gtype %s",
                                       gtype_value_type_to_string(v.type))));

            /* use the tmp context so we can clean up after each tuple is done */
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
