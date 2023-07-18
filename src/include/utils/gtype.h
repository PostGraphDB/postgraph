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

/*
 * Declarations for gtype data type support.
 */

#ifndef AG_GTYPE_H
#define AG_GTYPE_H

#include "access/htup_details.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/syscache.h"

#include "catalog/ag_namespace.h"
#include "catalog/pg_type.h"
#include "utils/graphid.h"

/* Tokens used when sequentially processing an gtype value */
typedef enum
{
    WAGT_DONE,
    WAGT_KEY,
    WAGT_VALUE,
    WAGT_ELEM,
    WAGT_BEGIN_ARRAY,
    WAGT_END_ARRAY,
    WAGT_BEGIN_OBJECT,
    WAGT_END_OBJECT
} gtype_iterator_token;

#define GTYPE_ITERATOR_TOKEN_IS_HASHABLE(x) \
    (x > WAGT_DONE && x < WAGT_BEGIN_ARRAY)

/* Strategy numbers for GIN index opclasses */
#define GTYPE_CONTAINS_STRATEGY_NUMBER    7
#define GTYPE_EXISTS_STRATEGY_NUMBER      9
#define GTYPE_EXISTS_ANY_STRATEGY_NUMBER 10
#define GTYPE_EXISTS_ALL_STRATEGY_NUMBER 11

/*
 * In the standard gtype_ops GIN opclass for gtype, we choose to index both
 * keys and values.  The storage format is text.  The first byte of the text
 * string distinguishes whether this is a key (always a string), null value,
 * boolean value, numeric value, or string value.  However, array elements
 * that are strings are marked as though they were keys; this imprecision
 * supports the definition of the "exists" operator, which treats array
 * elements like keys.  The remainder of the text string is empty for a null
 * value, "t" or "f" for a boolean value, a normalized print representation of
 * a numeric value, or the text of a string value.  However, if the length of
 * this text representation would exceed AGT_GIN_MAX_LENGTH bytes, we instead
 * hash the text representation and store an 8-hex-digit representation of the
 * uint32 hash value, marking the prefix byte with an additional bit to
 * distinguish that this has happened.  Hashing long strings saves space and
 * ensures that we won't overrun the maximum entry length for a GIN index.
 * (But AGT_GIN_MAX_LENGTH is quite a bit shorter than GIN's limit.  It's
 * chosen to ensure that the on-disk text datum will have a short varlena
 * header.) Note that when any hashed item appears in a query, we must recheck
 * index matches against the heap tuple; currently, this costs nothing because
 * we must always recheck for other reasons.
 */
#define AGT_GIN_FLAG_KEY    0x01 // key (or string array element)
#define AGT_GIN_FLAG_NULL   0x02 // null value
#define AGT_GIN_FLAG_BOOL   0x03 // boolean value
#define AGT_GIN_FLAG_NUM    0x04 // numeric value
#define AGT_GIN_FLAG_STR    0x05 // string value (if not an array element)
#define AGT_GIN_FLAG_HASHED 0x10 // OR'd into flag if value was hashed
#define AGT_GIN_MAX_LENGTH   125 // max length of text part before hashing

/* Convenience macros */
#define DATUM_GET_GTYPE_P(d) ((gtype *)PG_DETOAST_DATUM(d))
#define GTYPE_P_GET_DATUM(p) PointerGetDatum(p)
#define AG_GET_ARG_GTYPE_P(x) DATUM_GET_GTYPE_P(PG_GETARG_DATUM(x))
#define AG_RETURN_GTYPE_P(x) PG_RETURN_POINTER(x)

typedef struct gtype_pair gtype_pair;
typedef struct gtype_value gtype_value;

/*
 * gtypes are varlena objects, so must meet the varlena convention that the
 * first int32 of the object contains the total object size in bytes.  Be sure
 * to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * gtype is the on-disk representation, in contrast to the in-memory
 * gtype_value representation.  Often, gtype_values are just shims through
 * which a gtype buffer is accessed, but they can also be deep copied and
 * passed around.
 *
 * gtype is a tree structure. Each node in the tree consists of an agtentry
 * header and a variable-length content (possibly of zero size).  The agtentry
 * header indicates what kind of a node it is, e.g. a string or an array,
 * and provides the length of its variable-length portion.
 *
 * The agtentry and the content of a node are not stored physically together.
 * Instead, the container array or object has an array that holds the agtentrys
 * of all the child nodes, followed by their variable-length portions.
 *
 * The root node is an exception; it has no parent array or object that could
 * hold its agtentry. Hence, no agtentry header is stored for the root node.
 * It is implicitly known that the root node must be an array or an object,
 * so we can get away without the type indicator as long as we can distinguish
 * the two.  For that purpose, both an array and an object begin with a uint32
 * header field, which contains an AGT_FOBJECT or AGT_FARRAY flag.  When a
 * naked scalar value needs to be stored as an gtype value, what we actually
 * store is an array with one element, with the flags in the array's header
 * field set to AGT_FSCALAR | AGT_FARRAY.
 *
 * Overall, the gtype struct requires 4-bytes alignment. Within the struct,
 * the variable-length portion of some node types is aligned to a 4-byte
 * boundary, while others are not. When alignment is needed, the padding is
 * in the beginning of the node that requires it. For example, if a numeric
 * node is stored after a string node, so that the numeric node begins at
 * offset 3, the variable-length portion of the numeric node will begin with
 * one padding byte so that the actual numeric data is 4-byte aligned.
 */

/*
 * agtentry format.
 *
 * The least significant 28 bits store either the data length of the entry,
 * or its end+1 offset from the start of the variable-length portion of the
 * containing object.  The next three bits store the type of the entry, and
 * the high-order bit tells whether the least significant bits store a length
 * or an offset.
 *
 * The reason for the offset-or-length complication is to compromise between
 * access speed and data compressibility.  In the initial design each agtentry
 * always stored an offset, but this resulted in agtentry arrays with horrible
 * compressibility properties, so that TOAST compression of an gtype did not
 * work well.  Storing only lengths would greatly improve compressibility,
 * but it makes random access into large arrays expensive (O(N) not O(1)).
 * So what we do is store an offset in every AGT_OFFSET_STRIDE'th agtentry and
 * a length in the rest.  This results in reasonably compressible data (as
 * long as the stride isn't too small).  We may have to examine as many as
 * AGT_OFFSET_STRIDE agtentrys in order to find out the offset or length of any
 * given item, but that's still O(1) no matter how large the container is.
 *
 * We could avoid eating a flag bit for this purpose if we were to store
 * the stride in the container header, or if we were willing to treat the
 * stride as an unchangeable constant.  Neither of those options is very
 * attractive though.
 */
typedef uint32 agtentry;

#define AGTENTRY_OFFLENMASK 0x0FFF
#define AGTENTRY_TYPEMASK   0x7000
#define AGTENTRY_HAS_OFF    0x8000

/* values stored in the type bits */
#define AGTENTRY_IS_STRING     0x0000
#define AGTENTRY_IS_NUMERIC    0x1000
#define AGTENTRY_IS_BOOL_FALSE 0x2000
#define AGTENTRY_IS_BOOL_TRUE  0x3000
#define AGTENTRY_IS_NULL       0x4000
#define AGTENTRY_IS_CONTAINER  0x5000 /* array or object */
#define AGTENTRY_IS_GTYPE     0x7000 /* our type designator */

/* Access macros.  Note possible multiple evaluations */
#define AGTE_OFFLENFLD(agte_) \
    ((agte_)&AGTENTRY_OFFLENMASK)
#define AGTE_HAS_OFF(agte_) \
    (((agte_)&AGTENTRY_HAS_OFF) != 0)
#define AGTE_IS_STRING(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_STRING)
#define AGTE_IS_NUMERIC(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_NUMERIC)
#define AGTE_IS_CONTAINER(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_CONTAINER)
#define AGTE_IS_NULL(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_NULL)
#define AGTE_IS_BOOL_TRUE(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_BOOL_TRUE)
#define AGTE_IS_BOOL_FALSE(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_BOOL_FALSE)
#define AGTE_IS_BOOL(agte_) \
    (AGTE_IS_BOOL_TRUE(agte_) || AGTE_IS_BOOL_FALSE(agte_))
#define AGTE_IS_GTYPE(agte_) \
    (((agte_)&AGTENTRY_TYPEMASK) == AGTENTRY_IS_GTYPE)

/* Macro for advancing an offset variable to the next agtentry */
#define AGTE_ADVANCE_OFFSET(offset, agte) \
    do \
    { \
        agtentry agte_ = (agte); \
        if (AGTE_HAS_OFF(agte_)) \
            (offset) = AGTE_OFFLENFLD(agte_); \
        else \
            (offset) += AGTE_OFFLENFLD(agte_); \
    } while (0)

/*
 * We store an offset, not a length, every AGT_OFFSET_STRIDE children.
 * Caution: this macro should only be referenced when creating an gtype
 * value.  When examining an existing value, pay attention to the HAS_OFF
 * bits instead.  This allows changes in the offset-placement heuristic
 * without breaking on-disk compatibility.
 */
#define AGT_OFFSET_STRIDE 32

/*
 * An gtype array or object node, within an gtype Datum.
 *
 * An array has one child for each element, stored in array order.
 *
 * An object has two children for each key/value pair.  The keys all appear
 * first, in key sort order; then the values appear, in an order matching the
 * key order.  This arrangement keeps the keys compact in memory, making a
 * search for a particular key more cache-friendly.
 */
typedef struct gtype_container
{
    uint32 header; /* number of elements or key/value pairs, and flags */
    agtentry children[FLEXIBLE_ARRAY_MEMBER];

    /* the data for each child node follows. */
} gtype_container;

/* flags for the header-field in gtype_container*/
#define AGT_CMASK   0x0FFF /* mask for count field */
#define AGT_FSCALAR 0x1000 /* flag bits */
#define AGT_FOBJECT 0x2000
#define AGT_FARRAY  0x4000
#define AGT_FBINARY 0x8000 /* our binary objects */

/*
 * AGT_FBINARY utilizes the AGTV_BINARY mechanism,
 * it is not necessarily an gtype serialized (binary) value. We are just using
 * that mechanism to pass blobs of data more quickly between components. In the
 * case of the path from the VLE routine, the blob is a graphid array where the
 * first element contains the header embedded in it. This way it is just cast to
 * the graphid array to be used after verifying that it is an AGT_FBINARY and an
 * AGT_FBINARY_TYPE_VLE_PATH.
 */

/*
 * Flags for the gtype_container type AGT_FBINARY are in the AGT_CMASK (count)
 * field. We put the flags here as this is a strictly AGTV_BINARY blob of data
 * and count is irrelevant because there is only one. The additional flags allow
 * for differing types of user defined binary blobs. To be consistent and clear,
 * we create binary specific masks, flags, and macros.
 */
#define AGT_FBINARY_MASK          0x0FFF /* mask for binary flags */
#define GTYPE_FBINARY_CONTAINER_TYPE(agtc) \
    ((agtc)->header &AGT_FBINARY_MASK)
#define AGT_ROOT_DATA_FBINARY(agtp_) \
    VARDATA(agtp_);
#define AGT_FBINARY_TYPE_VLE_PATH 0x0001

/* convenience macros for accessing an gtype_container struct */
#define GTYPE_CONTAINER_SIZE(agtc)       ((agtc)->header & AGT_CMASK)
#define GTYPE_CONTAINER_IS_SCALAR(agtc) (((agtc)->header & AGT_FSCALAR) != 0)
#define GTYPE_CONTAINER_IS_OBJECT(agtc) (((agtc)->header & AGT_FOBJECT) != 0)
#define GTYPE_CONTAINER_IS_ARRAY(agtc)  (((agtc)->header & AGT_FARRAY)  != 0)
#define GTYPE_CONTAINER_IS_BINARY(agtc) (((agtc)->header & AGT_FBINARY) != 0)

// The top-level on-disk format for an gtype datum.
typedef struct
{
    int32 vl_len_; // varlena header (do not touch directly!)
    gtype_container root;
} gtype;

// convenience macros for accessing the root container in an gtype datum
#define AGT_ROOT_COUNT(agtp_) (*(uint32 *)VARDATA(agtp_) & AGT_CMASK)
#define AGT_ROOT_IS_SCALAR(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & AGT_FSCALAR) != 0)
#define AGT_ROOT_IS_OBJECT(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & AGT_FOBJECT) != 0)
#define AGT_ROOT_IS_ARRAY(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & AGT_FARRAY) != 0)
#define AGT_ROOT_IS_BINARY(agtp_) \
    ((*(uint32 *)VARDATA(agtp_) & AGT_FBINARY) != 0)
#define AGT_ROOT_BINARY_FLAGS(agtp_) \
    (*(uint32 *)VARDATA(agtp_) & AGT_FBINARY_MASK)

// values for the GTYPE header field to denote the stored data type
#define AGT_HEADER_INTEGER 0x0000
#define AGT_HEADER_FLOAT   0x0001
#define AGT_HEADER_VERTEX  0x0002
#define AGT_HEADER_EDGE    0x0003
#define AGT_HEADER_PATH    0x0004
#define AGT_HEADER_PARTIAL_PATH    0x0005
#define AGT_HEADER_TIMESTAMP 0x0006

#define AGT_IS_VERTEX(agt) \
    (AGT_ROOT_IS_SCALAR(agt) &&\
     AGTE_IS_GTYPE((agt)->root.children[0]) &&\
     (((agt)->root.children[1] & AGT_HEADER_VERTEX) == AGT_HEADER_VERTEX))

#define AGT_IS_EDGE(agt) \
    (AGT_ROOT_IS_SCALAR(agt) &&\
     AGTE_IS_GTYPE((agt)->root.children[0]) &&\
     (((agt)->root.children[1] & AGT_HEADER_EDGE) == AGT_HEADER_EDGE))

#define AGT_IS_INTEGER(agte_) \
    (((agte_) == AGT_HEADER_INTEGER))

#define AGT_IS_FLOAT(agte_) \
    (((agte_) == AGT_HEADER_FLOAT))

#define AGT_IS_TIMESTAMP(agt) \
    (AGTE_IS_GTYPE(agt->root.children[0]) && agt->root.children[1] == AGT_HEADER_TIMESTAMP)

#define AGT_IS_PATH(agt) \
    (AGTE_IS_GTYPE(agt->root.children[0]) && agt->root.children[1] == AGT_HEADER_PATH)

#define AGT_IS_PARTIAL_PATH(agt) \
    (AGTE_IS_GTYPE(agt->root.children[0]) && agt->root.children[1] == AGT_HEADER_PARTIAL_PATH)

enum gtype_value_type
{
    /* Scalar types */
    AGTV_NULL = 0x0,
    AGTV_STRING,
    AGTV_NUMERIC,
    AGTV_INTEGER,
    AGTV_FLOAT,
    AGTV_BOOL,
    AGTV_TIMESTAMP,
    AGTV_VERTEX,
    AGTV_EDGE,
    AGTV_PATH,
    AGTV_PARTIAL_PATH,
    /* Composite types */
    AGTV_ARRAY = 0x20,
    AGTV_OBJECT,
    /* Binary (i.e. struct gtype) AGTV_ARRAY/AGTV_OBJECT */
    AGTV_BINARY
};

/*
 * gtype_value: In-memory representation of gtype.  This is a convenient
 * deserialized representation, that can easily support using the "val"
 * union across underlying types during manipulation.  The gtype on-disk
 * representation has various alignment considerations.
 */
struct gtype_value
{
    enum gtype_value_type type;
    union {
        int64 int_value; // 8-byte Integer
        float8 float_value; // 8-byte Float
        Numeric numeric;
        bool boolean;
        struct { int len; char *val; /* Not necessarily null-terminated */ } string; // String primitive type
        struct { int num_elems; gtype_value *elems; bool raw_scalar; } array;       // Array container type
        struct { int num_pairs; gtype_pair *pairs; } object;                        // Associative container type
        struct { int len; gtype_container *data; } binary;                          // Array or object, in on-disk format
    } val;
};

#define IS_A_GTYPE_SCALAR(gtype_val) \
    ((gtype_val)->type >= AGTV_NULL && (gtype_val)->type < AGTV_ARRAY)

/*
 * Key/value pair within an Object.
 *
 * Pairs with duplicate keys are de-duplicated.  We store the originally
 * observed pair ordering for the purpose of removing duplicates in a
 * well-defined way (which is "last observed wins").
 */
struct gtype_pair
{
    gtype_value key; /* Must be a AGTV_STRING */
    gtype_value value; /* May be of any type */
    uint32 order; /* Pair's index in original sequence */
};

/* Conversion state used when parsing gtype from text, or for type coercion */
typedef struct gtype_parse_state
{
    gtype_value cont_val;
    Size size;
    struct gtype_parse_state *next;
    /*
     * This holds the last append_value scalar copy or the last append_element
     * scalar copy - it can only be one of the two. It is needed because when
     * an object or list is built, the upper level object or list will get a
     * copy of the result value on close. Our routines modify the value after
     * close and need this to update that value if necessary. Which is the
     * case for some casts.
     */
    gtype_value *last_updated_value;
} gtype_parse_state;

/*
 * gtype_iterator holds details of the type for each iteration. It also stores
 * an gtype varlena buffer, which can be directly accessed in some contexts.
 */
typedef enum
{
    AGTI_ARRAY_START,
    AGTI_ARRAY_ELEM,
    AGTI_OBJECT_START,
    AGTI_OBJECT_KEY,
    AGTI_OBJECT_VALUE
} agt_iterator_state;

typedef struct gtype_iterator
{
    gtype_container *container; // Container being iterated
    uint32 num_elems;            // Number of elements in children array (will be num_pairs for objects)
    bool is_scalar;              // Pseudo-array scalar value?
    agtentry *children;          // agtentrys for child nodes
    char *data_proper;           // Data proper. This points to the beginning of the variable-length data
    int curr_index;              // Current item in buffer (up to num_elems)
    uint32 curr_data_offset;     // Data offset corresponding to current item
    /*
     * If the container is an object, we want to return keys and values
     * alternately; so curr_data_offset points to the current key, and
     * curr_value_offset points to the current value.
     */
    uint32 curr_value_offset;
    agt_iterator_state state;    // Private state
    struct gtype_iterator *parent;
} gtype_iterator;

/* gtype parse state */
typedef struct gtype_in_state {
    gtype_parse_state *parse_state;
    gtype_value *res;
} gtype_in_state;

/* Support functions */
int reserve_from_buffer(StringInfo buffer, int len);
short pad_buffer_to_int(StringInfo buffer);
uint32 get_gtype_offset(const gtype_container *agtc, int index);
uint32 get_gtype_length(const gtype_container *agtc, int index);
int compare_gtype_containers_orderability(gtype_container *a, gtype_container *b);
gtype_value *find_gtype_value_from_container(gtype_container *container, uint32 flags, const gtype_value *key);
gtype_value *get_ith_gtype_value_from_container(gtype_container *container, uint32 i);
gtype_value *push_gtype_value(gtype_parse_state **pstate, gtype_iterator_token seq, gtype_value *agtval);
gtype_iterator *gtype_iterator_init(gtype_container *container);
gtype_iterator_token gtype_iterator_next(gtype_iterator **it, gtype_value *val, bool skip_nested);
gtype *gtype_value_to_gtype(gtype_value *val);
bool gtype_deep_contains(gtype_iterator **val, gtype_iterator **m_contained);
void gtype_hash_scalar_value(const gtype_value *scalar_val, uint32 *hash);
void gtype_hash_scalar_value_extended(const gtype_value *scalar_val, uint64 *hash, uint64 seed);
void convert_extended_array(StringInfo buffer, agtentry *pheader, gtype_value *val);
void convert_extended_object(StringInfo buffer, agtentry *pheader, gtype_value *val);
Datum get_numeric_datum_from_gtype_value(gtype_value *agtv);
bool is_numeric_result(gtype_value *lhs, gtype_value *rhs);

#define GET_GTYPE_VALUE_OBJECT_VALUE(agtv_object, search_key) \
        get_gtype_value_object_value(agtv_object, search_key, sizeof(search_key) - 1)

gtype_value *get_gtype_value_object_value(const gtype_value *agtv_object, char *search_key, int search_key_length);
char *gtype_to_cstring(StringInfo out, gtype_container *in, int estimated_len);
char *gtype_to_cstring_indent(StringInfo out, gtype_container *in, int estimated_len);

Datum gtype_from_cstring(char *str, int len);

size_t check_string_length(size_t len);
Datum integer_to_gtype(int64 i);
Datum float_to_gtype(float8 f);
Datum string_to_gtype(char *s);
Datum boolean_to_gtype(bool b);
void uniqueify_gtype_object(gtype_value *object);
char *gtype_value_type_to_string(enum gtype_value_type type);
bool is_decimal_needed(char *numstr);
int compare_gtype_scalar_values(gtype_value *a, gtype_value *b);
gtype_value *alter_property_value(gtype *properties, char *var_name, gtype *new_v, bool remove_property);
gtype *get_one_gtype_from_variadic_args(FunctionCallInfo fcinfo, int variadic_offset, int expected_nargs);
Datum make_vertex(Datum id, Datum label, Datum properties);
Datum make_edge(Datum id, Datum startid, Datum endid, Datum label, Datum properties);
Datum make_path(List *path);
Datum column_get_datum(TupleDesc tupdesc, HeapTuple tuple, int column, const char *attname, Oid typid, bool isnull);
gtype_value *gtype_value_build_vertex(graphid id, char *label, Datum properties);
gtype_value *gtype_value_build_edge(graphid id, char *label, graphid end_id, graphid start_id, Datum properties);
gtype_value *get_gtype_value(char *funcname, gtype *agt_arg, enum gtype_value_type type, bool error);
bool is_gtype_null(gtype *agt_arg);
gtype_value *string_to_gtype_value(char *s);
gtype_value *integer_to_gtype_value(int64 int_value);
void add_gtype(Datum val, bool is_null, gtype_in_state *result, Oid val_type, bool key_scalar);

Datum gtype_to_float8(PG_FUNCTION_ARGS);

#define GTYPEOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("gtype"), ObjectIdGetDatum(postgraph_namespace_id())))

#define GTYPEARRAYOID \
    (GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("_gtype"), ObjectIdGetDatum(postgraph_namespace_id())))

Datum gtype_object_field_impl(FunctionCallInfo fcinfo, gtype *gtype_in, char *key, int key_len, bool as_text);

void gtype_put_escaped_value(StringInfo out, gtype_value *scalar_val);

#endif
