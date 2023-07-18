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
 */

#include "utils/gtype_ext.h"
#include "utils/gtype.h"
#include "utils/graphid.h"

/* define the type and size of the agt_header */
#define AGT_HEADER_TYPE uint32
#define AGT_HEADER_SIZE sizeof(AGT_HEADER_TYPE)

static void ag_deserialize_composite(char *base, enum gtype_value_type type,
                                     gtype_value *result);

static short ag_serialize_header(StringInfo buffer, uint32 type)
{
    short padlen;
    int offset;

    padlen = pad_buffer_to_int(buffer);
    offset = reserve_from_buffer(buffer, AGT_HEADER_SIZE);
    *((AGT_HEADER_TYPE *)(buffer->data + offset)) = type;

    return padlen;
}

/*
 * Function serializes the data into the buffer provided.
 * Returns false if the type is not defined. Otherwise, true.
 */
bool ag_serialize_extended_type(StringInfo buffer, agtentry *agtentry,
                                gtype_value *scalar_val)
{
    short padlen;
    int numlen;
    int offset;

    switch (scalar_val->type)
    {
    case AGTV_INTEGER:
        padlen = ag_serialize_header(buffer, AGT_HEADER_INTEGER);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;

    case AGTV_FLOAT:
        padlen = ag_serialize_header(buffer, AGT_HEADER_FLOAT);

        /* copy in the float_value data */
        numlen = sizeof(scalar_val->val.float_value);
        offset = reserve_from_buffer(buffer, numlen);
        *((float8 *)(buffer->data + offset)) = scalar_val->val.float_value;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_TIMESTAMP:
        padlen = ag_serialize_header(buffer, AGT_HEADER_TIMESTAMP);

        offset = reserve_from_buffer(buffer, sizeof(int64));
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

	*agtentry = AGTENTRY_IS_GTYPE | (sizeof(int64) + AGT_HEADER_SIZE);
        break;
    case AGTV_VERTEX:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_VERTEX);
        convert_extended_object(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_GTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    case AGTV_EDGE:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_EDGE);
        convert_extended_object(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_GTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    case AGTV_PATH:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_PATH);
        convert_extended_array(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_GTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    case AGTV_PARTIAL_PATH:
    {
        uint32 object_ae = 0;

        padlen = ag_serialize_header(buffer, AGT_HEADER_PARTIAL_PATH);
        convert_extended_array(buffer, &object_ae, scalar_val);

        /*
         * Make sure that the end of the buffer is padded to the next offset and
         * add this padding to the length of the buffer used. This ensures that
         * everything stays aligned and eliminates errors caused by compounded
         * offsets in the deserialization routines.
         */
        object_ae += pad_buffer_to_int(buffer);

        *agtentry = AGTENTRY_IS_GTYPE |
                    ((AGTENTRY_OFFLENMASK & (int)object_ae) + AGT_HEADER_SIZE);
        break;
    }

    default:
        return false;
    }
    return true;
}

/*
 * Function deserializes the data from the buffer pointed to by base_addr.
 * NOTE: This function writes to the error log and exits for any UNKNOWN
 * AGT_HEADER type.
 */
void ag_deserialize_extended_type(char *base_addr, uint32 offset, gtype_value *result) {
    char *base = base_addr + INTALIGN(offset);
    AGT_HEADER_TYPE agt_header = *((AGT_HEADER_TYPE *)base);

    switch (agt_header)
    {
    case AGT_HEADER_INTEGER:
        result->type = AGTV_INTEGER;
        result->val.int_value = *((int64 *)(base + AGT_HEADER_SIZE));
        break;

    case AGT_HEADER_FLOAT:
        result->type = AGTV_FLOAT;
        result->val.float_value = *((float8 *)(base + AGT_HEADER_SIZE));
        break;

    case AGT_HEADER_TIMESTAMP:
        result->type = AGTV_TIMESTAMP;
        result->val.int_value = *((int64 *)(base + AGT_HEADER_SIZE));
        break;

    case AGT_HEADER_VERTEX:
        ag_deserialize_composite(base, AGTV_VERTEX, result);
        break;

    case AGT_HEADER_EDGE:
        ag_deserialize_composite(base, AGTV_EDGE, result);
        break;

    case AGT_HEADER_PATH:
        ag_deserialize_composite(base, AGTV_PATH, result);
        break;

    case AGT_HEADER_PARTIAL_PATH:
        ag_deserialize_composite(base, AGTV_PARTIAL_PATH, result);
        break;


    default:
        elog(ERROR, "Invalid AGT header value.");
    }
}

/*
 * Deserializes a composite type.
 */
static void ag_deserialize_composite(char *base, enum gtype_value_type type, gtype_value *result) {
    gtype_iterator *it = NULL;
    gtype_iterator_token tok;
    gtype_parse_state *parse_state = NULL;
    gtype_value *r = NULL;
    gtype_value *parsed_gtype_value = NULL;
    //offset container by the extended type header
    char *container_base = base + AGT_HEADER_SIZE;

    r = palloc(sizeof(gtype_value));

    it = gtype_iterator_init((gtype_container *)container_base);
    while ((tok = gtype_iterator_next(&it, r, true)) != WAGT_DONE)
    {
        parsed_gtype_value = push_gtype_value(
            &parse_state, tok, tok < WAGT_BEGIN_ARRAY ? r : NULL);
    }

    result->type = type;
    result->val = parsed_gtype_value->val;
}
