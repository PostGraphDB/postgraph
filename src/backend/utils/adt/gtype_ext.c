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
    case AGTV_TIMESTAMPTZ:
        padlen = ag_serialize_header(buffer, AGT_HEADER_TIMESTAMPTZ);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_DATE:
        padlen = ag_serialize_header(buffer, AGT_HEADER_DATE);

        /* copy in the date data */
        numlen = sizeof(int32);
        offset = reserve_from_buffer(buffer, numlen);
        *((int32 *)(buffer->data + offset)) = scalar_val->val.date;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
	break;
    case AGTV_TIME:
        padlen = ag_serialize_header(buffer, AGT_HEADER_TIME);
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_TIMETZ:
        padlen = ag_serialize_header(buffer, AGT_HEADER_TIMETZ);

        /* copy in the timetz data */
        numlen = sizeof(TimeTzADT);
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeADT *)(buffer->data + offset)) = scalar_val->val.timetz.time;
        *((int32 *)(buffer->data + offset + sizeof(TimeADT))) = scalar_val->val.timetz.zone;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_INTERVAL:
        padlen = ag_serialize_header(buffer, AGT_HEADER_INTERVAL);

        numlen = sizeof(TimeOffset) + (2 * sizeof(int32));
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeOffset *)(buffer->data + offset)) = scalar_val->val.interval.time;

        *((int32 *)(buffer->data + offset + sizeof(TimeOffset))) = scalar_val->val.interval.day;
        *((int32 *)(buffer->data + offset + sizeof(TimeOffset) + sizeof(int32))) = scalar_val->val.interval.month;

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_INET:
        padlen = ag_serialize_header(buffer, AGT_HEADER_INET);

	numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
	memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
	break;
    case AGTV_CIDR:
        padlen = ag_serialize_header(buffer, AGT_HEADER_CIDR);

        numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;
    case AGTV_MAC:
        padlen = ag_serialize_header(buffer, AGT_HEADER_MAC);

        numlen = sizeof(char) * 6;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.mac, numlen);

        *agtentry = AGTENTRY_IS_GTYPE | (padlen + numlen + AGT_HEADER_SIZE);
        break;

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
    case AGT_HEADER_TIMESTAMPTZ:
        result->type = AGTV_TIMESTAMPTZ;
        result->val.int_value = *((int64 *)(base + AGT_HEADER_SIZE));
        break;
    case AGT_HEADER_DATE:
        result->type = AGTV_DATE;
        result->val.date = *((int32 *)(base + AGT_HEADER_SIZE));
        break;
    case AGT_HEADER_TIME:
        result->type = AGTV_TIME;
        result->val.int_value = *((int64 *)(base + AGT_HEADER_SIZE));
        break;
    case AGT_HEADER_TIMETZ:
        result->type = AGTV_TIMETZ;
        result->val.timetz.time = *((TimeADT*)(base + AGT_HEADER_SIZE));
        result->val.timetz.zone = *((int32*)(base + AGT_HEADER_SIZE + sizeof(TimeADT)));
        break;
    case AGT_HEADER_INTERVAL:
        result->type = AGTV_INTERVAL;
        result->val.interval.time =  *((TimeOffset *)(base + AGT_HEADER_SIZE));
        result->val.interval.day =  *((int32 *)(base + AGT_HEADER_SIZE + sizeof(TimeOffset)));
        result->val.interval.month =  *((int32 *)(base + AGT_HEADER_SIZE + sizeof(TimeOffset) + sizeof(int32)));
        break;
    case AGT_HEADER_INET:
	result->type = AGTV_INET;
	memcpy(&result->val.inet, base + AGT_HEADER_SIZE, sizeof(char) * 22);
        break;
    case AGT_HEADER_CIDR:
        result->type = AGTV_CIDR;
        memcpy(&result->val.inet, base + AGT_HEADER_SIZE, sizeof(char) * 22);
        break;
    case AGT_HEADER_MAC:
        result->type = AGTV_MAC;
        memcpy(&result->val.mac, base + AGT_HEADER_SIZE, sizeof(char) * 6);
        break;

    default:
        elog(ERROR, "Invalid AGT header value.");
    }
}
