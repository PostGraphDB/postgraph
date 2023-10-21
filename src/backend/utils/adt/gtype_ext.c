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

#include "liblwgeom/liblwgeom.h"

/* define the type and size of the agt_header */
#define GT_HEADER_TYPE uint32
#define GT_HEADER_SIZE sizeof(GT_HEADER_TYPE)

static short ag_serialize_header(StringInfo buffer, uint32 type)
{
    short padlen;
    int offset;

    padlen = pad_buffer_to_int(buffer);
    offset = reserve_from_buffer(buffer, GT_HEADER_SIZE);
    *((GT_HEADER_TYPE *)(buffer->data + offset)) = type;

    return padlen;
}

/*
 * Function serializes the data into the buffer provided.
 * Returns false if the type is not defined. Otherwise, true.
 */
bool ag_serialize_extended_type(StringInfo buffer, gtentry *gtentry,
                                gtype_value *scalar_val)
{
    short padlen;
    int numlen;
    int offset;

    switch (scalar_val->type)
    {
    case AGTV_INTEGER:
        padlen = ag_serialize_header(buffer, GT_HEADER_INTEGER);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;

    case AGTV_FLOAT:
        padlen = ag_serialize_header(buffer, GT_HEADER_FLOAT);

        /* copy in the float_value data */
        numlen = sizeof(scalar_val->val.float_value);
        offset = reserve_from_buffer(buffer, numlen);
        *((float8 *)(buffer->data + offset)) = scalar_val->val.float_value;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_TIMESTAMP:
        padlen = ag_serialize_header(buffer, GT_HEADER_TIMESTAMP);

        offset = reserve_from_buffer(buffer, sizeof(int64));
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

	*gtentry = GTENTRY_IS_GTYPE | (sizeof(int64) + GT_HEADER_SIZE);
        break;
    case AGTV_TIMESTAMPTZ:
        padlen = ag_serialize_header(buffer, GT_HEADER_TIMESTAMPTZ);

        /* copy in the int_value data */
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_DATE:
        padlen = ag_serialize_header(buffer, GT_HEADER_DATE);

        /* copy in the date data */
        numlen = sizeof(int32);
        offset = reserve_from_buffer(buffer, numlen);
        *((int32 *)(buffer->data + offset)) = scalar_val->val.date;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
	break;
    case AGTV_TIME:
        padlen = ag_serialize_header(buffer, GT_HEADER_TIME);
        numlen = sizeof(int64);
        offset = reserve_from_buffer(buffer, numlen);
        *((int64 *)(buffer->data + offset)) = scalar_val->val.int_value;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_TIMETZ:
        padlen = ag_serialize_header(buffer, GT_HEADER_TIMETZ);

        /* copy in the timetz data */
        numlen = sizeof(TimeTzADT);
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeADT *)(buffer->data + offset)) = scalar_val->val.timetz.time;
        *((int32 *)(buffer->data + offset + sizeof(TimeADT))) = scalar_val->val.timetz.zone;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_INTERVAL:
        padlen = ag_serialize_header(buffer, GT_HEADER_INTERVAL);

        numlen = sizeof(TimeOffset) + (2 * sizeof(int32));
        offset = reserve_from_buffer(buffer, numlen);
        *((TimeOffset *)(buffer->data + offset)) = scalar_val->val.interval.time;

        *((int32 *)(buffer->data + offset + sizeof(TimeOffset))) = scalar_val->val.interval.day;
        *((int32 *)(buffer->data + offset + sizeof(TimeOffset) + sizeof(int32))) = scalar_val->val.interval.month;

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_INET:
        padlen = ag_serialize_header(buffer, GT_HEADER_INET);

	numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
	memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
	break;
    case AGTV_CIDR:
        padlen = ag_serialize_header(buffer, GT_HEADER_CIDR);

        numlen = sizeof(char) * 22;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.inet, numlen);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_MAC:
        padlen = ag_serialize_header(buffer, GT_HEADER_MAC);

        numlen = sizeof(char) * 6;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.mac, numlen);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_MAC8:
        padlen = ag_serialize_header(buffer, GT_HEADER_MAC8);

        numlen = sizeof(char) * 8;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.mac, numlen);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
   case AGTV_BOX2D:
        padlen = ag_serialize_header(buffer, GT_HEADER_BOX2D);

        numlen = sizeof(GBOX);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.gbox, sizeof(GBOX));

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;

    case AGTV_BOX3D:
        padlen = ag_serialize_header(buffer, GT_HEADER_BOX3D);

        numlen = sizeof(BOX3D);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.gbox, sizeof(BOX3D));

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;

    case AGTV_SPHEROID:
        padlen = ag_serialize_header(buffer, GT_HEADER_SPHEROID);

        numlen = sizeof(SPHEROID);
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, &scalar_val->val.spheroid, sizeof(SPHEROID));

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;

    case AGTV_GSERIALIZED:
        padlen = ag_serialize_header(buffer, GT_HEADER_GSERIALIZED);

        numlen = ((GSERIALIZED *)scalar_val->val.gserialized)->size / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.gserialized, ((GSERIALIZED *)scalar_val->val.gserialized)->size / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_BYTEA:
        padlen = ag_serialize_header(buffer, GT_HEADER_BYTEA);

        numlen = *((int *)(scalar_val->val.bytea)->vl_len_) / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.bytea, *((int *)(scalar_val->val.bytea)->vl_len_) / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_TSVECTOR:
        padlen = ag_serialize_header(buffer, GT_HEADER_TSVECTOR);

        numlen = (scalar_val->val.tsvector)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.tsvector, (scalar_val->val.tsvector)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_TSQUERY:
        padlen = ag_serialize_header(buffer, GT_HEADER_TSQUERY);

        numlen = (scalar_val->val.tsquery)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.tsquery, (scalar_val->val.tsquery)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_INT:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_INT);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_INT_MULTI:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_INT_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_NUM:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_NUM);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_NUM_MULTI:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_NUM_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_TS:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_TS);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_TS_MULTI:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_TS_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_TSTZ:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_TSTZ);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_TSTZ_MULTI:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_TSTZ_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_DATE:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_DATE);

        numlen = (scalar_val->val.range)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.range, (scalar_val->val.range)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
        break;
    case AGTV_RANGE_DATE_MULTI:
        padlen = ag_serialize_header(buffer, GT_HEADER_RANGE_DATE_MULTI);

        numlen = (scalar_val->val.multirange)->vl_len_ / 4;
        offset = reserve_from_buffer(buffer, numlen);
        memcpy(buffer->data + offset, scalar_val->val.multirange, (scalar_val->val.multirange)->vl_len_ / 4);

        *gtentry = GTENTRY_IS_GTYPE | (padlen + numlen + GT_HEADER_SIZE);
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
    GT_HEADER_TYPE agt_header = *((GT_HEADER_TYPE *)base);

    switch (agt_header)
    {
    case GT_HEADER_INTEGER:
        result->type = AGTV_INTEGER;
        result->val.int_value = *((int64 *)(base + GT_HEADER_SIZE));
        break;

    case GT_HEADER_FLOAT:
        result->type = AGTV_FLOAT;
        result->val.float_value = *((float8 *)(base + GT_HEADER_SIZE));
        break;
    case GT_HEADER_BYTEA:
        result->type = AGTV_BYTEA;
        result->val.bytea = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_TIMESTAMP:
        result->type = AGTV_TIMESTAMP;
        result->val.int_value = *((int64 *)(base + GT_HEADER_SIZE));
        break;
    case GT_HEADER_TIMESTAMPTZ:
        result->type = AGTV_TIMESTAMPTZ;
        result->val.int_value = *((int64 *)(base + GT_HEADER_SIZE));
        break;
    case GT_HEADER_DATE:
        result->type = AGTV_DATE;
        result->val.date = *((int32 *)(base + GT_HEADER_SIZE));
        break;
    case GT_HEADER_TIME:
        result->type = AGTV_TIME;
        result->val.int_value = *((int64 *)(base + GT_HEADER_SIZE));
        break;
    case GT_HEADER_TIMETZ:
        result->type = AGTV_TIMETZ;
        result->val.timetz.time = *((TimeADT*)(base + GT_HEADER_SIZE));
        result->val.timetz.zone = *((int32*)(base + GT_HEADER_SIZE + sizeof(TimeADT)));
        break;
    case GT_HEADER_INTERVAL:
        result->type = AGTV_INTERVAL;
        result->val.interval.time =  *((TimeOffset *)(base + GT_HEADER_SIZE));
        result->val.interval.day =  *((int32 *)(base + GT_HEADER_SIZE + sizeof(TimeOffset)));
        result->val.interval.month =  *((int32 *)(base + GT_HEADER_SIZE + sizeof(TimeOffset) + sizeof(int32)));
        break;
    case GT_HEADER_INET:
	result->type = AGTV_INET;
	memcpy(&result->val.inet, base + GT_HEADER_SIZE, sizeof(char) * 22);
        break;
    case GT_HEADER_CIDR:
        result->type = AGTV_CIDR;
        memcpy(&result->val.inet, base + GT_HEADER_SIZE, sizeof(char) * 22);
        break;
    case GT_HEADER_MAC:
        result->type = AGTV_MAC;
        memcpy(&result->val.mac, base + GT_HEADER_SIZE, sizeof(char) * 6);
        break;
    case GT_HEADER_MAC8:
        result->type = AGTV_MAC8;
        memcpy(&result->val.mac, base + GT_HEADER_SIZE, sizeof(char) * 8);
        break;
    case GT_HEADER_BOX2D:
        result->type = AGTV_BOX2D;
        memcpy(&result->val.gbox, base + GT_HEADER_SIZE, sizeof(GBOX));
        break;
    case GT_HEADER_BOX3D:
        result->type = AGTV_BOX3D;
        memcpy(&result->val.gbox, base + GT_HEADER_SIZE, sizeof(BOX3D));
        break;
    case GT_HEADER_SPHEROID:
        result->type = AGTV_SPHEROID;
        memcpy(&result->val.gbox, base + GT_HEADER_SIZE, sizeof(SPHEROID));
        break;
    case GT_HEADER_GSERIALIZED:
        result->type = AGTV_GSERIALIZED;
	result->val.gserialized = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_TSVECTOR:
        result->type = AGTV_TSVECTOR;
        result->val.gserialized = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_TSQUERY:
        result->type = AGTV_TSQUERY;
        result->val.tsquery = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_INT:
        result->type = AGTV_RANGE_INT;
        result->val.range = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_INT_MULTI:
        result->type = AGTV_RANGE_INT_MULTI;
        result->val.multirange = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_NUM:
        result->type = AGTV_RANGE_NUM;
        result->val.range = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_NUM_MULTI:
        result->type = AGTV_RANGE_NUM_MULTI;
        result->val.multirange = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_TS:
        result->type = AGTV_RANGE_TS;
        result->val.range = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_TS_MULTI:
        result->type = AGTV_RANGE_TS_MULTI;
        result->val.multirange = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_TSTZ:
        result->type = AGTV_RANGE_TSTZ;
        result->val.range = (base + GT_HEADER_SIZE); 
        break;
    case GT_HEADER_RANGE_TSTZ_MULTI:
        result->type = AGTV_RANGE_TSTZ_MULTI;
        result->val.multirange = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_DATE:
        result->type = AGTV_RANGE_DATE;
        result->val.range = (base + GT_HEADER_SIZE);
        break;
    case GT_HEADER_RANGE_DATE_MULTI:
        result->type = AGTV_RANGE_DATE_MULTI;
        result->val.multirange = (base + GT_HEADER_SIZE);
        break;
    default:
        elog(ERROR, "Invalid AGT header value.");
    }
}
