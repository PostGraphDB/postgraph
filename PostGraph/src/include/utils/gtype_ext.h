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
 */

#ifndef AG_GTYPE_EXT_H
#define AG_GTYPE_EXT_H

#include "postgres.h"

#include "utils/gtype.h"

/*
 * Function serializes the data into the buffer provided.
 * Returns false if the type is not defined. Otherwise, true.
 */
bool ag_serialize_extended_type(StringInfo buffer, gtentry *gtentry,
                                gtype_value *scalar_val);

/*
 * Function deserializes the data from the buffer pointed to by base_addr.
 * NOTE: This function writes to the error log and exits for any UNKNOWN
 * AGT_HEADER type.
 */
void ag_deserialize_extended_type(char *base_addr, uint32 offset,
                                  gtype_value *result);

#endif
