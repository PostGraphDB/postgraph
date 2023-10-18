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
 * Portions Copyright (c) 2021-2023, pgvector
 */

#ifndef VECTOR_H
#define VECTOR_H

#include "postgres.h"

#include "utils/gtype.h"

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim)       (sizeof(uint32) + sizeof(uint32) + (_dim * sizeof(float8)) + sizeof(uint32))
#define DatumGetVector(x)       ((Vector *) PG_DETOAST_DATUM(x))

typedef struct Vector
{
    uint16 dim;    /* number of dimensions */
    float8 *x;
} Vector;

//gtype_value *gtype_vector_in(char *str, int32 typmod);
//gtype_value *InitVectorGType(int dim);

static inline Vector *
InitVector(int dim)
{
    int size = VECTOR_SIZE(dim);

    Vector *result = (Vector *) palloc0(size);

    result->dim = dim;

    return result;
}

#endif
