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
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 */

#ifndef AG_GTYPE_VLE_H
#define AG_GTYPE_VLE_H

#include "utils/gtype.h"
#include "utils/age_global_graph.h"

/*
 * We declare the path_container here, and in this way, so that it may be
 * used elsewhere. However, we keep the contents private by defining it in
 * gtype_vle.c
 */
typedef struct path_container path_container;

/*
 * Function to take a AGTV_BINARY path_container and return a path as an
 * gtype_value.
 */
gtype_value *agtv_materialize_vle_path(gtype *agt_arg_vpc);

#endif
