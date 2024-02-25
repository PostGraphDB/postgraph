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

#ifndef AG_AG_FUNC_H
#define AG_AG_FUNC_H

#include "postgres.h"

#define CREATE_CLAUSE_FUNCTION_NAME "_cypher_create_clause"
#define SET_CLAUSE_FUNCTION_NAME "_cypher_set_clause"
#define DELETE_CLAUSE_FUNCTION_NAME "_cypher_delete_clause"
#define MERGE_CLAUSE_FUNCTION_NAME "_cypher_merge_clause"

bool is_oid_ag_func(Oid func_oid, const char *func_name);
Oid get_ag_func_oid(const char *func_name, const int nargs, ...);
Oid get_pg_func_oid(const char *func_name, const int nargs, ...);

#endif
