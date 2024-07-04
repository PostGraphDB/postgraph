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
 * Portions Copyright (c) 1996-2010, Bitnine Global
 */

#ifndef AG_CYPHER_EXECUTOR_H
#define AG_CYPHER_EXECUTOR_H

#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"

#define DELETE_SCAN_STATE_NAME "Cypher Delete"
#define SET_SCAN_STATE_NAME "Cypher Set"
#define CREATE_SCAN_STATE_NAME "Cypher Create"
#define MERGE_SCAN_STATE_NAME "Cypher Merge"

Node *create_cypher_create_plan_state(CustomScan *cscan);
extern const CustomExecMethods cypher_create_exec_methods;

Node *create_cypher_set_plan_state(CustomScan *cscan);
extern const CustomExecMethods cypher_set_exec_methods;

Node *create_cypher_delete_plan_state(CustomScan *cscan);
extern const CustomExecMethods cypher_delete_exec_methods;

Node *create_cypher_merge_plan_state(CustomScan *cscan);
extern const CustomExecMethods cypher_merge_exec_methods;

void init_executor_start_hook(void);

#endif
