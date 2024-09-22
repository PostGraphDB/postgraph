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
#ifndef AG_CYPHER_READFUNCS_H
#define AG_CYPHER_READFUNCS_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/nodes.h"

/*
 * Deserialization functions for AGE's ExtensibleNodes. We assign
 * each node to its deserialization functionin the DEFINE_NODE_METHODS
 * and DEFINE_NODE_METHODS_EXTENDED macros in ag_nodes.c.

 *
 * All functions are dependent on the pg_strtok function. We do not
 * setup pg_strtok. That is for the the caller to do. By default that
 * is the responsibility of Postgres' nodeRead function. We assume
 * that was setup correctly.
 */

void read_ag_node(ExtensibleNode *node);

// create data structures
void read_cypher_create_target_nodes(struct ExtensibleNode *node);
void read_cypher_create_path(struct ExtensibleNode *node);
void read_cypher_target_node(struct ExtensibleNode *node);

// set/remove data structures
void read_cypher_update_information(struct ExtensibleNode *node);
void read_cypher_update_item(struct ExtensibleNode *node);

// delete data structures
void read_cypher_delete_information(struct ExtensibleNode *node);
void read_cypher_delete_item(struct ExtensibleNode *node);

void read_cypher_merge_information(struct ExtensibleNode *node);

void read_cypher_create_graph(struct ExtensibleNode *node);
void read_cypher_use_graph(struct ExtensibleNode *node);
void read_cypher_drop_graph(struct ExtensibleNode *node);

#endif
