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

#ifndef AG_CYPHER_COPYFUNCS_H
#define AG_CYPHER_COPYFUNCS_H

#include "postgres.h"

#include "nodes/nodes.h"

/*
 * Functions that let AGE's ExtensibleNodes be compatible with
 * Postgres' copyObject. We assign each node to its copy function
 * in the DEFINE_NODE_METHODS and DEFINE_NODE_METHODS_EXTENDED
 * macros in ag_nodes.c
 */

void copy_ag_node(ExtensibleNode *newnode,
                         const ExtensibleNode *oldnode);

// create data structures
void copy_cypher_create_target_nodes(ExtensibleNode *newnode, const ExtensibleNode *from);
void copy_cypher_create_path(ExtensibleNode *newnode, const ExtensibleNode *from);
void copy_cypher_target_node(ExtensibleNode *newnode, const ExtensibleNode *from);

// set/remove data structures
void copy_cypher_update_information(ExtensibleNode *newnode, const ExtensibleNode *from);
void copy_cypher_update_item(ExtensibleNode *newnode, const ExtensibleNode *from);

// delete data structures
void copy_cypher_delete_information(ExtensibleNode *newnode, const ExtensibleNode *from);
void copy_cypher_delete_item(ExtensibleNode *newnode, const ExtensibleNode *from);

// merge data structure
void copy_cypher_merge_information(ExtensibleNode *newnode, const ExtensibleNode *from);
#endif
