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

#ifndef AG_CYPHER_OUTFUNCS_H
#define AG_CYPHER_OUTFUNCS_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/nodes.h"

// clauses
void out_cypher_group_by(StringInfo str, const ExtensibleNode *node);
void out_cypher_return(StringInfo str, const ExtensibleNode *node);
void out_cypher_with(StringInfo str, const ExtensibleNode *node);
void out_cypher_call(StringInfo str, const ExtensibleNode *node);
void out_cypher_match(StringInfo str, const ExtensibleNode *node);
void out_cypher_create(StringInfo str, const ExtensibleNode *node);
void out_cypher_set(StringInfo str, const ExtensibleNode *node);
void out_cypher_set_item(StringInfo str, const ExtensibleNode *node);
void out_cypher_delete(StringInfo str, const ExtensibleNode *node);
void out_cypher_unwind(StringInfo str, const ExtensibleNode *node);
void out_cypher_merge(StringInfo str, const ExtensibleNode *node);

// pattern
void out_cypher_path(StringInfo str, const ExtensibleNode *node);
void out_cypher_node(StringInfo str, const ExtensibleNode *node);
void out_cypher_relationship(StringInfo str, const ExtensibleNode *node);

// expression
void out_cypher_bool_const(StringInfo str, const ExtensibleNode *node);
void out_cypher_inet_const(StringInfo str, const ExtensibleNode *node);
void out_cypher_param(StringInfo str, const ExtensibleNode *node);
void out_cypher_map(StringInfo str, const ExtensibleNode *node);
void out_cypher_list(StringInfo str, const ExtensibleNode *node);

// string match
void out_cypher_string_match(StringInfo str, const ExtensibleNode *node);

// typecast
void out_cypher_typecast(StringInfo str, const ExtensibleNode *node);

// integer constant
void out_cypher_integer_const(StringInfo str, const ExtensibleNode *node);

// sub pattern
void out_cypher_sub_pattern(StringInfo str, const ExtensibleNode *node);

// create private data structures
void out_cypher_create_target_nodes(StringInfo str, const ExtensibleNode *node);
void out_cypher_create_path(StringInfo str, const ExtensibleNode *node);
void out_cypher_target_node(StringInfo str, const ExtensibleNode *node);

// set/remove private data structures
void out_cypher_update_information(StringInfo str, const ExtensibleNode *node);
void out_cypher_update_item(StringInfo str, const ExtensibleNode *node);

// delete private data structures
void out_cypher_delete_information(StringInfo str, const ExtensibleNode *node);
void out_cypher_delete_item(StringInfo str, const ExtensibleNode *node);

// merge private data structures
void out_cypher_merge_information(StringInfo str, const ExtensibleNode *node);

// ddl utils
void out_cypher_create_graph(StringInfo str, const ExtensibleNode *node);
void out_cypher_use_graph(StringInfo str, const ExtensibleNode *node);
void out_cypher_drop_graph(StringInfo str, const ExtensibleNode *node);

#endif
