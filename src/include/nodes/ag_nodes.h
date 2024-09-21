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

#ifndef AG_AG_NODES_H
#define AG_AG_NODES_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/nodes.h"

// This list must match node_names and node_methods.
typedef enum ag_node_tag
{
    ag_node_invalid_t = 0,

    // projection
    cypher_group_by_t,
    cypher_return_t,
    cypher_with_t,
    cypher_call_t,
    // reading clause
    cypher_match_t,
    // updating clause
    cypher_create_t,
    cypher_set_t,
    cypher_set_item_t,
    cypher_delete_t,
    cypher_unwind_t,
    cypher_merge_t,
    // pattern
    cypher_path_t,
    cypher_node_t,
    cypher_relationship_t,
    // expression
    cypher_bool_const_t,
    cypher_inet_const_t,
    cypher_param_t,
    cypher_map_t,
    cypher_list_t,
    // string match
    cypher_string_match_t,
    // typecast
    cypher_typecast_t,
    // integer constant
    cypher_integer_const_t,
    // sub patterns
    cypher_sub_pattern_t,
    // create data structures
    cypher_create_target_nodes_t,
    cypher_create_path_t,
    cypher_target_node_t,
    // set/remove data structures
    cypher_update_information_t,
    cypher_update_item_t,
    // delete data structures
    cypher_delete_information_t,
    cypher_delete_item_t,
    cypher_merge_information_t,
    // CREATE GRAPH ident
    cypher_create_graph_t,
    // USE GRAPH ident
    cypher_use_graph_t
} ag_node_tag;

void register_ag_nodes(void);

ExtensibleNode *_new_ag_node(Size size, ag_node_tag tag);

#define new_ag_node(size, tag) \
    ( \
        AssertMacro((size) >= sizeof(ExtensibleNode)), \
        AssertMacro(tag != ag_node_invalid_t), \
        _new_ag_node(size, tag) \
    )

#define make_ag_node(type) \
    ((type *)new_ag_node(sizeof(type), CppConcat(type, _t)))

static inline bool _is_ag_node(Node *node, const char *extnodename)
{
    ExtensibleNode *extnode;

    if (!IsA(node, ExtensibleNode))
        return false;

    extnode = (ExtensibleNode *)node;
    if (strcmp(extnode->extnodename, extnodename) == 0)
        return true;

    return false;
}

#define is_ag_node(node, type) _is_ag_node((Node *)(node), CppAsString(type))

#endif
