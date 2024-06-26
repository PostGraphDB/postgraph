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

#ifndef POSTGRAPH_GLOBAL_GRAPH_H
#define POSTGRAPH_GLOBAL_GRAPH_H

#include "utils/graphid.h"
#include "utils/queue.h"

typedef struct vertex_entry vertex_entry;
typedef struct edge_entry edge_entry;
typedef struct graph_context graph_context;

// GRAPH global context functions 
graph_context *manage_graph_contexts(char *graph_name, Oid graph_oid);
graph_context *find_graph_context(Oid graph_oid);
bool is_ggctx_invalid(graph_context *ggctx);
// GRAPH retrieval functions 
Queue *get_graph_vertices(graph_context *ggctx);
vertex_entry *get_vertex_entry(graph_context *ggctx, graphid vertex_id);
edge_entry *get_edge_entry(graph_context *ggctx, graphid edge_id);
// vertex entry accessor functions
graphid get_vertex_entry_id(vertex_entry *ve);
Queue *get_vertex_entry_edges_in(vertex_entry *ve);
Queue *get_vertex_entry_edges_out(vertex_entry *ve);
Queue *get_vertex_entry_edges_self(vertex_entry *ve);
Oid get_vertex_entry_label_table_oid(vertex_entry *ve);
Datum get_vertex_entry_properties(vertex_entry *ve);
// edge entry accessor functions 
graphid get_edge_entry_id(edge_entry *ee);
Oid get_edge_entry_label_table_oid(edge_entry *ee);
Datum get_edge_entry_properties(edge_entry *ee);
graphid get_start_id(edge_entry *ee);
graphid get_end_id(edge_entry *ee);
#endif
