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

#ifndef POSTGRAPH_GRAPHID_DS_H
#define POSTGRAPH_GRAPHID_DS_H

#include "utils/graphid.h"

#define IS_GRAPHID_STACK_EMPTY(queue) \
            get_queue_size(queue) == 0
#define PEEK_GRAPHID_STACK(queue) \
            (graphid) get_graphid(peek_queue_head(queue))

/*
 * We declare the GRAPHID data structures here, and in this way, so that they
 * may be used elsewhere. However, we keep the contents private by defining them
 * in graphid_ds.c
 */

/* declare the GraphIdNode */
typedef struct GraphIdNode GraphIdNode;

/* declare the ListGraphId container */
typedef struct ListGraphId ListGraphId;

/* GraphIdNode access functions */
GraphIdNode *next_GraphIdNode(GraphIdNode *node);
graphid get_graphid(GraphIdNode *node);

/* graphid queue functions */
/* create a new ListGraphId queue */
ListGraphId *new_graphid_queue(void);
/* free a ListGraphId queue */
void free_graphid_queue(ListGraphId *queue);
/* push a graphid onto a ListGraphId queue */
void push_graphid_queue(ListGraphId *queue, graphid id);
/* pop (remove) a GraphIdNode from the top of the queue */
graphid pop_graphid_queue(ListGraphId *queue);
/* peek (doesn't remove) at the head entry of a ListGraphId queue */
GraphIdNode *peek_queue_head(ListGraphId *queue);
/* peek (doesn't remove) at the tail entry of a ListGraphId queue */
GraphIdNode *peek_queue_tail(ListGraphId *queue);
/* return the size of a ListGraphId queue */
int64 get_queue_size(ListGraphId *queue);

/* graphid list functions */
/*
 * Helper function to add a graphid to the end of a ListGraphId container.
 * If the container is NULL, it creates the container with the entry.
 */
ListGraphId *append_graphid(ListGraphId *container, graphid id);
/* free a ListGraphId container */
void free_ListGraphId(ListGraphId *container);
/* return a reference to the head entry of a list */
GraphIdNode *get_list_head(ListGraphId *list);
/* get the size of the passed list */
int64 get_list_size(ListGraphId *list);

#endif
