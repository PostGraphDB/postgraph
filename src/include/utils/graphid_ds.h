/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
