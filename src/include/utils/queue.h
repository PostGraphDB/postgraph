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

#ifndef AG_AGE_GRAPHID_DS_H
#define AG_AGE_GRAPHID_DS_H

#include "utils/graphid.h"

#define IS_GRAPHID_STACK_EMPTY(queue) \
            queue_size(queue) == 0
#define PEEK_GRAPHID_STACK(queue) \
            (graphid) get_graphid(peek_queue_head(queue))

// declare the QueueNode 
typedef struct QueueNode QueueNode;

// declare the Queue container 
typedef struct Queue Queue;

// QueueNode access functions 
QueueNode *next_queue_node(QueueNode *queuenode);
graphid get_graphid(QueueNode *queuenode);

// graphid queue functions 
// create a new Queue queue 
Queue *new_graphid_queue(void);
// free a Queue queue 
void free_graphid_queue(Queue *queue);
// push a graphid onto a Queue queue 
void push_graphid_queue(Queue *queue, graphid id);
// pop (remove) a QueueNode from the top of the queue 
graphid pop_graphid_queue(Queue *queue);
// peek (doesn't remove) at the head entry of a Queue queue 
QueueNode *peek_queue_head(Queue *queue);
// peek (doesn't remove) at the tail entry of a Queue queue 
QueueNode *peek_queue_tail(Queue *queue);
// return the size of a Queue queue 
int64 queue_size(Queue *queue);

// graphid list functions 
/*
 * Helper function to add a graphid to the end of a Queue container.
 * If the container is NULL, it creates the container with the entry.
 */
Queue *append_graphid(Queue *container, graphid id);
// free a Queue container 
void free_queue(Queue *container);
// return a reference to the head entry of a list 
QueueNode *get_list_head(Queue *list);
// get the size of the passed list 
int64 get_list_size(Queue *list);

#endif
