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
