/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgres.h"

#include "utils/graphid.h"
#include "utils/queue.h"

/* defines */
/*
 * A simple linked list queuenode for graphid lists (int64). PG's implementation
 * has too much overhead for this type of list as it only directly supports
 * regular ints, not int64s, of which a graphid currently is.
 */
typedef struct QueueNode
{
    graphid id;
    struct QueueNode *next;
} QueueNode;

/* a container for a linked list of QueueNodes */
typedef struct Queue
{
    QueueNode *head;
    QueueNode *tail;
    int64 size;
} Queue;

/* declarations */

/* definitions */
/* return the next QueueNode */
QueueNode *next_queue_node(QueueNode *queuenode) {
    return queuenode->next;
}

/* return the graphid */
graphid get_graphid(QueueNode *queuenode) {
    return queuenode->id;
}

/* get the size of the passed queue */
int64 queue_size(Queue *queue) {
    return queue->size;
}

/* return a reference to the head entry in the queue, or NULL if empty */
QueueNode *peek_queue_head(Queue *queue) {
    if (queue == NULL)
        return NULL;

    return queue->head;
}

/* return a reference to the tail entry in the queue */
QueueNode *peek_queue_tail(Queue *queue)
{
    return queue->tail;
}

/* return a reference to the head entry of a list */
QueueNode *get_list_head(Queue *list)
{
    return list->head;
}

/* get the size of the passed list */
int64 get_list_size(Queue *list)
{
    return list->size;
}

/*
 * Helper function to add a graphid to the end of a Queue container.
 * If the container is NULL, it creates the container with the entry.
 */
Queue *append_graphid(Queue *container, graphid id)
{
    QueueNode *new_queuenode = NULL;

    /* create the new link */
    new_queuenode = palloc0(sizeof(QueueNode));
    new_queuenode->id = id;
    new_queuenode->next = NULL;

    if (container == NULL) {
        container = palloc0(sizeof(Queue));
        container->head = new_queuenode;
        container->tail = new_queuenode;
        container->size = 1;
    }
    else
    {
        container->tail->next = new_queuenode;
        container->tail = new_queuenode;
        container->size++;
    }

    return container;
}

/* free (delete) a Queue list */
void free_queue(Queue *container)
{
    QueueNode *curr_queuenode = NULL;
    QueueNode *next_queuenode = NULL;

    /* if the container is NULL we don't need to delete anything */
    if (container == NULL)
    {
        return;
    }

    /* otherwise, start from the head, free, and delete the links */
    curr_queuenode = container->head;
    while (curr_queuenode != NULL)
    {
        next_queuenode = curr_queuenode->next;
        /* we can do this because this is just a list of ints */
        pfree(curr_queuenode);
        curr_queuenode = next_queuenode;
    }

    /* free the container */
    pfree(container);
}

/* helper function to create a new, empty, graphid queue */
Queue *new_graphid_queue(void)
{
    Queue *queue = NULL;

    /* allocate the container for the queue */
    queue = palloc0(sizeof(Queue));

    /* set it to its initial values */
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;

    /* return the new queue */
    return queue;
}

/* helper function to free a graphid queue's contents but, not the container */
void free_graphid_queue(Queue *queue)
{
    Assert(queue != NULL);

    if (queue == NULL)
    {
        elog(ERROR, "free_graphid_queue: NULL queue");
    }

    /* while there are entries */
    while (queue->head != NULL)
    {
        /* get the next element in the queue */
        QueueNode *next = queue->head->next;

        /* free the head element */
        pfree(queue->head);
        /* move the head to the next */
        queue->head = next;
    }

    /* reset the tail and size */
    queue->tail = NULL;
    queue->size = 0;
}

/*
 * Helper function for a generic push graphid (int64) to a queue. If the queue
 * is NULL, it will error out.
 */
void push_graphid_queue(Queue *queue, graphid id)
{
    QueueNode *new_queuenode = NULL;

    Assert(queue != NULL);

    if (queue == NULL)
    {
        elog(ERROR, "push_graphid_queue: NULL queue");
    }

    /* create the new element */
    new_queuenode = palloc0(sizeof(QueueNode));
    new_queuenode->id = id;

    /* insert (push) the new element on the top */
    new_queuenode->next = queue->head;
    queue->head = new_queuenode;
    queue->size++;
}

/*
 * Helper function for a generic pop graphid (int64) from a queue. If the queue
 * is empty, it will error out. You should verify that the queue isn't empty
 * prior to calling.
 */
graphid pop_graphid_queue(Queue *queue)
{
    QueueNode *queuenode = NULL;
    graphid id;

    Assert(queue != NULL);
    Assert(queue->size != 0);

    if (queue == NULL)
        elog(ERROR, "pop_graphid_queue: NULL queue");

    if (queue->size <= 0)
        elog(ERROR, "pop_graphid_queue: empty queue");


    /* remove the element from the top of the queue */
    queuenode = queue->head;
    id = queuenode->id;
    queue->head = queue->head->next;
    queue->size--;

    /* return the id */
    return id;
}
