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
#include "utils/graphid_ds.h"

/* defines */
/*
 * A simple linked list node for graphid lists (int64). PG's implementation
 * has too much overhead for this type of list as it only directly supports
 * regular ints, not int64s, of which a graphid currently is.
 */
typedef struct GraphIdNode
{
    graphid id;
    struct GraphIdNode *next;
} GraphIdNode;

/* a container for a linked list of GraphIdNodes */
typedef struct ListGraphId
{
    GraphIdNode *head;
    GraphIdNode *tail;
    int64 size;
} ListGraphId;

/* declarations */

/* definitions */
/* return the next GraphIdNode */
GraphIdNode *next_GraphIdNode(GraphIdNode *node)
{
    return node->next;
}

/* return the graphid */
graphid get_graphid(GraphIdNode *node)
{
    return node->id;
}

/* get the size of the passed queue */
int64 get_queue_size(ListGraphId *queue)
{
    return queue->size;
}

/* return a reference to the head entry in the queue, or NULL if empty */
GraphIdNode *peek_queue_head(ListGraphId *queue)
{
    if (queue == NULL)
    {
        return NULL;
    }

    return queue->head;
}

/* return a reference to the tail entry in the queue */
GraphIdNode *peek_queue_tail(ListGraphId *queue)
{
    return queue->tail;
}

/* return a reference to the head entry of a list */
GraphIdNode *get_list_head(ListGraphId *list)
{
    return list->head;
}

/* get the size of the passed list */
int64 get_list_size(ListGraphId *list)
{
    return list->size;
}

/*
 * Helper function to add a graphid to the end of a ListGraphId container.
 * If the container is NULL, it creates the container with the entry.
 */
ListGraphId *append_graphid(ListGraphId *container, graphid id)
{
    GraphIdNode *new_node = NULL;

    /* create the new link */
    new_node = palloc0(sizeof(GraphIdNode));
    new_node->id = id;
    new_node->next = NULL;

    /*
     * If the container is NULL then this is a new list. So, create the
     * container and add in the new link.
     */
    if (container == NULL)
    {
        container = palloc0(sizeof(ListGraphId));
        container->head = new_node;
        container->tail = new_node;
        container->size = 1;
    }
    /* otherwise, this is an existing list, append id */
    else
    {
        container->tail->next = new_node;
        container->tail = new_node;
        container->size++;
    }

    return container;
}

/* free (delete) a ListGraphId list */
void free_ListGraphId(ListGraphId *container)
{
    GraphIdNode *curr_node = NULL;
    GraphIdNode *next_node = NULL;

    /* if the container is NULL we don't need to delete anything */
    if (container == NULL)
    {
        return;
    }

    /* otherwise, start from the head, free, and delete the links */
    curr_node = container->head;
    while (curr_node != NULL)
    {
        next_node = curr_node->next;
        /* we can do this because this is just a list of ints */
        pfree(curr_node);
        curr_node = next_node;
    }

    /* free the container */
    pfree(container);
}

/* helper function to create a new, empty, graphid queue */
ListGraphId *new_graphid_queue(void)
{
    ListGraphId *queue = NULL;

    /* allocate the container for the queue */
    queue = palloc0(sizeof(ListGraphId));

    /* set it to its initial values */
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;

    /* return the new queue */
    return queue;
}

/* helper function to free a graphid queue's contents but, not the container */
void free_graphid_queue(ListGraphId *queue)
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
        GraphIdNode *next = queue->head->next;

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
void push_graphid_queue(ListGraphId *queue, graphid id)
{
    GraphIdNode *new_node = NULL;

    Assert(queue != NULL);

    if (queue == NULL)
    {
        elog(ERROR, "push_graphid_queue: NULL queue");
    }

    /* create the new element */
    new_node = palloc0(sizeof(GraphIdNode));
    new_node->id = id;

    /* insert (push) the new element on the top */
    new_node->next = queue->head;
    queue->head = new_node;
    queue->size++;
}

/*
 * Helper function for a generic pop graphid (int64) from a queue. If the queue
 * is empty, it will error out. You should verify that the queue isn't empty
 * prior to calling.
 */
graphid pop_graphid_queue(ListGraphId *queue)
{
    GraphIdNode *node = NULL;
    graphid id;

    Assert(queue != NULL);
    Assert(queue->size != 0);

    if (queue == NULL)
    {
        elog(ERROR, "pop_graphid_queue: NULL queue");
    }

    if (queue->size <= 0)
    {
        elog(ERROR, "pop_graphid_queue: empty queue");
    }


    /* remove the element from the top of the queue */
    node = queue->head;
    id = node->id;
    queue->head = queue->head->next;
    queue->size--;
    /* free the element */
    pfree(node);

    /* return the id */
    return id;
}
