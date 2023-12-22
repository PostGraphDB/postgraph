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
 */


#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "commands/label_commands.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "commands/label_commands.h"
#include "common/hashfn.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/global_graph.h"
#include "utils/queue.h"
#include "utils/gtype.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"
#include "utils/queue.h"

// defines 
#define VERTEX_HTAB_NAME "Vertex to edge lists " // added a space at end for 
#define EDGE_HTAB_NAME "Edge to vertex mapping " // the graph name to follow 
#define VERTEX_HTAB_INITIAL_SIZE 1000000
#define EDGE_HTAB_INITIAL_SIZE 1000000

// internal data structures implementation 

// vertex entry for the vertex_hastable 
typedef struct vertex_entry
{
    graphid id;             // vertex id, it is also the hash key 
    Queue *in;         // List of entering edges graphids (int64) 
    Queue *out;        // List of exiting edges graphids (int64) 
    Queue *loop;       // List of selfloop edges graphids (int64) 
    Oid oid;    // the label table oid 
    Datum properties;       // datum property value 
} vertex_entry;

// edge entry for the edge_hashtable 
typedef struct edge_entry
{
    graphid id;               // edge id, it is also the hash key 
    Oid oid;      // the label table oid 
    Datum properties;         // datum property value 
    graphid start_id;       // start vertex 
    graphid end_id;         // end vertex 
} edge_entry;

/*
 * GRAPH global context per graph. They are chained together via next.
 * Be aware that the global pointer will point to the root BUT that
 * the root will change as new graphs are added to the top.
 */
typedef struct graph_context
{
    char *graph_name;              // graph name 
    Oid graph_oid;                 // graph oid for searching 
    HTAB *vertex_hashtable;        // hashtable to hold vertex edge lists 
    HTAB *edge_hashtable;          // hashtable to hold edge to vertex map 
    TransactionId xmin;            // transaction ids for this graph 
    TransactionId xmax;
    CommandId curcid;              // currentCommandId graph was created with 
    int64 vertex_cnt;     // number of loaded vertices in this graph 
    int64 edge_cnt;        // number of loaded edges in this graph 
    Queue *vertices;         // vertices for vertex hashtable cleanup 
    struct graph_context *next; // next graph 
} graph_context;

// global variable to hold the per process GRAPH global context 
static graph_context *global_graph_contexts = NULL;

// declarations 
// GRAPH global context functions 
static void free_graph_context(graph_context *ggctx);
static void create_hashtables(graph_context *ggctx);
static void load_hashtables(graph_context *ggctx);
static void load_vertex_hashtable(graph_context *ggctx);
static void load_edge_hashtable(graph_context *ggctx);
static void freeze_hashtables(graph_context *ggctx);
static List *get_labels(Snapshot snapshot, Oid graph_oid, char label_type);
static bool insert_edge(graph_context *ggctx, graphid id, Datum properties, graphid start_id, graphid end_id, Oid oid);
static void insert_vertex(graph_context *ggctx, graphid start_id, graphid end_id, graphid id);
static void insert_vertex_entry(graph_context *ggctx, graphid id, Oid oid, Datum properties);

//XXX: To Be Removed
bool is_ggctx_invalid(graph_context *ggctx) {
    Snapshot snap = GetActiveSnapshot();
    return (ggctx->xmin != snap->xmin || ggctx->xmax != snap->xmax || ggctx->curcid != snap->curcid);

}
/*
 * Helper function to create the global vertex and edge hashtables. One
 * hashtable will hold the vertex, its edges (both incoming and exiting) as a
 * list, and its properties datum. The other hashtable will hold the edge, its
 * properties datum, and its source and target vertex.
 */
static void create_hashtables(graph_context *ggctx)
{
    HASHCTL vertex_ctl;
    HASHCTL edge_ctl;

    // initialize the vertex hashtable 
    MemSet(&vertex_ctl, 0, sizeof(vertex_ctl));
    vertex_ctl.keysize = sizeof(int64);
    vertex_ctl.entrysize = sizeof(vertex_entry);
    vertex_ctl.hash = tag_hash;
    ggctx->vertex_hashtable = hash_create(VERTEX_HTAB_NAME, VERTEX_HTAB_INITIAL_SIZE, &vertex_ctl, HASH_ELEM | HASH_FUNCTION);

    // initialize the edge hashtable 
    MemSet(&edge_ctl, 0, sizeof(edge_ctl));
    edge_ctl.keysize = sizeof(int64);
    edge_ctl.entrysize = sizeof(edge_entry);
    edge_ctl.hash = tag_hash;
    ggctx->edge_hashtable = hash_create(EDGE_HTAB_NAME, EDGE_HTAB_INITIAL_SIZE, &edge_ctl, HASH_ELEM | HASH_FUNCTION);
}

// helper function to get a List of all label names for the specified graph 
static List *get_labels(Snapshot snapshot, Oid graph_oid, char label_type) {
    List *labels = NIL;
    ScanKeyData scan_keys[2];
    Relation ag_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;

    // we need a valid snapshot 
    Assert(snapshot != NULL);

    // setup scan keys to get all edges for the given graph oid 
    ScanKeyInit(&scan_keys[1], Anum_ag_label_graph, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[0], Anum_ag_label_kind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(label_type));

    // setup the table to be scanned, ag_label in this case 
    ag_label = table_open(ag_label_relation_id(), ShareLock);
    scan_desc = table_beginscan(ag_label, snapshot, 2, scan_keys);

    // get the tupdesc - we don't need to release this one 
    tupdesc = RelationGetDescr(ag_label);

    // get all of the label names 
    while((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL) {
        Name label;
        bool is_null = false;

        // something is wrong if this tuple isn't valid 
        Assert(HeapTupleIsValid(tuple));
        // get the label name 
        label = DatumGetName(heap_getattr(tuple, Anum_ag_label_name, tupdesc, &is_null));
        Assert(!is_null);
        // add it to our list 
        labels = lappend(labels, label);
    }

    // close up scan 
    table_endscan(scan_desc);
    table_close(ag_label, ShareLock);

    return labels;
}

/*
 * Helper function to insert one edge/edge->vertex, key/value pair, in the
 * current GRAPH global edge hashtable.
 */
static bool insert_edge(graph_context *ggctx, graphid id, Datum properties, graphid start_id, graphid end_id, Oid oid)
{
    edge_entry *value = NULL;
    bool found = false;

    // search for the edge 
    value = (edge_entry *)hash_search(ggctx->edge_hashtable, (void *)&id, HASH_ENTER, &found);
    /*
     * If we found the key, either we have a duplicate, or we made a mistake and
     * inserted it already. Either way, this isn't good so don't insert it and
     * return false. Likewise, if the value returned is NULL, don't do anything,
     * just return false. This way the caller can decide what to do.
     */
    if (found || value == NULL)
        return false;

    // not sure if we really need to zero out the entry, as we set everything 
    MemSet(value, 0, sizeof(edge_entry));

    /*
     * Set the edge id - this is important as this is the hash key value used
     * for hash function collisions.
     */
    value->id = id;
    value->properties = properties;
    value->start_id = start_id;
    value->end_id = end_id;
    value->oid = oid;

    // increment the number of loaded edges 
    ggctx->edge_cnt++;

    return true;
}

/*
 * Helper function to insert an entire vertex into the current GRAPH global
 * vertex hashtable. It will return false if there is a duplicate.
 */
static void insert_vertex_entry(graph_context *ggctx, graphid id, Oid oid, Datum properties) {
    vertex_entry *ve = NULL;
    bool found;

    // search for the vertex 
    ve = (vertex_entry *)hash_search(ggctx->vertex_hashtable, (void *)&id, HASH_ENTER, &found);

    // again, MemSet may not be needed here 
    MemSet(ve, 0, sizeof(vertex_entry));

    /*
     * Set the vertex id - this is important as this is the hash key value
     * used for hash function collisions.
     */
    ve->id = id;
    // set the label table oid for this vertex 
    ve->oid = oid;
    // set the datum vertex properties 
    ve->properties = properties;
    // set the NIL edge list 
    ve->in = NULL;
    ve->out = NULL;
    ve->loop = NULL;

    // we also need to store the vertex id for clean up of vertex lists 
    ggctx->vertices = append_graphid(ggctx->vertices, id);

    // increment the number of loaded vertices 
    ggctx->vertex_cnt++;
}

/*
 * Helper function to append one edge to an existing vertex in the current
 * global vertex hashtable.
 */
static void insert_vertex(graph_context *ggctx, graphid start_id, graphid end_id, graphid id) {
    vertex_entry *value = NULL;
    bool found = false;
    bool is_selfloop = false;

    // is it a self loop 
    is_selfloop = (start_id == end_id);

    // search for the start vertex of the edge 
    value = (vertex_entry *)hash_search(ggctx->vertex_hashtable, (void *)&start_id, HASH_FIND, &found);
    // vertices were preloaded so it must be there 
    Assert(found);

    // if it is a self loop, add the edge to loop and we're done 
    if (is_selfloop) {
        value->loop = append_graphid(value->loop, id);
        return;
    }

    // add the edge to the out list of the start vertex 
    value->out = append_graphid(value->out, id);

    // search for the end vertex of the edge 
    value = (vertex_entry *)hash_search(ggctx->vertex_hashtable, (void *)&end_id, HASH_FIND, &found);
    // vertices were preloaded so it must be there 
    Assert(found);

    // add the edge to the in list of the end vertex 
    value->in = append_graphid(value->in, id);
}

// helper routine to load all vertices into the GRAPH global vertex hashtable 
static void load_vertex_hashtable(graph_context *ggctx) {
    ListCell *lc;

    Oid graph_oid = ggctx->graph_oid;
    Oid graph_namespace_oid = get_namespace_oid(ggctx->graph_name, false);

    Snapshot snapshot = GetActiveSnapshot();

    List *label_names = get_labels(snapshot, graph_oid, LABEL_TYPE_VERTEX);

    foreach (lc, label_names) {
        TableScanDesc scan_desc;
        HeapTuple tuple;
        TupleDesc tupdesc;

        char *label_name = lfirst(lc);
        
        Oid oid = get_relname_relid(label_name, graph_namespace_oid);
       
        Relation graph_vertex_label = table_open(oid, ShareLock);
        scan_desc = table_beginscan(graph_vertex_label, snapshot, 0, NULL);
      
        tupdesc = RelationGetDescr(graph_vertex_label);
                     
        while((tuple = heap_getnext(scan_desc, ForwardScanDirection))) {
            graphid id;
            Datum properties;
            bool isnull;
            HeapTupleData htd;

            Assert(HeapTupleIsValid(tuple));

            HeapTupleHeader hth = tuple->t_data;
            htd.t_len = HeapTupleHeaderGetDatumLength(hth);
            htd.t_data = hth;

            Assert(HeapTupleIsValid(tuple));
            // id
            id = DATUM_GET_GRAPHID(heap_getattr(&htd, 1, tupdesc, &isnull));
            // properties
            properties = heap_getattr(&htd, 2, tupdesc, &isnull);

            // insert vertex into vertex hashtable 
            insert_vertex_entry(ggctx, id, oid, properties);
        }

        table_endscan(scan_desc);
        table_close(graph_vertex_label, ShareLock);
    }
}

/*
 * Helper function to load all of the GRAPH global hashtables (vertex & edge)
 * for the current global context.
 */
static void load_hashtables(graph_context *ggctx)
{
    // initialize statistics 
    ggctx->vertex_cnt = 0;
    ggctx->edge_cnt = 0;

    load_vertex_hashtable(ggctx);
    load_edge_hashtable(ggctx);
}

/*
 * Helper routine to load all edges into the GRAPH global edge and vertex
 * hashtables.
 */
static void load_edge_hashtable(graph_context *ggctx) {
    Oid graph_oid;
    Oid graph_namespace_oid;
    Snapshot snapshot;
    List *labels = NIL;
    ListCell *lc;

    graph_oid = ggctx->graph_oid;
    graph_namespace_oid = get_namespace_oid(ggctx->graph_name, false);
    snapshot = GetActiveSnapshot();

    labels = get_labels(snapshot, graph_oid, LABEL_TYPE_EDGE);

    foreach (lc, labels) {
        HeapTuple tuple;
        char *label = lfirst(lc);

        Oid oid = get_relname_relid(label, graph_namespace_oid);
        Relation graph_edge_label = table_open(oid, ShareLock);
        TableScanDesc scan_desc = table_beginscan(graph_edge_label, snapshot, 0, NULL);
        TupleDesc tupdesc = RelationGetDescr(graph_edge_label);
        Assert (tupdesc->natts == 4);

        while((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL) {
	    graphid id, start_id, end_id;
            Datum properties;
            bool inserted = false;
            bool isnull;
            HeapTupleHeader hth;
            HeapTupleData tmptup, *htd;

            hth = tuple->t_data;
            tmptup.t_len = HeapTupleHeaderGetDatumLength(hth);
            tmptup.t_data = hth;
            htd = &tmptup;

            Assert(HeapTupleIsValid(tuple));
            // id
            id = DATUM_GET_GRAPHID(heap_getattr(htd, 1, tupdesc, &isnull));
            // start_id
            start_id = DATUM_GET_GRAPHID(heap_getattr(htd, 2, tupdesc, &isnull));
            // end_id
            end_id = DATUM_GET_GRAPHID(heap_getattr(htd, 3, tupdesc, &isnull));
            // properties
            properties = heap_getattr(htd, 4, tupdesc, &isnull);

            // insert edge into edge hashtable 
            inserted = insert_edge(ggctx, id, properties, start_id, end_id, oid);
            Assert (inserted);

            // insert the edge into the start and end vertices edge lists 
            insert_vertex(ggctx, start_id, end_id, id);
        }

        // end the scan and close the relation 
        table_endscan(scan_desc);
        table_close(graph_edge_label, ShareLock);
    }
}

static void freeze_hashtables(graph_context *ggctx) {
    hash_freeze(ggctx->vertex_hashtable);
    hash_freeze(ggctx->edge_hashtable);
}

/*
 * Helper function to free the entire specified GRAPH global context. After
 * running this you should not use the pointer in ggctx.
 */
static void free_graph_context(graph_context *ggctx)
{
    QueueNode *curr_vertex = NULL;

    // don't do anything if NULL 
    if (!ggctx)
        return;

    // free the graph name 
    pfree(ggctx->graph_name);
    ggctx->graph_name = NULL;

    ggctx->graph_oid = InvalidOid;
    ggctx->next = NULL;

    // free the vertex edge lists, starting with the head 
    curr_vertex = peek_queue_head(ggctx->vertices);
    while (curr_vertex) {
        QueueNode *next_vertex = NULL;
        vertex_entry *value = NULL;
        bool found = false;
        graphid id;

        // get the next vertex in the list, if any 
        next_vertex = next_queue_node(curr_vertex);

        // get the current vertex id 
        id = get_graphid(curr_vertex);

        // retrieve the vertex entry 
        value = (vertex_entry *)hash_search(ggctx->vertex_hashtable, (void *)&id, HASH_FIND, &found);
        Assert(found);

        // free the edge list associated with this vertex 
        free_queue(value->in);
        free_queue(value->out);
        free_queue(value->loop);

        value->in = NULL;
        value->out = NULL;
        value->loop = NULL;

        // move to the next vertex 
        curr_vertex = next_vertex;
    }

    // free the vertices list 
    free_queue(ggctx->vertices);
    ggctx->vertices = NULL;

    // free the hashtables 
    hash_destroy(ggctx->vertex_hashtable);
    hash_destroy(ggctx->edge_hashtable);

    ggctx->vertex_hashtable = NULL;
    ggctx->edge_hashtable = NULL;

    pfree(ggctx);
    ggctx = NULL;
}

/*
 * It will create the
 * context for the graph specified, provided it isn't already built and valid.
 * During processing it will free (delete) all invalid GRAPH contexts. It
 * returns the GRAPH global context for the specified graph.
 */
graph_context *manage_graph_contexts(char *graph_name, Oid graph_oid) {
    graph_context *new_ggctx = NULL;
    graph_context *curr_ggctx = NULL;
    graph_context *prev_ggctx = NULL;

    // we need a higher context, or one that isn't destroyed by SRF exit 
    MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

    // free the invalidated GRAPH global contexts first 
    prev_ggctx = NULL;
    curr_ggctx = global_graph_contexts;
    while (curr_ggctx) {
        graph_context *next_ggctx = curr_ggctx->next;

        if (is_ggctx_invalid(curr_ggctx)) {
            if (prev_ggctx == NULL)
                global_graph_contexts = next_ggctx;
            else
                prev_ggctx->next = curr_ggctx->next;

            free_graph_context(curr_ggctx);
        } else {
            prev_ggctx = curr_ggctx;
        }

        curr_ggctx = next_ggctx;
    }

    curr_ggctx = global_graph_contexts;
    while (curr_ggctx) {
        if (curr_ggctx->graph_oid == graph_oid) {
            MemoryContextSwitchTo(oldctx);
            return curr_ggctx;
        }
        curr_ggctx = curr_ggctx->next;
    }

    new_ggctx = palloc0(sizeof(graph_context));

    if (global_graph_contexts)
        new_ggctx->next = global_graph_contexts;
    else
        new_ggctx->next = NULL;

    global_graph_contexts = new_ggctx;

    new_ggctx->graph_name = pstrdup(graph_name);
    new_ggctx->graph_oid = graph_oid;

    new_ggctx->xmin = GetActiveSnapshot()->xmin;
    new_ggctx->xmax = GetActiveSnapshot()->xmax;
    new_ggctx->curcid = GetActiveSnapshot()->curcid;

    new_ggctx->vertices = NULL;

    create_hashtables(new_ggctx);
    load_hashtables(new_ggctx);
    freeze_hashtables(new_ggctx);

    MemoryContextSwitchTo(oldctx);

    return new_ggctx;
}

/*
 * Helper function to retrieve a vertex_entry from the graph's vertex hash
 * table. If there isn't one, it returns a NULL. The latter is necessary for
 * checking if the vsid and veid entries exist.
 */
vertex_entry *get_vertex_entry(graph_context *ggctx, graphid id)
{
    vertex_entry *ve = NULL;
    bool found = false;

    // retrieve the current vertex entry 
    ve = (vertex_entry *)hash_search(ggctx->vertex_hashtable, (void *)&id, HASH_FIND, &found);
    return ve;
}

// helper function to retrieve an edge_entry from the graph's edge hash table 
edge_entry *get_edge_entry(graph_context *ggctx, graphid id)
{
    edge_entry *ee = NULL;
    bool found = false;

    // retrieve the current edge entry 
    ee = (edge_entry *)hash_search(ggctx->edge_hashtable, (void *)&id, HASH_FIND, &found);
    // it should be found, otherwise we have problems 
    Assert(found);

    return ee;
}

/*
 * Helper function to find the graph_context used by the specified
 * graph_oid. If not found, it returns NULL.
 */
graph_context *find_graph_context(Oid graph_oid) {
    graph_context *ggctx = global_graph_contexts;

    while(ggctx) {
        if (ggctx->graph_oid == graph_oid)
            return ggctx;

        ggctx = ggctx->next;
    }

    return NULL;
}

// graph vertices accessor 
Queue *get_graph_vertices(graph_context *ggctx) {
    return ggctx->vertices;
}

// vertex_entry accessor functions 
graphid get_vertex_entry_id(vertex_entry *ve) {
    return ve->id;
}

Queue *get_vertex_entry_edges_in(vertex_entry *ve) {
    return ve->in;
}

Queue *get_vertex_entry_edges_out(vertex_entry *ve) {
    return ve->out;
}

Queue *get_vertex_entry_edges_self(vertex_entry *ve) {
    return ve->loop;
}

Oid get_vertex_entry_label_table_oid(vertex_entry *ve) {
    return ve->oid;
}

Datum get_vertex_entry_properties(vertex_entry *ve) {
    return ve->properties;
}

// edge_entry accessor functions 
graphid get_edge_entry_id(edge_entry *ee) {
    return ee->id;
}

Oid get_edge_entry_label_table_oid(edge_entry *ee) {
    return ee->oid;
}

Datum get_edge_entry_properties(edge_entry *ee) {
    return ee->properties;
}

graphid get_start_id(edge_entry *ee) {
    return ee->start_id;
}

graphid get_end_id(edge_entry *ee) {
    return ee->end_id;
}
