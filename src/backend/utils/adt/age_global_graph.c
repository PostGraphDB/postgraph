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
#include "utils/age_global_graph.h"
#include "utils/age_graphid_ds.h"
#include "utils/agtype.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/graphid.h"
#include "utils/age_graphid_ds.h"

/* defines */
#define VERTEX_HTAB_NAME "Vertex to edge lists " /* added a space at end for */
#define EDGE_HTAB_NAME "Edge to vertex mapping " /* the graph name to follow */
#define VERTEX_HTAB_INITIAL_SIZE 1000000
#define EDGE_HTAB_INITIAL_SIZE 1000000

/* internal data structures implementation */

/* vertex entry for the vertex_hastable */
typedef struct vertex_entry
{
    graphid vertex_id;             /* vertex id, it is also the hash key */
    ListGraphId *edges_in;         /* List of entering edges graphids (int64) */
    ListGraphId *edges_out;        /* List of exiting edges graphids (int64) */
    ListGraphId *edges_self;       /* List of selfloop edges graphids (int64) */
    Oid vertex_label_table_oid;    /* the label table oid */
    Datum vertex_properties;       /* datum property value */
} vertex_entry;

/* edge entry for the edge_hashtable */
typedef struct edge_entry
{
    graphid edge_id;               /* edge id, it is also the hash key */
    Oid edge_label_table_oid;      /* the label table oid */
    Datum edge_properties;         /* datum property value */
    graphid start_vertex_id;       /* start vertex */
    graphid end_vertex_id;         /* end vertex */
} edge_entry;

/*
 * GRAPH global context per graph. They are chained together via next.
 * Be aware that the global pointer will point to the root BUT that
 * the root will change as new graphs are added to the top.
 */
typedef struct graph_context
{
    char *graph_name;              /* graph name */
    Oid graph_oid;                 /* graph oid for searching */
    HTAB *vertex_hashtable;        /* hashtable to hold vertex edge lists */
    HTAB *edge_hashtable;          /* hashtable to hold edge to vertex map */
    TransactionId xmin;            /* transaction ids for this graph */
    TransactionId xmax;
    CommandId curcid;              /* currentCommandId graph was created with */
    int64 num_loaded_vertices;     /* number of loaded vertices in this graph */
    int64 num_loaded_edges;        /* number of loaded edges in this graph */
    ListGraphId *vertices;         /* vertices for vertex hashtable cleanup */
    struct graph_context *next; /* next graph */
} graph_context;

/* global variable to hold the per process GRAPH global context */
static graph_context *global_graph_contexts = NULL;

/* declarations */
/* GRAPH global context functions */
static void free_specific_graph_context(graph_context *ggctx);
static void create_GRAPH_global_hashtables(graph_context *ggctx);
static void load_GRAPH_global_hashtables(graph_context *ggctx);
static void load_vertex_hashtable(graph_context *ggctx);
static void load_edge_hashtable(graph_context *ggctx);
static void freeze_GRAPH_global_hashtables(graph_context *ggctx);
static List *get_ag_labels_names(Snapshot snapshot, Oid graph_oid,
                                 char label_type);
static bool insert_edge(graph_context *ggctx, graphid edge_id,
                        Datum edge_properties, graphid start_vertex_id,
                        graphid end_vertex_id, Oid edge_label_table_oid);
static bool insert_vertex_edge(graph_context *ggctx,
                               graphid start_vertex_id, graphid end_vertex_id,
                               graphid edge_id);
static bool insert_vertex_entry(graph_context *ggctx, graphid vertex_id,
                                Oid vertex_label_table_oid,
                                Datum vertex_properties);
/* definitions */

/*
 * Helper function to determine validity of the passed graph_context.
 * This is based off of the current active snaphot, to see if the graph could
 * have been modified. Ideally, we should find a way to more accurately know
 * whether the particular graph was modified.
 */
bool is_ggctx_invalid(graph_context *ggctx)
{
    Snapshot snap = GetActiveSnapshot();

    /*
     * If the transaction ids (xmin or xmax) or currentCommandId (curcid) have
     * changed, then we have a graph that was updated. This means that the
     * global context for this graph is no longer valid.
     */
    return (ggctx->xmin != snap->xmin ||
            ggctx->xmax != snap->xmax ||
            ggctx->curcid != snap->curcid);

}
/*
 * Helper function to create the global vertex and edge hashtables. One
 * hashtable will hold the vertex, its edges (both incoming and exiting) as a
 * list, and its properties datum. The other hashtable will hold the edge, its
 * properties datum, and its source and target vertex.
 */
static void create_GRAPH_global_hashtables(graph_context *ggctx)
{
    HASHCTL vertex_ctl;
    HASHCTL edge_ctl;
    char *graph_name = NULL;
    char *vhn = NULL;
    char *ehn = NULL;
    int glen;
    int vlen;
    int elen;

    /* get the graph name and length */
    graph_name = ggctx->graph_name;
    glen = strlen(graph_name);
    /* get the vertex htab name length */
    vlen = strlen(VERTEX_HTAB_NAME);
    /* get the edge htab name length */
    elen = strlen(EDGE_HTAB_NAME);
    /* allocate the space and build the names */
    vhn = palloc0(vlen + glen + 1);
    ehn = palloc0(elen + glen + 1);
    /* copy in the names */
    strcpy(vhn, VERTEX_HTAB_NAME);
    strcpy(ehn, EDGE_HTAB_NAME);
    /* add in the graph name */
    vhn = strncat(vhn, graph_name, glen);
    ehn = strncat(ehn, graph_name, glen);

    /* initialize the vertex hashtable */
    MemSet(&vertex_ctl, 0, sizeof(vertex_ctl));
    vertex_ctl.keysize = sizeof(int64);
    vertex_ctl.entrysize = sizeof(vertex_entry);
    vertex_ctl.hash = tag_hash;
    ggctx->vertex_hashtable = hash_create(vhn, VERTEX_HTAB_INITIAL_SIZE,
                                          &vertex_ctl,
                                          HASH_ELEM | HASH_FUNCTION);
    pfree(vhn);

    /* initialize the edge hashtable */
    MemSet(&edge_ctl, 0, sizeof(edge_ctl));
    edge_ctl.keysize = sizeof(int64);
    edge_ctl.entrysize = sizeof(edge_entry);
    edge_ctl.hash = tag_hash;
    ggctx->edge_hashtable = hash_create(ehn, EDGE_HTAB_INITIAL_SIZE, &edge_ctl,
                                        HASH_ELEM | HASH_FUNCTION);
    pfree(ehn);
}

/* helper function to get a List of all label names for the specified graph */
static List *get_ag_labels_names(Snapshot snapshot, Oid graph_oid,
                                 char label_type)
{
    List *labels = NIL;
    ScanKeyData scan_keys[2];
    Relation ag_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;

    /* we need a valid snapshot */
    Assert(snapshot != NULL);

    /* setup scan keys to get all edges for the given graph oid */
    ScanKeyInit(&scan_keys[1], Anum_ag_label_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[0], Anum_ag_label_kind, BTEqualStrategyNumber,
                F_CHAREQ, CharGetDatum(label_type));

    /* setup the table to be scanned, ag_label in this case */
    ag_label = table_open(ag_label_relation_id(), ShareLock);
    scan_desc = table_beginscan(ag_label, snapshot, 2, scan_keys);

    /* get the tupdesc - we don't need to release this one */
    tupdesc = RelationGetDescr(ag_label);
    /* bail if the number of columns differs - this table has 5 */
    Assert(tupdesc->natts == Natts_ag_label);

    /* get all of the label names */
    while((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        Name label;
        bool is_null = false;

        /* something is wrong if this tuple isn't valid */
        Assert(HeapTupleIsValid(tuple));
        /* get the label name */
        label = DatumGetName(heap_getattr(tuple, Anum_ag_label_name, tupdesc,
                                          &is_null));
        Assert(!is_null);
        /* add it to our list */
        labels = lappend(labels, label);
    }

    /* close up scan */
    table_endscan(scan_desc);
    table_close(ag_label, ShareLock);

    return labels;
}

/*
 * Helper function to insert one edge/edge->vertex, key/value pair, in the
 * current GRAPH global edge hashtable.
 */
static bool insert_edge(graph_context *ggctx, graphid edge_id,
                        Datum edge_properties, graphid start_vertex_id,
                        graphid end_vertex_id, Oid edge_label_table_oid)
{
    edge_entry *value = NULL;
    bool found = false;

    /* search for the edge */
    value = (edge_entry *)hash_search(ggctx->edge_hashtable, (void *)&edge_id,
                                      HASH_ENTER, &found);
    /*
     * If we found the key, either we have a duplicate, or we made a mistake and
     * inserted it already. Either way, this isn't good so don't insert it and
     * return false. Likewise, if the value returned is NULL, don't do anything,
     * just return false. This way the caller can decide what to do.
     */
    if (found || value == NULL)
    {
        return false;
    }

    /* not sure if we really need to zero out the entry, as we set everything */
    MemSet(value, 0, sizeof(edge_entry));

    /*
     * Set the edge id - this is important as this is the hash key value used
     * for hash function collisions.
     */
    value->edge_id = edge_id;
    value->edge_properties = edge_properties;
    value->start_vertex_id = start_vertex_id;
    value->end_vertex_id = end_vertex_id;
    value->edge_label_table_oid = edge_label_table_oid;

    /* increment the number of loaded edges */
    ggctx->num_loaded_edges++;

    return true;
}

/*
 * Helper function to insert an entire vertex into the current GRAPH global
 * vertex hashtable. It will return false if there is a duplicate.
 */
static bool insert_vertex_entry(graph_context *ggctx, graphid vertex_id,
                                Oid vertex_label_table_oid,
                                Datum vertex_properties)
{
    vertex_entry *ve = NULL;
    bool found = false;

    /* search for the vertex */
    ve = (vertex_entry *)hash_search(ggctx->vertex_hashtable,
                                     (void *)&vertex_id, HASH_ENTER, &found);

    /* we should never have duplicates, return false */
    if (found)
    {
        return false;
    }

    /* again, MemSet may not be needed here */
    MemSet(ve, 0, sizeof(vertex_entry));

    /*
     * Set the vertex id - this is important as this is the hash key value
     * used for hash function collisions.
     */
    ve->vertex_id = vertex_id;
    /* set the label table oid for this vertex */
    ve->vertex_label_table_oid = vertex_label_table_oid;
    /* set the datum vertex properties */
    ve->vertex_properties = vertex_properties;
    /* set the NIL edge list */
    ve->edges_in = NULL;
    ve->edges_out = NULL;
    ve->edges_self = NULL;

    /* we also need to store the vertex id for clean up of vertex lists */
    ggctx->vertices = append_graphid(ggctx->vertices, vertex_id);

    /* increment the number of loaded vertices */
    ggctx->num_loaded_vertices++;

    return true;
}

/*
 * Helper function to append one edge to an existing vertex in the current
 * global vertex hashtable.
 */
static bool insert_vertex_edge(graph_context *ggctx,
                               graphid start_vertex_id, graphid end_vertex_id,
                               graphid edge_id)
{
    vertex_entry *value = NULL;
    bool found = false;
    bool is_selfloop = false;

    /* is it a self loop */
    is_selfloop = (start_vertex_id == end_vertex_id);

    /* search for the start vertex of the edge */
    value = (vertex_entry *)hash_search(ggctx->vertex_hashtable,
                                        (void *)&start_vertex_id, HASH_FIND,
                                        &found);
    /* vertices were preloaded so it must be there */
    Assert(found);
    if (!found)
    {
        return found;
    }

    /* if it is a self loop, add the edge to edges_self and we're done */
    if (is_selfloop)
    {
        value->edges_self = append_graphid(value->edges_self, edge_id);
        return found;
    }

    /* add the edge to the edges_out list of the start vertex */
    value->edges_out = append_graphid(value->edges_out, edge_id);

    /* search for the end vertex of the edge */
    value = (vertex_entry *)hash_search(ggctx->vertex_hashtable,
                                        (void *)&end_vertex_id, HASH_FIND,
                                        &found);
    /* vertices were preloaded so it must be there */
    Assert(found);
    if (!found)
    {
        return found;
    }

    /* add the edge to the edges_in list of the end vertex */
    value->edges_in = append_graphid(value->edges_in, edge_id);

    return found;
}

/* helper routine to load all vertices into the GRAPH global vertex hashtable */
static void load_vertex_hashtable(graph_context *ggctx)
{
    Oid graph_oid;
    Oid graph_namespace_oid;
    Snapshot snapshot;
    List *vertex_label_names = NIL;
    ListCell *lc;

    /* get the specific graph OID and namespace (schema) OID */
    graph_oid = ggctx->graph_oid;
    graph_namespace_oid = get_namespace_oid(ggctx->graph_name, false);
    /* get the active snapshot */
    snapshot = GetActiveSnapshot();
    /* get the names of all of the vertex label tables */
    vertex_label_names = get_ag_labels_names(snapshot, graph_oid,
                                             LABEL_TYPE_VERTEX);
    /* go through all vertex label tables in list */
    foreach (lc, vertex_label_names)
    {
        Relation graph_vertex_label;
        TableScanDesc scan_desc;
        HeapTuple tuple;
        char *vertex_label_name;
        Oid vertex_label_table_oid;
        TupleDesc tupdesc;

        /* get the vertex label name */
        vertex_label_name = lfirst(lc);
        /* get the vertex label name's OID */
        vertex_label_table_oid = get_relname_relid(vertex_label_name,
                                                   graph_namespace_oid);
        /* open the relation (table) and begin the scan */
        graph_vertex_label = table_open(vertex_label_table_oid, ShareLock);
        scan_desc = table_beginscan(graph_vertex_label, snapshot, 0, NULL);
        /* get the tupdesc - we don't need to release this one */
        tupdesc = RelationGetDescr(graph_vertex_label);
        /* bail if the number of columns differs */
        if (tupdesc->natts != 2)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                     errmsg("Invalid number of attributes for %s.%s",
                     ggctx->graph_name, vertex_label_name)));
        }
        /* get all tuples in table and insert them into graph hashtables */
        while((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
        {
            graphid vertex_id;
            Datum vertex_properties;
            bool inserted = false;

            /* something is wrong if this isn't true */
            Assert(HeapTupleIsValid(tuple));
            /* get the vertex id */
            vertex_id = DatumGetInt64(column_get_datum(tupdesc, tuple, 0, "id",
                                                       GRAPHIDOID, true));
            /* get the vertex properties datum */
            vertex_properties = column_get_datum(tupdesc, tuple, 1,
                                                 "properties", AGTYPEOID, true);

            /* insert vertex into vertex hashtable */
            inserted = insert_vertex_entry(ggctx, vertex_id,
                                           vertex_label_table_oid,
                                           vertex_properties);

            /* this insert must not fail, it means there is a duplicate */
            if (!inserted)
            {
                 elog(ERROR, "insert_vertex_entry: failed due to duplicate");
            }
        }

        /* end the scan and close the relation */
        table_endscan(scan_desc);
        table_close(graph_vertex_label, ShareLock);
    }
}

/*
 * Helper function to load all of the GRAPH global hashtables (vertex & edge)
 * for the current global context.
 */
static void load_GRAPH_global_hashtables(graph_context *ggctx)
{
    /* initialize statistics */
    ggctx->num_loaded_vertices = 0;
    ggctx->num_loaded_edges = 0;

    /* insert all of our vertices */
    load_vertex_hashtable(ggctx);

    /* insert all of our edges */
    load_edge_hashtable(ggctx);
}

/*
 * Helper routine to load all edges into the GRAPH global edge and vertex
 * hashtables.
 */
static void load_edge_hashtable(graph_context *ggctx)
{
    Oid graph_oid;
    Oid graph_namespace_oid;
    Snapshot snapshot;
    List *edge_label_names = NIL;
    ListCell *lc;

    /* get the specific graph OID and namespace (schema) OID */
    graph_oid = ggctx->graph_oid;
    graph_namespace_oid = get_namespace_oid(ggctx->graph_name, false);
    /* get the active snapshot */
    snapshot = GetActiveSnapshot();
    /* get the names of all of the edge label tables */
    edge_label_names = get_ag_labels_names(snapshot, graph_oid,
                                           LABEL_TYPE_EDGE);
    /* go through all edge label tables in list */
    foreach (lc, edge_label_names)
    {
        Relation graph_edge_label;
        TableScanDesc scan_desc;
        HeapTuple tuple;
        char *edge_label_name;
        Oid edge_label_table_oid;
        TupleDesc tupdesc;

        /* get the edge label name */
        edge_label_name = lfirst(lc);
        /* get the edge label name's OID */
        edge_label_table_oid = get_relname_relid(edge_label_name,
                                                 graph_namespace_oid);
        /* open the relation (table) and begin the scan */
        graph_edge_label = table_open(edge_label_table_oid, ShareLock);
        scan_desc = table_beginscan(graph_edge_label, snapshot, 0, NULL);
        /* get the tupdesc - we don't need to release this one */
        tupdesc = RelationGetDescr(graph_edge_label);
        /* bail if the number of columns differs */
        if (tupdesc->natts != 4)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                     errmsg("Invalid number of attributes for %s.%s",
                     ggctx->graph_name, edge_label_name)));
        }
        /* get all tuples in table and insert them into graph hashtables */
        while((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
        {
            graphid edge_id;
            graphid edge_vertex_start_id;
            graphid edge_vertex_end_id;
            Datum edge_properties;
            bool inserted = false;

            /* something is wrong if this isn't true */
            Assert(HeapTupleIsValid(tuple));
            /* get the edge id */
            edge_id = DatumGetInt64(column_get_datum(tupdesc, tuple, 0, "id",
                                                     GRAPHIDOID, true));
            /* get the edge start_id (start vertex id) */
            edge_vertex_start_id = DatumGetInt64(column_get_datum(tupdesc,
                                                                  tuple, 1,
                                                                  "start_id",
                                                                  GRAPHIDOID,
                                                                  true));
            /* get the edge end_id (end vertex id)*/
            edge_vertex_end_id = DatumGetInt64(column_get_datum(tupdesc, tuple,
                                                                2, "end_id",
                                                                GRAPHIDOID,
                                                                true));
            /* get the edge properties datum */
            edge_properties = column_get_datum(tupdesc, tuple, 3, "properties",
                                               AGTYPEOID, true);

            /* insert edge into edge hashtable */
            inserted = insert_edge(ggctx, edge_id, edge_properties,
                                   edge_vertex_start_id, edge_vertex_end_id,
                                   edge_label_table_oid);

            /* this insert must not fail */
            if (!inserted)
            {
                 elog(ERROR, "insert_edge: failed to insert");
            }

            /* insert the edge into the start and end vertices edge lists */
            inserted = insert_vertex_edge(ggctx, edge_vertex_start_id,
                                          edge_vertex_end_id, edge_id);
            /* this insert must not fail */
            if (!inserted)
            {
                 elog(ERROR, "insert_vertex_edge: failed to insert");
            }
        }

        /* end the scan and close the relation */
        table_endscan(scan_desc);
        table_close(graph_edge_label, ShareLock);
    }
}

/*
 * Helper function to freeze the GRAPH global hashtables from additional
 * inserts. This may, or may not, be useful. Currently, these hashtables are
 * only seen by the creating process and only for reading.
 */
static void freeze_GRAPH_global_hashtables(graph_context *ggctx)
{
    hash_freeze(ggctx->vertex_hashtable);
    hash_freeze(ggctx->edge_hashtable);
}

/*
 * Helper function to free the entire specified GRAPH global context. After
 * running this you should not use the pointer in ggctx.
 */
static void free_specific_graph_context(graph_context *ggctx)
{
    GraphIdNode *curr_vertex = NULL;

    /* don't do anything if NULL */
    if (ggctx == NULL)
    {
        return;
    }

    /* free the graph name */
    pfree(ggctx->graph_name);
    ggctx->graph_name = NULL;

    ggctx->graph_oid = InvalidOid;
    ggctx->next = NULL;

    /* free the vertex edge lists, starting with the head */
    curr_vertex = peek_stack_head(ggctx->vertices);
    while (curr_vertex != NULL)
    {
        GraphIdNode *next_vertex = NULL;
        vertex_entry *value = NULL;
        bool found = false;
        graphid vertex_id;

        /* get the next vertex in the list, if any */
        next_vertex = next_GraphIdNode(curr_vertex);

        /* get the current vertex id */
        vertex_id = get_graphid(curr_vertex);

        /* retrieve the vertex entry */
        value = (vertex_entry *)hash_search(ggctx->vertex_hashtable,
                                            (void *)&vertex_id, HASH_FIND,
                                            &found);
        /* this is bad if it isn't found */
        Assert(found);

        /* free the edge list associated with this vertex */
        free_ListGraphId(value->edges_in);
        free_ListGraphId(value->edges_out);
        free_ListGraphId(value->edges_self);

        value->edges_in = NULL;
        value->edges_out = NULL;
        value->edges_self = NULL;

        /* move to the next vertex */
        curr_vertex = next_vertex;
    }

    /* free the vertices list */
    free_ListGraphId(ggctx->vertices);
    ggctx->vertices = NULL;

    /* free the hashtables */
    hash_destroy(ggctx->vertex_hashtable);
    hash_destroy(ggctx->edge_hashtable);

    ggctx->vertex_hashtable = NULL;
    ggctx->edge_hashtable = NULL;

    /* free the context */
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

    /* we need a higher context, or one that isn't destroyed by SRF exit */
    MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

    /* free the invalidated GRAPH global contexts first */
    prev_ggctx = NULL;
    curr_ggctx = global_graph_contexts;
    while (curr_ggctx) {
        graph_context *next_ggctx = curr_ggctx->next;

        if (is_ggctx_invalid(curr_ggctx)) {
            if (prev_ggctx == NULL)
                global_graph_contexts = next_ggctx;
            else
                prev_ggctx->next = curr_ggctx->next;

            free_specific_graph_context(curr_ggctx);
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

    create_GRAPH_global_hashtables(new_ggctx);
    load_GRAPH_global_hashtables(new_ggctx);
    freeze_GRAPH_global_hashtables(new_ggctx);

    MemoryContextSwitchTo(oldctx);

    return new_ggctx;
}

/*
 * Helper function to retrieve a vertex_entry from the graph's vertex hash
 * table. If there isn't one, it returns a NULL. The latter is necessary for
 * checking if the vsid and veid entries exist.
 */
vertex_entry *get_vertex_entry(graph_context *ggctx, graphid vertex_id)
{
    vertex_entry *ve = NULL;
    bool found = false;

    /* retrieve the current vertex entry */
    ve = (vertex_entry *)hash_search(ggctx->vertex_hashtable,
                                     (void *)&vertex_id, HASH_FIND, &found);
    return ve;
}

/* helper function to retrieve an edge_entry from the graph's edge hash table */
edge_entry *get_edge_entry(graph_context *ggctx, graphid edge_id)
{
    edge_entry *ee = NULL;
    bool found = false;

    /* retrieve the current edge entry */
    ee = (edge_entry *)hash_search(ggctx->edge_hashtable, (void *)&edge_id,
                                   HASH_FIND, &found);
    /* it should be found, otherwise we have problems */
    Assert(found);

    return ee;
}

/*
 * Helper function to find the graph_context used by the specified
 * graph_oid. If not found, it returns NULL.
 */
graph_context *find_graph_context(Oid graph_oid)
{
    graph_context *ggctx = NULL;

    /* get the root */
    ggctx = global_graph_contexts;

    while(ggctx != NULL)
    {
        /* if we found it return it */
        if (ggctx->graph_oid == graph_oid)
        {
            return ggctx;
        }

        /* advance to the next context */
        ggctx = ggctx->next;
    }

    /* we did not find it so return NULL */
    return NULL;
}

/* graph vertices accessor */
ListGraphId *get_graph_vertices(graph_context *ggctx)
{
    return ggctx->vertices;
}

/* vertex_entry accessor functions */
graphid get_vertex_entry_id(vertex_entry *ve)
{
    return ve->vertex_id;
}

ListGraphId *get_vertex_entry_edges_in(vertex_entry *ve)
{
    return ve->edges_in;
}

ListGraphId *get_vertex_entry_edges_out(vertex_entry *ve)
{
    return ve->edges_out;
}

ListGraphId *get_vertex_entry_edges_self(vertex_entry *ve)
{
    return ve->edges_self;
}


Oid get_vertex_entry_label_table_oid(vertex_entry *ve)
{
    return ve->vertex_label_table_oid;
}

Datum get_vertex_entry_properties(vertex_entry *ve)
{
    return ve->vertex_properties;
}

/* edge_entry accessor functions */
graphid get_edge_entry_id(edge_entry *ee)
{
    return ee->edge_id;
}

Oid get_edge_entry_label_table_oid(edge_entry *ee)
{
    return ee->edge_label_table_oid;
}

Datum get_edge_entry_properties(edge_entry *ee)
{
    return ee->edge_properties;
}

graphid get_edge_entry_start_vertex_id(edge_entry *ee)
{
    return ee->start_vertex_id;
}

graphid get_edge_entry_end_vertex_id(edge_entry *ee)
{
    return ee->end_vertex_id;
}
