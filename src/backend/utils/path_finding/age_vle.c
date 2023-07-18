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
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "utils/lsyscache.h"

#include "utils/age_vle.h"
#include "catalog/ag_graph.h"
#include "utils/graphid.h"
#include "utils/queue.h"
#include "nodes/cypher_nodes.h"
#include "utils/vertex.h"
#include "utils/edge.h"
#include "utils/variable_edge.h"

// defines 
#define GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc) \
            (graphid *) (&vpc->graphid_array_data)
#define EDGE_STATE_HTAB_NAME "Edge state "
#define EDGE_STATE_HTAB_INITIAL_SIZE 100000
#define EXISTS_HTAB_NAME "known edges"
#define EXISTS_HTAB_NAME_INITIAL_SIZE 1000
#define MAXIMUM_NUMBER_OF_CACHED_LOCAL_CONTEXTS 5

// edge state entry for the edge_state_hashtable 
typedef struct edge_state_entry
{
    graphid edge_id;               // edge id, it is also the hash key 
    bool used_in_path;             // like visited but more descriptive 
    bool has_been_matched;         // have we checked for a  match 
    bool matched;                  // is it a match 
} edge_state_entry;

// path function to use.
typedef enum
{                                  // Given a path (u)-[e]-(v)                
    VLE_FUNCTION_PATHS_FROM,       // Paths from a (u) without a provided (v) 
    VLE_FUNCTION_PATHS_BETWEEN,    // Paths between a (u) and a provided (v)  
    VLE_FUNCTION_NONE
} VLE_path_function;

typedef struct path_finding_context
{
    char *graph_name;              // name of the graph 
    Oid graph_oid;                 // graph oid for searching 
    graph_context *ggctx;   // global graph context pointer 
    graphid vsid;                  // starting vertex id 
    graphid veid;                  // ending vertex id 
    char *label_name;         // edge label name for match 
    gtype *properties; // edge property constraint as gtype 
    int64 lidx;                    // lower (start) bound index 
    int64 uidx;                    // upper (end) bound index 
    bool uidx_infinite;            // flag if the upper bound is omitted 
    cypher_rel_dir edge_direction; // the direction of the edge 
    HTAB *edge_state_hashtable;    // local state hashtable for our edges 
    HTAB *exists_hash;
    Queue *dfs_vertex_queue; // dfs queue for vertices 
    Queue *dfs_edge_queue;   // dfs queue for edges 
    Queue *dfs_path_queue;   // dfs queue containing the path 
    VLE_path_function path_function; // which path function to use 
    QueueNode *next_vertex;      // for VLE_FUNCTION_PATHS_TO 
    struct path_finding_context *next;  // the next chained path_finding_context 
} path_finding_context;

/*
 * Container to hold the graphid array that contains one valid path. This
 * structure will allow it to be easily passed as an GTYPE pointer. The
 * structure is set up to contains a BINARY container that can be accessed by
 * functions that need to process the path.
 */
typedef struct path_container
{
    char vl_len_[4]; // Do not touch this field! 
    uint32 header;
    uint32 graph_oid;
    int64 graphid_array_size;
    int64 container_size_bytes;
    graphid graphid_array_data;
} path_container;

// gtype functions 
static bool is_an_edge_match(path_finding_context *path_ctx, edge_entry *ee);
// VLE local context functions 
static path_finding_context *build_vle_context(FunctionCallInfo fcinfo, FuncCallContext *funcctx);
static void create_hashtable(path_finding_context *path_ctx);
// VLE graph traversal functions 
static edge_state_entry *get_edge_state(path_finding_context *path_ctx, graphid edge_id);
// graphid data structures 
static void load_initial_dfs_queues(path_finding_context *path_ctx);
static bool dfs_find_a_path_between(path_finding_context *path_ctx);
static bool dfs_find_a_path_from(path_finding_context *path_ctx);
static bool do_vsid_and_veid_exist(path_finding_context *path_ctx);
static void add_valid_vertex_edges(path_finding_context *path_ctx, graphid vertex_id);
static graphid get_next_vertex(path_finding_context *path_ctx, edge_entry *ee);
static bool is_edge_in_path(path_finding_context *path_ctx, graphid edge_id);
// VLE path and edge building functions 
static path_container *create_path_container(int64 path_size);
static path_container *build_path_container(path_finding_context *path_ctx);
static gtype_value *build_path(path_container *vpc);
static gtype_value *build_edge_list(path_container *vpc);
VariableEdge *agtv_materialize_vle_path_1(gtype *agt);

static void 
append_to_buffer(StringInfo buffer, const char *data, int len) {
    int offset = reserve_from_buffer(buffer, len);
    memcpy(buffer->data + offset, data, len);
}


// helper function to create the local VLE edge state hashtable. 
static void create_hashtable(path_finding_context *path_ctx) {
    HASHCTL edge_state_ctl;
    char *graph_name = NULL;
    char *eshn = NULL;
    int glen;
    int elen;

    // get the graph name and length 
    graph_name = path_ctx->graph_name;

    // initialize the edge state hashtable 
    MemSet(&edge_state_ctl, 0, sizeof(edge_state_ctl));
    edge_state_ctl.keysize = sizeof(int64);
    edge_state_ctl.entrysize = sizeof(edge_state_entry);
    edge_state_ctl.hash = tag_hash;
    path_ctx->edge_state_hashtable = hash_create(EDGE_STATE_HTAB_NAME, EDGE_STATE_HTAB_INITIAL_SIZE, &edge_state_ctl, HASH_ELEM | HASH_FUNCTION);


    MemSet(&edge_state_ctl, 0, sizeof(edge_state_ctl));
    edge_state_ctl.keysize = sizeof(int64);
    edge_state_ctl.entrysize = sizeof(edge_state_entry);
    edge_state_ctl.hash = tag_hash;

    HASHCTL exists_ctl;
    MemSet(&exists_ctl, 0, sizeof(exists_ctl));
    exists_ctl.keysize = sizeof(int64);
    exists_ctl.entrysize = sizeof(int64);
    exists_ctl.hash = tag_hash;    
    path_ctx->exists_hash = hash_create(EXISTS_HTAB_NAME, 20, &exists_ctl, HASH_ELEM | HASH_FUNCTION);

}

/*
 * Helper function to compare the edge constraint (properties we are looking
 * for in a matching edge) against an edge entry's property.
 */
static bool is_an_edge_match(path_finding_context *path_ctx, edge_entry *ee)
{
    gtype *edge_property = NULL;
    gtype_container *agtc_edge_property = NULL;
    gtype_container *agtc_properties = NULL;
    gtype_iterator *constraint_it = NULL;
    gtype_iterator *property_it = NULL;
    char *label_name = NULL;
    int num_propertiess = 0;
    int num_edge_properties = 0;

    // get the number of conditions from the prototype edge 
    num_propertiess = AGT_ROOT_COUNT(path_ctx->properties);

    /*
     * We only care about verifying that we have all of the property conditions.
     * We don't care about extra unmatched properties. If there aren't any edge
     * constraints, then the edge passes by default.
     */
    if (path_ctx->label_name == NULL && num_propertiess == 0)
        return true;

    // get the edge label name from the oid 
    label_name = get_rel_name(get_edge_entry_label_table_oid(ee));
    // get our edge's properties 
    edge_property = DATUM_GET_GTYPE_P(get_edge_entry_properties(ee));
    // get the containers 
    agtc_properties = &path_ctx->properties->root;
    agtc_edge_property = &edge_property->root;
    // get the number of properties in the edge to be matched 
    num_edge_properties = GTYPE_CONTAINER_SIZE(agtc_edge_property);

    /*
     * Check to see if the edge_properties object has AT LEAST as many pairs
     * to compare as the properties object has pairs. If not, it
     * can't possibly match.
     */
    if (num_propertiess > num_edge_properties)
        return false;

    /*
     * Check for a label constraint. If the label name is NULL, there isn't one.
     */
    if (path_ctx->label_name != NULL && strcmp(path_ctx->label_name, label_name) != 0)
        return false;

    // get the iterators 
    constraint_it = gtype_iterator_init(agtc_properties);
    property_it = gtype_iterator_init(agtc_edge_property);

    // return the value of deep contains 
    return gtype_deep_contains(&property_it, &constraint_it);
}

// helper function to check if our start and end vertices exist 
static bool do_vsid_and_veid_exist(path_finding_context *path_ctx)
{
    // if we are only using the starting vertex 
    if (path_ctx->path_function == VLE_FUNCTION_PATHS_FROM)
        return (get_vertex_entry(path_ctx->ggctx, path_ctx->vsid) != NULL);

    // if we are using both start and end 
    return ((get_vertex_entry(path_ctx->ggctx, path_ctx->vsid) != NULL) && (get_vertex_entry(path_ctx->ggctx, path_ctx->veid) != NULL));
}

// load the initial edges into the dfs_edge_queue 
static void load_initial_dfs_queues(path_finding_context *path_ctx)
{
    if (!do_vsid_and_veid_exist(path_ctx))
        return;

    // add in the edges for the start vertex 
    add_valid_vertex_edges(path_ctx, path_ctx->vsid);
}

/*
 * build the local VLE context.
 */
static path_finding_context *build_vle_context(FunctionCallInfo fcinfo, FuncCallContext *funcctx) {
    MemoryContext oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // get the graph name - this is a required argument 
    gtype_value *agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(0), AGTV_STRING, true);
    char *graph_name = pnstrdup(agtv_temp->val.string.val, agtv_temp->val.string.len);
    Oid graph_oid = get_graph_oid(graph_name);

    /*
     * Create or retrieve the GRAPH global context for this graph. This function
     * will also purge off invalidated contexts.
    */
    graph_context *ggctx = manage_graph_contexts(graph_name, graph_oid);

    // allocate and initialize local VLE context 
    path_finding_context *path_ctx = palloc0(sizeof(path_finding_context));

    // set the graph name and id 
    path_ctx->graph_name = graph_name;
    path_ctx->graph_oid = graph_oid;

    // set the global context referenced by this local VLE context 
    path_ctx->ggctx = ggctx;

    // initialize the next vertex, in this case the first 
    path_ctx->next_vertex = peek_queue_head(get_graph_vertices(ggctx));
    Assert(path_ctx->next_vertex);
    
    // start id
  //  agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(1), AGTV_VERTEX, false);
    vertex *v = AG_GET_ARG_VERTEX(1);
    path_ctx->vsid = *((int64 *)(&v->children[0]));
/*
    if (agtv_temp->type == AGTV_VERTEX)
        agtv_temp = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_temp, "id");
    else if (agtv_temp == NULL || agtv_temp->type != AGTV_INTEGER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid start id")));
*/
 //   path_ctx->vsid = agtv_temp->val.int_value;

    // end id - determines which path function is used.
    if (PG_ARGISNULL(2)) {
            path_ctx->path_function = VLE_FUNCTION_PATHS_FROM;
        path_ctx->veid = 0;
    } else {
	vertex *v = AG_GET_ARG_VERTEX(2);
	path_ctx->veid = *((int64 *)(&v->children[0]));
/*
        agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(2), AGTV_VERTEX, false);
	if (agtv_temp->type == AGTV_VERTEX)
            agtv_temp = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_temp, "id");
        else if (agtv_temp->type != AGTV_INTEGER)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid end id")));
  */      
        path_ctx->path_function = VLE_FUNCTION_PATHS_BETWEEN;
  //      path_ctx->veid = agtv_temp->val.int_value;
    }

    // get the VLE edge prototype 
    agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(3), AGTV_EDGE, true);

    // get the edge prototype's property conditions 
    gtype_value *agtv_object = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_temp, "properties");
    // store the properties as an gtype 
    path_ctx->properties = gtype_value_to_gtype(agtv_object);

    // get the edge prototype's label name 
    agtv_temp = GET_GTYPE_VALUE_OBJECT_VALUE(agtv_temp, "label");
    if (agtv_temp->type == AGTV_STRING && agtv_temp->val.string.len != 0)
        path_ctx->label_name = pnstrdup(agtv_temp->val.string.val, agtv_temp->val.string.len);
    else
        path_ctx->label_name = NULL;

    // get the left range index 
    if (PG_ARGISNULL(4) || is_gtype_null(AG_GET_ARG_GTYPE_P(4))) {
        path_ctx->lidx = 1;
    } else {
        agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(4), AGTV_INTEGER, true);
        path_ctx->lidx = agtv_temp->val.int_value;
    }

    // get the right range index. NULL means infinite 
    if (PG_ARGISNULL(5) || is_gtype_null(AG_GET_ARG_GTYPE_P(5))) {
        path_ctx->uidx_infinite = true;
        path_ctx->uidx = 0;
    } else {
        agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(5), AGTV_INTEGER, true);
        path_ctx->uidx = agtv_temp->val.int_value;
        path_ctx->uidx_infinite = false;
    }
    // get edge direction 
    agtv_temp = get_gtype_value("age_vle", AG_GET_ARG_GTYPE_P(6), AGTV_INTEGER, true);
    path_ctx->edge_direction = agtv_temp->val.int_value;

    create_hashtable(path_ctx);

    // initialize the dfs queues 
    path_ctx->dfs_vertex_queue = new_graphid_queue();
    path_ctx->dfs_edge_queue = new_graphid_queue();
    path_ctx->dfs_path_queue = new_graphid_queue();

    // load in the starting edge(s) 
    load_initial_dfs_queues(path_ctx);

    path_ctx->next = NULL;

    MemoryContextSwitchTo(oldctx);

    return path_ctx;
}

/*
 * Helper function to get the specified edge's state. If it does not find it, it
 * creates and initializes it.
 */
static edge_state_entry *get_edge_state(path_finding_context *path_ctx, graphid edge_id) {
    edge_state_entry *ese = NULL;
    bool found = false;

    // retrieve the edge_state_entry from the edge state hashtable 
    ese = (edge_state_entry *)hash_search(path_ctx->edge_state_hashtable, (void *)&edge_id, HASH_ENTER, &found);

    // if it isn't found, it needs to be created and initialized 
    if (!found) {
        // the edge id is also the hash key for resolving collisions 
        ese->edge_id = edge_id;
        ese->used_in_path = false;
        ese->has_been_matched = false;
        ese->matched = false;
    }
    return ese;
}

/*
 * Helper function to get the id of the next vertex to move to. This is to
 * simplify finding the next vertex due to the VLE edge's direction.
 */
static graphid get_next_vertex(path_finding_context *path_ctx, edge_entry *ee)
{
    graphid terminal_vertex_id;

    // get the result based on the specified VLE edge direction 
    switch (path_ctx->edge_direction) {
        case CYPHER_REL_DIR_RIGHT:
            terminal_vertex_id = get_end_id(ee);
            break;

        case CYPHER_REL_DIR_LEFT:
            terminal_vertex_id = get_start_id(ee);
            break;

        case CYPHER_REL_DIR_NONE:
        {
            Queue *vertex_queue = NULL;
            graphid parent_vertex_id;

            vertex_queue = path_ctx->dfs_vertex_queue;
            /*
             * Get the parent vertex of this edge. When we are looking at edges
             * as bi-directional, where we go to next depends on where we came
             * from. This is because we can go against an edge.
             */
            parent_vertex_id = PEEK_GRAPHID_STACK(vertex_queue);
            // find the terminal vertex 
            if (get_start_id(ee) == parent_vertex_id)
                terminal_vertex_id = get_end_id(ee);
            else if (get_end_id(ee) == parent_vertex_id)
                terminal_vertex_id = get_start_id(ee);
            else
                elog(ERROR, "get_next_vertex: no parent match");

            break;
        }

        default:
            elog(ERROR, "get_next_vertex: unknown edge direction");
    }

    return terminal_vertex_id;
}

/*
 * Helper function to find one path BETWEEN two vertices.
 *
 * Note: On the very first entry into this function, the starting vertex's edges
 * should have already been loaded into the edge queue (this should have been
 * done by the SRF initialization phase).
 *
 * This function will always return on either a valid path found (true) or none
 * found (false). If one is found, the position (vertex & edge) will still be in
 * the queue. Each successive invocation within the SRF will then look for the
 * next available path until there aren't any left.
 */
static bool dfs_find_a_path_between(path_finding_context *path_ctx)
{
    Assert(path_ctx);

    Queue *vertex_queue = path_ctx->dfs_vertex_queue;
    Queue *edge_queue = path_ctx->dfs_edge_queue;
    Queue *path_queue = path_ctx->dfs_path_queue;
    graphid end_vertex_id = path_ctx->veid;

    // while we have edges to process 
    while (!(IS_GRAPHID_STACK_EMPTY(edge_queue))) {
        graphid edge_id;
        graphid next_vertex_id;
        edge_state_entry *ese = NULL;
        edge_entry *ee = NULL;
        bool found = false;

        // get an edge, but leave it on the queue for now 
        edge_id = PEEK_GRAPHID_STACK(edge_queue);
        // get the edge's state 
        ese = get_edge_state(path_ctx, edge_id);
        /*
         * If the edge is already in use, it means that the edge is in the path.
         * So, we need to see if it is the last path entry (we are backing up -
         * we need to remove the edge from the path queue and reset its state
         * and from the edge queue as we are done with it) or an interior edge
         * in the path (loop - we need to remove the edge from the edge queue
         * and start with the next edge).
         */
        if (ese->used_in_path) {
            graphid path_edge_id;

            // get the edge id on the top of the path queue (last edge) 
            path_edge_id = PEEK_GRAPHID_STACK(path_queue);
            /*
             * If the ids are the same, we're backing up. So, remove it from the
             * path queue and reset used_in_path.
             */
            if (edge_id == path_edge_id) {
                pop_graphid_queue(path_queue);
                ese->used_in_path = false;
            }
            // now remove it from the edge queue 
            pop_graphid_queue(edge_queue);
            /*
             * Remove its source vertex, if we are looking at edges as
             * bi-directional. We only maintain the vertex queue when the
             * edge_direction is CYPHER_REL_DIR_NONE. This is to save space
             * and time.
             */
            if (path_ctx->edge_direction == CYPHER_REL_DIR_NONE)
                pop_graphid_queue(vertex_queue);
            // move to the next edge 
            continue;
        }

        /*
         * Mark it and push it on the path queue. There is no need to push it on
         * the edge queue as it is already there.
         */
        ese->used_in_path = true;
        push_graphid_queue(path_queue, edge_id);

        // now get the edge entry so we can get the next vertex to move to 
        ee = get_edge_entry(path_ctx->ggctx, edge_id);
        next_vertex_id = get_next_vertex(path_ctx, ee);

        /*
         * Is this the end of a path that meets our requirements? Is its length
         * within the bounds specified?
         */
        if (next_vertex_id == end_vertex_id && queue_size(path_queue) >= path_ctx->lidx &&
            (path_ctx->uidx_infinite || queue_size(path_queue) <= path_ctx->uidx))
            found = true;
        
	/*
         * If we have found the end vertex but, we are not within our upper
         * bounds, we need to back up. We still need to continue traversing
         * the graph if we aren't within our lower bounds, though.
         */
        if (next_vertex_id == end_vertex_id && !path_ctx->uidx_infinite && queue_size(path_queue) > path_ctx->uidx)
            continue;

        // add in the edges for the next vertex if we won't exceed the bounds 
        if (path_ctx->uidx_infinite || queue_size(path_queue) < path_ctx->uidx)
            add_valid_vertex_edges(path_ctx, next_vertex_id);

        if (found)
            return true;
    }

    return false;
}

/*
 * find one path from a start vertex.
 *
 * Note: On the very first entry into this function, the starting vertex's edges
 * should have already been loaded into the edge queue (this should have been
 * done by the SRF initialization phase).
 *
 * This function will always return on either a valid path found (true) or none
 * found (false). If one is found, the position (vertex & edge) will still be in
 * the queue. Each successive invocation within the SRF will then look for the
 * next available path until there aren't any left.
 */
static bool dfs_find_a_path_from(path_finding_context *path_ctx)
{
    Assert(path_ctx);

    // for ease of reading 
    Queue *vertex_queue = path_ctx->dfs_vertex_queue;
    Queue *edge_queue = path_ctx->dfs_edge_queue;
    Queue *path_queue = path_ctx->dfs_path_queue;

    // while we have edges to process 
    while (!IS_GRAPHID_STACK_EMPTY(edge_queue)) {
        graphid edge_id;
        graphid next_vertex_id;
        edge_state_entry *ese = NULL;
        edge_entry *ee = NULL;
        bool found = false;

        // get an edge, but leave it on the queue for now 
        edge_id = PEEK_GRAPHID_STACK(edge_queue);
        // get the edge's state 
        ese = get_edge_state(path_ctx, edge_id);
        /*
         * If the edge is already in use, it means that the edge is in the path.
         * So, we need to see if it is the last path entry (we are backing up -
         * we need to remove the edge from the path queue and reset its state
         * and from the edge queue as we are done with it) or an interior edge
         * in the path (loop - we need to remove the edge from the edge queue
         * and start with the next edge).
         */
        if (ese->used_in_path) {
            graphid path_edge_id;

            // get the edge id on the top of the path queue (last edge) 
            path_edge_id = PEEK_GRAPHID_STACK(path_queue);
            /*
             * If the ids are the same, we're backing up. So, remove it from the
             * path queue and reset used_in_path.
             */
            if (edge_id == path_edge_id) {
                pop_graphid_queue(path_queue);
                ese->used_in_path = false;
            }
            // now remove it from the edge queue 
            pop_graphid_queue(edge_queue);
            /*
             * Remove its source vertex, if we are looking at edges as
             * un-directional. We only maintain the vertex queue when the
             * edge_direction is CYPHER_REL_DIR_NONE. This is to save space
             * and time.
             */
            if (path_ctx->edge_direction == CYPHER_REL_DIR_NONE)
                pop_graphid_queue(vertex_queue);
            // move to the next edge 
            continue;
        }

        /*
         * Mark it and push it on the path queue. There is no need to push it on
         * the edge queue as it is already there.
         */
        ese->used_in_path = true;
        push_graphid_queue(path_queue, edge_id);

        // now get the edge entry so we can get the next vertex to move to 
        ee = get_edge_entry(path_ctx->ggctx, edge_id);
        next_vertex_id = get_next_vertex(path_ctx, ee);

        /*
         * Is this a path that meets our requirements? Is its length within the * bounds specified?
         */
        if (queue_size(path_queue) >= path_ctx->lidx && (path_ctx->uidx_infinite || queue_size(path_queue) <= path_ctx->uidx))
            found = true;

        // add in the edges for the next vertex if we won't exceed the bounds 
        if (path_ctx->uidx_infinite || queue_size(path_queue) < path_ctx->uidx)
            add_valid_vertex_edges(path_ctx, next_vertex_id);

        if (found)
            return true;
    }

    return false;
}

/*
 * Helper routine to quickly check if an edge_id is in the path queue. It is
 * only meant as a quick check to avoid doing a much more costly hash search for
 * smaller sized lists. But, it is O(n) so it should only be used for small
 * path_queues and where appropriate.
 */
static bool is_edge_in_path(path_finding_context *path_ctx, graphid edge_id) {
    QueueNode *edge = peek_queue_head(path_ctx->dfs_path_queue);

    // go through the path queue, return true if we find the edge 
    while (edge) {
        if (get_graphid(edge) == edge_id)
            return true;

        edge = next_queue_node(edge);
    }

    return false;
}

// add in valid vertex edges as part of the dfs path algorithm.
static void add_valid_vertex_edges(path_finding_context *path_ctx, graphid vertex_id) {
    Queue *edges = NULL;

    // get the vertex entry 
    vertex_entry *ve = get_vertex_entry(path_ctx->ggctx, vertex_id);
    Assert(ve);

    Queue *vertex_queue = path_ctx->dfs_vertex_queue;
    Queue *edge_queue = path_ctx->dfs_edge_queue;

    // set to the first edge for each edge list for the specified direction 
    QueueNode *edge_out = NULL;
    if (path_ctx->edge_direction != CYPHER_REL_DIR_LEFT) {
        edges = get_vertex_entry_edges_out(ve);
        edge_out = (edges != NULL) ? get_list_head(edges) : NULL;
    }

    QueueNode *edge_in = NULL;
    if (path_ctx->edge_direction != CYPHER_REL_DIR_RIGHT) {
        edges = get_vertex_entry_edges_in(ve);
        edge_in = (edges != NULL) ? get_list_head(edges) : NULL;
    }

    // set to the first selfloop edge 
    edges = get_vertex_entry_edges_self(ve);
    QueueNode *edge_self = (edges != NULL) ? get_list_head(edges) : NULL;

    // add in valid vertex edges 
    while (edge_out || edge_in || edge_self) {
        graphid edge_id;

        // get the edge_id from the next available edge
        if (edge_out)
            edge_id = get_graphid(edge_out);
        else if (edge_in)
            edge_id = get_graphid(edge_in);
        else
            edge_id = get_graphid(edge_self);

        // get the edge entry 
        edge_entry *ee = get_edge_entry(path_ctx->ggctx, edge_id);
        Assert(ee);
            
        // get its state 
        edge_state_entry *ese = get_edge_state(path_ctx, edge_id);
        /*
         * Don't add any edges that we have already seen because they will
         * cause a loop to form.
         */
        if (!ese->used_in_path) {
            // validate the edge if it hasn't been already 
            if (!ese->has_been_matched && is_an_edge_match(path_ctx, ee)) {
                ese->has_been_matched = true;
                ese->matched = true;
            } else if (!ese->has_been_matched) {
                ese->has_been_matched = true;
                ese->matched = false;
            }

            if (ese->has_been_matched && ese->matched) {
                /*
                 * We need to maintain our source vertex for each edge added
                 * if the edge_direction is CYPHER_REL_DIR_NONE. This is due
                 * to the edges having a fixed direction and the dfs
                 * algorithm working strictly through edges. With an
                 * un-directional edge, you don't know the vertex that
                 * you just came from. So, we need to store it.
                 */
                 if (path_ctx->edge_direction == CYPHER_REL_DIR_NONE)
                     push_graphid_queue(vertex_queue, get_vertex_entry_id(ve));
                 push_graphid_queue(edge_queue, edge_id);
            }
        }

        // get the next edge
        if (edge_out)
            edge_out = next_queue_node(edge_out);
        else if (edge_in)
            edge_in = next_queue_node(edge_in);
        else
            edge_self = next_queue_node(edge_self);
    }
}

/*
 * Helper function to create the VLE path container that holds the graphid array
 * containing the found path. The path_size is the total number of vertices and
 * edges in the path.
 */
static path_container *create_path_container(int64 path_size)
{
    path_container *vpc = NULL;
    int container_size_bytes = 0;

    /*
     * For the total container size (in graphids int64s) we need to add the
     * following space (in graphids) to hold each of the following fields -
     *
     *     One for the VARHDRSZ which is a int32 and a pad of 32.
     *     One for both the header and graph oid (they are both 32 bits).
     *     One for the size of the graphid_array_size.
     *     One for the container_size_bytes.
     *
     */
    container_size_bytes = sizeof(graphid) * (path_size + 4);

    // allocate the container 
    vpc = palloc(container_size_bytes);

    // initialze the PG headers 
    SET_VARSIZE(vpc, container_size_bytes);

    // initialize the container 
    vpc->header = AGT_FBINARY | AGT_FBINARY_TYPE_VLE_PATH;
    vpc->graphid_array_size = path_size;
    vpc->container_size_bytes = container_size_bytes;

    return vpc;
}

/*
 * Helper function to build a path_container containing the graphid array
 * from the path_queue. The graphid array will be a complete path (vertices and
 * edges interleaved) -
 *
 *     start vertex, first edge,... nth edge, end vertex
 *
 * The path_container is allocated in such a way as to wrap the array and
 * include the following additional data -
 *
 *     The header is to allow the graphid array to be encoded as an gtype
 *     container of type BINARY. This way the array doesn't need to be
 *     transformed back and forth.
 *
 *     The graph oid to facilitate the retrieval of the correct vertex and edge
 *     entries.
 *
 *     The total number of elements in the array.
 *
 *     The total size of the container for copying.
 */
static path_container *build_path_container(path_finding_context *path_ctx) {
    Queue *queue = path_ctx->dfs_path_queue;
    graphid *graphid_array = NULL;
    QueueNode *edge = NULL;
    graphid vid = 0;

    if (!queue)
        return NULL;

    int ssize = queue_size(queue);

    /*
     * Create the container. Note that the path size will always be 2 times the
     * number of edges plus 1 -> (u)-[e]-(v)
     */
    path_container *vpc = create_path_container((ssize * 2) + 1);

    // set the graph_oid 
    vpc->graph_oid = path_ctx->graph_oid;

    // get the graphid_array from the container 
    graphid_array = GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc);

    // get and store the start vertex 
    vid = path_ctx->vsid;
    graphid_array[0] = vid;

    // get the head of the queue 
    edge = peek_queue_head(queue);

    /*
     * We need to fill in the array from the back to the front. This is due
     * to the order of the path queue - last in first out.
     */
    int index = vpc->graphid_array_size - 2;

    // copy while we have an edge to copy 
    while (edge) {
        // 0 is the vsid, we should never get here 
        Assert(index > 0);

        // store and set to the next edge 
        graphid_array[index] = get_graphid(edge);
        edge = next_queue_node(edge);

        // we need to skip over the interior vertices 
        index -= 2;
    }

    // now add in the interior vertices, starting from the first edge 
    for (index = 1; index < vpc->graphid_array_size - 1; index += 2) {
        edge_entry *ee = get_edge_entry(path_ctx->ggctx, graphid_array[index]);
        
	vid = (vid == get_start_id(ee)) ? get_end_id(ee) : get_start_id(ee);
        
	graphid_array[index+1] = vid;
    }

    // return the container 
    return vpc;
}

// helper function to build a VPC for just the start vertex 

// build an AGTV_ARRAY of edges from an array of graphids.
static gtype_value *build_edge_list(path_container *vpc) {
    gtype_in_state edges_result;

    // get the graph context
    graph_context *ggctx = find_graph_context(vpc->graph_oid);
    Assert(ggctx != NULL);

    // get the graphid_array and size 
    graphid *graphid_array = GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc);
    int graphid_array_size = vpc->graphid_array_size;

    // initialize our gtype array 
    MemSet(&edges_result, 0, sizeof(gtype_in_state));
    edges_result.res = push_gtype_value(&edges_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    for (int index = 1; index < graphid_array_size - 1; index += 2) {
        // get the edge entry from the hashtable 
        edge_entry *ee = get_edge_entry(ggctx, graphid_array[index]);
        // get the label name from the oid 
        char *label_name = get_rel_name(get_edge_entry_label_table_oid(ee));
        // reconstruct the edge 
        gtype_value *agtv_edge = gtype_value_build_edge(get_edge_entry_id(ee), label_name,
                                            get_end_id(ee),
                                            get_start_id(ee),
                                            get_edge_entry_properties(ee));
        // push the edge
        edges_result.res = push_gtype_value(&edges_result.parse_state, WAGT_ELEM, agtv_edge);
    }

    edges_result.res = push_gtype_value(&edges_result.parse_state, WAGT_END_ARRAY, NULL);

    return edges_result.res;
}

// Build an array of type AGTV_PATH from an array of graphids.
static gtype_value *build_path(path_container *vpc) {
    graph_context *ggctx = NULL;
    gtype_in_state path_result;

    ggctx = find_graph_context(vpc->graph_oid);
   
    Assert(ggctx != NULL);

    graphid *graphid_array = GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc);
    int graphid_array_size = vpc->graphid_array_size;

    MemSet(&path_result, 0, sizeof(gtype_in_state));
    path_result.res = push_gtype_value(&path_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    for (int index = 0; index < graphid_array_size; index += 2) {
        // get the vertex entry from the hashtable
	if (index != 0 && index + 1 != graphid_array_size) { 
            vertex_entry *ve = get_vertex_entry(ggctx, graphid_array[index]);
            char *label_name = get_rel_name(get_vertex_entry_label_table_oid(ve));
            gtype_value *agtv_vertex = gtype_value_build_vertex(get_vertex_entry_id(ve),
			                                        label_name,
								get_vertex_entry_properties(ve));
            path_result.res = push_gtype_value(&path_result.parse_state, WAGT_ELEM, agtv_vertex);
	}

	if (index + 1 == graphid_array_size)
		break;

        // get the edge entry from the hashtable 
        edge_entry *ee = get_edge_entry(ggctx, graphid_array[index+1]);
        // get the label name from the oid 
        char *label_name = get_rel_name(get_edge_entry_label_table_oid(ee));
        // reconstruct the edge 
        gtype_value *agtv_edge = gtype_value_build_edge(get_edge_entry_id(ee), label_name,
                                                        get_end_id(ee), get_start_id(ee),
                                                        get_edge_entry_properties(ee));
        // push the edge
        path_result.res = push_gtype_value(&path_result.parse_state, WAGT_ELEM, agtv_edge);
    }

    // close our gtype array 
    path_result.res = push_gtype_value(&path_result.parse_state, WAGT_END_ARRAY, NULL);

    path_result.res->type = AGTV_PARTIAL_PATH;

    return path_result.res;
}

/*
 * function that takes the following input and returns a row called edges
 * of type gtype BINARY path_container (this is an internal structure for
 * returning a graphid array of the path. You need to use internal routines to
 * properly use this data) -
 *
 *     0 - gtype REQUIRED (graph name as string)
 *     1 - gtype REQUIRED (start vertex as a vertex or the integer id)
 *     2 - gtype OPTIONAL (end vertex as a vertex or the integer id)
 *     3 - gtype REQUIRED (edge prototype to match as an edge)
 *     4 - gtype OPTIONAL lidx (lower range index)
 *     5 - gtype OPTIONAL uidx (upper range index)
 *     6 - gtype REQUIRED edge direction (enum) as an integer. REQUIRED
 */
PG_FUNCTION_INFO_V1(age_vle);
Datum age_vle(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    bool found_a_path;
    MemoryContext oldctx;

    // Initialization for the first call to the SRF 
    if (SRF_IS_FIRSTCALL()) {
        // all of these arguments need to be non NULL 
        if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(3) || PG_ARGISNULL(6))
             ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("age_vle: invalid NULL argument passed")));

        funcctx = SRF_FIRSTCALL_INIT();

        funcctx->user_fctx = build_vle_context(fcinfo, funcctx);

        //if (((path_finding_context *)funcctx->user_fctx)->lidx == 0)
          //  SRF_RETURN_NEXT(funcctx, PointerGetDatum(build_path_container(funcctx->user_fctx)));

          //  SRF_RETURN_NEXT(funcctx, PointerGetDatum(gtype_value_to_gtype(agtv_materialize_vle_path(build_path_container(funcctx->user_fctx)))));

    }

    // stuff done on every call of the function 
    funcctx = SRF_PERCALL_SETUP();

    /*
     * All work done in dfs_find_a_path needs to be done in a context that
     * survives multiple SRF calls. So switch to the appropriate context.
     */
    oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // find one path based on specific input 
    switch (((path_finding_context *)funcctx->user_fctx)->path_function)
    {
        case VLE_FUNCTION_PATHS_BETWEEN:
            found_a_path = dfs_find_a_path_between(funcctx->user_fctx);
            break;

        case VLE_FUNCTION_PATHS_FROM:
            found_a_path = dfs_find_a_path_from(funcctx->user_fctx);
            break;
    }


    // switch back to a more volatile context 
    MemoryContextSwitchTo(oldctx);

    /*
     * If we find a path, we need to convert the path_queue into a list that
     * the outside world can use.
     */
    if (found_a_path) {
        path_container *vpc = NULL;
        Assert(((path_finding_context *)funcctx->user_fctx)->dfs_path_queue > 0);

        /*
         * Build the graphid array into a path_container from the
         * path_queue. This will also correct for the path_queue being last
         * in, first out.
         */
        vpc = build_path_container(funcctx->user_fctx);

        // return the result and signal that the function is not yet done 
        SRF_RETURN_NEXT(funcctx, PointerGetDatum(agtv_materialize_vle_path_1(vpc)));
	//SRF_RETURN_NEXT(funcctx, PointerGetDatum(gtype_value_to_gtype(agtv_materialize_vle_path(vpc))));
        //SRF_RETURN_NEXT(funcctx, PointerGetDatum(vpc));
    } else {
	hash_destroy(((path_finding_context *)funcctx->user_fctx)->edge_state_hashtable);
        hash_destroy(((path_finding_context *)funcctx->user_fctx)->exists_hash);
        SRF_RETURN_DONE(funcctx);
    }
}

/*
     StringInfoData buffer;
    initStringInfo(&buffer);
    Datum *args;
    bool *nulls;
    Oid *types;
    prentry nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    int32 size;

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);

    append_to_buffer(&buffer, (char *)&nargs, sizeof(prentry));

    for (int i = 0; i < nargs; i++) {
        if (i % 2 == 1) {
            if (types[i] != VERTEXOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                         errmsg("arguement %i build_traversal() must be a vertex", i)));
            if (i + 1 == nargs)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                         errmsg("VariableEdges must end with an edge")));

            append_to_buffer(&buffer, DATUM_GET_VERTEX(args[i]), VARSIZE_ANY(args[i]));
        }
        else {

            if (types[i] != EDGEOID)
                 ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                         errmsg("arguement %i build_traversal() must be an edge", i)));

            append_to_buffer(&buffer, DATUM_GET_EDGE(args[i]), VARSIZE_ANY(args[i]));
        }
    }

    VariableEdge *p = (VariableEdge *)buffer.data;

    SET_VARSIZE(p, buffer.len);

    AG_RETURN_VARIABLE_EDGE(p);
vertex *create_vertex(graphid id, char *label, gtype *properties);
edge *create_edge(graphid id,graphid start_id,graphid end_id, char *label, gtype *properties);
 */

VariableEdge *agtv_materialize_vle_path_1(gtype *agt) {
    Assert(agt!= NULL);
    Assert(AGT_ROOT_IS_BINARY(agt));
    Assert(AGT_ROOT_BINARY_FLAGS(agt) == AGT_FBINARY_TYPE_VLE_PATH);
/*
    return build_path((path_container *)agt);
i*/

    StringInfoData buffer;
    initStringInfo(&buffer);
    int32 size;

    path_container *vpc = (path_container *)agt;
    graphid *graphid_array = GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc);
    int graphid_array_size = vpc->graphid_array_size;

        graph_context *ggctx = NULL;

    ggctx = find_graph_context(vpc->graph_oid);

    Assert(ggctx != NULL);

    // header
    reserve_from_buffer(&buffer, VARHDRSZ);
    size = graphid_array_size - 2;
    append_to_buffer(&buffer, (char *)&size, sizeof(prentry));
/*
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("%i %i", graphid_array_size, size)));
*/
    int cnt = 0;
    for (int index = 0; index < graphid_array_size; index += 2) {
        // get the vertex entry from the hashtable
        if (index != 0 && index + 1 != graphid_array_size) {
            vertex_entry *ve = get_vertex_entry(ggctx, graphid_array[index]);

	    char *label_name = get_rel_name(get_vertex_entry_label_table_oid(ve));
	    graphid id = get_vertex_entry_id(ve);
	    gtype *prop = DATUM_GET_GTYPE_P(get_vertex_entry_properties(ve));
            Datum d = VERTEX_GET_DATUM(create_vertex(id, label_name, prop));

            append_to_buffer(&buffer, DATUM_GET_VERTEX(d), VARSIZE(d));
            cnt++;
        }
        if (index + 1 == graphid_array_size)
                break;

        // get the edge entry from the hashtable 
        edge_entry *ee = get_edge_entry(ggctx, graphid_array[index+1]);
        
        char *label_name = get_rel_name(get_edge_entry_label_table_oid(ee));
            graphid id = get_vertex_entry_id(ee);
            graphid startid = get_start_id(ee);
	    graphid endid = get_end_id(ee);
	    gtype *prop = DATUM_GET_GTYPE_P(get_edge_entry_properties(ee));
            Datum d = EDGE_GET_DATUM(create_edge(id, startid, endid, label_name, prop));

	    append_to_buffer(&buffer, DATUM_GET_EDGE(d), VARSIZE(d));
        cnt++;
    }

    VariableEdge *p = (VariableEdge *)buffer.data;

    SET_VARSIZE(p, buffer.len);

    return p;
}



gtype_value *agtv_materialize_vle_path(gtype *agt) {
    Assert(agt!= NULL);
    Assert(AGT_ROOT_IS_BINARY(agt));
    Assert(AGT_ROOT_BINARY_FLAGS(agt) == AGT_FBINARY_TYPE_VLE_PATH);
/*
    return build_path((path_container *)agt);
i*/
    path_container *vpc = (path_container *)agt;
    graph_context *ggctx = NULL;
    gtype_in_state path_result;

    ggctx = find_graph_context(vpc->graph_oid);

    Assert(ggctx != NULL);
        
    graphid *graphid_array = GET_GRAPHID_ARRAY_FROM_CONTAINER(vpc);
    int graphid_array_size = vpc->graphid_array_size;
    
    MemSet(&path_result, 0, sizeof(gtype_in_state));
    path_result.res = push_gtype_value(&path_result.parse_state, WAGT_BEGIN_ARRAY, NULL);

    int cnt = 0;
    for (int index = 0; index < graphid_array_size; index += 2) {
        // get the vertex entry from the hashtable
        if (index != 0 && index + 1 != graphid_array_size) {
            vertex_entry *ve = get_vertex_entry(ggctx, graphid_array[index]);
            char *label_name = get_rel_name(get_vertex_entry_label_table_oid(ve));
            gtype_value *agtv_vertex = gtype_value_build_vertex(get_vertex_entry_id(ve),
                                                                label_name,
                                                                get_vertex_entry_properties(ve));
            path_result.res = push_gtype_value(&path_result.parse_state, WAGT_ELEM, agtv_vertex);
	    cnt++;
        }

        if (index + 1 == graphid_array_size)
                break;

        // get the edge entry from the hashtable 
        edge_entry *ee = get_edge_entry(ggctx, graphid_array[index+1]);
        // get the label name from the oid 
        char *label_name = get_rel_name(get_edge_entry_label_table_oid(ee));
        // reconstruct the edge 
        gtype_value *agtv_edge = gtype_value_build_edge(get_edge_entry_id(ee), label_name,
                                                        get_end_id(ee), get_start_id(ee),
                                                        get_edge_entry_properties(ee));
        // push the edge
        path_result.res = push_gtype_value(&path_result.parse_state, WAGT_ELEM, agtv_edge);
        cnt++;
    }

    // close our gtype array 
    path_result.res = push_gtype_value(&path_result.parse_state, WAGT_END_ARRAY, NULL);

    path_result.res->val.array.num_elems = cnt;
    path_result.res->type = AGTV_PARTIAL_PATH;

    return path_result.res;

}

PG_FUNCTION_INFO_V1(age_match_vle_terminal_edge_start);
Datum age_match_vle_terminal_edge_start(PG_FUNCTION_ARGS) {
    Datum *args;
    bool *nulls;
    Oid *types;

    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    gtype *agt_arg_path = AG_GET_ARG_GTYPE_P(1);

    if (GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) == 0)
        PG_RETURN_BOOL(false);

    Assert(AGT_IS_PARTIAL_PATH(agt_arg_path));

    gtype_value *vertex = get_ith_gtype_value_from_container(&agt_arg_path->root.children[2], 0);
    if (!vertex)
        PG_RETURN_BOOL(false);

    // start id
    graphid vsid;
    if (types[0] == GTYPEOID) {
        gtype *agt_arg_vsid = DATUM_GET_GTYPE_P(args[0]);

        gtype_value *agtv_temp = get_ith_gtype_value_from_container(&agt_arg_vsid->root, 0);

        Assert(agtv_temp->type == AGTV_INTEGER);
        vsid = agtv_temp->val.int_value;
    }
    else if (types[0] == GRAPHIDOID) {
        vsid = DATUM_GET_GRAPHID(args[0]);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("match_vle_terminal_edge() arguement 1 must be an gtype integer or a graphid")));
    }

    // compare the path beginning or end points 
    PG_RETURN_BOOL(vertex->val.object.pairs[3].value.val.int_value == vsid);
}


PG_FUNCTION_INFO_V1(age_match_vle_terminal_edge_left_check_end);
Datum age_match_vle_terminal_edge_left_check_end(PG_FUNCTION_ARGS) {
    Datum *args;
    bool *nulls;
    Oid *types;

    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    gtype *agt_arg_path = AG_GET_ARG_GTYPE_P(1);

    if (GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) == 0)
        PG_RETURN_BOOL(false);

    Assert(AGT_IS_PARTIAL_PATH(agt_arg_path));

    gtype_value *vertex = get_ith_gtype_value_from_container(&agt_arg_path->root.children[2], 0);
    if (!vertex)
        PG_RETURN_BOOL(false);

    // start id
    graphid vsid;
    if (types[0] == GTYPEOID) {
        gtype *agt_arg_vsid = DATUM_GET_GTYPE_P(args[0]);
    
        gtype_value *agtv_temp = get_ith_gtype_value_from_container(&agt_arg_vsid->root, 0);

        Assert(agtv_temp->type == AGTV_INTEGER);
        vsid = agtv_temp->val.int_value;
    }
    else if (types[0] == GRAPHIDOID) {
        vsid = DATUM_GET_GRAPHID(args[0]);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("match_vle_terminal_edge() arguement 1 must be an gtype integer or a graphid")));
    }
    
    // compare the path beginning or end points 
    PG_RETURN_BOOL(vertex->val.object.pairs[2].value.val.int_value == vsid);
}   

PG_FUNCTION_INFO_V1(age_match_vle_terminal_right_check_start);
Datum age_match_vle_terminal_right_check_start(PG_FUNCTION_ARGS) {
    Datum *args;
    bool *nulls;
    Oid *types;
    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    gtype *agt_arg_path = AG_GET_ARG_GTYPE_P(1);

    Assert(AGT_IS_PARTIAL_PATH(agt_arg_path));

    if (GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) == 0)
        PG_RETURN_BOOL(false);

    gtype_value *vertex = get_ith_gtype_value_from_container(&agt_arg_path->root.children[2], GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) - 1);

    if (!vertex)
        PG_RETURN_BOOL(false);

    // start id
    graphid vsid;
    if (types[0] == GTYPEOID) {
        gtype *agt_arg_vsid = DATUM_GET_GTYPE_P(args[0]);

        gtype_value *agtv_temp = get_ith_gtype_value_from_container(&agt_arg_vsid->root, 0);

        Assert(agtv_temp->type == AGTV_INTEGER);
        vsid = agtv_temp->val.int_value;
    }
    else if (types[0] == GRAPHIDOID) {
        vsid = DATUM_GET_GRAPHID(args[0]);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("match_vle_terminal_edge() arguement 1 must be an gtype integer or a graphid")));
    }

    // compare the path beginning or end points 
    PG_RETURN_BOOL(vertex->val.object.pairs[3].value.val.int_value == vsid);
}


PG_FUNCTION_INFO_V1(age_match_vle_terminal_edge_end);
Datum age_match_vle_terminal_edge_end(PG_FUNCTION_ARGS) {
    Datum *args;
    bool *nulls;
    Oid *types;
    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
    gtype *agt_arg_path = AG_GET_ARG_GTYPE_P(1);

    
    Assert(AGT_IS_PARTIAL_PATH(agt_arg_path));

    if (GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) == 0)
        PG_RETURN_BOOL(false);

    gtype_value *vertex = get_ith_gtype_value_from_container(&agt_arg_path->root.children[2], GTYPE_CONTAINER_SIZE((gtype_container *)&agt_arg_path->root.children[2]) - 1);

    if (!vertex)
        PG_RETURN_BOOL(false);

    // start id
    graphid vsid;
    if (types[0] == GTYPEOID) {
        gtype *agt_arg_vsid = DATUM_GET_GTYPE_P(args[0]);

        gtype_value *agtv_temp = get_ith_gtype_value_from_container(&agt_arg_vsid->root, 0);

        Assert(agtv_temp->type == AGTV_INTEGER);
        vsid = agtv_temp->val.int_value;
    }
    else if (types[0] == GRAPHIDOID) {
        vsid = DATUM_GET_GRAPHID(args[0]);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("match_vle_terminal_edge() arguement 1 must be an gtype integer or a graphid")));
    }

    // compare the path beginning or end points 
    PG_RETURN_BOOL(vertex->val.object.pairs[2].value.val.int_value == vsid);
}


static const gtype_value agtv_nstr = {
    .type = AGTV_STRING,
    .val.string = {0, NULL}
};

static const gtype_value agtv_zero = {
    .type = AGTV_INTEGER,
    .val.int_value = 0
};

// PG helper function to build an gtype (Datum) edge for matching 
PG_FUNCTION_INFO_V1(age_build_vle_match_edge);
Datum age_build_vle_match_edge(PG_FUNCTION_ARGS) {
    gtype_in_state result;
    memset(&result, 0, sizeof(gtype_in_state));

    // start object 
    result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);

    // id 
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, &agtv_zero);
    
    // label 
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("label"));
    if (!PG_ARGISNULL(0)) {
        gtype_value *agtv_temp = get_gtype_value("build_vle_match_edge", AG_GET_ARG_GTYPE_P(0), AGTV_STRING, true);
        result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, agtv_temp);
    } else {
        result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, &agtv_nstr);
    }

    // end_id 
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("end_id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, &agtv_zero);
    // start_id 
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("start_id"));
    result.res = push_gtype_value(&result.parse_state, WAGT_VALUE, &agtv_zero);

    // properties 
    result.res = push_gtype_value(&result.parse_state, WAGT_KEY, string_to_gtype_value("properties"));
    if (!PG_ARGISNULL(1)) {
        gtype *properties = NULL;

        properties = AG_GET_ARG_GTYPE_P(1);

        if (!AGT_ROOT_IS_OBJECT(properties))
            PG_RETURN_NULL();

        add_gtype((Datum)properties, false, &result, GTYPEOID, false);

    } else {
        result.res = push_gtype_value(&result.parse_state, WAGT_BEGIN_OBJECT, NULL);
        result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);
    }

    result.res = push_gtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    result.res->type = AGTV_EDGE;

    PG_RETURN_POINTER(gtype_value_to_gtype(result.res));
}

/*
 * This function checks the edges in a MATCH clause to see if they are unique or
 * not. Filters out all the paths where the edge uniques rules are not met.
 * Arguements can be a combination of gtype ints and path_containers.
 */
PG_FUNCTION_INFO_V1(_ag_enforce_edge_uniqueness);
Datum _ag_enforce_edge_uniqueness(PG_FUNCTION_ARGS) {
    Datum *args;
    bool *nulls;
    Oid *types;

    int nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

    // configure the hash table 
    HASHCTL exists_ctl;
    MemSet(&exists_ctl, 0, sizeof(exists_ctl));
    exists_ctl.keysize = sizeof(int64);
    exists_ctl.entrysize = sizeof(int64);
    exists_ctl.hash = tag_hash;

    HTAB *exists_hash = hash_create(EXISTS_HTAB_NAME, nargs, &exists_ctl, HASH_ELEM | HASH_FUNCTION);

    // insert arguments into hash table 
    for (int i = 0; i < nargs; i++) {

        if (nulls[i])
             ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("_ag_enforce_edge_uniqueness argument %d must not be NULL", i)));

        if (types[i] == GRAPHIDOID) {
            graphid edge_id = DatumGetInt64(args[i]);

	    bool found;
            int64 *value = (int64 *)hash_search(exists_hash, (void *)&edge_id, HASH_ENTER, &found);

            // if we found it, we're done, we have a duplicate 
            if (found)
                PG_RETURN_BOOL(false);

            *value = edge_id;

            continue;
        }
        else if (types[i] == GTYPEOID) {
            gtype *agt_i = DATUM_GET_GTYPE_P(args[i]);
            gtype_container *array = &agt_i->root.children[2];

            for (int j = 0; i <= GTYPE_CONTAINER_SIZE(array); i +=2) {
                gtype_value *agt = get_ith_gtype_value_from_container(array, j);

                bool found;
                int64 *value = (int64 *)hash_search(exists_hash,
				                    (void *)&agt->val.object.pairs[0].value.val.int_value,
						    HASH_ENTER, &found);

                    if (found)
                        PG_RETURN_BOOL(false);
		
	    }
        }
	else if (types[i] == VARIABLEEDGEOID) {
            VariableEdge *ve = DATUM_GET_VARIABLE_EDGE(args[i]);
            char *ptr = &ve->children[1];
            for (int i = 0; i < ve->children[0]; i++, ptr = ptr + VARSIZE(ptr)) {
                if (i % 2 == 1) {
                    continue;
                } else {
	            edge *e = (edge *)ptr;

                    graphid i =  *((int64 *)(&e->children[0]));
	            bool found;
                    int64 *value = (int64 *)hash_search(exists_hash, (void *)&i, HASH_ENTER, &found);
                     if (found)
                        PG_RETURN_BOOL(false);

	        }
            }

	}
    }

    PG_RETURN_BOOL(true);
}
