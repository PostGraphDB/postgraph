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

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

#include "optimizer/cypher_pathnode.h"
#include "optimizer/cypher_paths.h"
#include "utils/ag_func.h"

typedef enum cypher_clause_kind
{
    CYPHER_CLAUSE_NONE,
    CYPHER_CLAUSE_CREATE,
    CYPHER_CLAUSE_SET,
    CYPHER_CLAUSE_DELETE,
    CYPHER_CLAUSE_MERGE
} cypher_clause_kind;

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;

static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                             RangeTblEntry *rte);
static cypher_clause_kind get_cypher_clause_kind(RangeTblEntry *rte);
static void handle_cypher_create_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);
static void handle_cypher_set_clause(PlannerInfo *root, RelOptInfo *rel,
                                     Index rti, RangeTblEntry *rte);
static void handle_cypher_delete_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);
static void handle_cypher_merge_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);

void set_rel_pathlist_init(void)
{
    prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = set_rel_pathlist;
}

void set_rel_pathlist_fini(void)
{
    set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
}

static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                             RangeTblEntry *rte)
{
    if (prev_set_rel_pathlist_hook)
        prev_set_rel_pathlist_hook(root, rel, rti, rte);

    switch (get_cypher_clause_kind(rte))
    {
    case CYPHER_CLAUSE_CREATE:
        handle_cypher_create_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_SET:
        handle_cypher_set_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_DELETE:
        handle_cypher_delete_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_MERGE:
        handle_cypher_merge_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_NONE:
        break;
    default:
        ereport(ERROR, (errmsg_internal("invalid cypher_clause_kind")));
    }
}

/*
 * Check to see if the rte is a Cypher clause. An rte is only a Cypher clause
 * if it is a subquery, with the last entry in its target list, that is a
 * FuncExpr.
 */
static cypher_clause_kind get_cypher_clause_kind(RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;

    // If it's not a subquery, it's not a Cypher clause.
    if (rte->rtekind != RTE_SUBQUERY)
        return CYPHER_CLAUSE_NONE;

    // Make sure the targetList isn't NULL. NULL means potential EXIST subclause
    if (rte->subquery->targetList == NULL)
        return CYPHER_CLAUSE_NONE;

    // A Cypher clause function is always the last entry.
    te = llast(rte->subquery->targetList);

    // If the last entry is not a FuncExpr, it's not a Cypher clause.
    if (!IsA(te->expr, FuncExpr))
        return CYPHER_CLAUSE_NONE;

    fe = (FuncExpr *)te->expr;

    if (is_oid_ag_func(fe->funcid, CREATE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_CREATE;
    if (is_oid_ag_func(fe->funcid, SET_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_SET;
    if (is_oid_ag_func(fe->funcid, DELETE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_DELETE;
    if (is_oid_ag_func(fe->funcid, MERGE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_MERGE;
    else
        return CYPHER_CLAUSE_NONE;
}

// replace all possible paths with our CustomPath
static void handle_cypher_delete_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    // Add the pattern to the CustomPath
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    // pass the const that holds the data structure to the path.
    custom_private = fe->args;

    cp = create_cypher_delete_path(root, rel, custom_private);

    // Discard any pre-existing paths
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    add_path(rel, (Path *)cp);
}

/*
 * Take the paths possible for the RelOptInfo that represents our
 * _cypher_delete_clause function replace them with our delete clause
 * path. The original paths will be children to the new delete path.
 */
static void handle_cypher_create_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    // Add the pattern to the CustomPath
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    // pass the const that holds the data structure to the path.
    custom_private = fe->args;

    cp = create_cypher_create_path(root, rel, custom_private);

    // Discard any pre-existing paths, they should be under the cp path
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    // Add the new path to the rel.
    add_path(rel, (Path *)cp);
}

#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"

static Material *
make_material(Plan *lefttree)
{
	Material   *node = makeNode(Material);
	Plan	   *plan = &node->plan;
    CustomScan *cs = lefttree;

	plan->targetlist = cs->custom_scan_tlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	return node;
}
/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor usually won't use this info, but it's needed by EXPLAIN.
 * Also copy the parallel-related flags, which the executor *will* use.
 */
static void
copy_generic_path_info(Plan *dest, Path *src)
{
	dest->startup_cost = src->startup_cost;
	dest->total_cost = src->total_cost;
	dest->plan_rows = src->rows;
	dest->plan_width = src->pathtarget->width;
	dest->parallel_aware = src->parallel_aware;
	dest->parallel_safe = src->parallel_safe;
}

static Material *
create_material_plan(PlannerInfo *root, CustomPath *cp)
{
	Material   *plan;
	Plan	   *subplan;

	plan = make_material(cp);

	copy_generic_path_info(&plan->plan, (Path *) cp);

	return plan;
}

MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
{
	MaterialPath *pathnode = makeNode(MaterialPath);

	Assert(subpath->parent == rel);

	pathnode->path.pathtype = T_Material;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->subpath = subpath;

	cost_material(&pathnode->path,
				  subpath->startup_cost,
				  subpath->total_cost,
				  subpath->rows,
				  subpath->pathtarget->width);

	return pathnode;
}

// replace all possible paths with our CustomPath
static void handle_cypher_set_clause(PlannerInfo *root, RelOptInfo *rel,
                                     Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    // Add the pattern to the CustomPath
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    // pass the const that holds the data structure to the path.
    custom_private = fe->args;

    cp = create_cypher_set_path(root, rel, custom_private);

    // Discard any pre-existing paths
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    //add_path(rel, create_material_path(rel, (Path *)cp));
    add_path(rel, (Path *)cp);
}

// replace all possible paths with our CustomPath
static void handle_cypher_merge_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    // Add the pattern to the CustomPath
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    // pass the const that holds the data structure to the path.
    custom_private = fe->args;

    cp = create_cypher_merge_path(root, rel, custom_private);

    // Discard any pre-existing paths
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    add_path(rel, (Path *)cp);
}
