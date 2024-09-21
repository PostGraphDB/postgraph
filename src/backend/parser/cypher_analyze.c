/*
 * PostGraph
 * Copyright (C) 2023 by PostGraph
 *
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
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */

#include "postgraph.h"

#include "utils/probes.h"
#include "tcop/tcopprot.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "utils/builtins.h"

#include "catalog/ag_graph.h"
#include "nodes/ag_nodes.h"
#include "parser/cypher_analyze.h"
#include "parser/cypher_clause.h"
#include "parser/cypher_item.h"
#include "parser/cypher_parse_node.h"
#include "parser/cypher_parser.h"
#include "utils/ag_func.h"
#include "utils/gtype.h"

static post_parse_analyze_hook_type prev_post_parse_analyze_hook;

static void post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static bool convert_cypher_walker(Node *node, ParseState *pstate);
static bool is_rte_cypher(RangeTblEntry *rte);
static bool is_func_cypher(FuncExpr *funcexpr);
static void convert_cypher_to_subquery(RangeTblEntry *rte, ParseState *pstate);
static Name expr_get_const_name(Node *expr);
static const char *expr_get_const_cstring(Node *expr, const char *source_str);
static int get_query_location(const int location, const char *source_str);
static Query *analyze_cypher(List *stmt, ParseState *parent_pstate, const char *query_str, int query_loc, char *graph_name, uint32 graph_oid, Param *params);
static Query *analyze_cypher_and_coerce(List *stmt, RangeTblFunction *rtfunc, ParseState *parent_pstate, const char *query_str, int query_loc, char *graph_name, uint32 graph_oid, Param *params);
static List *cypher_parse(char *string);
static List * cypher_parse_analyze_hook (RawStmt *parsetree, const char *query_string,
					                     Oid *paramTypes, int numParams,
					                     QueryEnvironment *queryEnv);

void
post_parse_analyze_init(void) {
    prev_post_parse_analyze_hook = post_parse_analyze_hook;
    post_parse_analyze_hook = post_parse_analyze;
}

void
post_parse_analyze_fini(void) {
    post_parse_analyze_hook = prev_post_parse_analyze_hook;
}

void parse_init(void){
   parse_hook = cypher_parse;
}

void parse_fini(void){
   parse_hook = NULL;
}

static RawStmt *
makeRawStmt(Node *stmt, int stmt_location)
{
	RawStmt    *rs = makeNode(RawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

static List *cypher_parse(char *string){
	List	   *raw_parsetree_list;

	//TRACE_POSTGRESQL_QUERY_PARSE_START(string);

	//if (log_parser_stats)
		//ResetUsage();
/*
	raw_parsetree_list = raw_parser(string, RAW_PARSE_DEFAULT);

    if (list_length(raw_parsetree_list) != 1)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("cypher can only handle one parse tree at a time")));
    else {
    */
        //RawStmt *rawStmt = linitial(raw_parsetree_list);
        //raw_parsetree_list = list_make1(makeRawStmt(rawStmt->stmt, 0));

        List *stmt = parse_cypher(string);
        raw_parsetree_list = list_make1(makeRawStmt(stmt, 0));


    //}



	if (log_parser_stats)
		ShowUsage("PARSER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass raw parsetrees through copyObject() */
	{
		List	   *new_list = copyObject(raw_parsetree_list);

		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, raw_parsetree_list))
			elog(WARNING, "copyObject() failed to produce an equal raw parse tree");
		else
			raw_parsetree_list = new_list;
	}
#endif

	/*
	 * Currently, outfuncs/readfuncs support is missing for many raw parse
	 * tree nodes, so we don't try to implement WRITE_READ_PARSE_PLAN_TREES
	 * here.
	 */

	//TRACE_POSTGRESQL_QUERY_PARSE_DONE(string);

	return raw_parsetree_list;
}


void parse_analyze_init(void){
   parse_analyze_hook = cypher_parse_analyze_hook;
}

void parse_analyze_fini(void){
   parse_analyze_hook = NULL;
}

/*
 * Creates the function expression that represents the clause. Adds the
 * extensible node that represents the metadata that the clause needs to
 * handle the clause in the execution phase.
 */
static FuncExpr *make_clause_create_graph_func_expr(char *graph_name) {
    Const *c = makeConst(TEXTOID, -1, InvalidOid, strlen(graph_name), CStringGetTextDatum(graph_name), false, false);

    Oid func_oid = get_ag_func_oid("create_graph", 1, TEXTOID);

    return makeFuncExpr(func_oid, VOIDOID, list_make1(c), InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
}

static Query *
cypher_create_graph_utility(ParseState *pstate, const char *graph_name) {
    Query *query;
    TargetEntry *tle;
    FuncExpr *func_expr;

    query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->targetList = NIL;

    func_expr = make_clause_create_graph_func_expr(graph_name);

    // Create the target entry
    tle = makeTargetEntry((Expr *)func_expr, pstate->p_next_resno++, "create_graph", false);
    query->targetList = lappend(query->targetList, tle);

    query->rtable = pstate->p_rtable;
    query->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    return query;
}

static FuncExpr *make_clause_use_graph_func_expr(char *graph_name) {
    Const *c = makeConst(TEXTOID, -1, InvalidOid, strlen(graph_name), CStringGetTextDatum(graph_name), false, false);

    Oid func_oid = get_ag_func_oid("use_graph", 1, TEXTOID);

    return makeFuncExpr(func_oid, VOIDOID, list_make1(c), InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
}

static Query *
cypher_use_graph_utility(ParseState *pstate, const char *graph_name) {
    Query *query;
    TargetEntry *tle;
    FuncExpr *func_expr;

    query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->targetList = NIL;

    func_expr = make_clause_use_graph_func_expr(graph_name);

    // Create the target entry
    tle = makeTargetEntry((Expr *)func_expr, pstate->p_next_resno++, "use_graph", false);
    query->targetList = lappend(query->targetList, tle);

    query->rtable = pstate->p_rtable;
    query->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    return query;
}


/*
 * parse_analyze
 *		Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.  Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 */
Query *
cypher_parse_analyze(RawStmt *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams,
			  QueryEnvironment *queryEnv)
{
	ParseState *pstate = make_cypher_parsestate(NULL);
	Query	   *query;
	JumbleState *jstate = NULL;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;

	if (numParams > 0)
		parse_fixed_parameters(pstate, paramTypes, numParams);

	pstate->p_queryEnv = queryEnv;

	//query = transformTopLevelStmt(pstate, parseTree);
    if (list_length(parseTree->stmt) == 1) {
      Node *n = linitial(parseTree->stmt);

      if (is_ag_node(n, cypher_create_graph)) {
        cypher_create_graph *ccg = n;
        //ereport(ERROR, errmsg("Here"));

        query = cypher_create_graph_utility(pstate, ccg->graph_name);

        query->canSetTag = true;

        if (IsQueryIdEnabled())
		  jstate = JumbleQuery(query, sourceText);

	    free_parsestate(pstate);

	    pgstat_report_query_id(query->queryId, false);
		PushActiveSnapshot(GetTransactionSnapshot());
	    return query;

      } else if (is_ag_node(n, cypher_use_graph)) {

        cypher_use_graph *ccg = n;
        //ereport(ERROR, errmsg("Here"));

        query = cypher_use_graph_utility(pstate, ccg->graph_name);

        query->canSetTag = true;

        if (IsQueryIdEnabled())
		  jstate = JumbleQuery(query, sourceText);

	    free_parsestate(pstate);

	    pgstat_report_query_id(query->queryId, false);
		PushActiveSnapshot(GetTransactionSnapshot());
	    return query;
      }
    }
    query = analyze_cypher(parseTree->stmt, pstate, sourceText, 0, NULL, CurrentGraphOid, NULL);
	
    if (IsQueryIdEnabled())
		jstate = JumbleQuery(query, sourceText);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);

	pgstat_report_query_id(query->queryId, false);

	return query;
}


static List * cypher_parse_analyze_hook (RawStmt *parsetree, const char *query_string,
					                     Oid *paramTypes, int numParams,
					                     QueryEnvironment *queryEnv) {
	Query	   *query;
	List	   *querytree_list;

	TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	query = cypher_parse_analyze(parsetree, query_string, paramTypes, numParams,
						  queryEnv);
    //query = analyze_cypher(parsetree->stmt, pstate, query_string, 0, NULL, CurrentGraphOid, NULL);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

	/*
	 * (2) Rewrite the queries, as necessary
	 */
	querytree_list = pg_rewrite_query(query);

	TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

	return querytree_list;
}


static
void post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate) {
    if (prev_post_parse_analyze_hook)
        prev_post_parse_analyze_hook(pstate, query, jstate);

    convert_cypher_walker((Node *)query, pstate);
}

// find cypher() calls in FROM clauses and convert them to SELECT subqueries
static bool
convert_cypher_walker(Node *node, ParseState *pstate) {
    if (!node)
        return false;

    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *)node;

        switch (rte->rtekind) {
        case RTE_SUBQUERY:
            // traverse other RTE_SUBQUERYs
            return convert_cypher_walker((Node *)rte->subquery, pstate);
        case RTE_FUNCTION:
            if (is_rte_cypher(rte))
                convert_cypher_to_subquery(rte, pstate);
            return false;
        default:
            return false;
        }
    }

    /*
     * This handles a cypher() call with other function calls in a ROWS FROM
     * expression. We can let the FuncExpr case below handle it but do this
     * here to throw a better error message.
     */
    if (IsA(node, RangeTblFunction)) {
        RangeTblFunction *rtfunc = (RangeTblFunction *)node;
        FuncExpr *funcexpr = (FuncExpr *)rtfunc->funcexpr;

        /*
         * It is better to throw a kind error message here instead of the
         * internal error message that cypher() throws later when it is called.
         */
        if (is_func_cypher(funcexpr))
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cypher(...) in ROWS FROM is not supported"),
                     parser_errposition(pstate, exprLocation((Node *)funcexpr))));

        return expression_tree_walker((Node *)funcexpr->args, convert_cypher_walker, pstate);
    }

    /*
     * This handles cypher() calls in expressions. Those in RTE_FUNCTIONs are
     * handled by either convert_cypher_to_subquery() or the RangeTblFunction
     * case above.
     */
    if (IsA(node, FuncExpr)) {
        FuncExpr *funcexpr = (FuncExpr *)node;

        if (is_func_cypher(funcexpr))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cypher(...) in expressions is not supported"),
                     errhint("Use subquery instead if possible."),
                     parser_errposition(pstate, exprLocation(node))));

        return expression_tree_walker((Node *)funcexpr->args, convert_cypher_walker, pstate);
    }

    if (IsA(node, Query)) {
        /*
         * QTW_EXAMINE_RTES
         *     We convert RTE_FUNCTION (cypher()) to RTE_SUBQUERY (SELECT)
         *     in-place.
         *
         * QTW_IGNORE_RT_SUBQUERIES
         *     After the conversion, we don't need to traverse the resulting
         *     RTE_SUBQUERY. However, we need to traverse other RTE_SUBQUERYs.
         *     This is done manually by the RTE_SUBQUERY case above.
         *
         * QTW_IGNORE_JOINALIASES
         *     We are not interested in this.
         */
        int flags = QTW_EXAMINE_RTES_BEFORE | QTW_IGNORE_RT_SUBQUERIES | QTW_IGNORE_JOINALIASES;

        return query_tree_walker((Query *)node, convert_cypher_walker, pstate, flags);
    }

    return expression_tree_walker(node, convert_cypher_walker, pstate);
}

static bool
is_rte_cypher(RangeTblEntry *rte) {
    // The planner expects RangeTblFunction nodes in rte->functions list. We cannot replace one of them to a SELECT subquery.
    if (list_length(rte->functions) != 1)
        return false;

    /*
     * A plain function call or a ROWS FROM expression with one function call
     * reaches here. At this point, it is impossible to distinguish between the
     * two. However, it doesn't matter because they are identical in terms of
     * their meaning.
     */
    RangeTblFunction *rtfunc = linitial(rte->functions);
    FuncExpr *funcexpr = (FuncExpr *)rtfunc->funcexpr;
    return is_func_cypher(funcexpr);
}

/*
 * Return true if the qualified name of the given function is
 * <"postgraph"."cypher">. Otherwise, return false.
 */
static bool
is_func_cypher(FuncExpr *funcexpr) {
    return is_oid_ag_func(funcexpr->funcid, "cypher");
}

// convert cypher() call to SELECT subquery in-place
static void
convert_cypher_to_subquery(RangeTblEntry *rte, ParseState *pstate) {
    RangeTblFunction *rtfunc = linitial(rte->functions);
    FuncExpr *funcexpr = (FuncExpr *)rtfunc->funcexpr;

    /*
     * We cannot apply this feature directly to SELECT subquery because the
     * planner does not support it. Adding a "row_number() OVER ()" expression
     * to the subquery as a result target might be a workaround but we throw an
     * error for now.
     */
    if (rte->funcordinality)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("WITH ORDINALITY is not supported"),
                 parser_errposition(pstate, exprLocation((Node *)funcexpr))));

    Node *arg = linitial(funcexpr->args);
    Assert(exprType(arg) == NAMEOID);

    Name graph_name = expr_get_const_name(arg);
    if (!graph_name)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("a name constant is expected"),
                        parser_errposition(pstate, exprLocation(arg))));

    uint32 graph_oid = get_graph_oid(NameStr(*graph_name));
    if (!OidIsValid(graph_oid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("graph \"%s\" does not exist", NameStr(*graph_name)),
                 parser_errposition(pstate, exprLocation(arg))));

    arg = lsecond(funcexpr->args);
    Assert(exprType(arg) == CSTRINGOID);

    /*
     * Since cypher() function is nothing but an interface to get a Cypher * query, it must take a string constant as an argument so that the query
     * can be parsed and analyzed at this point to create a Query tree of it.
     */
    const char *query_str = expr_get_const_cstring(arg, pstate->p_sourcetext);
    if (!query_str)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("a dollar-quoted string constant is expected"),
                        parser_errposition(pstate, exprLocation(arg))));

    int query_loc = get_query_location(((Const *)arg)->location, pstate->p_sourcetext);

    // Check to see if the cypher function had any parameters passed to it, if so make sure Postgres parsed the second argument to a Param node.
    Param *params = NULL;
    if (list_length(funcexpr->args) == 3) {
        arg = lthird(funcexpr->args);
        if (!IsA(arg, Param))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("third argument of cypher function must be a parameter"),
                     parser_errposition(pstate, exprLocation(arg))));

        params = (Param *)arg;
    }

    // install error context callback to adjust an error position for parse_cypher() since locations that parse_cypher() stores are 0 based
    errpos_ecb_state ecb_state;
    setup_errpos_ecb(&ecb_state, pstate, query_loc);

    List *stmt = parse_cypher(query_str);

    cancel_errpos_ecb(&ecb_state);

    Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
    pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
    // transformRangeFunction() always sets p_lateral_active to true.
    // FYI, rte is RTE_FUNCTION and is being converted to RTE_SUBQUERY here.
    pstate->p_lateral_active = true;

    /*
     * Cypher queries that end with CREATE clause do not need to have the
     * coercion logic applied to them because we are forcing the column
     * definition list to be a particular way in this case.
     */
    Query *query;
    if (is_ag_node(llast(stmt), cypher_create) || is_ag_node(llast(stmt), cypher_set) || is_ag_node(llast(stmt), cypher_delete) || is_ag_node(llast(stmt), cypher_merge)) {
        // column definition list must be ... AS relname(colname gtype) ...
        if (!(rtfunc->funccolcount == 1 && linitial_oid(rtfunc->funccoltypes) == GTYPEOID))
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("column definition list for CREATE clause must contain a single gtype attribute"),
                     errhint("... cypher($$ ... CREATE ... $$) AS t(c gtype) ..."),
                     parser_errposition(pstate, exprLocation(rtfunc->funcexpr))));

        query = analyze_cypher(stmt, pstate, query_str, query_loc, NameStr(*graph_name), graph_oid, params);
    } else {
        query = analyze_cypher_and_coerce(stmt, rtfunc, pstate, query_str, query_loc, NameStr(*graph_name), graph_oid, params);
    }

    pstate->p_lateral_active = false;
    pstate->p_expr_kind = EXPR_KIND_NONE;

    // rte->functions and rte->funcordinality are kept for debugging.
    // rte->alias, rte->eref, and rte->lateral need to be the same.
    // rte->inh is always false for both RTE_FUNCTION and RTE_SUBQUERY.
    // rte->inFromCl is always true for RTE_FUNCTION.
    rte->rtekind = RTE_SUBQUERY;
    rte->subquery = query;
}

static Name expr_get_const_name(Node *expr) {
    Const *con;

    if (!IsA(expr, Const))
        return NULL;

    con = (Const *)expr;
    if (con->constisnull)
        return NULL;

    return DatumGetName(con->constvalue);
}

static const char *
expr_get_const_cstring(Node *expr, const char *source_str) {
    if (!IsA(expr, Const))
        return NULL;

    Const *con = (Const *)expr;
    if (con->constisnull)
        return NULL;

    Assert(con->location > -1);
    const char *p = source_str + con->location;
    if (*p != '$')
        return NULL;

    return DatumGetCString(con->constvalue);
}

static int
get_query_location(const int location, const char *source_str) {
    Assert(location > -1);

    const char *p = source_str + location;

    Assert(*p == '$');

    return strchr(p + 1, '$') - source_str + 1;
}

static Query *
analyze_cypher(List *stmt, ParseState *parent_pstate, const char *query_str, int query_loc, char *graph_name, uint32 graph_oid, Param *params) {
    // Since the first clause in stmt is the innermost subquery, the order of the clauses is inverted.
    cypher_clause *clause = NULL;
    ListCell *lc;
    foreach (lc, stmt) {
        cypher_clause *next;

        next = palloc(sizeof(*next));
        next->next = NULL;
        next->self = lfirst(lc);
        next->prev = clause;

        if (clause != NULL)
            clause->next = next;
        clause = next;
    }

    // convert ParseState into cypher_parsestate temporarily to pass it to make_cypher_parsestate()
    cypher_parsestate parent_cpstate;
    parent_cpstate.pstate = *parent_pstate;
    parent_cpstate.graph_name = NULL;
    parent_cpstate.params = NULL;

    cypher_parsestate *cpstate = make_cypher_parsestate(&parent_cpstate);

    ParseState *pstate = (ParseState *)cpstate;

    // we don't want functions that go up the pstate parent chain to access the * original SQL query pstate.
    pstate->parentParseState = NULL;
    
    // override p_sourcetext with query_str to make parser_errposition() work correctly with errpos_ecb()
    pstate->p_sourcetext = query_str;

    cpstate->graph_name = graph_name;
    cpstate->graph_oid = graph_oid;
    cpstate->params = params;
    cpstate->default_alias_num = 0;
    cpstate->entities = NIL;

    // install error context callback to adjust an error position since locations in stmt are 0 based
    errpos_ecb_state ecb_state;
    setup_errpos_ecb(&ecb_state, parent_pstate, query_loc);

    Query *query = transform_cypher_clause(cpstate, clause);

    cancel_errpos_ecb(&ecb_state);

    free_cypher_parsestate(cpstate);

    return query;
}

/*
 * Since some target entries of subquery may be referenced for sorting (ORDER * BY), we cannot apply the coercion directly to the expressions of the target
 * entries. Therefore, we do the coercion by doing SELECT over subquery.
 */
static Query *
analyze_cypher_and_coerce(List *stmt, RangeTblFunction *rtfunc, ParseState *parent_pstate, const char *query_str, int query_loc, char *graph_name, uint32 graph_oid, Param *params) {
    const bool lateral = false;
    ParseNamespaceItem *pnsi;
    int rtindex;
    int attr_count = 0;

    ParseState *pstate = make_parsestate(parent_pstate);

    Query *query = makeNode(Query);
    query->commandType = CMD_SELECT;

    // Below is similar to transform_prev_cypher_clause().
    Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
    pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
    pstate->p_lateral_active = lateral;

    Query *subquery = analyze_cypher(stmt, pstate, query_str, query_loc, graph_name, graph_oid, (Param *)params);

    pstate->p_lateral_active = false;
    pstate->p_expr_kind = EXPR_KIND_NONE;

    // ALIAS Syntax makes `RESJUNK`. So, It must be skipping.
    ListCell *lc;
    foreach(lc, subquery->targetList) {
        TargetEntry *te = lfirst(lc);
        if (!te->resjunk)
            attr_count++;
    }

    // check the number of attributes first
    if (attr_count != rtfunc->funccolcount)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("return row and column definition list do not match"),
                 parser_errposition(pstate, exprLocation(rtfunc->funcexpr))));

    pnsi = addRangeTableEntryForSubquery(pstate, subquery, makeAlias("_", NIL), lateral, true);

    rtindex = list_length(pstate->p_rtable);
    Assert(rtindex == 1); // rte is the only RangeTblEntry in pstate

    addNSItemToQuery(pstate, pnsi, true, true, true);

    query->targetList = expandNSItemAttrs(pstate, pnsi, 0, -1);

    markTargetListOrigins(pstate, query->targetList);

    // do the type coercion for each target entry
    ListCell *lc1 = list_head(rtfunc->funccolnames);
    ListCell *lc2 = list_head(rtfunc->funccoltypes);
    ListCell *lc3 = list_head(rtfunc->funccoltypmods);
    foreach (lc, query->targetList) {
        TargetEntry *te = lfirst(lc);

        Assert(!te->resjunk);

        Node *expr = (Node *)te->expr;

        Oid current_type = exprType(expr);
        Oid target_type = lfirst_oid(lc2);
        if (current_type != target_type) {
            int32 target_typmod = lfirst_int(lc3);

            /*
             * The coercion context of this coercion is COERCION_EXPLICIT because the target type is explicitly metioned in the column
             * definition list and we need to do this by looking up all possible coercion.
             */
            Node *new_expr = coerce_to_target_type(pstate, expr, current_type, target_type, target_typmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
            if (!new_expr) {
                char *colname = strVal(lfirst(lc1));

                ereport(ERROR, (errcode(ERRCODE_CANNOT_COERCE),
                         errmsg("cannot cast type %s to %s for column \"%s\"", format_type_be(current_type), format_type_be(target_type), colname),
                         parser_errposition(pstate, exprLocation(rtfunc->funcexpr))));
            }

            te->expr = (Expr *)new_expr;
        }

        lc1 = lnext(rtfunc->funccolnames, lc1);
        lc2 = lnext(rtfunc->funccoltypes, lc2);
        lc3 = lnext(rtfunc->funccoltypmods, lc3);
    }

    query->rtable = pstate->p_rtable;
    query->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    assign_query_collations(pstate, query);

    free_parsestate(pstate);

    return query;
}
