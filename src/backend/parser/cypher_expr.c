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
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 */

#include "postgraph.h"

#include "catalog/pg_type.h"
#include "catalog/pg_aggregate.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/cypher_clause.h"
#include "parser/parse_node.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "commands/label_commands.h"
#include "nodes/cypher_nodes.h"
#include "parser/cypher_expr.h"
#include "parser/cypher_item.h"
#include "parser/cypher_parse_node.h"
#include "parser/cypher_transform_entity.h"
#include "utils/ag_func.h"
#include "utils/gtype.h"
#include "utils/gtype_typecasting.h"

#define is_a_slice(node) \
    (IsA((node), A_Indices) && ((A_Indices *)(node))->is_slice)

static Node *transform_cypher_expr_recurse(cypher_parsestate *cpstate, Node *expr);
static Node *transform_a_const(cypher_parsestate *cpstate, A_Const *ac);
static Node *transform_column_ref(cypher_parsestate *cpstate, ColumnRef *cref);
static Node *transform_a_indirection(cypher_parsestate *cpstate, A_Indirection *a_ind);
static Node *transform_a_expr_op(cypher_parsestate *cpstate, A_Expr *a);
static Node *transform_bool_expr(cypher_parsestate *cpstate, BoolExpr *expr);
static Node *transform_cypher_bool_const(cypher_parsestate *cpstate, cypher_bool_const *bc);
static Node *transform_cypher_inet_const(cypher_parsestate *cpstate, cypher_inet_const *inet);
static Node *transform_cypher_integer_const(cypher_parsestate *cpstate, cypher_integer_const *ic);
static Node *transform_cypher_param(cypher_parsestate *cpstate, cypher_param *cp);
static Node *transform_cypher_map(cypher_parsestate *cpstate, cypher_map *cm);
static Node *transform_cypher_list(cypher_parsestate *cpstate, cypher_list *cl);
static Node *transform_cypher_string_match(cypher_parsestate *cpstate, cypher_string_match *csm_node);
static Node *transform_cypher_typecast(cypher_parsestate *cpstate, cypher_typecast *ctypecast);
static Node *transform_case_expr(cypher_parsestate *cpstate, CaseExpr *cexpr);
static Node *transform_coalesce_expr(cypher_parsestate *cpstate, CoalesceExpr *cexpr);
static Node *transform_sub_link(cypher_parsestate *cpstate, SubLink *sublink);
static Node *transform_func_call(cypher_parsestate *cpstate, FuncCall *fn);
static Node *transform_column_ref_for_indirection(cypher_parsestate *cpstate, ColumnRef *cr);
static Node *transformSQLValueFunction(cypher_parsestate *cpstate, SQLValueFunction *svf);
static List *make_qualified_function_name(cypher_parsestate *cpstate, List *lst, List *targs);
static void unify_hypothetical_args(ParseState *pstate, List *fargs, int numAggregatedArgs,
                                    Oid *actual_arg_types, Oid *declared_arg_types);


typedef struct
{
        ParseState *pstate;
        int                     min_varlevel;
        int                     min_agglevel;
        int                     sublevels_up;
} check_agg_arguments_context;

typedef struct
{
        ParseState *pstate;
        Query      *qry;
        bool            hasJoinRTEs;
        List       *groupClauses;
        List       *groupClauseCommonVars;
        bool            have_non_var_grouping;
        List      **func_grouped_rels;
        int                     sublevels_up;
        bool            in_agg_direct_args;
} check_ungrouped_columns_context;
static int      check_agg_arguments(ParseState *pstate, List *directargs, List *args, Expr *filter);
static bool check_agg_arguments_walker(Node *node, check_agg_arguments_context *context);
static void check_ungrouped_columns(Node *node, ParseState *pstate, Query *qry,
                                    List *groupClauses, List *groupClauseCommonVars,
                                    bool have_non_var_grouping, List **func_grouped_rels);
static bool check_ungrouped_columns_walker(Node *node, check_ungrouped_columns_context *context);
static void finalize_grouping_exprs(Node *node, ParseState *pstate, Query *qry,
                                    List *groupClauses, bool hasJoinRTEs,
                                    bool have_non_var_grouping);
static bool finalize_grouping_exprs_walker(Node *node, check_ungrouped_columns_context *context);
static void check_agglevels_and_constraints(ParseState *pstate, Node *expr);
static List *expand_groupingset_node(GroupingSet *gs);
static Node *make_agg_arg(Oid argtype, Oid argcollation);


static char *make_property_alias(char *var_name) {
    char *str = palloc(strlen(var_name) + 8);

    str[0] = '_';
    str[1] = 'p';
    str[2] = 'r';
    str[3] = '_';

    int i = 0;
    for (; i < strlen(var_name); i++)
        str[i + 4] = var_name[i];

    str[i + 5] = '_';
    str[i + 6] = '_';
    str[i + 7] = '\n';

    return str;
}


Node *
transform_cypher_expr(cypher_parsestate *cpstate, Node *expr, ParseExprKind expr_kind) {
    ParseState *pstate = (ParseState *)cpstate;
    ParseExprKind old_expr_kind;
    Node *result;

    // save and restore identity of expression type we're parsing
    Assert(expr_kind != EXPR_KIND_NONE);
    old_expr_kind = pstate->p_expr_kind;
    pstate->p_expr_kind = expr_kind;

    result = transform_cypher_expr_recurse(cpstate, expr);

    pstate->p_expr_kind = old_expr_kind;

    return result;
}

static Node *
transform_cypher_expr_recurse(cypher_parsestate *cpstate, Node *expr) {
    if (!expr)
        return NULL;

    check_stack_depth();

    switch (nodeTag(expr))
    {
    case T_A_Const:
        return transform_a_const(cpstate, (A_Const *)expr);
    case T_ColumnRef:
        return transform_column_ref(cpstate, (ColumnRef *)expr);
    case T_A_Indirection:
        return transform_a_indirection(cpstate, (A_Indirection *)expr);
    case T_A_Expr:
    {
        A_Expr *a = (A_Expr *)expr;

        switch (a->kind)
        {
        case AEXPR_OP:
            return transform_a_expr_op(cpstate, a);
        default:
            ereport(ERROR, (errmsg_internal("unrecognized A_Expr kind: %d", a->kind)));
        }
    }
    case T_BoolExpr:
        return transform_bool_expr(cpstate, (BoolExpr *)expr);
    case T_NullTest:
    {
        NullTest *n = (NullTest *)expr;

        n->arg = (Expr *)transform_cypher_expr_recurse(cpstate, (Node *)n->arg);
        n->argisrow = type_is_rowtype(exprType((Node *)n->arg));

        return expr;
    }
    case T_CaseExpr:
        return transform_case_expr(cpstate, (CaseExpr *) expr);
    case T_CaseTestExpr:
        return expr;
    case T_CoalesceExpr:
        return transform_coalesce_expr(cpstate, (CoalesceExpr *) expr);
    case T_ExtensibleNode:
        if (is_ag_node(expr, cypher_bool_const))
            return transform_cypher_bool_const(cpstate, (cypher_bool_const *)expr);
        if (is_ag_node(expr, cypher_inet_const))
            return transform_cypher_inet_const(cpstate, (cypher_inet_const *)expr);
    if (is_ag_node(expr, cypher_integer_const))
            return transform_cypher_integer_const(cpstate, (cypher_integer_const *)expr);
        if (is_ag_node(expr, cypher_param))
            return transform_cypher_param(cpstate, (cypher_param *)expr);
        if (is_ag_node(expr, cypher_map))
            return transform_cypher_map(cpstate, (cypher_map *)expr);
        if (is_ag_node(expr, cypher_list))
            return transform_cypher_list(cpstate, (cypher_list *)expr);
        if (is_ag_node(expr, cypher_string_match))
            return transform_cypher_string_match(cpstate, (cypher_string_match *)expr);
        if (is_ag_node(expr, cypher_typecast))
            return transform_cypher_typecast(cpstate, (cypher_typecast *)expr);

        ereport(ERROR, (errmsg_internal("unrecognized ExtensibleNode: %s",
                        ((ExtensibleNode *)expr)->extnodename)));

    case T_FuncCall:
        return transform_func_call(cpstate, (FuncCall *)expr);
    case T_SubLink:
        return transform_sub_link(cpstate, (SubLink *)expr);
    case T_SQLValueFunction:
        return transformSQLValueFunction(cpstate, (SQLValueFunction *)expr);
    case T_String:
        return makeConst(GTYPEOID, -1, InvalidOid, -1, string_to_gtype(strVal(expr)), false, false);
    default:
        ereport(ERROR, (errmsg_internal("unrecognized node type: %d", nodeTag(expr))));
    }

    return NULL;
}

/*
 * Aggregate functions and grouping operations (which are combined in the spec
 * as <set function specification>) are very similar with regard to level and
 * nesting restrictions (though we allow a lot more things than the spec does).
 * Centralise those restrictions here.
 */
static void
check_agglevels_and_constraints(ParseState *pstate, Node *expr)
{
    List       *directargs = NIL;
    List       *args = NIL;
    Expr       *filter = NULL;
    int            min_varlevel;
    int            location = -1;
    Index       *p_levelsup;
    const char *err;
    bool        errkind;
    bool        isAgg = IsA(expr, Aggref);

    if (isAgg)
    {
        Aggref       *agg = (Aggref *) expr;

        directargs = agg->aggdirectargs;
        args = agg->args;
        filter = agg->aggfilter;
        location = agg->location;
        p_levelsup = &agg->agglevelsup;
    }
    else
    {
        GroupingFunc *grp = (GroupingFunc *) expr;

        args = grp->args;
        location = grp->location;
        p_levelsup = &grp->agglevelsup;
    }

    /*
     * Check the arguments to compute the aggregate's level and detect
     * improper nesting.
     */
    min_varlevel = check_agg_arguments(pstate,
                                       directargs,
                                       args,
                                       filter);

    *p_levelsup = min_varlevel;

    /* Mark the correct pstate level as having aggregates */
    while (min_varlevel-- > 0)
        pstate = pstate->parentParseState;
    pstate->p_hasAggs = true;

    /*
     * Check to see if the aggregate function is in an invalid place within
     * its aggregation query.
     *
     * For brevity we support two schemes for reporting an error here: set
     * "err" to a custom message, or set "errkind" true if the error context
     * is sufficiently identified by what ParseExprKindName will return, *and*
     * what it will return is just a SQL keyword.  (Otherwise, use a custom
     * message to avoid creating translation problems.)
     */
    err = NULL;
    errkind = false;
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_NONE:
            Assert(false);        /* can't happen */
            break;
        case EXPR_KIND_OTHER:

            /*
             * Accept aggregate/grouping here; caller must throw error if
             * wanted
             */
            break;
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            if (isAgg)
                err = _("aggregate functions are not allowed in JOIN conditions");
            else
                err = _("grouping operations are not allowed in JOIN conditions");

            break;
        case EXPR_KIND_FROM_SUBSELECT:
            /* Should only be possible in a LATERAL subquery */
            Assert(pstate->p_lateral_active);

            /*
             * Aggregate/grouping scope rules make it worth being explicit
             * here
             */
            if (isAgg)
                err = _("aggregate functions are not allowed in FROM clause of their own query level");
            else
                err = _("grouping operations are not allowed in FROM clause of their own query level");

            break;
        case EXPR_KIND_FROM_FUNCTION:
            if (isAgg)
                err = _("aggregate functions are not allowed in functions in FROM");
            else
                err = _("grouping operations are not allowed in functions in FROM");

            break;
        case EXPR_KIND_WHERE:
            errkind = true;
            break;
        case EXPR_KIND_POLICY:
            if (isAgg)
                err = _("aggregate functions are not allowed in policy expressions");
            else
                err = _("grouping operations are not allowed in policy expressions");

            break;
        case EXPR_KIND_HAVING:
            /* okay */
            break;
        case EXPR_KIND_FILTER:
            errkind = true;
            break;
        case EXPR_KIND_WINDOW_PARTITION:
            /* okay */
            break;
        case EXPR_KIND_WINDOW_ORDER:
            /* okay */
            break;
        case EXPR_KIND_WINDOW_FRAME_RANGE:
            if (isAgg)
                err = _("aggregate functions are not allowed in window RANGE");
            else
                err = _("grouping operations are not allowed in window RANGE");

            break;
        case EXPR_KIND_WINDOW_FRAME_ROWS:
            if (isAgg)
                err = _("aggregate functions are not allowed in window ROWS");
            else
                err = _("grouping operations are not allowed in window ROWS");

            break;
        case EXPR_KIND_WINDOW_FRAME_GROUPS:
            if (isAgg)
                err = _("aggregate functions are not allowed in window GROUPS");
            else
                err = _("grouping operations are not allowed in window GROUPS");

            break;
        case EXPR_KIND_SELECT_TARGET:
            /* okay */
            break;
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
            errkind = true;
            break;
        case EXPR_KIND_GROUP_BY:
            errkind = true;
            break;
        case EXPR_KIND_ORDER_BY:
            /* okay */
            break;
        case EXPR_KIND_DISTINCT_ON:
            /* okay */
            break;
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
            errkind = true;
            break;
        case EXPR_KIND_RETURNING:
            errkind = true;
            break;
        case EXPR_KIND_VALUES:
        case EXPR_KIND_VALUES_SINGLE:
            errkind = true;
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            if (isAgg)
                err = _("aggregate functions are not allowed in check constraints");
            else
                err = _("grouping operations are not allowed in check constraints");

            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:

            if (isAgg)
                err = _("aggregate functions are not allowed in DEFAULT expressions");
            else
                err = _("grouping operations are not allowed in DEFAULT expressions");

            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            if (isAgg)
                err = _("aggregate functions are not allowed in index expressions");
            else
                err = _("grouping operations are not allowed in index expressions");

            break;
        case EXPR_KIND_INDEX_PREDICATE:
            if (isAgg)
                err = _("aggregate functions are not allowed in index predicates");
            else
                err = _("grouping operations are not allowed in index predicates");

            break;
        case EXPR_KIND_STATS_EXPRESSION:
            if (isAgg)
                err = _("aggregate functions are not allowed in statistics expressions");
            else
                err = _("grouping operations are not allowed in statistics expressions");

            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            if (isAgg)
                err = _("aggregate functions are not allowed in transform expressions");
            else
                err = _("grouping operations are not allowed in transform expressions");

            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            if (isAgg)
                err = _("aggregate functions are not allowed in EXECUTE parameters");
            else
                err = _("grouping operations are not allowed in EXECUTE parameters");

            break;
        case EXPR_KIND_TRIGGER_WHEN:
            if (isAgg)
                err = _("aggregate functions are not allowed in trigger WHEN conditions");
            else
                err = _("grouping operations are not allowed in trigger WHEN conditions");

            break;
        case EXPR_KIND_PARTITION_BOUND:
            if (isAgg)
                err = _("aggregate functions are not allowed in partition bound");
            else
                err = _("grouping operations are not allowed in partition bound");

            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            if (isAgg)
                err = _("aggregate functions are not allowed in partition key expressions");
            else
                err = _("grouping operations are not allowed in partition key expressions");

            break;
        case EXPR_KIND_GENERATED_COLUMN:

            if (isAgg)
                err = _("aggregate functions are not allowed in column generation expressions");
            else
                err = _("grouping operations are not allowed in column generation expressions");

            break;

        case EXPR_KIND_CALL_ARGUMENT:
            if (isAgg)
                err = _("aggregate functions are not allowed in CALL arguments");
            else
                err = _("grouping operations are not allowed in CALL arguments");

            break;

        case EXPR_KIND_COPY_WHERE:
            if (isAgg)
                err = _("aggregate functions are not allowed in COPY FROM WHERE conditions");
            else
                err = _("grouping operations are not allowed in COPY FROM WHERE conditions");

            break;

        case EXPR_KIND_CYCLE_MARK:
            errkind = true;
            break;

            /*
             * There is intentionally no default: case here, so that the
             * compiler will warn if we add a new ParseExprKind without
             * extending this switch.  If we do see an unrecognized value at
             * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
             * which is sane anyway.
             */
    }

    if (err)
        ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                 errmsg_internal("%s", err),
                 parser_errposition(pstate, location)));

    if (errkind)
    {
        if (isAgg)
            /* translator: %s is name of a SQL construct, eg GROUP BY */
            err = _("aggregate functions are not allowed in %s");
        else
            /* translator: %s is name of a SQL construct, eg GROUP BY */
            err = _("grouping operations are not allowed in %s");

        ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                 errmsg_internal(err,
                                 ParseExprKindName(pstate->p_expr_kind)),
                 parser_errposition(pstate, location)));
    }
}



/*
 * check_agg_arguments
 *      Scan the arguments of an aggregate function to determine the
 *      aggregate's semantic level (zero is the current select's level,
 *      one is its parent, etc).
 *
 * The aggregate's level is the same as the level of the lowest-level variable
 * or aggregate in its aggregated arguments (including any ORDER BY columns)
 * or filter expression; or if it contains no variables at all, we presume it
 * to be local.
 *
 * Vars/Aggs in direct arguments are *not* counted towards determining the
 * agg's level, as those arguments aren't evaluated per-row but only
 * per-group, and so in some sense aren't really agg arguments.  However,
 * this can mean that we decide an agg is upper-level even when its direct
 * args contain lower-level Vars/Aggs, and that case has to be disallowed.
 * (This is a little strange, but the SQL standard seems pretty definite that
 * direct args are not to be considered when setting the agg's level.)
 *
 * We also take this opportunity to detect any aggregates or window functions
 * nested within the arguments.  We can throw error immediately if we find
 * a window function.  Aggregates are a bit trickier because it's only an
 * error if the inner aggregate is of the same semantic level as the outer,
 * which we can't know until we finish scanning the arguments.
 */
static int
check_agg_arguments(ParseState *pstate,
                    List *directargs,
                    List *args,
                    Expr *filter)
{
    int            agglevel;
    check_agg_arguments_context context;

    context.pstate = pstate;
    context.min_varlevel = -1;    /* signifies nothing found yet */
    context.min_agglevel = -1;
    context.sublevels_up = 0;

    (void) check_agg_arguments_walker((Node *) args, &context);
    (void) check_agg_arguments_walker((Node *) filter, &context);

    /*
     * If we found no vars nor aggs at all, it's a level-zero aggregate;
     * otherwise, its level is the minimum of vars or aggs.
     */
    if (context.min_varlevel < 0)
    {
        if (context.min_agglevel < 0)
            agglevel = 0;
        else
            agglevel = context.min_agglevel;
    }
    else if (context.min_agglevel < 0)
        agglevel = context.min_varlevel;
    else
        agglevel = Min(context.min_varlevel, context.min_agglevel);

    /*
     * If there's a nested aggregate of the same semantic level, complain.
     */
    if (agglevel == context.min_agglevel)
    {
        int            aggloc;

        aggloc = locate_agg_of_level((Node *) args, agglevel);
        if (aggloc < 0)
            aggloc = locate_agg_of_level((Node *) filter, agglevel);
        ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                 errmsg("aggregate function calls cannot be nested"),
                 parser_errposition(pstate, aggloc)));
    }

    /*
     * Now check for vars/aggs in the direct arguments, and throw error if
     * needed.  Note that we allow a Var of the agg's semantic level, but not
     * an Agg of that level.  In principle such Aggs could probably be
     * supported, but it would create an ordering dependency among the
     * aggregates at execution time.  Since the case appears neither to be
     * required by spec nor particularly useful, we just treat it as a
     * nested-aggregate situation.
     */
    if (directargs)
    {
        context.min_varlevel = -1;
        context.min_agglevel = -1;
        (void) check_agg_arguments_walker((Node *) directargs, &context);
        if (context.min_varlevel >= 0 && context.min_varlevel < agglevel)
            ereport(ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                     errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments"),
                     parser_errposition(pstate,
                                        locate_var_of_level((Node *) directargs,
                                                            context.min_varlevel))));
        if (context.min_agglevel >= 0 && context.min_agglevel <= agglevel)
            ereport(ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                     errmsg("aggregate function calls cannot be nested"),
                     parser_errposition(pstate,
                                        locate_agg_of_level((Node *) directargs,
                                                            context.min_agglevel))));
    }
    return agglevel;
}

static bool
check_agg_arguments_walker(Node *node,
                           check_agg_arguments_context *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var))
    {
        int            varlevelsup = ((Var *) node)->varlevelsup;

        /* convert levelsup to frame of reference of original query */
        varlevelsup -= context->sublevels_up;
        /* ignore local vars of subqueries */
        if (varlevelsup >= 0)
        {
            if (context->min_varlevel < 0 ||
                context->min_varlevel > varlevelsup)
                context->min_varlevel = varlevelsup;
        }
        return false;
    }
    if (IsA(node, Aggref))
    {
        int            agglevelsup = ((Aggref *) node)->agglevelsup;

        /* convert levelsup to frame of reference of original query */
        agglevelsup -= context->sublevels_up;
        /* ignore local aggs of subqueries */
        if (agglevelsup >= 0)
        {
            if (context->min_agglevel < 0 ||
                context->min_agglevel > agglevelsup)
                context->min_agglevel = agglevelsup;
        }
        /* Continue and descend into subtree */
    }
    if (IsA(node, GroupingFunc))
    {
        int            agglevelsup = ((GroupingFunc *) node)->agglevelsup;

        /* convert levelsup to frame of reference of original query */
        agglevelsup -= context->sublevels_up;
        /* ignore local aggs of subqueries */
        if (agglevelsup >= 0)
        {
            if (context->min_agglevel < 0 ||
                context->min_agglevel > agglevelsup)
                context->min_agglevel = agglevelsup;
        }
        /* Continue and descend into subtree */
    }

    /*
     * SRFs and window functions can be rejected immediately, unless we are
     * within a sub-select within the aggregate's arguments; in that case
     * they're OK.
     */
    if (context->sublevels_up == 0)
    {
        if ((IsA(node, FuncExpr) && ((FuncExpr *) node)->funcretset) ||
            (IsA(node, OpExpr) && ((OpExpr *) node)->opretset))
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("aggregate function calls cannot contain set-returning function calls"),
                     errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                     parser_errposition(context->pstate, exprLocation(node))));
        if (IsA(node, WindowFunc))
            ereport(ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                     errmsg("aggregate function calls cannot contain window function calls"),
                     parser_errposition(context->pstate,
                                        ((WindowFunc *) node)->location)));
    }
    if (IsA(node, Query))
    {
        /* Recurse into subselects */
        bool        result;

        context->sublevels_up++;
        result = query_tree_walker((Query *) node,
                                   check_agg_arguments_walker,
                                   (void *) context,
                                   0);
        context->sublevels_up--;
        return result;
    }

    return expression_tree_walker(node,
                                  check_agg_arguments_walker,
                                  (void *) context);
}

/*
 * transformAggregateCall -
 *        Finish initial transformation of an aggregate call
 *
 * parse_func.c has recognized the function as an aggregate, and has set up
 * all the fields of the Aggref except aggargtypes, aggdirectargs, args,
 * aggorder, aggdistinct and agglevelsup.  The passed-in args list has been
 * through standard expression transformation and type coercion to match the
 * agg's declared arg types, while the passed-in aggorder list hasn't been
 * transformed at all.
 *
 * Here we separate the args list into direct and aggregated args, storing the
 * former in agg->aggdirectargs and the latter in agg->args.  The regular
 * args, but not the direct args, are converted into a targetlist by inserting
 * TargetEntry nodes.  We then transform the aggorder and agg_distinct
 * specifications to produce lists of SortGroupClause nodes for agg->aggorder
 * and agg->aggdistinct.  (For a regular aggregate, this might result in
 * adding resjunk expressions to the targetlist; but for ordered-set
 * aggregates the aggorder list will always be one-to-one with the aggregated
 * args.)
 *
 * We must also determine which query level the aggregate actually belongs to,
 * set agglevelsup accordingly, and mark p_hasAggs true in the corresponding
 * pstate level.
 */
void
transform_aggregate_call(ParseState *pstate, Aggref *agg,
                       List *args, List *aggorder, bool agg_distinct)
{
    List       *argtypes = NIL;
    List       *tlist = NIL;
    List       *torder = NIL;
    List       *tdistinct = NIL;
    AttrNumber    attno = 1;
    int            save_next_resno;
    ListCell   *lc;

    if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
    {
        /*
         * For an ordered-set agg, the args list includes direct args and
         * aggregated args; we must split them apart.
         */
        int            numDirectArgs = list_length(args) - list_length(aggorder);
        List       *aargs;
        ListCell   *lc2;

        Assert(numDirectArgs >= 0);

        aargs = list_copy_tail(args, numDirectArgs);
        agg->aggdirectargs = list_truncate(args, numDirectArgs);

        /*
         * Build a tlist from the aggregated args, and make a sortlist entry
         * for each one.  Note that the expressions in the SortBy nodes are
         * ignored (they are the raw versions of the transformed args); we are
         * just looking at the sort information in the SortBy nodes.
         */
        forboth(lc, aargs, lc2, aggorder)
        {
            Expr       *arg = (Expr *) lfirst(lc);
            SortBy       *sortby = (SortBy *) lfirst(lc2);
            TargetEntry *tle;

            /* We don't bother to assign column names to the entries */
            tle = makeTargetEntry(arg, attno++, NULL, false);
            tlist = lappend(tlist, tle);

            torder = addTargetToSortList(pstate, tle,
                                         torder, tlist, sortby);
        }

        /* Never any DISTINCT in an ordered-set agg */
        Assert(!agg_distinct);
    }
    else
    {
        /* Regular aggregate, so it has no direct args */
        agg->aggdirectargs = NIL;

        /*
         * Transform the plain list of Exprs into a targetlist.
         */
        foreach(lc, args)
        {
            Expr       *arg = (Expr *) lfirst(lc);
            TargetEntry *tle;

            /* We don't bother to assign column names to the entries */
            tle = makeTargetEntry(arg, attno++, NULL, false);
            tlist = lappend(tlist, tle);
        }

        /*
         * If we have an ORDER BY, transform it.  This will add columns to the
         * tlist if they appear in ORDER BY but weren't already in the arg
         * list.  They will be marked resjunk = true so we can tell them apart
         * from regular aggregate arguments later.
         *
         * We need to mess with p_next_resno since it will be used to number
         * any new targetlist entries.
         */
        save_next_resno = pstate->p_next_resno;
        pstate->p_next_resno = attno;

        torder = transform_cypher_order_by(pstate,
                                     aggorder,
                                     &tlist,
                                     EXPR_KIND_ORDER_BY,
                                     true /* force SQL99 rules */ );

        /*
         * If we have DISTINCT, transform that to produce a distinctList.
         */
        if (agg_distinct)
        {
            tdistinct = transformDistinctClause(pstate, &tlist, torder, true);

            /*
             * Remove this check if executor support for hashed distinct for
             * aggregates is ever added.
             */
            foreach(lc, tdistinct)
            {
                SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);

                if (!OidIsValid(sortcl->sortop))
                {
                    Node       *expr = get_sortgroupclause_expr(sortcl, tlist);

                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                             errmsg("could not identify an ordering operator for type %s",
                                    format_type_be(exprType(expr))),
                             errdetail("Aggregates with DISTINCT must be able to sort their inputs."),
                             parser_errposition(pstate, exprLocation(expr))));
                }
            }
        }

        pstate->p_next_resno = save_next_resno;
    }

    /* Update the Aggref with the transformation results */
    agg->args = tlist;
    agg->aggorder = torder;
    agg->aggdistinct = tdistinct;

    /*
     * Now build the aggargtypes list with the type OIDs of the direct and
     * aggregated args, ignoring any resjunk entries that might have been
     * added by ORDER BY/DISTINCT processing.  We can't do this earlier
     * because said processing can modify some args' data types, in particular
     * by resolving previously-unresolved "unknown" literals.
     */
    foreach(lc, agg->aggdirectargs)
    {
        Expr       *arg = (Expr *) lfirst(lc);

        argtypes = lappend_oid(argtypes, exprType((Node *) arg));
    }
    foreach(lc, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);

        if (tle->resjunk)
            continue;            /* ignore junk */
        argtypes = lappend_oid(argtypes, exprType((Node *) tle->expr));
    }
    agg->aggargtypes = argtypes;

    check_agglevels_and_constraints(pstate, (Node *) agg);
}

/*
 * transformGroupingFunc
 *        Transform a GROUPING expression
 *
 * GROUPING() behaves very like an aggregate.  Processing of levels and nesting
 * is done as for aggregates.  We set p_hasAggs for these expressions too.
 */
Node *
transformGroupingFunc(ParseState *pstate, GroupingFunc *p)
{
    ListCell   *lc;
    List       *args = p->args;
    List       *result_list = NIL;
    GroupingFunc *result = makeNode(GroupingFunc);

    if (list_length(args) > 31)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg("GROUPING must have fewer than 32 arguments"),
                 parser_errposition(pstate, p->location)));

    foreach(lc, args)
    {
        Node       *current_result;

        current_result = transformExpr(pstate, (Node *) lfirst(lc), pstate->p_expr_kind);

        /* acceptability of expressions is checked later */

        result_list = lappend(result_list, current_result);
    }

    result->args = result_list;
    result->location = p->location;

    check_agglevels_and_constraints(pstate, (Node *) result);

    return (Node *) result;
}

/*
 *    Parse a function call
 *
 *    For historical reasons, Postgres tries to treat the notations tab.col
 *    and col(tab) as equivalent: if a single-argument function call has an
 *    argument of complex type and the (unqualified) function name matches
 *    any attribute of the type, we can interpret it as a column projection.
 *    Conversely a function of a single complex-type argument can be written
 *    like a column reference, allowing functions to act like computed columns.
 *
 *    If both interpretations are possible, we prefer the one matching the
 *    syntactic form, but otherwise the form does not matter.
 *
 *    Hence, both cases come through here.  If fn is null, we're dealing with
 *    column syntax not function syntax.  In the function-syntax case,
 *    the FuncCall struct is needed to carry various decoration that applies
 *    to aggregate and window functions.
 *
 *    Also, when fn is null, we return NULL on failure rather than
 *    reporting a no-such-function error.
 *
 *    The argument expressions (in fargs) must have been transformed
 *    already.  However, nothing in *fn has been transformed.
 *
 *    last_srf should be a copy of pstate->p_last_srf from just before we
 *    started transforming fargs.  If the caller knows that fargs couldn't
 *    contain any SRF calls, last_srf can just be pstate->p_last_srf.
 *
 *    proc_call is true if we are considering a CALL statement, so that the
 *    name must resolve to a procedure name, not anything else.  This flag
 *    also specifies that the argument list includes any OUT-mode arguments.
 */
Node *
parse_func_or_column(ParseState *pstate, List *funcname, List *fargs,
                     Node *last_srf, FuncCall *fn, bool proc_call, int location)
{
    bool        is_column = (fn == NULL);
    List       *agg_order = (fn ? fn->agg_order : NIL);
    Expr       *agg_filter = NULL;
    WindowDef  *over = (fn ? fn->over : NULL);
    bool        agg_within_group = (fn ? fn->agg_within_group : false);
    bool        agg_star = (fn ? fn->agg_star : false);
    bool        agg_distinct = (fn ? fn->agg_distinct : false);
    bool        func_variadic = (fn ? fn->func_variadic : false);
    CoercionForm funcformat = (fn ? fn->funcformat : COERCE_EXPLICIT_CALL);
    bool        could_be_projection;
    Oid            rettype;
    Oid            funcid;
    ListCell   *l;
    Node       *first_arg = NULL;
    int            nargs;
    int            nargsplusdefs;
    Oid            actual_arg_types[FUNC_MAX_ARGS];
    Oid           *declared_arg_types;
    List       *argnames;
    List       *argdefaults;
    Node       *retval;
    bool        retset;
    int            nvargs;
    Oid            vatype;
    FuncDetailCode fdresult;
    char        aggkind = 0;
    ParseCallbackState pcbstate;

    /*
     * If there's an aggregate filter, transform it using transformWhereClause
     */
    if (fn && fn->agg_filter != NULL)
        agg_filter = (Expr *) transform_cypher_expr(pstate, fn->agg_filter, EXPR_KIND_FILTER);
//        agg_filter = (Expr *) transformWhereClause(pstate, fn->agg_filter, EXPR_KIND_FILTER, "FILTER");

    /*
     * Most of the rest of the parser just assumes that functions do not have
     * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
     * against array overruns, etc.  Of course, this may not be a function,
     * but the test doesn't hurt.
     */
    if (list_length(fargs) > FUNC_MAX_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg_plural("cannot pass more than %d argument to a function",
                               "cannot pass more than %d arguments to a function",
                               FUNC_MAX_ARGS,
                               FUNC_MAX_ARGS),
                 parser_errposition(pstate, location)));

    /*
     * Extract arg type info in preparation for function lookup.
     *
     * If any arguments are Param markers of type VOID, we discard them from
     * the parameter list. This is a hack to allow the JDBC driver to not have
     * to distinguish "input" and "output" parameter symbols while parsing
     * function-call constructs.  Don't do this if dealing with column syntax,
     * nor if we had WITHIN GROUP (because in that case it's critical to keep
     * the argument count unchanged).
     */
    nargs = 0;
    foreach(l, fargs)
    {
        Node       *arg = lfirst(l);
        Oid            argtype = exprType(arg);

        if (argtype == VOIDOID && IsA(arg, Param) &&
            !is_column && !agg_within_group)
        {
            fargs = foreach_delete_current(fargs, l);
            continue;
        }

        actual_arg_types[nargs++] = argtype;
    }

    /*
     * Check for named arguments; if there are any, build a list of names.
     *
     * We allow mixed notation (some named and some not), but only with all
     * the named parameters after all the unnamed ones.  So the name list
     * corresponds to the last N actual parameters and we don't need any extra
     * bookkeeping to match things up.
     */
    argnames = NIL;
    foreach(l, fargs)
    {
        Node       *arg = lfirst(l);

        if (IsA(arg, NamedArgExpr))
        {
            NamedArgExpr *na = (NamedArgExpr *) arg;
            ListCell   *lc;

            /* Reject duplicate arg names */
            foreach(lc, argnames)
            {
                if (strcmp(na->name, (char *) lfirst(lc)) == 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("argument name \"%s\" used more than once",
                                    na->name),
                             parser_errposition(pstate, na->location)));
            }
            argnames = lappend(argnames, na->name);
        }
        else
        {
            if (argnames != NIL)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("positional argument cannot follow named argument"),
                         parser_errposition(pstate, exprLocation(arg))));
        }
    }

    if (fargs)
    {
        first_arg = linitial(fargs);
        Assert(first_arg != NULL);
    }

    /*
     * Decide whether it's legitimate to consider the construct to be a column
     * projection.  For that, there has to be a single argument of complex
     * type, the function name must not be qualified, and there cannot be any
     * syntactic decoration that'd require it to be a function (such as
     * aggregate or variadic decoration, or named arguments).
     */
    could_be_projection = (nargs == 1 && !proc_call &&
                           agg_order == NIL && agg_filter == NULL &&
                           !agg_star && !agg_distinct && over == NULL &&
                           !func_variadic && argnames == NIL &&
                           list_length(funcname) == 1 &&
                           (actual_arg_types[0] == RECORDOID ||
                            ISCOMPLEX(actual_arg_types[0])));

    /*
     * If it's column syntax, check for column projection case first.
     */
    if (could_be_projection && is_column)
    {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("ParseComplexProjection not Implemented")));
/*
        retval = ParseComplexProjection(pstate,
                                        strVal(linitial(funcname)),
                                        first_arg,
                                        location);
        if (retval)
            return retval;
*/
        /*
         * If ParseComplexProjection doesn't recognize it as a projection,
         * just press on.
         */
    }

    /*
     * func_get_detail looks up the function in the catalogs, does
     * disambiguation for polymorphic functions, handles inheritance, and
     * returns the funcid and type and set or singleton status of the
     * function's return value.  It also returns the true argument types to
     * the function.
     *
     * Note: for a named-notation or variadic function call, the reported
     * "true" types aren't really what is in pg_proc: the types are reordered
     * to match the given argument order of named arguments, and a variadic
     * argument is replaced by a suitable number of copies of its element
     * type.  We'll fix up the variadic case below.  We may also have to deal
     * with default arguments.
     */

    setup_parser_errposition_callback(&pcbstate, pstate, location);

    fdresult = func_get_detail(funcname, fargs, argnames, nargs,
                               actual_arg_types,
                               !func_variadic, true, proc_call,
                               &funcid, &rettype, &retset,
                               &nvargs, &vatype,
                               &declared_arg_types, &argdefaults);

    cancel_parser_errposition_callback(&pcbstate);

    /*
     * Check for various wrong-kind-of-routine cases.
     */

    /* If this is a CALL, reject things that aren't procedures */
    if (proc_call &&
        (fdresult == FUNCDETAIL_NORMAL ||
         fdresult == FUNCDETAIL_AGGREGATE ||
         fdresult == FUNCDETAIL_WINDOWFUNC ||
         fdresult == FUNCDETAIL_COERCION))
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s is not a procedure",
                        func_signature_string(funcname, nargs,
                                              argnames,
                                              actual_arg_types)),
                 errhint("To call a function, use SELECT."),
                 parser_errposition(pstate, location)));
    /* Conversely, if not a CALL, reject procedures */
    if (fdresult == FUNCDETAIL_PROCEDURE && !proc_call)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s is a procedure",
                        func_signature_string(funcname, nargs,
                                              argnames,
                                              actual_arg_types)),
                 errhint("To call a procedure, use CALL."),
                 parser_errposition(pstate, location)));

    if (fdresult == FUNCDETAIL_NORMAL ||
        fdresult == FUNCDETAIL_PROCEDURE ||
        fdresult == FUNCDETAIL_COERCION)
    {
        /*
         * In these cases, complain if there was anything indicating it must
         * be an aggregate or window function.
         */
        if (agg_star)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s(*) specified, but %s is not an aggregate function",
                            NameListToString(funcname),
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        if (agg_distinct)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("DISTINCT specified, but %s is not an aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        if (agg_within_group)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("WITHIN GROUP specified, but %s is not an aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        if (agg_order != NIL)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("ORDER BY specified, but %s is not an aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        if (agg_filter)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("FILTER specified, but %s is not an aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        if (over)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("OVER specified, but %s is not a window function nor an aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
    }

    /*
     * So far so good, so do some fdresult-type-specific processing.
     */
    if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
    {
        /* Nothing special to do for these cases. */
    }
    else if (fdresult == FUNCDETAIL_AGGREGATE)
    {
        /*
         * It's an aggregate; fetch needed info from the pg_aggregate entry.
         */
        HeapTuple    tup;
        Form_pg_aggregate classForm;
        int            catDirectArgs;

        tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid));
        if (!HeapTupleIsValid(tup)) /* should not happen */
            elog(ERROR, "cache lookup failed for aggregate %u", funcid);
        classForm = (Form_pg_aggregate) GETSTRUCT(tup);
        aggkind = classForm->aggkind;
        catDirectArgs = classForm->aggnumdirectargs;
        ReleaseSysCache(tup);

        /* Now check various disallowed cases. */
        if (AGGKIND_IS_ORDERED_SET(aggkind))
        {
            int            numAggregatedArgs;
            int            numDirectArgs;

            if (!agg_within_group)
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("WITHIN GROUP is required for ordered-set aggregate %s",
                                NameListToString(funcname)),
                         parser_errposition(pstate, location)));
            if (over)
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("OVER is not supported for ordered-set aggregate %s",
                                NameListToString(funcname)),
                         parser_errposition(pstate, location)));
            /* gram.y rejects DISTINCT + WITHIN GROUP */
            Assert(!agg_distinct);
            /* gram.y rejects VARIADIC + WITHIN GROUP */
            Assert(!func_variadic);

            /*
             * Since func_get_detail was working with an undifferentiated list
             * of arguments, it might have selected an aggregate that doesn't
             * really match because it requires a different division of direct
             * and aggregated arguments.  Check that the number of direct
             * arguments is actually OK; if not, throw an "undefined function"
             * error, similarly to the case where a misplaced ORDER BY is used
             * in a regular aggregate call.
             */
            numAggregatedArgs = list_length(agg_order);
            numDirectArgs = nargs - numAggregatedArgs;
            Assert(numDirectArgs >= 0);

            if (!OidIsValid(vatype))
            {
                /* Test is simple if aggregate isn't variadic */
                if (numDirectArgs != catDirectArgs)
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                             errmsg("function %s does not exist",
                                    func_signature_string(funcname, nargs,
                                                          argnames,
                                                          actual_arg_types)),
                             errhint_plural("There is an ordered-set aggregate %s, but it requires %d direct argument, not %d.",
                                            "There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
                                            catDirectArgs,
                                            NameListToString(funcname),
                                            catDirectArgs, numDirectArgs),
                             parser_errposition(pstate, location)));
            }
            else
            {
                /*
                 * If it's variadic, we have two cases depending on whether
                 * the agg was "... ORDER BY VARIADIC" or "..., VARIADIC ORDER
                 * BY VARIADIC".  It's the latter if catDirectArgs equals
                 * pronargs; to save a catalog lookup, we reverse-engineer
                 * pronargs from the info we got from func_get_detail.
                 */
                int            pronargs;

                pronargs = nargs;
                if (nvargs > 1)
                    pronargs -= nvargs - 1;
                if (catDirectArgs < pronargs)
                {
                    /* VARIADIC isn't part of direct args, so still easy */
                    if (numDirectArgs != catDirectArgs)
                        ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                                 errmsg("function %s does not exist",
                                        func_signature_string(funcname, nargs,
                                                              argnames,
                                                              actual_arg_types)),
                                 errhint_plural("There is an ordered-set aggregate %s, but it requires %d direct argument, not %d.",
                                                "There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
                                                catDirectArgs,
                                                NameListToString(funcname),
                                                catDirectArgs, numDirectArgs),
                                 parser_errposition(pstate, location)));
                }
                else
                {
                    /*
                     * Both direct and aggregated args were declared variadic.
                     * For a standard ordered-set aggregate, it's okay as long
                     * as there aren't too few direct args.  For a
                     * hypothetical-set aggregate, we assume that the
                     * hypothetical arguments are those that matched the
                     * variadic parameter; there must be just as many of them
                     * as there are aggregated arguments.
                     */
                    if (aggkind == AGGKIND_HYPOTHETICAL)
                    {
                        if (nvargs != 2 * numAggregatedArgs)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                                     errmsg("function %s does not exist",
                                            func_signature_string(funcname, nargs,
                                                                  argnames,
                                                                  actual_arg_types)),
                                     errhint("To use the hypothetical-set aggregate %s, the number of hypothetical direct arguments (here %d) must match the number of ordering columns (here %d).",
                                             NameListToString(funcname),
                                             nvargs - numAggregatedArgs, numAggregatedArgs),
                                     parser_errposition(pstate, location)));
                    }
                    else
                    {
                        if (nvargs <= numAggregatedArgs)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                                     errmsg("function %s does not exist",
                                            func_signature_string(funcname, nargs,
                                                                  argnames,
                                                                  actual_arg_types)),
                                     errhint_plural("There is an ordered-set aggregate %s, but it requires at least %d direct argument.",
                                                    "There is an ordered-set aggregate %s, but it requires at least %d direct arguments.",
                                                    catDirectArgs,
                                                    NameListToString(funcname),
                                                    catDirectArgs),
                                     parser_errposition(pstate, location)));
                    }
                }
            }

            /* Check type matching of hypothetical arguments */
            if (aggkind == AGGKIND_HYPOTHETICAL)
/*            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("unify_hypothetical_args not implemented")));*/
                unify_hypothetical_args(pstate, fargs, numAggregatedArgs,
                                        actual_arg_types, declared_arg_types);
        }
        else
        {
            /* Normal aggregate, so it can't have WITHIN GROUP */
            if (agg_within_group)
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
                                NameListToString(funcname)),
                         parser_errposition(pstate, location)));
        }
    }
    else if (fdresult == FUNCDETAIL_WINDOWFUNC)
    {
        /*
         * True window functions must be called with a window definition.
         */
        if (!over)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("window function %s requires an OVER clause",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
        /* And, per spec, WITHIN GROUP isn't allowed */
        if (agg_within_group)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("window function %s cannot have WITHIN GROUP",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));
    }
    else if (fdresult == FUNCDETAIL_COERCION)
    {
        /*
         * We interpreted it as a type coercion. coerce_type can handle these
         * cases, so why duplicate code...
         */
        return coerce_type(pstate, linitial(fargs),
                           actual_arg_types[0], rettype, -1,
                           COERCION_EXPLICIT, COERCE_EXPLICIT_CALL, location);
    }
    else if (fdresult == FUNCDETAIL_MULTIPLE)
    {
        /*
         * We found multiple possible functional matches.  If we are dealing
         * with attribute notation, return failure, letting the caller report
         * "no such column" (we already determined there wasn't one).  If
         * dealing with function notation, report "ambiguous function",
         * regardless of whether there's also a column by this name.
         */
        if (is_column)
            return NULL;

        if (proc_call)
            ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                     errmsg("procedure %s is not unique",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("Could not choose a best candidate procedure. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                     errmsg("function %s is not unique",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("Could not choose a best candidate function. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
    }
    else
    {
        /*
         * Not found as a function.  If we are dealing with attribute
         * notation, return failure, letting the caller report "no such
         * column" (we already determined there wasn't one).
         */
        if (is_column)
            return NULL;

        /*
         * Check for column projection interpretation, since we didn't before.
         */
        if (could_be_projection)
        {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("ParseComplexProjection not Implemented")));
/*
            retval = ParseComplexProjection(pstate,
                                            strVal(linitial(funcname)),
                                            first_arg,
                                            location);
            if (retval)
                return retval;
*/
        }

        /*
         * No function, and no column either.  Since we're dealing with
         * function notation, report "function does not exist".
         */
        if (list_length(agg_order) > 1 && !agg_within_group)
        {
            /* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function %s does not exist",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("No aggregate function matches the given name and argument types. "
                             "Perhaps you misplaced ORDER BY; ORDER BY must appear "
                             "after all regular arguments of the aggregate."),
                     parser_errposition(pstate, location)));
        }
        else if (proc_call)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("procedure %s does not exist",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("No procedure matches the given name and argument types. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function %s does not exist",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("No function matches the given name and argument types. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
    }

    /*
     * If there are default arguments, we have to include their types in
     * actual_arg_types for the purpose of checking generic type consistency.
     * However, we do NOT put them into the generated parse node, because
     * their actual values might change before the query gets run.  The
     * planner has to insert the up-to-date values at plan time.
     */
    nargsplusdefs = nargs;
    foreach(l, argdefaults)
    {
        Node       *expr = (Node *) lfirst(l);

        /* probably shouldn't happen ... */
        if (nargsplusdefs >= FUNC_MAX_ARGS)
            ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                     errmsg_plural("cannot pass more than %d argument to a function",
                                   "cannot pass more than %d arguments to a function",
                                   FUNC_MAX_ARGS,
                                   FUNC_MAX_ARGS),
                     parser_errposition(pstate, location)));

        actual_arg_types[nargsplusdefs++] = exprType(expr);
    }

    /*
     * enforce consistency with polymorphic argument and return types,
     * possibly adjusting return type or declared_arg_types (which will be
     * used as the cast destination by make_fn_arguments)
     */
    rettype = enforce_generic_type_consistency(actual_arg_types,
                                               declared_arg_types,
                                               nargsplusdefs,
                                               rettype,
                                               false);

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

    /*
     * If the function isn't actually variadic, forget any VARIADIC decoration
     * on the call.  (Perhaps we should throw an error instead, but
     * historically we've allowed people to write that.)
     */
    if (!OidIsValid(vatype))
    {
        Assert(nvargs == 0);
        func_variadic = false;
    }

    /*
     * If it's a variadic function call, transform the last nvargs arguments
     * into an array --- unless it's an "any" variadic.
     */
    if (nvargs > 0 && vatype != ANYOID)
    {
        ArrayExpr  *newa = makeNode(ArrayExpr);
        int            non_var_args = nargs - nvargs;
        List       *vargs;

        Assert(non_var_args >= 0);
        vargs = list_copy_tail(fargs, non_var_args);
        fargs = list_truncate(fargs, non_var_args);

        newa->elements = vargs;
        /* assume all the variadic arguments were coerced to the same type */
        newa->element_typeid = exprType((Node *) linitial(vargs));
        newa->array_typeid = get_array_type(newa->element_typeid);
        if (!OidIsValid(newa->array_typeid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("could not find array type for data type %s",
                            format_type_be(newa->element_typeid)),
                     parser_errposition(pstate, exprLocation((Node *) vargs))));
        /* array_collid will be set by parse_collate.c */
        newa->multidims = false;
        newa->location = exprLocation((Node *) vargs);

        fargs = lappend(fargs, newa);

        /* We could not have had VARIADIC marking before ... */
        Assert(!func_variadic);
        /* ... but now, it's a VARIADIC call */
        func_variadic = true;
    }

    /*
     * If an "any" variadic is called with explicit VARIADIC marking, insist
     * that the variadic parameter be of some array type.
     */
    if (nargs > 0 && vatype == ANYOID && func_variadic)
    {
        Oid            va_arr_typid = actual_arg_types[nargs - 1];

        if (!OidIsValid(get_base_element_type(va_arr_typid)))
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("VARIADIC argument must be an array"),
                     parser_errposition(pstate,
                                        exprLocation((Node *) llast(fargs)))));
    }

    /* if it returns a set, check that's OK */
    if (retset)
        check_srf_call_placement(pstate, last_srf, location);

    /* build the appropriate output structure */
    if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
    {
        FuncExpr   *funcexpr = makeNode(FuncExpr);

        funcexpr->funcid = funcid;
        funcexpr->funcresulttype = rettype;
        funcexpr->funcretset = retset;
        funcexpr->funcvariadic = func_variadic;
        funcexpr->funcformat = funcformat;
        /* funccollid and inputcollid will be set by parse_collate.c */
        funcexpr->args = fargs;
        funcexpr->location = location;

        retval = (Node *) funcexpr;
    }
    else if (fdresult == FUNCDETAIL_AGGREGATE && !over)
    {
        /* aggregate function */
        Aggref       *aggref = makeNode(Aggref);

        aggref->aggfnoid = funcid;
        aggref->aggtype = rettype;
        /* aggcollid and inputcollid will be set by parse_collate.c */
        aggref->aggtranstype = InvalidOid;    /* will be set by planner */
        /* aggargtypes will be set by transformAggregateCall */
        /* aggdirectargs and args will be set by transformAggregateCall */
        /* aggorder and aggdistinct will be set by transformAggregateCall */
        aggref->aggfilter = agg_filter;
        aggref->aggstar = agg_star;
        aggref->aggvariadic = func_variadic;
        aggref->aggkind = aggkind;
        /* agglevelsup will be set by transformAggregateCall */
        aggref->aggsplit = AGGSPLIT_SIMPLE; /* planner might change this */
        aggref->aggno = -1;        /* planner will set aggno and aggtransno */
        aggref->aggtransno = -1;
        aggref->location = location;

        /*
         * Reject attempt to call a parameterless aggregate without (*)
         * syntax.  This is mere pedantry but some folks insisted ...
         */
        if (fargs == NIL && !agg_star && !agg_within_group)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s(*) must be used to call a parameterless aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));

        if (retset)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                     errmsg("aggregates cannot return sets"),
                     parser_errposition(pstate, location)));

        /*
         * We might want to support named arguments later, but disallow it for
         * now.  We'd need to figure out the parsed representation (should the
         * NamedArgExprs go above or below the TargetEntry nodes?) and then
         * teach the planner to reorder the list properly.  Or maybe we could
         * make transformAggregateCall do that?  However, if you'd also like
         * to allow default arguments for aggregates, we'd need to do it in
         * planning to avoid semantic problems.
         */
        if (argnames != NIL)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("aggregates cannot use named arguments"),
                     parser_errposition(pstate, location)));

        /* parse_agg.c does additional aggregate-specific processing */
        transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

        retval = (Node *) aggref;
    }
    else
    {
        /* window function */
        WindowFunc *wfunc = makeNode(WindowFunc);

        Assert(over);            /* lack of this was checked above */
        Assert(!agg_within_group);    /* also checked above */

        wfunc->winfnoid = funcid;
        wfunc->wintype = rettype;
        /* wincollid and inputcollid will be set by parse_collate.c */
        wfunc->args = fargs;
        /* winref will be set by transformWindowFuncCall */
        wfunc->winstar = agg_star;
        wfunc->winagg = (fdresult == FUNCDETAIL_AGGREGATE);
        wfunc->aggfilter = agg_filter;
        wfunc->location = location;

        /*
         * agg_star is allowed for aggregate functions but distinct isn't
         */
        if (agg_distinct)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("DISTINCT is not implemented for window functions"),
                     parser_errposition(pstate, location)));

        /*
         * Reject attempt to call a parameterless aggregate without (*)
         * syntax.  This is mere pedantry but some folks insisted ...
         */
        if (wfunc->winagg && fargs == NIL && !agg_star)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s(*) must be used to call a parameterless aggregate function",
                            NameListToString(funcname)),
                     parser_errposition(pstate, location)));

        /*
         * ordered aggs not allowed in windows yet
         */
        if (agg_order != NIL)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("aggregate ORDER BY is not implemented for window functions"),
                     parser_errposition(pstate, location)));

        /*
         * FILTER is not yet supported with true window functions
         */
        if (!wfunc->winagg && agg_filter)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("FILTER is not implemented for non-aggregate window functions"),
                     parser_errposition(pstate, location)));

        /*
         * Window functions can't either take or return sets
         */
        if (pstate->p_last_srf != last_srf)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("window function calls cannot contain set-returning function calls"),
                     errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
                     parser_errposition(pstate,
                                        exprLocation(pstate->p_last_srf))));

        if (retset)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                     errmsg("window functions cannot return sets"),
                     parser_errposition(pstate, location)));

        /* parse_agg.c does additional window-func-specific processing */
        transformWindowFuncCall(pstate, wfunc, over);

        retval = (Node *) wfunc;
    }

    /* if it returns a set, remember it for error checks at higher levels */
    if (retset)
        pstate->p_last_srf = retval;

    return retval;
}


static Node *
transform_a_const(cypher_parsestate *cpstate, A_Const *ac) {
    Value *v = &ac->val;
    Datum d = (Datum)0;
    bool is_null = false;
    Const *c;

    switch (nodeTag(v))
    {
    case T_Integer:
        d = integer_to_gtype((int64)intVal(v));
        break;
    case T_Float:
        {
            char *n = strVal(v);
            int64 i;

            if (scanint8(n, true, &i)) {
                d = integer_to_gtype(i);
            } else {
                float8 f = float8in_internal(n, NULL, "double precision", n);

                d = float_to_gtype(f);
            }
        }
        break;
    case T_String:
        d = string_to_gtype(strVal(v));
        break;
    case T_Null:
        is_null = true;
        break;
    default:
        ereport(ERROR,
                (errmsg_internal("unrecognized node type: %d", nodeTag(v))));
        return NULL;
    }

    // typtypmod, typcollation, typlen, and typbyval of gtype are hard-coded.
    c = makeConst(GTYPEOID, -1, InvalidOid, -1, d, is_null, false);
    c->location = ac->location;
    return (Node *)c;
}

static Node *
transformWholeRowRef(ParseState *pstate, ParseNamespaceItem *nsitem,
                                         int sublevels_up, int location)
{
        /*
         * Build the appropriate referencing node.  Normally this can be a
         * whole-row Var, but if the nsitem is a JOIN USING alias then it contains
         * only a subset of the columns of the underlying join RTE, so that will
         * not work.  Instead we immediately expand the reference into a RowExpr.
         * Since the JOIN USING's common columns are fully determined at this
         * point, there seems no harm in expanding it now rather than during
         * planning.
         *
         * Note that if the RTE is a function returning scalar, we create just a
         * plain reference to the function value, not a composite containing a
         * single column.  This is pretty inconsistent at first sight, but it's
         * what we've done historically.  One argument for it is that "rel" and
         * "rel.*" mean the same thing for composite relations, so why not for
         * scalar functions...
         */
        if (nsitem->p_names == nsitem->p_rte->eref)
        {
                Var                *result;

                result = makeWholeRowVar(nsitem->p_rte, nsitem->p_rtindex,
                                                                 sublevels_up, true);

                /* location is not filled in by makeWholeRowVar */
                result->location = location;

                /* mark relation as requiring whole-row SELECT access */
                markVarForSelectPriv(pstate, result);

                return (Node *) result;
        }
        else
        {
                RowExpr    *rowexpr;
                List       *fields;

                /*
                 * We want only as many columns as are listed in p_names->colnames,
                 * and we should use those names not whatever possibly-aliased names
                 * are in the RTE.  We needn't worry about marking the RTE for SELECT
                 * access, as the common columns are surely so marked already.
                 */
                expandRTE(nsitem->p_rte, nsitem->p_rtindex,
                                  sublevels_up, location, false,
                                  NULL, &fields);
                rowexpr = makeNode(RowExpr);
                rowexpr->args = list_truncate(fields,
                                                                          list_length(nsitem->p_names->colnames));
                rowexpr->row_typeid = RECORDOID;
                rowexpr->row_format = COERCE_IMPLICIT_CAST;
                rowexpr->colnames = copyObject(nsitem->p_names->colnames);
                rowexpr->location = location;

                return (Node *) rowexpr;
        }
}


static Node *
transform_column_ref(cypher_parsestate *cpstate, ColumnRef *cref) {
    ParseState *pstate = (ParseState *)cpstate;
    Node *field1 = NULL;
    Node *field2 = NULL;
    char *colname = NULL;
    char *nspname = NULL;
    char *relname = NULL;
    Node *node = NULL;
    int levels_up;
    ParseNamespaceItem *pnsi;

    switch (list_length(cref->fields))
    {
        case 1:
            {
                transform_entity *te;
                field1 = (Node*)linitial(cref->fields);

                Assert(IsA(field1, String));
                colname = strVal(field1);

                node = colNameToVar(pstate, colname, false, cref->location);
                if (node != NULL)
                    break;

                te = find_variable(cpstate, colname) ;
                if (te != NULL && te->expr != NULL) {
                    node = (Node *)te->expr;
                    break;
                }
                else {
                    ParseNamespaceItem *nsitem = refnameNamespaceItem(pstate, NULL, colname, cref->location, &levels_up);

                    if (nsitem) {
                        return transformWholeRowRef(pstate, nsitem, levels_up, cref->location);
                }

                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                                    errmsg("could not find rte for %s", colname),
                                    parser_errposition(pstate, cref->location)));
                }
                break;
            }
        case 2:
            {
                Oid inputTypeId = InvalidOid;
                Oid targetTypeId = InvalidOid;

                field1 = (Node*)linitial(cref->fields);
                field2 = (Node*)lsecond(cref->fields);

                Assert(IsA(field1, String));
                relname = strVal(field1);

                if (IsA(field2, String))
                    colname = strVal(field2);

                /* locate the referenced RTE */
                pnsi = refnameNamespaceItem(pstate, nspname, relname,
                                           cref->location, &levels_up);

                if (pnsi == NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_COLUMN),
                             errmsg("could not find rte for %s.%s", relname, colname),
                             parser_errposition(pstate, cref->location)));

                Assert(IsA(field2, String));

                /* try to identify as a column of the RTE */
                node = scanNSItemForColumn(pstate, pnsi, 0, colname, cref->location);

                if (!node)
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_COLUMN),
                             errmsg("could not find column %s in rel %s of rte", colname, relname),
                             parser_errposition(pstate, cref->location)));

                /* coerce it to GTYPE if possible */
                inputTypeId = exprType(node);
                /*targetTypeId = GTYPEOID;

                if (can_coerce_type(1, &inputTypeId, &targetTypeId, COERCION_EXPLICIT)) {
                    node = coerce_type(pstate, node, inputTypeId, targetTypeId,
                                       -1, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
                }*/
                break;
            }
        default:
            {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("improper qualified name (too many dotted names): %s",
                                NameListToString(cref->fields)),
                         parser_errposition(pstate, cref->location)));
                break;
            }
    }

    if (!node)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("variable `%s` does not exist", colname),
                 parser_errposition(pstate, cref->location)));

    return node;
}

/*
 * There are some operators where the result of referencing
 * the verties's or edge's properties or referencing the
 * vertex or edge itself will yield the same results, at the
 * cost of longer runtine and loss of index visibility for the
 * later way. For those operators, we wanna just reference the 
 * vertex or edge properties directly, when its possible.
 */
static bool
is_a_valid_operator(List *lst) {
    if (list_length(lst) != 1)
        return false;

    if (!IsA(linitial(lst), String))
        return false;

    Value *str = linitial(lst);

    if (!strcmp(str->val.str, "?") || !strcmp(str->val.str, "?|") || !strcmp(str->val.str, "?&"))
        return true;

    return false;
}

static Node *
transform_a_expr_op(cypher_parsestate *cpstate, A_Expr *a) {
    ParseState *pstate = (ParseState *)cpstate;
    Node *lexpr = NULL;

    Node *last_srf = pstate->p_last_srf;

    if (a->lexpr != NULL && is_a_valid_operator(a->name) && IsA(a->lexpr, ColumnRef)) {
        ColumnRef *cr = a->lexpr;
        ParseNamespaceItem *pnsi;
        Node *field1 = linitial(cr->fields);
        char *relname;
        int levels_up = 0;

        field1 = linitial(cr->fields);
        Assert(IsA(field1, String));
        relname = strVal(field1);

        // locate the referenced RTE
        pnsi = refnameNamespaceItem(pstate, NULL, relname, cr->location, &levels_up);
        if (pnsi)
            lexpr = scanNSItemForColumn(pstate, pnsi, 0, "properties", cr->location);
    }

    if (lexpr == NULL)
         lexpr = transform_cypher_expr_recurse(cpstate, a->lexpr);

    Node *rexpr = transform_cypher_expr_recurse(cpstate, a->rexpr);

    return (Node *)make_op(pstate, a->name, lexpr, rexpr, last_srf, a->location);
}

static Node *
transform_bool_expr(cypher_parsestate *cpstate, BoolExpr *expr) {
    ParseState *pstate = (ParseState *)cpstate;
    List *args = NIL;
    const char *opname;
    ListCell *la;

    switch (expr->boolop)
    {
    case AND_EXPR:
        opname = "AND";
        break;
    case OR_EXPR:
        opname = "OR";
        break;
    case NOT_EXPR:
        opname = "NOT";
        break;
    default:
        ereport(ERROR, (errmsg_internal("unrecognized boolop: %d",
                                        (int)expr->boolop)));
        return NULL;
    }

    foreach (la, expr->args)
    {
        Node *arg = lfirst(la);

        arg = transform_cypher_expr_recurse(cpstate, arg);
        arg = coerce_to_boolean(pstate, arg, opname);

        args = lappend(args, arg);
    }

    return (Node *)makeBoolExpr(expr->boolop, args, expr->location);
}

static Node *
transform_cypher_bool_const(cypher_parsestate *cpstate, cypher_bool_const *b) {

    Datum agt = boolean_to_gtype(b->boolean);

    // typtypmod, typcollation, typlen, and typbyval of gtype are hard-coded.
    Const *c = makeConst(GTYPEOID, -1, InvalidOid, -1, agt, false, false);
    c->location = b->location;

    return (Node *)c;
}


static Node *
transform_cypher_inet_const(cypher_parsestate *cpstate, cypher_inet_const *inet) {

    Datum agt = _gtype_toinet(GTYPE_P_GET_DATUM(string_to_gtype(inet->inet)));

    // typtypmod, typcollation, typlen, and typbyval of gtype are hard-coded.
    Const *c = makeConst(GTYPEOID, -1, InvalidOid, -1, agt, false, false);
    c->location = inet->location;

    return (Node *)c;
}

static Node *
transform_cypher_integer_const(cypher_parsestate *cpstate, cypher_integer_const *i) {

    Datum agt = integer_to_gtype(i->integer);

    // typtypmod, typcollation, typlen, and typbyval of gtype are hard-coded.
    Const *c = makeConst(GTYPEOID, -1, InvalidOid, -1, agt, false, false);
    c->location = i->location;

    return (Node *)c;
}

static Node *
transform_cypher_param(cypher_parsestate *cpstate, cypher_param *p) {
    ParseState *pstate = (ParseState *)cpstate;
    Const *const_str;

    if (!cpstate->params)
        ereport( ERROR,
            (errcode(ERRCODE_UNDEFINED_PARAMETER),
             errmsg("parameters argument is missing from cypher() function call"),
             parser_errposition(pstate, p->location)));

    const_str = makeConst(GTYPEOID, -1, InvalidOid, -1, string_to_gtype(p->name), false, false);

    return (Node *)make_op(pstate, list_make1(makeString("->")), (Node *)cpstate->params, (Node *)const_str, pstate->p_last_srf,  -1);
}

static Node *
transform_cypher_map(cypher_parsestate *cpstate, cypher_map *cm) {
    List *newkeyvals = NIL;
    ListCell *le;
    FuncExpr *fexpr;
    Oid func_oid;

    Assert(list_length(cm->keyvals) % 2 == 0);

    le = list_head(cm->keyvals);
    while (le != NULL) {
        Node *key = lfirst(le);
        le = lnext(cm->keyvals, le);

        Node *val = lfirst(le);
        le = lnext(cm->keyvals, le);

        Node *newval = transform_cypher_expr_recurse(cpstate, val);

        // typtypmod, typcollation, typlen, and typbyval of gtype are hard-coded.
        Const *newkey = makeConst(TEXTOID, -1, InvalidOid, -1, CStringGetTextDatum(strVal(key)), false, false);

        newkeyvals = lappend(lappend(newkeyvals, newkey), newval);
    }

    if (list_length(newkeyvals) == 0)
        func_oid = get_ag_func_oid("gtype_build_map", 0);
    else
        func_oid = get_ag_func_oid("gtype_build_map", 1, ANYOID);

    fexpr = makeFuncExpr(func_oid, GTYPEOID, newkeyvals, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
    fexpr->location = cm->location;

    return (Node *)fexpr;
}

static Node *
transform_cypher_list(cypher_parsestate *cpstate, cypher_list *cl) {
    List *args = NIL;
    ListCell *lc;
    foreach (lc, cl->elems) {
        args = lappend(args, transform_cypher_expr_recurse(cpstate, lfirst(lc)));
    }

    Oid oid;
    if (list_length(args) == 0)
        oid = get_ag_func_oid("gtype_build_list", 0);
    else
        oid = get_ag_func_oid("gtype_build_list", 1, ANYOID);

    FuncExpr *expr = makeFuncExpr(oid, GTYPEOID, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
    expr->location = cl->location;

    return (Node *)expr;
}

/*
 * Transforms a column ref for indirection. Try to find the rte that the
 * columnRef is references and pass the properties of that rte as what the
 * columnRef is referencing. Otherwise, reference the Var
 */
static Node *
transform_column_ref_for_indirection(cypher_parsestate *cpstate, ColumnRef *cr) {
    ParseState *pstate = (ParseState *)cpstate;
    ParseNamespaceItem *pnsi;
    Node *field1 = linitial(cr->fields);
    char *relname;
    Node *node;
    int levels_up = 0;

    field1 = linitial(cr->fields);
    Assert(IsA(field1, String));
    relname = strVal(field1);

    // locate the referenced RTE
    pnsi = refnameNamespaceItem(pstate, NULL, relname, cr->location, &levels_up);

    // This column ref is referencing something that was created in a previous query and is a variable.
    if (!pnsi)
        return transform_cypher_expr_recurse(cpstate, (Node *)cr);

    // try to identify the properties column of the RTE
    node = scanNSItemForColumn(pstate, pnsi, 0, "properties", cr->location);

    if (!node)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find rte for %s", relname)));

    return node;
}

static Node *
transform_a_indirection(cypher_parsestate *cpstate, A_Indirection *a_ind) {
    ParseState *pstate = (ParseState *)cpstate;

    /*
     * we can mark the last srf here, because if any part of this function finds another
     * srf, its a problem, no need to track this throughout
     *
     * XXX: There might be a use case that allows us to use SRFs with Indirection... leave it
     * as a potential todo for now.
     */
    Node *last_srf = pstate->p_last_srf;

    Assert(a_ind != NULL && list_length(a_ind->indirection) > 0);

    // transform primary indirection argument
    Node *cur;
    if (IsA(a_ind->arg, ColumnRef))
        cur = transform_column_ref_for_indirection(cpstate, (ColumnRef *)a_ind->arg);
    else
        cur = transform_cypher_expr_recurse(cpstate, a_ind->arg);

    // iterate through each indirection value
    ListCell *lc;
    foreach (lc, a_ind->indirection)
    {
        Node *node = lfirst(lc);

        if (is_a_slice(node)) {
            A_Indices *indices = (A_Indices *)node;

            List *args = list_make1(cur);

            // lower bound
            if (!indices->lidx)
                args = lappend(args, makeConst(GTYPEOID, -1, InvalidOid, -1, (Datum)NULL, true, false));
            else
                args = lappend(args, transform_cypher_expr_recurse(cpstate, indices->lidx));

            // upper bound
            if (!indices->uidx)
                args = lappend(args, makeConst(GTYPEOID, -1, InvalidOid, -1, (Datum)NULL, true, false));
            else
                args = lappend(args, transform_cypher_expr_recurse(cpstate, indices->uidx));

            Oid oid = get_ag_func_oid("gtype_access_slice", 3, GTYPEOID, GTYPEOID, GTYPEOID);
            FuncExpr *func_expr = makeFuncExpr(oid, GTYPEOID, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
            func_expr->location = exprLocation(cur);

            cur = (Node *)func_expr;
        } else if (IsA(node, A_Indices)) {
            // a[i]
            A_Indices *indices = (A_Indices *)node;

            node = transform_cypher_expr_recurse(cpstate, indices->uidx);
            cur = (Node *)make_op(pstate, list_make1(makeString("->")), cur, node, pstate->p_last_srf,  -1);
        } else if (IsA(node, ColumnRef)) {
            // a.i
            ColumnRef *cr = (ColumnRef*)node;
            List *fields = cr->fields;
            Value *string = linitial(fields);

            Const *const_str = makeConst(GTYPEOID, -1, InvalidOid, -1, string_to_gtype(strVal(string)), false, false);

            cur = (Node *)make_op(pstate, list_make1(makeString("->")), cur, (Node *)const_str, pstate->p_last_srf,  -1);

        } else {
            ereport(ERROR, (errmsg("invalid indirection node %d", nodeTag(node))));
        }

        // TODO: Add Regression Tests
        if (pstate->p_last_srf != last_srf)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-returning functions are not allowed in indirection"),
                parser_errposition(pstate, exprLocation(pstate->p_last_srf))));

    }
    
    return cur;
}

static Node *
transform_cypher_string_match(cypher_parsestate *cpstate, cypher_string_match *csm_node) {
    const char *func_name;
    switch (csm_node->operation)
    {
    case CSMO_STARTS_WITH:
        func_name = "gtype_string_match_starts_with";
        break;
    case CSMO_ENDS_WITH:
        func_name = "gtype_string_match_ends_with";
        break;
    case CSMO_CONTAINS:
        func_name = "gtype_string_match_contains";
        break;
    default:
        ereport(ERROR, (errmsg_internal("unknown Cypher string match operation")));
    }

    Oid oid = get_ag_func_oid(func_name, 2, GTYPEOID, GTYPEOID);

    List *args = list_make2(transform_cypher_expr_recurse(cpstate, csm_node->lhs),
                            transform_cypher_expr_recurse(cpstate, csm_node->rhs));

    FuncExpr *expr = makeFuncExpr(oid, GTYPEOID, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
    expr->location = csm_node->location;

    return (Node *)expr;
}

/*
 * Function to create a typecasting node
 */
static Node *
transform_cypher_typecast(cypher_parsestate *cpstate, cypher_typecast *ctypecast) {
    List *fname;
    FuncCall *fnode;

    /* verify input parameter */
    Assert (cpstate != NULL);
    Assert (ctypecast != NULL);

    /* create the qualified function name, schema first */
    fname = list_make1(makeString(CATALOG_SCHEMA));

    /* append the name of the requested typecast function */
    if (pg_strcasecmp(ctypecast->typecast, "numeric") == 0)
        fname = lappend(fname, makeString("tonumeric"));
    else if (pg_strcasecmp(ctypecast->typecast, "float") == 0)
        fname = lappend(fname, makeString("tofloat"));
    else if (pg_strcasecmp(ctypecast->typecast, "int") == 0 || pg_strcasecmp(ctypecast->typecast, "integer") == 0)
        fname = lappend(fname, makeString("tointeger"));
    else if (pg_strcasecmp(ctypecast->typecast, "timestamp") == 0)
        fname = lappend(fname, makeString("totimestamp"));
    else if (pg_strcasecmp(ctypecast->typecast, "timestamptz") == 0)
        fname = lappend(fname, makeString("totimestamptz"));
    else if (pg_strcasecmp(ctypecast->typecast, "date") == 0)
        fname = lappend(fname, makeString("todate"));
    else if (pg_strcasecmp(ctypecast->typecast, "time") == 0)
        fname = lappend(fname, makeString("totime"));
    else if (pg_strcasecmp(ctypecast->typecast, "timetz") == 0)
        fname = lappend(fname, makeString("totimetz"));
    else if (pg_strcasecmp(ctypecast->typecast, "interval") == 0)
        fname = lappend(fname, makeString("tointerval"));
    else if (pg_strcasecmp(ctypecast->typecast, "vector") == 0)
        fname = lappend(fname, makeString("tovector"));
    else if (pg_strcasecmp(ctypecast->typecast, "inet") == 0)
        fname = lappend(fname, makeString("toinet"));
    else if (pg_strcasecmp(ctypecast->typecast, "cidr") == 0)
        fname = lappend(fname, makeString("tocidr"));
    else if (pg_strcasecmp(ctypecast->typecast, "macaddr") == 0)
        fname = lappend(fname, makeString("tomacaddr"));
    else if (pg_strcasecmp(ctypecast->typecast, "macaddr8") == 0)
        fname = lappend(fname, makeString("tomacaddr8"));
    else if (pg_strcasecmp(ctypecast->typecast, "point") == 0)
        fname = lappend(fname, makeString("topoint"));
    else if (pg_strcasecmp(ctypecast->typecast, "line") == 0)
        fname = lappend(fname, makeString("toline"));
    else if (pg_strcasecmp(ctypecast->typecast, "lseg") == 0)
        fname = lappend(fname, makeString("tolseg"));
    else if (pg_strcasecmp(ctypecast->typecast, "path") == 0)
        fname = lappend(fname, makeString("topath"));
    else if (pg_strcasecmp(ctypecast->typecast, "polygon") == 0)
        fname = lappend(fname, makeString("topolygon"));
    else if (pg_strcasecmp(ctypecast->typecast, "circle") == 0)
        fname = lappend(fname, makeString("tocircle"));
    else if (pg_strcasecmp(ctypecast->typecast, "box") == 0)
        fname = lappend(fname, makeString("tobox"));
    else if (pg_strcasecmp(ctypecast->typecast, "box2d") == 0)
        fname = lappend(fname, makeString("tobox2d"));
    else if (pg_strcasecmp(ctypecast->typecast, "box3d") == 0)
        fname = lappend(fname, makeString("tobox3d"));
    else if (pg_strcasecmp(ctypecast->typecast, "spheroid") == 0)
        fname = lappend(fname, makeString("tospheroid"));
    else if (pg_strcasecmp(ctypecast->typecast, "geometry") == 0)
        fname = lappend(fname, makeString("togeometry"));
    else if (pg_strcasecmp(ctypecast->typecast, "tsvector") == 0)
        fname = lappend(fname, makeString("totsvector"));
    else if (pg_strcasecmp(ctypecast->typecast, "tsquery") == 0)
        fname = lappend(fname, makeString("totsquery"));
    else
        ereport(ERROR, (errmsg_internal("typecast \'%s\' not supported", ctypecast->typecast)));

    // make a function call node
    fnode = makeFuncCall(fname, list_make1(ctypecast->expr), COERCE_SQL_SYNTAX, ctypecast->location);

    return transform_func_call(cpstate, fnode);
}


/*
 * unify_hypothetical_args()
 *
 * Ensure that each hypothetical direct argument of a hypothetical-set
 * aggregate has the same type as the corresponding aggregated argument.
 * Modify the expressions in the fargs list, if necessary, and update
 * actual_arg_types[].
 *
 * If the agg declared its args non-ANY (even ANYELEMENT), we need only a
 * sanity check that the declared types match; make_fn_arguments will coerce
 * the actual arguments to match the declared ones.  But if the declaration
 * is ANY, nothing will happen in make_fn_arguments, so we need to fix any
 * mismatch here.  We use the same type resolution logic as UNION etc.
 */
static void
unify_hypothetical_args(ParseState *pstate,
                        List *fargs,
                        int numAggregatedArgs,
                        Oid *actual_arg_types,
                        Oid *declared_arg_types)
{
    int            numDirectArgs,
                numNonHypotheticalArgs;
    int            hargpos;

    numDirectArgs = list_length(fargs) - numAggregatedArgs;
    numNonHypotheticalArgs = numDirectArgs - numAggregatedArgs;
    /* safety check (should only trigger with a misdeclared agg) */
    if (numNonHypotheticalArgs < 0)
        elog(ERROR, "incorrect number of arguments to hypothetical-set aggregate");

    /* Check each hypothetical arg and corresponding aggregated arg */
    for (hargpos = numNonHypotheticalArgs; hargpos < numDirectArgs; hargpos++)
    {
        int            aargpos = numDirectArgs + (hargpos - numNonHypotheticalArgs);
        ListCell   *harg = list_nth_cell(fargs, hargpos);
        ListCell   *aarg = list_nth_cell(fargs, aargpos);
        Oid            commontype;
        int32        commontypmod;

        /* A mismatch means AggregateCreate didn't check properly ... */
        if (declared_arg_types[hargpos] != declared_arg_types[aargpos])
            elog(ERROR, "hypothetical-set aggregate has inconsistent declared argument types");

        /* No need to unify if make_fn_arguments will coerce */
        if (declared_arg_types[hargpos] != ANYOID)
            continue;

        /*
         * Select common type, giving preference to the aggregated argument's
         * type (we'd rather coerce the direct argument once than coerce all
         * the aggregated values).
         */
        commontype = select_common_type(pstate,
                                        list_make2(lfirst(aarg), lfirst(harg)),
                                        "WITHIN GROUP",
                                        NULL);
        commontypmod = select_common_typmod(pstate,
                                            list_make2(lfirst(aarg), lfirst(harg)),
                                            commontype);

        /*
         * Perform the coercions.  We don't need to worry about NamedArgExprs
         * here because they aren't supported with aggregates.
         */
        lfirst(harg) = coerce_type(pstate,
                                   (Node *) lfirst(harg),
                                   actual_arg_types[hargpos],
                                   commontype, commontypmod,
                                   COERCION_IMPLICIT,
                                   COERCE_IMPLICIT_CAST,
                                   -1);
        actual_arg_types[hargpos] = commontype;
        lfirst(aarg) = coerce_type(pstate,
                                   (Node *) lfirst(aarg),
                                   actual_arg_types[aargpos],
                                   commontype, commontypmod,
                                   COERCION_IMPLICIT,
                                   COERCE_IMPLICIT_CAST,
                                   -1);
        actual_arg_types[aargpos] = commontype;
    }
}

static List *
make_qualified_function_name(cypher_parsestate *cpstate, List *lst, List *targs) {
    char *name = ((Value*)linitial(lst))->val.str;
    int pnlen = strlen(name);
    char *ag_name = palloc(pnlen + 1);
    int i;
        
    /*
     * change all functions to lower case, this is mostly uncessary, but some
     * functions are keywords in SQL, we need to escape those functions in the sql
     * script to a lower case version, do that for all functions to make life
     * easier.
     */ 
    for (i = 0; i < pnlen; i++)
        ag_name[i] = tolower(name[i]);
        
    // Add NULL at the end
    ag_name[i] = '\0';

    // add the catalog schema and create list        
    List *fname;
    if (strcmp(ag_name, "now") == 0 || strcmp(ag_name, "transaction_timestamp") == 0 ||
        strcmp(ag_name, "statement_timestamp") == 0 ||  strcmp(ag_name, "clock_timestamp") == 0 ||
        strcmp(ag_name, "timeofday") == 0 || strcmp(ag_name, "row_number") == 0 ||
        strcmp(name, "rank") == 0 || strcmp(name, "dense_rank") == 0 ||
        strcmp(name, "percent_rank") == 0 || strcmp(name, "cume_dist") == 0 || 
        strcmp(name, "lag") == 0 || strcmp(name, "lead") == 0 || strcmp(name, "first_value") == 0 ||
        strcmp(name, "last_value") == 0 || strcmp(name, "ntile") == 0 || strcmp(name, "nth_value") == 0) 
        fname = list_make2(makeString("pg_catalog"), makeString(ag_name));
    else
        fname = list_make2(makeString(CATALOG_SCHEMA), makeString(ag_name));

    // Some functions need the graph name passed to them in order to work
    if (strcmp("vle", ag_name) == 0) {
        char *graph_name = cpstate->graph_name;
        Datum d = string_to_gtype(graph_name);
        Const *c = makeConst(GTYPEOID, -1, InvalidOid, -1, d, false, false);

        targs = lcons(c, targs);
    }

    return fname;

}

static Node *
transform_func_call(cypher_parsestate *cpstate, FuncCall *fn) {
    ParseState *pstate = (ParseState *)cpstate;
    Node *last_srf = pstate->p_last_srf;
    ListCell *lc;

    // transform args
    List *args = NIL;
    foreach(lc, fn->args) {
        args = lappend(args, transform_cypher_expr_recurse(cpstate, (Node *)lfirst(lc)));
    }

    // if the function name is not qualified assume the function is in our catalog.
    List *fname;
    if (list_length(fn->funcname) == 1)
        fname = make_qualified_function_name(cpstate, fn->funcname, args);
    else
        fname = fn->funcname;

    // Passed our new fname to the normal function transform logic
    Node *retval = parse_func_or_column(pstate, fname, args, last_srf, fn, false, fn->location);


    if (list_length(fn->funcname) == 1) {
        char *name = strVal(linitial(fname));
    if (strcmp(name, "now") == 0 || strcmp(name, "transaction_timestamp") == 0 ||
        strcmp(name, "statement_timestamp") == 0 || strcmp(name, "clock_timestamp") == 0 ||
        strcmp(name, "timeofday") == 0 || strcmp(name, "row_number") == 0 ||
        strcmp(name, "rank") == 0 || strcmp(name, "dense_rank") == 0 ||
        strcmp(name, "percent_rank") == 0 || strcmp(name, "cume_dist") == 0) {
            Node *result;
            Oid inputType;
            Oid targetType;
            int32 targetTypmod;
    
            typenameTypeIdAndMod(cpstate, makeTypeName("gtype"), &targetType, &targetTypmod);

            inputType = exprType(retval);

            result = coerce_to_target_type(cpstate, retval, inputType, targetType, targetTypmod,
                                           COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);

            if (result == NULL)
                ereport(ERROR, (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast type %s to %s", format_type_be(inputType), format_type_be(targetType)),
                        parser_coercion_errposition(cpstate, -1, retval)));

            retval = result;
    }
    }
    // flag that an aggregate was found
    if (retval != NULL && retval->type == T_Aggref)
        cpstate->exprHasAgg = true;

    return retval;
}

static Node *
transform_coalesce_expr(cypher_parsestate *cpstate, CoalesceExpr *cexpr) {
    ParseState *pstate = (ParseState *)cpstate;
    Node *last_srf = pstate->p_last_srf;
    ListCell *lc;

    CoalesceExpr *newcexpr = makeNode(CoalesceExpr);

    // transform each argument
    List *newargs = NIL;
    foreach(lc, cexpr->args) {
        Node *newe = transform_cypher_expr_recurse(cpstate, (Node *)lfirst(lc));

        newargs = lappend(newargs, newe);
    }

    newcexpr->coalescetype = select_common_type(pstate, newargs, "COALESCE", NULL);

    if (newcexpr->coalescetype != GTYPEOID)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("COALESCE in a cypher query expects to return gtype"),
                 parser_errposition(pstate, exprLocation((Node*)cexpr))));


    // coearce each agruement to gtype
    List *newcoercedargs = NIL;
    foreach(lc, newargs) {
        Node *e = (Node *)lfirst(lc);
        Node *newe = coerce_to_common_type(pstate, e, newcexpr->coalescetype, "COALESCE");

        newcoercedargs = lappend(newcoercedargs, newe);
    }

    if (pstate->p_last_srf != last_srf)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-returning functions are not allowed in COALESCE"),
                 parser_errposition(pstate, exprLocation(pstate->p_last_srf))));

    newcexpr->args = newcoercedargs;
    newcexpr->location = cexpr->location;
    return (Node *) newcexpr;
}

static Node *
transform_case_expr(cypher_parsestate *cpstate, CaseExpr *cexpr) {
    ParseState *pstate = (ParseState *)cpstate;
    Node *last_srf = pstate->p_last_srf;
    ListCell *lc;

    CaseExpr *newcexpr = makeNode(CaseExpr);

    // transform the test expression
    Node *arg = transform_cypher_expr_recurse(cpstate, (Node *) cexpr->arg);

    // generate placeholder for test expression
    CaseTestExpr *placeholder;
    if (arg) {
        if (exprType(arg) == UNKNOWNOID)
            arg = coerce_to_common_type(pstate, arg, GTYPEOID, "CASE");

        assign_expr_collations(pstate, arg);

        placeholder = makeNode(CaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
        placeholder->collation = exprCollation(arg);
    } else {
        placeholder = NULL;
    }

    newcexpr->arg = (Expr *) arg;

    // transform the case statements
    List *newargs = NIL;
    List *resultexprs = NIL;
    foreach(lc, cexpr->args) {
        CaseWhen *w = lfirst(lc);
        CaseWhen *neww = makeNode(CaseWhen);
        Node *warg = (Node *)w->expr;

        //shorthand form was specified, so default to equality check
        if (placeholder)
            warg = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", (Node *) placeholder, warg, w->location);

        neww->expr = (Expr *) transform_cypher_expr_recurse(cpstate, warg);

        neww->expr = (Expr *) coerce_to_boolean(pstate, (Node *) neww->expr, "CASE/WHEN");

        warg = (Node *) w->result;
        neww->result = (Expr *) transform_cypher_expr_recurse(cpstate, warg);
        neww->location = w->location;

        newargs = lappend(newargs, neww);
        resultexprs = lappend(resultexprs, neww->result);
    }

    newcexpr->args = newargs;

    // transform the default clause
    Node *defresult = (Node *) cexpr->defresult;
    if (!defresult) {
        A_Const *n = makeNode(A_Const);

        n->val.type = T_Null;
        n->location = -1;
        defresult = (Node *) n;
    }

    newcexpr->defresult = (Expr *) transform_cypher_expr_recurse(cpstate, defresult);

    resultexprs = lcons(newcexpr->defresult, resultexprs);

    Oid ptype = select_common_type(pstate, resultexprs, "CASE", NULL);

    Assert(OidIsValid(ptype));

    newcexpr->casetype = ptype;

    // Convert default result clause, if necessary
    newcexpr->defresult = (Expr *)
        coerce_to_common_type(pstate, (Node *) newcexpr->defresult, ptype, "CASE/ELSE");

    /* Convert when-clause results, if necessary */
    foreach(lc, newcexpr->args) {
        CaseWhen *w = (CaseWhen *) lfirst(lc);

        w->result = (Expr *) coerce_to_common_type(pstate, (Node *) w->result, ptype, "CASE/WHEN");
    }

    if (pstate->p_last_srf != last_srf)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-returning functions are not allowed in CASE"),
                 parser_errposition(pstate, exprLocation(pstate->p_last_srf))));

    newcexpr->location = cexpr->location;

    return (Node *) newcexpr;
}

/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */
static Node *
make_row_comparison_op(ParseState *pstate, List *opname, List *largs, List *rargs, int location)
{
    RowCompareExpr *rcexpr;
    RowCompareType rctype;
    List *opexprs;
    List *opnos;
    List *opfamilies;
    ListCell *l, *r;
    List **opinfo_lists;
    Bitmapset  *strats;
    int nopers;
    int i;

    nopers = list_length(largs);
    if (nopers != list_length(rargs))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("unequal number of entries in row expressions"),
                        parser_errposition(pstate, location)));

    /*
     * We can't compare zero-length rows because there is no principled basis
     * for figuring out what the operator is.
     */
    if (nopers == 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot compare rows of zero length"),
                        parser_errposition(pstate, location)));
    /*
     * Identify all the pairwise operators, using make_op so that behavior is
     * the same as in the simple scalar case.
     */
    opexprs = NIL;
    forboth(l, largs, r, rargs)
    {
        Node *larg = (Node *) lfirst(l);
        Node *rarg = (Node *) lfirst(r);
        OpExpr *cmp;

        cmp = castNode(OpExpr, make_op(pstate, opname, larg, rarg, pstate->p_last_srf, location));

        /*
         * We don't use coerce_to_boolean here because we insist on the
         * operator yielding boolean directly, not via coercion.  If it
         * doesn't yield bool it won't be in any index opfamilies...
         */
        if (cmp->opresulttype != BOOLOID)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("row comparison operator must yield type boolean, " "not type %s",
                                   format_type_be(cmp->opresulttype)),
                            parser_errposition(pstate, location)));
        if (expression_returns_set((Node *) cmp))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("row comparison operator must not return a set"),
                            parser_errposition(pstate, location)));
        opexprs = lappend(opexprs, cmp);
    }
    /*
     * If rows are length 1, just return the single operator.  In this case we
     * don't insist on identifying btree semantics for the operator (but we
     * still require it to return boolean).
     */
    if (nopers == 1)
                return (Node *) linitial(opexprs);

    /*
     * Now we must determine which row comparison semantics (= <> < <= > >=)
     * apply to this set of operators.  We look for btree opfamilies
     * containing the operators, and see which interpretations (strategy
     * numbers) exist for each operator.
     */
    opinfo_lists = (List **) palloc(nopers * sizeof(List *));
    strats = NULL;
    i = 0;
    foreach(l, opexprs)
    {
        Oid opno = ((OpExpr *) lfirst(l))->opno;
        Bitmapset *this_strats;
        ListCell *j;

        opinfo_lists[i] = get_op_btree_interpretation(opno);

        /*
         * convert strategy numbers into a Bitmapset to make the intersection
         * calculation easy.
         */
        this_strats = NULL;
        foreach(j, opinfo_lists[i])
        {
            OpBtreeInterpretation *opinfo = lfirst(j);

            this_strats = bms_add_member(this_strats, opinfo->strategy);
        }
        if (i == 0)
            strats = this_strats;
        else
            strats = bms_int_members(strats, this_strats);
        i++;
    }
    /*
     * If there are multiple common interpretations, we may use any one of
     * them ... this coding arbitrarily picks the lowest btree strategy
     * number.
     */
    i = bms_first_member(strats);
    if (i < 0)
    {
        /* No common interpretation, so fail */
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
            errhint("Row comparison operators must be associated with btree operator families."),
            parser_errposition(pstate, location)));
    }
    rctype = (RowCompareType) i;

    /*
     * For = and <> cases, we just combine the pairwise operators with AND or
     * OR respectively.
     */
    if (rctype == ROWCOMPARE_EQ)
        return (Node *) makeBoolExpr(AND_EXPR, opexprs, location);
    if (rctype == ROWCOMPARE_NE)
        return (Node *) makeBoolExpr(OR_EXPR, opexprs, location);

    /*
     * Otherwise we need to choose exactly which opfamily to associate with
     * each operator.
     */
    opfamilies = NIL;
    for (i = 0; i < nopers; i++)
    {
        Oid opfamily = InvalidOid;
        ListCell *j;

        foreach(j, opinfo_lists[i])
        {
             OpBtreeInterpretation *opinfo = lfirst(j);

             if (opinfo->strategy == rctype)
             {
                 opfamily = opinfo->opfamily_id;
                 break;
             }
        }
        if (OidIsValid(opfamily))
            opfamilies = lappend_oid(opfamilies, opfamily);
        else /* should not happen */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("could not determine interpretation of row comparison operator %s",
                                   strVal(llast(opname))),
                            errdetail("There are multiple equally-plausible candidates."),
                            parser_errposition(pstate, location)));
    }

    /*
     * Now deconstruct the OpExprs and create a RowCompareExpr.
     *
     * Note: can't just reuse the passed largs/rargs lists, because of
     * possibility that make_op inserted coercion operations.
     */
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach(l, opexprs)
    {
        OpExpr *cmp = (OpExpr *) lfirst(l);

        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL; /* assign_expr_collations will fix this */
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node *) rcexpr;
}


static Node *
transform_sub_link(cypher_parsestate *cpstate, SubLink *sublink) {
    ParseState *pstate = (ParseState*)cpstate;
    const char *err;
    /*
     * Check to see if the sublink is in an invalid place within the query. We
     * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE, but generally
     * not in utility statements.
     */
    err = NULL;
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_OTHER: /* Accept sublink here; caller must throw error if wanted */
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
        case EXPR_KIND_FROM_SUBSELECT:
        case EXPR_KIND_FROM_FUNCTION:
        case EXPR_KIND_WHERE:
        case EXPR_KIND_POLICY:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_FILTER:
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
        case EXPR_KIND_WINDOW_FRAME_RANGE:
        case EXPR_KIND_WINDOW_FRAME_ROWS:
        case EXPR_KIND_WINDOW_FRAME_GROUPS:
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_INSERT_TARGET:
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
        case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
        case EXPR_KIND_RETURNING:
        case EXPR_KIND_VALUES:
        case EXPR_KIND_VALUES_SINGLE:
        case EXPR_KIND_CYCLE_MARK:
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            err = _("cannot use subquery in check constraint");
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = _("cannot use subquery in DEFAULT expression");
            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            err = _("cannot use subquery in index expression");
            break;
        case EXPR_KIND_INDEX_PREDICATE:
            err = _("cannot use subquery in index predicate");
            break;
        case EXPR_KIND_STATS_EXPRESSION:
            err = _("cannot use subquery in statistics expression");
            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            err = _("cannot use subquery in transform expression");
            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            err = _("cannot use subquery in EXECUTE parameter");
            break;
        case EXPR_KIND_TRIGGER_WHEN:
            err = _("cannot use subquery in trigger WHEN condition");
            break;
        case EXPR_KIND_PARTITION_BOUND:
            err = _("cannot use subquery in partition bound");
            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            err = _("cannot use subquery in partition key expression");
            break;
        case EXPR_KIND_CALL_ARGUMENT:
            err = _("cannot use subquery in CALL argument");
            break;
        case EXPR_KIND_COPY_WHERE:
            err = _("cannot use subquery in COPY FROM WHERE condition");
            break;
        case EXPR_KIND_GENERATED_COLUMN:
            err = _("cannot use subquery in column generation expression");
            break;
    }
    if (err)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg_internal("%s", err),
                        parser_errposition(pstate, sublink->location)));

    pstate->p_hasSubLinks = true;

    //transform the sub-query.
    Query *query = cypher_parse_sub_analyze(sublink->subselect, cpstate, NULL, false, true);

    if (!IsA(query, Query) || query->commandType != CMD_SELECT)
        elog(ERROR, "unexpected non-SELECT command in SubLink");

    sublink->subselect = (Node *)query;

    if (sublink->subLinkType == EXISTS_SUBLINK) {
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    } else if (sublink->subLinkType == MULTIEXPR_SUBLINK) {
        /* Same as EXPR case, except no restriction on number of columns */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    } else {
        /* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
        Node *lefthand;
        List *left_list;
        List *right_list;
        ListCell *l;

        /*
         * If the source was "x IN (select)", convert to "x = ANY (select)".
         */
        if (sublink->operName == NIL)
            sublink->operName = list_make1(makeString("="));

        /*
         * Transform lefthand expression, and convert to a list
         */
        lefthand = transform_cypher_expr_recurse(pstate, sublink->testexpr);
        if (lefthand && IsA(lefthand, RowExpr))
            left_list = ((RowExpr *) lefthand)->args;
        else
            left_list = list_make1(lefthand);

        /*
         * Build a list of PARAM_SUBLINK nodes representing the output columns
         * of the subquery.
         */
        right_list = NIL;
        foreach(l, query->targetList)
        {
            TargetEntry *tent = (TargetEntry *) lfirst(l);
            Param      *param;

            if (tent->resjunk)
                 continue;

            param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((Node *) tent->expr);
            param->paramtypmod = exprTypmod((Node *) tent->expr);
            param->paramcollid = exprCollation((Node *) tent->expr);
            param->location = -1;

            right_list = lappend(right_list, param);
        }

        /*
         * We could rely on make_row_comparison_op to complain if the list
         * lengths differ, but we prefer to generate a more specific error
         * message.
         */
        if (list_length(left_list) < list_length(right_list))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery has too many columns"),
                            parser_errposition(pstate, sublink->location)));
        if (list_length(left_list) > list_length(right_list))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery has too few columns"),
                            parser_errposition(pstate, sublink->location)));

        /*
         * Identify the combining operator(s) and generate a suitable
         * row-comparison expression.
         */
        sublink->testexpr = make_row_comparison_op(pstate, sublink->operName, left_list, right_list, sublink->location);
    }

    return (Node *)sublink;
}

static Node *
transformSQLValueFunction(cypher_parsestate *cpstate, SQLValueFunction *svf)
{
        /*
         * All we need to do is insert the correct result type and (where needed)
         * validate the typmod, so we just modify the node in-place.
         */
        switch (svf->op)
        {
                case SVFOP_CURRENT_DATE:
                        svf->type = DATEOID;
                        break;
                case SVFOP_CURRENT_TIME:
                        svf->type = TIMETZOID;
                        break;
                case SVFOP_CURRENT_TIME_N:
                        svf->type = TIMETZOID;
                        svf->typmod = anytime_typmod_check(true, svf->typmod);
                        break;
                case SVFOP_CURRENT_TIMESTAMP:
                        svf->type = TIMESTAMPTZOID;
                        break;
                case SVFOP_CURRENT_TIMESTAMP_N:
                        svf->type = TIMESTAMPTZOID;
                        svf->typmod = anytimestamp_typmod_check(true, svf->typmod);
                        break;
                case SVFOP_LOCALTIME:
                        svf->type = TIMEOID;
                        break;
                case SVFOP_LOCALTIME_N:
                        svf->type = TIMEOID;
                        svf->typmod = anytime_typmod_check(false, svf->typmod);
                        break;
                case SVFOP_LOCALTIMESTAMP:
                        svf->type = TIMESTAMPOID;
                        break;
                case SVFOP_LOCALTIMESTAMP_N:
                        svf->type = TIMESTAMPOID;
                        svf->typmod = anytimestamp_typmod_check(false, svf->typmod);
                        break;
                case SVFOP_CURRENT_ROLE:
                case SVFOP_CURRENT_USER:
                case SVFOP_USER:
                case SVFOP_SESSION_USER:
                case SVFOP_CURRENT_CATALOG:
                case SVFOP_CURRENT_SCHEMA:
                        svf->type = NAMEOID;
                        break;
        }

//        return (Node *) svf;


        Node *result;
        //Node       *arg = tc->arg;
        Node *expr = (Node *) svf;
        Oid inputType;
        Oid targetType;
        int32 targetTypmod;

        typenameTypeIdAndMod(cpstate, makeTypeName("gtype"), &targetType, &targetTypmod);

        inputType = exprType(expr);
        if (inputType == InvalidOid)
                return expr;

        result = coerce_to_target_type(cpstate, expr, inputType, targetType, targetTypmod,
                                       COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);

    if (result == NULL)
                ereport(ERROR, (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast type %s to %s", format_type_be(inputType), format_type_be(targetType)),
                        parser_coercion_errposition(cpstate, -1, expr)));

        return result;
}

