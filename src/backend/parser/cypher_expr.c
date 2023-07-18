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
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
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

#define is_a_slice(node) \
    (IsA((node), A_Indices) && ((A_Indices *)(node))->is_slice)

static Node *transform_cypher_expr_recurse(cypher_parsestate *cpstate, Node *expr);
static Node *transform_a_const(cypher_parsestate *cpstate, A_Const *ac);
static Node *transform_column_ref(cypher_parsestate *cpstate, ColumnRef *cref);
static Node *transform_a_indirection(cypher_parsestate *cpstate, A_Indirection *a_ind);
static Node *transform_a_expr_op(cypher_parsestate *cpstate, A_Expr *a);
static Node *transform_bool_expr(cypher_parsestate *cpstate, BoolExpr *expr);
static Node *transform_cypher_bool_const(cypher_parsestate *cpstate, cypher_bool_const *bc);
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
static List *make_qualified_function_name(cypher_parsestate *cpstate, List *lst, List *targs);

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
        break;
    default:
        ereport(ERROR, (errmsg_internal("unrecognized node type: %d", nodeTag(expr))));
    }

    return NULL;
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
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_COLUMN),
                             errmsg("could not find rte for %s.%s", relname, colname),
                             parser_errposition(pstate, cref->location)));
                    break;
                }

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

static Node *
transform_a_expr_op(cypher_parsestate *cpstate, A_Expr *a) {
    ParseState *pstate = (ParseState *)cpstate;

    Node *last_srf = pstate->p_last_srf;

    Node *lexpr = transform_cypher_expr_recurse(cpstate, a->lexpr);
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
            // a[1..10]
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
        fname = lappend(fname, makeString("age_tonumeric"));
    else if (pg_strcasecmp(ctypecast->typecast, "float") == 0)
        fname = lappend(fname, makeString("age_tofloat"));
    else if (pg_strcasecmp(ctypecast->typecast, "int") == 0 || pg_strcasecmp(ctypecast->typecast, "integer") == 0)
        fname = lappend(fname, makeString("age_tointeger"));
    else if (pg_strcasecmp(ctypecast->typecast, "timestamp") == 0)
        fname = lappend(fname, makeString("age_totimestamp"));
    else
        ereport(ERROR, (errmsg_internal("typecast \'%s\' not supported", ctypecast->typecast)));

    // make a function call node
    fnode = makeFuncCall(fname, list_make1(ctypecast->expr), COERCE_SQL_SYNTAX, ctypecast->location);

    return transform_func_call(cpstate, fnode);
}

static List *
make_qualified_function_name(cypher_parsestate *cpstate, List *lst, List *targs) {
    char *name = ((Value*)linitial(lst))->val.str;
    int pnlen = strlen(name);
    char *ag_name = palloc(pnlen + 5);
    int i;
        
    // catalog functions are prefixed with age_
    strncpy(ag_name, "age_", 4);
        
    /*
     * change all functions to lower case, this is mostly uncessary, but some
     * functions are keywords in SQL, we need to escape those functions in the sql
     * script to a lower case version, do that for all functions to make life
     * easier.
     */ 
    for (i = 0; i < pnlen; i++)
        ag_name[i + 4] = tolower(name[i]);
        
    // Add NULL at the end
    ag_name[i + 4] = '\0';

    // add the catalog schema and create list        
    List *fname = list_make2(makeString(CATALOG_SCHEMA), makeString(ag_name));

    // Some functions need the graph name passed to them in order to work
    if (strcmp("startNode", name) == 0 || strcmp("endNode", name) == 0 || strcmp("vle", name) == 0 || strcmp("vertex_stats", name) == 0) {
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

    // XXX: Needs to Be Tested
    Assert(!fn->agg_within_group);

    // if the function name is not qualified assume the function is in our catalog.
    List *fname;
    if (list_length(fn->funcname) == 1)
        fname = make_qualified_function_name(cpstate, fn->funcname, args);
    else
        fname = fn->funcname;

    // Passed our new fname to the normal function transform logic
    Node *retval = ParseFuncOrColumn(pstate, fname, args, last_srf, fn, false, fn->location);

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

static Node *
transform_sub_link(cypher_parsestate *cpstate, SubLink *sublink) {
    ParseState *pstate = (ParseState*)cpstate;

    /*
     * Check to see if the sublink is in an invalid place within the query. We
     * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE, but generally
     * not in utility statements.
     */
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_OTHER: /* Accept sublink here; caller must throw error if wanted */
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_FROM_SUBSELECT:
        case EXPR_KIND_WHERE:
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg_internal("unsupported SubLink"),
                            parser_errposition(pstate, sublink->location)));
    }

    pstate->p_hasSubLinks = true;

    //transform the sub-query.
    Query *query = cypher_parse_sub_analyze(sublink->subselect, cpstate, NULL, false, true);

    Assert (IsA(query, Query) && query->commandType == CMD_SELECT);

    sublink->subselect = (Node *)query;

    if (sublink->subLinkType == EXISTS_SUBLINK) {
        /*
         * EXISTS needs no test expression or combining operator. These fields
         * should be null already, but make sure.
         */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else
        elog(ERROR, "unsupported SubLink type");

    return (Node *)sublink;
}
