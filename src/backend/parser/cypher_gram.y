%{
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
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */

#include "postgraph.h"

#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "parser/parser.h"

#include "nodes/ag_nodes.h"
#include "nodes/cypher_nodes.h"
#include "parser/ag_scanner.h"
#include "parser/cypher_gram.h"

// override the default action for locations
#define YYLLOC_DEFAULT(current, rhs, n) \
    do \
    { \
        if ((n) > 0) \
            current = (rhs)[1]; \
        else \
            current = -1; \
    } while (0)

#define YYMALLOC palloc
#define YYFREE pfree
%}

%locations
%name-prefix="cypher_yy"
%pure-parser

%lex-param {ag_scanner_t scanner}
%parse-param {ag_scanner_t scanner}
%parse-param {cypher_yy_extra *extra}

%union {
    /* types in cypher_yylex() */
    int integer;
    char *string;
    const char *keyword;

    /* extra types */
    bool boolean;
    Node *node;
    List *list;
    struct WindowDef *windef;
}

%token <integer> INTEGER
%token <string> DECIMAL STRING

%token <string> IDENTIFIER
%token <string> INET
%token <string> PARAMETER
%token <string> OPERATOR

/* operators that have more than 1 character */
%token NOT_EQ LT_EQ GT_EQ DOT_DOT TYPECAST PLUS_EQ

/* keywords in alphabetical order */
%token <keyword> ALL AND ANY AS ASC ASCENDING
                 BETWEEN BY
                 CALL CASE COALESCE CONTAINS CREATE CUBE CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP
                 DATE DECADE DELETE DESC DESCENDING DETACH DISTINCT
                 ELSE END_P ENDS EXCEPT EXCLUDE EXISTS EXTRACT
                 GROUP GROUPS GROUPING
                 FALSE_P FILTER FIRST_P FOLLOWING FROM
                 HAVING
                 ILIKE IN INTERSECT INTERVAL IS
                 LAST_P LIKE LIMIT LOCALTIME LOCALTIMESTAMP
                 MATCH MERGE 
                 NO NOT NULL_P NULLS_LA
                 OPTIONAL OTHERS OR ORDER OVER OVERLAPS
                 PARTITION PRECEDING
                 RANGE REMOVE RETURN ROLLUP ROW ROWS
                 SET SETS SKIP SOME STARTS
                 TIME TIES THEN TIMESTAMP TRUE_P
                 UNBOUNDED UNION UNWIND USING
                 WHEN WHERE WINDOW WITH WITHIN WITHOUT
                 XOR
                 YIELD
                 ZONE

/* query */
%type <node> stmt
%type <list> single_query query_part_init query_part_last cypher_stmt
             reading_clause_list updating_clause_list_0 updating_clause_list_1
%type <node> reading_clause updating_clause

/* RETURN and WITH clause */
%type <node> empty_grouping_set cube_clause rollup_clause group_item having_opt return return_item sort_item skip_opt limit_opt with
%type <list> group_item_list return_item_list order_by_opt sort_item_list group_by_opt within_group_clause
%type <integer> order_opt opt_nulls_order 

/* MATCH clause */
%type <node> match cypher_varlen_opt cypher_range_opt cypher_range_idx
             cypher_range_idx_opt
%type <integer> Iconst
%type <boolean> optional_opt

/* CREATE clause */
%type <node> create

/* UNWIND clause */
%type <node> unwind

/* SET and REMOVE clause */
%type <node> set set_item remove remove_item
%type <list> set_item_list remove_item_list

/* DELETE clause */
%type <node> delete
%type <boolean> detach_opt

/* MERGE clause */
%type <node> merge

/* CALL ... YIELD clause */
%type <node> call_stmt yield_item
%type <list> yield_item_list

/* common */
%type <node> where_opt

/* pattern */
%type <list> pattern simple_path_opt_parens simple_path
%type <node> path anonymous_path
             path_node path_relationship path_relationship_body
             properties_opt
%type <string> label_opt

/* expression */
%type <node> a_expr expr_opt expr_atom expr_literal map list in_expr

%type <node> expr_case expr_case_when expr_case_default
%type <list> expr_case_when_list

%type <node> expr_func expr_func_norm expr_func_subexpr
%type <list> expr_list expr_list_opt map_keyval_list_opt map_keyval_list

%type <node> filter_clause
%type <list> window_clause window_definition_list opt_partition_clause
%type <windef> window_definition over_clause window_specification opt_frame_clause
               frame_extent frame_bound
%type <integer>    opt_window_exclusion_clause
%type <string> all_op
/* names */
%type <string> property_key_name var_name var_name_opt label_name
%type <string> symbolic_name schema_name temporal_cast
%type <keyword> reserved_keyword safe_keywords conflicted_keywords
%type <list> func_name

/*set operations*/
%type <boolean> all_or_distinct

/* precedence: lowest to highest */
%left UNION INTERSECT EXCEPT
%left OR
%left AND
%left XOR
%right NOT
%left '=' NOT_EQ '<' LT_EQ '>' GT_EQ '~' '!'
%left OPERATOR
%left '+' '-'
%left '*' '/' '%'
%left '^' '&' '|'
%nonassoc IN IS
%right UNARY_MINUS
%nonassoc CONTAINS ENDS EQ_TILDE STARTS LIKE ILIKE
%left '[' ']' '(' ')'
%left '.'
%left TYPECAST

%{
// logical operators
static Node *make_or_expr(Node *lexpr, Node *rexpr, int location);
static Node *make_and_expr(Node *lexpr, Node *rexpr, int location);
static Node *make_xor_expr(Node *lexpr, Node *rexpr, int location);
static Node *make_not_expr(Node *expr, int location);

// arithmetic operators
static Node *do_negate(Node *n, int location);
static void do_negate_float(Value *v);

// indirection
static Node *append_indirection(Node *expr, Node *selector);

// literals
static Node *make_int_const(int i, int location);
static Node *make_float_const(char *s, int location);
static Node *make_string_const(char *s, int location);
static Node *make_bool_const(bool b, int location);
static Node *make_inet_const(char *s, int location);
static Node *make_null_const(int location);

// typecast
static Node *make_typecast_expr(Node *expr, char *typecast, int location);

// sql value
static Node *makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod, int location);
// setops
static Node *make_set_op(SetOperation op, bool all_or_distinct, List *larg, List *rarg);

// comparison
static bool is_A_Expr_a_comparison_operation(A_Expr *a);
%}
%%

/*
 * query
 */

stmt:
    cypher_stmt semicolon_opt
        {
            /*
             * If there is no transition for the lookahead token and the
             * clauses can be reduced to single_query, the parsing is
             * considered successful although it actually isn't.
             *
             * For example, when `MATCH ... CREATE ... MATCH ... ;` query is
             * being parsed, there is no transition for the second `MATCH ...`
             * because the query is wrong but `MATCH .. CREATE ...` is correct
             * so it will be reduced to query_part_last anyway even if there
             * are more tokens to read.
             *
             * Throw syntax error in this case.
             */
            if (yychar != YYEOF)
                yyerror(&yylloc, scanner, extra, "syntax error");

            extra->result = $1;
        }
    ;

cypher_stmt:
    single_query
        {
            $$ = $1;
        }
    | '(' cypher_stmt ')'
        {
            $$ = $2;
        }
    | cypher_stmt UNION all_or_distinct cypher_stmt
        {
            $$ = list_make1(make_set_op(SETOP_UNION, $3, $1, $4));
        }
    | cypher_stmt INTERSECT all_or_distinct cypher_stmt
        {
            $$ = list_make1(make_set_op(SETOP_INTERSECT, $3, $1, $4));
        }
    | cypher_stmt EXCEPT all_or_distinct cypher_stmt
        {
            $$ = list_make1(make_set_op(SETOP_EXCEPT, $3, $1, $4));
        }
    ;

call_stmt:
    CALL expr_func_norm AS var_name where_opt
        {
            cypher_call *call = make_ag_node(cypher_call);
            call->cck = CCK_FUNCTION;
            call->func = $2;
            call->yield_list = NIL;
            call->alias = $4;
            call->where = $5;
            call->cypher = NIL;
            call->query_tree = NULL;
            $$ = call;
       }
    | CALL expr_func_norm YIELD yield_item_list where_opt
        {
            cypher_call *call = make_ag_node(cypher_call);
            call->cck = CCK_FUNCTION;
            call->func = $2;
            call->yield_list = $3;
            call->alias = $4;
            call->where = $5;
            call->cypher = NIL;
            call->query_tree = NULL;
            $$ = call;

            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("CALL... [YIELD] not supported yet"),
                     ag_scanner_errposition(@1, scanner)));
        }
    | CALL '{' cypher_stmt '}'
        {
            cypher_call *call = make_ag_node(cypher_call);
            call->cck = CCK_CYPHER_SUBQUERY;
            call->func = NULL;
            call->yield_list = NIL;
            call->alias = NULL;
            call->where = NULL;
            call->cypher = $3;
            call->query_tree = NULL;
            $$ = call;
        }
    ;

yield_item_list:
    yield_item
        {
            $$ = list_make1($1);
        }
    | yield_item_list ',' yield_item
        {
            $$ = lappend($1, $3);
        }
    ;

yield_item:
    a_expr AS var_name
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = $3;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | a_expr
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = NULL;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | '*'
        {
            ColumnRef *cr;
            ResTarget *rt;

            cr = makeNode(ColumnRef);
            cr->fields = list_make1(makeNode(A_Star));
            cr->location = @1;

            rt = makeNode(ResTarget);
            rt->name = NULL;
            rt->indirection = NIL;
            rt->val = (Node *)cr;
            rt->location = @1;

            $$ = (Node *)rt;
        }
    ;


semicolon_opt:
    /* empty */
    | ';'
    ;

all_or_distinct:
    ALL
    {
        $$ = true;
    }
    | DISTINCT
    {
        $$ = false;
    }
    | /*EMPTY*/
    {
        $$ = false;
    }

/*
 * The overall structure of single_query looks like below.
 *
 * ( reading_clause* updating_clause* with )*
 * reading_clause* ( updating_clause+ | updating_clause* return )
 */
single_query:
    query_part_init query_part_last
        {
            $$ = list_concat($1, $2);
        }
    ;

query_part_init:
    /* empty */
        {
            $$ = NIL;
        }
    | query_part_init reading_clause_list updating_clause_list_0 with
        {
            $$ = lappend(list_concat(list_concat($1, $2), $3), $4);
        }
    ;

query_part_last:
    reading_clause_list updating_clause_list_1
        {
            $$ = list_concat($1, $2);
        }
    | reading_clause_list updating_clause_list_0 return
        {
            $$ = lappend(list_concat($1, $2), $3);
        }
    ;

reading_clause_list:
    /* empty */
        {
            $$ = NIL;
        }
    | reading_clause_list reading_clause
        {
            $$ = lappend($1, $2);
        }
    ;

reading_clause:
    match
    | unwind
    ;

updating_clause_list_0:
    /* empty */
        {
            $$ = NIL;
        }
    | updating_clause_list_1
    ;

updating_clause_list_1:
    updating_clause
        {
            $$ = list_make1($1);
        }
    | updating_clause_list_1 updating_clause
        {
            $$ = lappend($1, $2);
        }
    ;

updating_clause:
    create
    | set
    | remove
    | delete
    | merge
    | call_stmt
    ;

cypher_varlen_opt:
    '*' cypher_range_opt
        {
            A_Indices *n = (A_Indices *) $2;

            if (n->lidx == NULL)
                n->lidx = make_int_const(1, @2);

            if (n->uidx != NULL)
            {
                A_Const    *lidx = (A_Const *) n->lidx;
                A_Const    *uidx = (A_Const *) n->uidx;

                if (lidx->val.val.ival > uidx->val.val.ival)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("invalid range"),
                                    ag_scanner_errposition(@2, scanner)));
            }
            $$ = (Node *) n;
        }
    | /* EMPTY */
        {
            $$ = NULL;
        }
    ;

cypher_range_opt:
    cypher_range_idx
        {
            A_Indices  *n;

            n = makeNode(A_Indices);
            n->lidx = copyObject($1);
            n->uidx = $1;
            $$ = (Node *) n;
        }
    | cypher_range_idx_opt DOT_DOT cypher_range_idx_opt
        {
            A_Indices  *n;

            n = makeNode(A_Indices);
            n->lidx = $1;
            n->uidx = $3;
            $$ = (Node *) n;
        }
    | /* EMPTY */
        {
            $$ = (Node *) makeNode(A_Indices);
        }
    ;

cypher_range_idx:
    Iconst
        {
            $$ = make_int_const($1, @1);
        }
    ;

cypher_range_idx_opt:
                        cypher_range_idx
                        | /* EMPTY */                   { $$ = NULL; }
                ;

Iconst: INTEGER
;
/*
 * RETURN and WITH clause
 */
having_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | HAVING a_expr
        {
            $$ = $2;
        }
;


group_by_opt:
    /* empty */
        {
            $$ = NIL;
        }
    | GROUP BY group_item_list
        {
            $$ = $3;
        }
;

group_item_list:
    group_item { $$ = list_make1($1); }
        | group_item_list ',' group_item { $$ = lappend($1, $3); }
;

group_item:
    a_expr { $$ = $1; }
        | cube_clause { $$ = $1; }
        | rollup_clause { $$ = $1; }
        | empty_grouping_set { $$ = $1; }
;

rollup_clause:
    ROLLUP '(' expr_list ')'
        {
            $$ = (Node *) makeGroupingSet(GROUPING_SET_ROLLUP, $3, @1);
        }
;

cube_clause:
     CUBE '(' expr_list ')'
     {
         $$ = (Node *) makeGroupingSet(GROUPING_SET_CUBE, $3, @1);
     }
;

empty_grouping_set:
    '(' ')'
    {
        $$ = (Node *) makeGroupingSet(GROUPING_SET_EMPTY, NIL, @1);
    }
;

return:
    RETURN DISTINCT return_item_list group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
        {
            cypher_return *n;

            n = make_ag_node(cypher_return);
            n->distinct = true;
            n->items = $3;
            n->real_group_clause = $4;
            n->having = $5;
            n->window_clause = $6;
            n->order_by = $7;
            n->skip = $8;
            n->limit = $9;
            n->where = NULL;

            $$ = (Node *)n;
        }
    | RETURN return_item_list group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
        {
            cypher_return *n;

            n = make_ag_node(cypher_return);
            n->distinct = false;
            n->items = $2;
            n->real_group_clause = $3;
            n->having = $4;
            n->window_clause = $5;
            n->order_by = $6;
            n->skip = $7;
            n->limit = $8;
            n->where = NULL;

            $$ = (Node *)n;
        }

    ;

return_item_list:
    return_item
        {
            $$ = list_make1($1);
        }
    | return_item_list ',' return_item
        {
            $$ = lappend($1, $3);
        }
    ;

return_item:
    a_expr AS var_name
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = $3;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | a_expr
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = NULL;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | '*'
        {
            ColumnRef *cr;
            ResTarget *rt;

            cr = makeNode(ColumnRef);
            cr->fields = list_make1(makeNode(A_Star));
            cr->location = @1;

            rt = makeNode(ResTarget);
            rt->name = NULL;
            rt->indirection = NIL;
            rt->val = (Node *)cr;
            rt->location = @1;

            $$ = (Node *)rt;
        }
    ;

opt_nulls_order: NULLS_LA FIRST_P { $$ = SORTBY_NULLS_FIRST; }
     | NULLS_LA LAST_P { $$ = SORTBY_NULLS_LAST; }
     | /*EMPTY*/ { $$ = SORTBY_NULLS_DEFAULT; }
;       
                        


order_by_opt:
    /* empty */
        {
            $$ = NIL;
        }
    | ORDER BY sort_item_list
        {
            $$ = $3;
        }
    ;

sort_item_list:
    sort_item
        {
            $$ = list_make1($1);
        }
    | sort_item_list ',' sort_item
        {
            $$ = lappend($1, $3);
        }
    ;

sort_item:
    a_expr USING all_op opt_nulls_order
    {
        SortBy *n;

        n = makeNode(SortBy);
        n->node = $1;
        n->sortby_dir = SORTBY_USING;
        n->sortby_nulls = $4;
        n->useOp = list_make2(makeString("postgraph"), makeString($3));
        n->location = @3;

        $$ = (Node *)n;

    }
    | a_expr order_opt opt_nulls_order
    {
        SortBy *n;

        n = makeNode(SortBy);
        n->node = $1;
        n->sortby_dir = $2;
        n->sortby_nulls = $3;
        n->useOp = NIL;
        n->location = -1; // no operator

        $$ = (Node *)n;
    }
;

order_opt:
    /* empty */
        {
            $$ = SORTBY_DEFAULT;
        }
    | ASC
        {
            $$ = SORTBY_ASC;
        }
    | ASCENDING
        {
            $$ = SORTBY_ASC;
        }
    | DESC
        {
            $$ = SORTBY_DESC;
        }
    | DESCENDING
        {
            $$ = SORTBY_DESC;
        }
    ;

skip_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | SKIP a_expr
        {
            $$ = $2;
        }
    ;

limit_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | LIMIT a_expr
        {
            $$ = $2;
        }
    ;

with:
    WITH DISTINCT return_item_list where_opt group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
        {
            ListCell *li;
            cypher_with *n;

            // check expressions are aliased
            foreach (li, $3)
            {
                ResTarget *item = lfirst(li);

                // variable does not have to be aliased
                if (IsA(item->val, ColumnRef) || item->name)
                    continue;

                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("expression item must be aliased"),
                         errhint("Items can be aliased by using AS."),
                         ag_scanner_errposition(item->location, scanner)));
            }

            n = make_ag_node(cypher_with);
            n->distinct = true;
            n->items = $3;
            n->real_group_clause = $5;
            n->having = $6;
            n->window_clause = $7;
            n->order_by = $8;
            n->skip = $9;
            n->limit = $10;
            n->where = $4;

            $$ = (Node *)n;
        }
    | WITH return_item_list where_opt group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
        {
            ListCell *li;
            cypher_with *n;

            // check expressions are aliased
            foreach (li, $2)
            {
                ResTarget *item = lfirst(li);

                // variable does not have to be aliased
                if (IsA(item->val, ColumnRef) || item->name)
                    continue;

                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("expression item must be aliased"),
                         errhint("Items can be aliased by using AS."),
                         ag_scanner_errposition(item->location, scanner)));
            }

            n = make_ag_node(cypher_with);
            n->distinct = false;
            n->items = $2;
            n->real_group_clause = $4;
            n->having = $5;
            n->window_clause = $6;
            n->order_by = $7;
            n->skip = $8;
            n->limit = $9;
            n->where = $3;

            $$ = (Node *)n;
        }
    ;

/*
 * MATCH clause
 */

match:
    optional_opt MATCH pattern where_opt order_by_opt
        {
            cypher_match *n;

            n = make_ag_node(cypher_match);
            n->optional = $1;
            n->pattern = $3;
            n->where = $4;
            n->order_by = $5;

            $$ = (Node *)n;
        }
    ;

optional_opt:
    OPTIONAL
        {
            $$ = true;
        }
    | /* EMPTY */
        {
            $$ = false;
        }
    ;


unwind:
    UNWIND a_expr AS var_name
        {
            ResTarget  *res;
            cypher_unwind *n;

            res = makeNode(ResTarget);
            res->name = $4;
            res->val = (Node *) $2;
            res->location = @2;

            n = make_ag_node(cypher_unwind);
            n->target = res;
            $$ = (Node *) n;
        }

/*
 * CREATE clause
 */

create:
    CREATE pattern
        {
            cypher_create *n;

            n = make_ag_node(cypher_create);
            n->pattern = $2;

            $$ = (Node *)n;
        }
    ;

/*
 * SET and REMOVE clause
 */

set:
    SET set_item_list
        {
            cypher_set *n;

            n = make_ag_node(cypher_set);
            n->items = $2;
            n->is_remove = false;
            n->location = @1;

            $$ = (Node *)n;
        }
    ;

set_item_list:
    set_item
        {
            $$ = list_make1($1);
        }
    | set_item_list ',' set_item
        {
            $$ = lappend($1, $3);
        }
    ;

set_item:
    a_expr '=' a_expr
        {
            cypher_set_item *n;

            n = make_ag_node(cypher_set_item);
            n->prop = $1;
            n->expr = $3;
            n->is_add = false;
            n->location = @1;

            $$ = (Node *)n;
        }
   | a_expr PLUS_EQ a_expr
        {
            cypher_set_item *n;

            n = make_ag_node(cypher_set_item);
            n->prop = $1;
            n->expr = $3;
            n->is_add = true;
            n->location = @1;

            $$ = (Node *)n;
        }
    ;

remove:
    REMOVE remove_item_list
        {
            cypher_set *n;

            n = make_ag_node(cypher_set);
            n->items = $2;
            n->is_remove = true;
             n->location = @1;

            $$ = (Node *)n;
        }
    ;

remove_item_list:
    remove_item
        {
            $$ = list_make1($1);
        }
    | remove_item_list ',' remove_item
        {
            $$ = lappend($1, $3);
        }
    ;

remove_item:
    a_expr
        {
            cypher_set_item *n;

            n = make_ag_node(cypher_set_item);
            n->prop = $1;
            n->expr = make_null_const(-1);
            n->is_add = false;

            $$ = (Node *)n;
        }
    ;

/*
 * DELETE clause
 */

delete:
    detach_opt DELETE expr_list
        {
            cypher_delete *n;

            n = make_ag_node(cypher_delete);
            n->detach = $1;
            n->exprs = $3;
            n->location = @1;

            $$ = (Node *)n;
        }
    ;

detach_opt:
    DETACH
        {
            $$ = true;
        }
    | /* EMPTY */
        {
            $$ = false;
        }
    ;

/*
 * MERGE clause
 */
merge:
    MERGE path
        {
            cypher_merge *n;

            n = make_ag_node(cypher_merge);
            n->path = $2;

            $$ = (Node *)n;
        }
    ;

/*
 * common
 */

where_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | WHERE a_expr
        {
            $$ = $2;
        }
    ;

/*
 * pattern
 */

/* pattern is a set of one or more paths */
pattern:
    path
        {
            $$ = list_make1($1);
        }
    | pattern ',' path
        {
            $$ = lappend($1, $3);
        }
    ;

/* path is a series of connected nodes and relationships */
path:
    anonymous_path
    | var_name '=' anonymous_path /* named path */
        {
            cypher_path *p;

            p = (cypher_path *)$3;
            p->var_name = $1;

            $$ = (Node *)p;
        }

    ;

anonymous_path:
    simple_path_opt_parens
        {
            cypher_path *n;

            n = make_ag_node(cypher_path);
            n->path = $1;
            n->var_name = NULL;
            n->location = @1;

            $$ = (Node *)n;
        }
    ;

simple_path_opt_parens:
    simple_path
    | '(' simple_path ')'
        {
            $$ = $2;
        }
    ;

simple_path:
    path_node
        {
            $$ = list_make1($1);
        }
    | simple_path path_relationship path_node
        {
            $$ = lappend(lappend($1, $2), $3);
        }
    ;

path_node:
    '(' var_name_opt label_opt properties_opt ')'
        {
            cypher_node *n;

            n = make_ag_node(cypher_node);
            n->name = $2;
            n->label = $3;
            n->props = $4;
            n->location = @1;

            $$ = (Node *)n;
        }
    ;

path_relationship:
    '-' path_relationship_body '-'
        {
            cypher_relationship *n = (cypher_relationship *)$2;

            n->dir = CYPHER_REL_DIR_NONE;
            n->location = @2;

            $$ = $2;
        }
    | '-' path_relationship_body '-' '>'
        {
            cypher_relationship *n = (cypher_relationship *)$2;

            n->dir = CYPHER_REL_DIR_RIGHT;
            n->location = @2;

            $$ = $2;
        }
    | '<' '-' path_relationship_body '-'
        {
            cypher_relationship *n = (cypher_relationship *)$3;

            n->dir = CYPHER_REL_DIR_LEFT;
            n->location = @3;

            $$ = $3;
        }
    ;

path_relationship_body:
    '[' var_name_opt label_opt cypher_varlen_opt properties_opt ']'
        {
            cypher_relationship *n;

            n = make_ag_node(cypher_relationship);
            n->name = $2;
            n->label = $3;
            n->varlen = $4;
            n->props = $5;

            $$ = (Node *)n;
        }
    |
    /* empty */
        {
            cypher_relationship *n;

            n = make_ag_node(cypher_relationship);
            n->name = NULL;
            n->label = NULL;
            n->varlen = NULL;
            n->props = NULL;

            $$ = (Node *)n;
        }
    ;

label_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | ':' label_name
        {
            $$ = $2;
        }
    ;

properties_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | map
    | PARAMETER
        {
            cypher_param *n;

            n = make_ag_node(cypher_param);
            n->name = $1;
            n->location = @1;

            $$ = (Node *)n;
        }

    ;

/*
 * expression
 */
a_expr:
    a_expr OR a_expr
        {
            $$ = make_or_expr($1, $3, @2);
        }
    | a_expr AND a_expr
        {
            $$ = make_and_expr($1, $3, @2);
        }
    | a_expr XOR a_expr
        {
            $$ = make_xor_expr($1, $3, @2);
        }
    | NOT a_expr
        {
            $$ = make_not_expr($2, @1);
        }
    | a_expr '=' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2);
        }
    | a_expr LIKE a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~~", $1, $3, @2);
        } 
    | a_expr NOT LIKE a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2);
        }  
    | a_expr ILIKE a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~~*", $1, $3, @2);
        } 
    | a_expr NOT ILIKE a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, $4, @2);
        }  
    | a_expr NOT_EQ a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2);
        }
    | a_expr '<' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2);
        }
    | a_expr LT_EQ a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2);
        }
    | a_expr '>' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2);
        }
    | a_expr GT_EQ a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2);
        }
    | a_expr '+' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2);
        }
    | a_expr '-' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2);
        }
    | a_expr '*' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2);
        }
    | a_expr '/' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2);
        }
    | a_expr '%' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2);
        }
    | a_expr '^' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2);
        }
    | a_expr OPERATOR a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, $2, $1, $3, @2);
        }
    | OPERATOR a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, $1, NULL, $2, @1);
        }
    | a_expr '<' '-' '>' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<->", $1, $5, @3);
        }
    | a_expr IN a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "@=", $1, $3, @2);
        }
    | a_expr IS NULL_P %prec IS
        {
            NullTest *n;

            n = makeNode(NullTest);
            n->arg = (Expr *)$1;
            n->nulltesttype = IS_NULL;
            n->location = @2;

            $$ = (Node *)n;
        }
    | a_expr IS NOT NULL_P %prec IS
        {
            NullTest *n;

            n = makeNode(NullTest);
            n->arg = (Expr *)$1;
            n->nulltesttype = IS_NOT_NULL;
            n->location = @2;

            $$ = (Node *)n;
        }
    | '-' a_expr %prec UNARY_MINUS
        {
            $$ = do_negate($2, @1);
        
        }
    | '~' a_expr %prec UNARY_MINUS
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~", NULL, $2, @1);
        }
    | a_expr STARTS WITH a_expr %prec STARTS
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_STARTS_WITH;
            n->lhs = $1;
            n->rhs = $4;
            n->location = @2;

            $$ = (Node *)n;
        }
    | a_expr ENDS WITH a_expr %prec ENDS
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_ENDS_WITH;
            n->lhs = $1;
            n->rhs = $4;
            n->location = @2;

            $$ = (Node *)n;
        }
    | a_expr CONTAINS a_expr
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_CONTAINS;
            n->lhs = $1;
            n->rhs = $3;
            n->location = @2;

            $$ = (Node *)n;
        }
    | a_expr '~' a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~", $1, $3, @2);
        }
    | a_expr '[' a_expr ']'  %prec '.'
        {
            A_Indices *i;

            i = makeNode(A_Indices);
            i->is_slice = false;
            i->lidx = NULL;
            i->uidx = $3;

            $$ = append_indirection($1, (Node *)i);
        }
    | a_expr '[' expr_opt DOT_DOT expr_opt ']'
        {
            A_Indices *i;

            i = makeNode(A_Indices);
            i->is_slice = true;
            i->lidx = $3;
            i->uidx = $5;

            $$ = append_indirection($1, (Node *)i);
        }
    | a_expr '.' a_expr
        {
            $$ = append_indirection($1, $3);
        }
    | a_expr TYPECAST schema_name
        {
            $$ = make_typecast_expr($1, $3, @2);
        }
    | temporal_cast a_expr %prec TYPECAST
        {
            $$ = make_typecast_expr($2, $1, @1);
        }
    | a_expr all_op ALL in_expr //%prec OPERATOR
        {
            cypher_sub_pattern *sub = $4;
            sub->kind = CSP_ALL;

            SubLink *n = makeNode(SubLink);
            n->subLinkType = ALL_SUBLINK;
            n->subLinkId = 0;
            n->testexpr = $1;
            n->operName = list_make2(makeString("postgraph"), makeString($2));
            n->subselect = (Node *) sub;
            n->location = @1;
            $$ = (Node *) n;

        }
    | a_expr all_op ANY in_expr //%prec OPERATOR
        {
            cypher_sub_pattern *sub = $4;

            SubLink *n = makeNode(SubLink);
            n->subLinkType = ANY_SUBLINK;
            n->subLinkId = 0;
            n->testexpr = $1;
            n->operName = list_make2(makeString("postgraph"), makeString($2));
            n->subselect = (Node *) sub;
            n->location = @1;
            $$ = (Node *) n;

        }
    | a_expr all_op SOME in_expr //%prec OPERATOR
        {
            cypher_sub_pattern *sub = $4;

            SubLink *n = makeNode(SubLink);
            n->subLinkType = ANY_SUBLINK;
            n->subLinkId = 0;
            n->testexpr = $1;
            n->operName = list_make2(makeString("postgraph"), makeString($2));
            n->subselect = (Node *) sub;
            n->location = @1;
            $$ = (Node *) n;

        }
    | expr_atom
    ;

all_op:
    OPERATOR
    {
        $$ = $1;
    }
    | '<'
    {
        $$ = "<";
    }
    | '>'
    {
        $$ = ">";
    }
    | '='
        {
            $$ = "=";
        }
    | NOT_EQ
        {
            $$ = "<>";
        }
    | LT_EQ
        {
            $$ = "<=";
        }
    | GT_EQ
        {
            $$ = ">=";
        }
    | '+'
        {
            $$ = "+";
        }
    | '-'
        {
            $$ = "-";
        }
    | '*'
        {
            $$ = "*";
        }
    | '/'
        {
            $$ = "/";
        }
    | '%'
        {
            $$ = "%";
        }
    | '^'
        {
            $$ = "^";
        }
    | '~'
        {
            $$ = "~";
        }
    | '<' '-' '>'
        {
            $$ = "<->";
        }
;

expr_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | a_expr
    ;

expr_list:
    a_expr
        {
            $$ = list_make1($1);
        }
    | expr_list ',' a_expr
        {
            $$ = lappend($1, $3);
        }
    ;

expr_list_opt:
    /* empty */
        {
            $$ = NIL;
        }
    | expr_list
    ;

expr_func:
    expr_func_norm within_group_clause filter_clause over_clause 
    {
        FuncCall *fc = $1;
        /*
         * The order clause for WITHIN GROUP and the one for
         * plain-aggregate ORDER BY share a field, so we have to
         * check here that at most one is present.  We also check
         * for DISTINCT and VARIADIC here to give a better error
         * location.  Other consistency checks are deferred to
         * parse analysis.
         */
        if ($2 != NIL)
        {
             if (fc->agg_order != NIL)
                  ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                  errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP"),
                                  ag_scanner_errposition(@2, scanner)));
             if (fc->agg_distinct)
                  ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                  errmsg("cannot use DISTINCT with WITHIN GROUP"),
                                  ag_scanner_errposition(@2, scanner)));
              if (fc->func_variadic)
                   ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                   errmsg("cannot use VARIADIC with WITHIN GROUP"),
                                   ag_scanner_errposition(@2, scanner)));
              fc->agg_order = $2;
              fc->agg_within_group = true;
        }


        fc->over = $4;
        fc->agg_filter = $3;
        $$ = fc;
    }
    | expr_func_subexpr
    ;

/*
 * Aggregate decoration clauses
 */
within_group_clause:
    WITHIN GROUP '(' order_by_opt ')' { $$ = $4; }
    | /*EMPTY*/                        { $$ = NIL; }
; 

filter_clause:
     FILTER '(' WHERE a_expr ')' { $$ = $4; }
     | /*EMPTY*/               { $$ = NULL; }
     ;      

/*
 * Window Definitions
 */                                     
window_clause:
   WINDOW window_definition_list { $$ = $2; }
   | /*EMPTY*/ { $$ = NIL; }
   ;

window_definition_list:
   window_definition  { $$ = list_make1($1); }
   | window_definition_list ',' window_definition { $$ = lappend($1, $3); }
   ;

window_definition:
   IDENTIFIER AS window_specification
   {
       WindowDef *n = $3;
       n->name = $1;
       $$ = n;
   }
   ;           
over_clause:
    OVER window_specification { $$ = $2; }
    | OVER symbolic_name
        {
            WindowDef *n = makeNode(WindowDef);
            n->name = $2;
            n->refname = NULL;
            n->partitionClause = NIL;
            n->orderClause = NIL;
            n->frameOptions = FRAMEOPTION_DEFAULTS;
            n->startOffset = NULL;
            n->endOffset = NULL;
            n->location = @2;
            $$ = n;
       }
     | /*EMPTY*/
         { $$ = NULL; }
     ;


window_specification: //TODO opt_existing_window_name
    '(' opt_partition_clause /*opt_sort_clause*/ order_by_opt opt_frame_clause ')'
     {
         WindowDef *n = makeNode(WindowDef);
         n->name = NULL;
         n->refname = NULL;//$2;
         n->partitionClause = $2;
         n->orderClause = $3;
         /* copy relevant fields of opt_frame_clause */
         n->frameOptions = $4->frameOptions;
         n->startOffset = $4->startOffset;
         n->endOffset = $4->endOffset;
         n->location = @1;
         $$ = n;
      }
      ;


opt_partition_clause: PARTITION BY expr_list { $$ = $3; }
                        | /*EMPTY*/ { $$ = NIL; }
                ;

/*
 * For frame clauses, we return a WindowDef, but only some fields are used:
 * frameOptions, startOffset, and endOffset.
 */
opt_frame_clause:
    RANGE frame_extent opt_window_exclusion_clause
    {
        WindowDef *n = $2;
        n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_RANGE;
        n->frameOptions |= $3;
        $$ = n;
    }
    | ROWS frame_extent opt_window_exclusion_clause
    {
        WindowDef *n = $2;
        n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS;
        n->frameOptions |= $3;
        $$ = n;
    }
    | GROUPS frame_extent opt_window_exclusion_clause
    {
        WindowDef *n = $2;
        n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_GROUPS;
        n->frameOptions |= $3;
        $$ = n;
    }
    |
    /*EMPTY*/
    {
        WindowDef *n = makeNode(WindowDef);
        n->frameOptions = FRAMEOPTION_DEFAULTS;
        n->startOffset = NULL;
        n->endOffset = NULL;
        $$ = n;
    }

frame_extent: frame_bound
    {
        WindowDef *n = $1;
        /* reject invalid cases */
        if (n->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
                            ag_scanner_errposition(@1, scanner)));
        if (n->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING)
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame starting from following row cannot end with current row"),
                            ag_scanner_errposition(@1, scanner)));
        n->frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
        $$ = n;
    }
    | BETWEEN frame_bound AND frame_bound
    {
        WindowDef *n1 = $2;
        WindowDef *n2 = $4;
        /* form merged options */
        int             frameOptions = n1->frameOptions;
        /* shift converts START_ options to END_ options */
        frameOptions |= n2->frameOptions << 1;
        frameOptions |= FRAMEOPTION_BETWEEN;
        /* reject invalid cases */
        if (frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
                            ag_scanner_errposition(@2, scanner)));
        if (frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING)
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame end cannot be UNBOUNDED PRECEDING"),
                            ag_scanner_errposition(@4, scanner)));
        if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) && (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING))
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame starting from current row cannot have preceding rows"),
                            ag_scanner_errposition(@4, scanner)));
        if ((frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) &&
                (frameOptions & (FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_CURRENT_ROW)))
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("frame starting from following row cannot have preceding rows"),
                            ag_scanner_errposition(@4, scanner)));
        n1->frameOptions = frameOptions;
        n1->endOffset = n2->startOffset;
        $$ = n1;
    }
    ;

/*
 * This is used for both frame start and frame end, with output set up on
 * the assumption it's frame start; the frame_extent productions must reject
 * invalid cases.
 */
frame_bound:
     UNBOUNDED PRECEDING
        {
              WindowDef *n = makeNode(WindowDef);
              n->frameOptions = FRAMEOPTION_START_UNBOUNDED_PRECEDING;
              n->startOffset = NULL;
              n->endOffset = NULL;
        $$ = n;
        }
     | UNBOUNDED FOLLOWING
          {
              WindowDef *n = makeNode(WindowDef);
              n->frameOptions = FRAMEOPTION_START_UNBOUNDED_FOLLOWING;
              n->startOffset = NULL;
              n->endOffset = NULL;
              $$ = n;
          }
     | CURRENT ROW
          {
              WindowDef *n = makeNode(WindowDef);
              n->frameOptions = FRAMEOPTION_START_CURRENT_ROW;
              n->startOffset = NULL;
              n->endOffset = NULL;
              $$ = n;
          }
     | a_expr PRECEDING
          {
              WindowDef *n = makeNode(WindowDef);
              n->frameOptions = FRAMEOPTION_START_OFFSET_PRECEDING;
              n->startOffset = $1;
              n->endOffset = NULL;
              $$ = n;
          }
     | a_expr FOLLOWING
          {
              WindowDef *n = makeNode(WindowDef);
              n->frameOptions = FRAMEOPTION_START_OFFSET_FOLLOWING;
              n->startOffset = $1;
              n->endOffset = NULL;
              $$ = n;
          }
;

opt_window_exclusion_clause:
    EXCLUDE CURRENT ROW   { $$ = FRAMEOPTION_EXCLUDE_CURRENT_ROW; }
    | EXCLUDE GROUP         { $$ = FRAMEOPTION_EXCLUDE_GROUP; }
    | EXCLUDE TIES          { $$ = FRAMEOPTION_EXCLUDE_TIES; }
    | EXCLUDE NO OTHERS     { $$ = 0; }
    | /*EMPTY*/             { $$ = 0; }
;


expr_func_norm:
    func_name '(' ')'
        {
            $$ = (Node *)makeFuncCall($1, NIL, COERCE_SQL_SYNTAX, @1);
        }
    | func_name '(' expr_list ')'
        {
            $$ = (Node *)makeFuncCall($1, $3, COERCE_SQL_SYNTAX, @1);
        }
    /* borrowed from PG's grammar */
    | func_name '(' '*' ')'
        {
            /*
             * We consider AGGREGATE(*) to invoke a parameterless
             * aggregate.  This does the right thing for COUNT(*),
             * and there are no other aggregates in SQL that accept
             * '*' as parameter.
             *
             * The FuncCall node is also marked agg_star = true,
             * so that later processing can detect what the argument
             * really was.
             */
             FuncCall *n = (FuncCall *)makeFuncCall($1, NIL, COERCE_SQL_SYNTAX, @1);
             n->agg_star = true;
             $$ = (Node *)n;
         }
    | func_name '(' DISTINCT  expr_list ')'
        {
            FuncCall *n = (FuncCall *)makeFuncCall($1, $4, COERCE_SQL_SYNTAX, @1);
            n->agg_order = NIL;
            n->agg_distinct = true;
            $$ = (Node *)n;
        }
    ;

expr_func_subexpr:
    CURRENT_DATE
        {
            $$ = makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1);
        }
    | CURRENT_TIME
        {
            $$ = makeSQLValueFunction(SVFOP_CURRENT_TIME, -1, @1);
        }
    | CURRENT_TIME '(' INTEGER ')'
        {
            $$ = makeSQLValueFunction(SVFOP_CURRENT_TIME, $3, @1);
        }
    | CURRENT_TIMESTAMP
        {
            $$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1);
        }
    | CURRENT_TIMESTAMP '(' INTEGER ')'
        {
            $$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, $3, @1);
        }
    | COALESCE '(' expr_list ')'
        {
            CoalesceExpr *c;

            c = makeNode(CoalesceExpr);
            c->args = $3;
            c->location = @1;
            $$ = (Node *) c;
        }
    | LOCALTIME
        {
            $$ = makeSQLValueFunction(SVFOP_LOCALTIME, -1, @1);
        }
    | LOCALTIME '(' INTEGER ')'
        {
            $$ = makeSQLValueFunction(SVFOP_LOCALTIME, $3, @1);
        }
    | LOCALTIMESTAMP
        {
            $$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, -1, @1);
        }
    | LOCALTIMESTAMP '(' INTEGER ')'
        {
            $$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, $3, @1);
        }
    | EXTRACT '(' IDENTIFIER FROM a_expr ')'
        {
            $$ = (Node *)makeFuncCall(list_make1(makeString($1)),
                                      list_make2(make_string_const($3, @3), $5),
                                      COERCE_SQL_SYNTAX, @1);
        }
    | '(' a_expr ',' a_expr ')' OVERLAPS '(' a_expr ',' a_expr ')'
        {
            $$ = makeFuncCall(list_make1(makeString("overlaps")), list_make4($2, $4, $8, $10), COERCE_SQL_SYNTAX, @6);
        }
    ;

temporal_cast:
    TIMESTAMP
        {
            $$ = pnstrdup("timestamp", 9);
        }
    | TIMESTAMP WITHOUT TIME ZONE
        {
            $$ = pnstrdup("timestamp", 9);
        }
    | TIMESTAMP WITH TIME ZONE
        {
            $$ = pnstrdup("timestamptz", 11);
        }
    | DATE
        {
            $$ = pnstrdup("date", 4);
        }
    | TIME
        {
            $$ = pnstrdup("time", 4); 
        }
    | TIME WITHOUT TIME ZONE
        {
            $$ = pnstrdup("time", 4);
        }
    | TIME WITH TIME ZONE
        {
            $$ = pnstrdup("timetz", 6);
        }
    | INTERVAL
        {
            $$ = pnstrdup("interval", 8);
        }
    ;

                                        
in_expr:
    '(' cypher_stmt ')'
        {               
            cypher_sub_pattern *sub;

            sub = make_ag_node(cypher_sub_pattern);
            sub->pattern = $2;

            $$ = (Node *) sub;
       }               
       //| '(' expr_list ')' { $$ = (Node *)$2; }
   ;    

expr_atom:
    expr_literal
    | PARAMETER
        {
            cypher_param *n;

            n = make_ag_node(cypher_param);
            n->name = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | '(' a_expr ')'
        {
            $$ = $2;
        }
    | expr_case
    | var_name
        {
            ColumnRef *n;
            
            n = makeNode(ColumnRef);
            n->fields = list_make1(makeString($1));
            n->location = @1;
            
            $$ = (Node *)n;
        }   
    | expr_func
    | EXISTS '(' anonymous_path ')'
        {
            cypher_sub_pattern *sub;
            SubLink    *n;

            sub = make_ag_node(cypher_sub_pattern);
            sub->kind = CSP_EXISTS;
            sub->pattern = list_make1($3);
            cypher_match *match = make_ag_node(cypher_match);
            match->pattern = list_make1($3);//subpat->pattern;
            match->where = NULL;
            sub->pattern = list_make1(match);
            n = makeNode(SubLink);
            n->subLinkType = EXISTS_SUBLINK;
            n->subLinkId = 0;
            n->testexpr = NULL;
            n->operName = NIL;
            n->subselect = (Node *) sub;
            n->location = @1;
            $$ = (Node *) n;
        }
    | EXISTS '(' cypher_stmt ')'
        {
            cypher_sub_pattern *sub;
            SubLink    *n;

            sub = make_ag_node(cypher_sub_pattern);
            sub->kind = CSP_EXISTS;
            sub->pattern = $3;

            n = makeNode(SubLink);
            n->subLinkType = EXISTS_SUBLINK;
            n->subLinkId = 0;
            n->testexpr = NULL;
            n->operName = NIL;
            n->subselect = (Node *) sub;
            n->location = @1;
            $$ = (Node *) n;
        }
    ;

expr_literal:
    INTEGER
        {
            $$ = make_int_const($1, @1);
        }
    | DECIMAL
        {
            $$ = make_float_const($1, @1);
        }
    | STRING
        {
            $$ = make_string_const($1, @1);
        }
    | TRUE_P
        {
            $$ = make_bool_const(true, @1);
        }
    | FALSE_P
        {
            $$ = make_bool_const(false, @1);
        }
    | NULL_P
        {
            $$ = make_null_const(@1);
        }
    | INET
        {
            $$ = make_inet_const($1, @1);
        }
    | map
    | list
    ;

map:
    '{' map_keyval_list_opt '}'
        {
            cypher_map *n;

            n = make_ag_node(cypher_map);
            n->keyvals = $2;

            $$ = (Node *)n;
        }
    ;

map_keyval_list_opt:
    /* empty */
        {
            $$ = NIL;
        }
    | map_keyval_list
    ;

map_keyval_list:
    property_key_name ':' a_expr
        {
            $$ = list_make2(makeString($1), $3);
        }
    | map_keyval_list ',' property_key_name ':' a_expr
        {
            $$ = lappend(lappend($1, makeString($3)), $5);
        }
    ;

list:
    '[' expr_list_opt ']'
        {
            cypher_list *n;

            n = make_ag_node(cypher_list);
            n->elems = $2;

            $$ = (Node *)n;
        }
    ;

expr_case:
    CASE a_expr expr_case_when_list expr_case_default END_P
        {
            CaseExpr *n;

            n = makeNode(CaseExpr);
            n->casetype = InvalidOid;
            n->arg = (Expr *) $2;
            n->args = $3;
            n->defresult = (Expr *) $4;
            n->location = @1;
            $$ = (Node *) n;
        }
    | CASE expr_case_when_list expr_case_default END_P
        {
            CaseExpr *n;

            n = makeNode(CaseExpr);
            n->casetype = InvalidOid;
            n->args = $2;
            n->defresult = (Expr *) $3;
            n->location = @1;
            $$ = (Node *) n;
        }
    ;

expr_case_when_list:
    expr_case_when
        {
            $$ = list_make1($1);
        }
    | expr_case_when_list expr_case_when
        {
            $$ = lappend($1, $2);
        }
    ;

expr_case_when:
    WHEN a_expr THEN a_expr
        {
            CaseWhen   *n;

            n = makeNode(CaseWhen);
            n->expr = (Expr *) $2;
            n->result = (Expr *) $4;
            n->location = @1;
            $$ = (Node *) n;
        }
    ;

expr_case_default:
    ELSE a_expr
        {
            $$ = $2;
        }
    | /* EMPTY */
        {
            $$ = NULL;
        }
    ;
/*
expr_var:
    var_name
        {
            ColumnRef *n;

            n = makeNode(ColumnRef);
            n->fields = list_make1(makeString($1));
            n->location = @1;

            $$ = (Node *)n;
        }
    ;
*/

/*
 * names
 */
func_name:
    symbolic_name
        {
            $$ = list_make1(makeString($1));
        }
    /*
     * symbolic_name '.' symbolic_name is already covered with the
     * rule expr '.' expr above. This rule is to allow most reserved
     * keywords to be used as well. So, it essentially makes the
     * rule schema_name '.' symbolic_name for func_name
     */
    | safe_keywords '.' symbolic_name
        {
            $$ = list_make2(makeString((char *)$1), makeString($3));
        }
    ;

property_key_name:
    schema_name
    ;

var_name:
    symbolic_name
    ;

var_name_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | var_name
    ;

label_name:
    schema_name
    ;

symbolic_name:
    IDENTIFIER
    | RANGE 
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }
    | ROW
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }
    | LAST_P
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }
    | FIRST_P
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }

    ;

schema_name:
    symbolic_name
    | reserved_keyword
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }
    ;

reserved_keyword:
    safe_keywords
    | conflicted_keywords
    ;

/*
 * All keywords need to be copied and properly terminated with a null before
 * using them, pnstrdup effectively does this for us.
 */

safe_keywords:
    //ALL          { $$ = pnstrdup($1, 3); }
    //|
    AND        { $$ = pnstrdup($1, 3); }
    | AS         { $$ = pnstrdup($1, 2); }
    | ASC        { $$ = pnstrdup($1, 3); }
    | ASCENDING  { $$ = pnstrdup($1, 9); }
    | BY         { $$ = pnstrdup($1, 2); }
    | CALL       { $$ = pnstrdup($1, 4); }
    | CASE       { $$ = pnstrdup($1, 4); }
    | COALESCE   { $$ = pnstrdup($1, 8); }
    | CONTAINS   { $$ = pnstrdup($1, 8); }
    | CREATE     { $$ = pnstrdup($1, 6); }
    | DATE       { $$ = pnstrdup($1, 4); }
    | DELETE     { $$ = pnstrdup($1, 6); }
    | DESC       { $$ = pnstrdup($1, 4); }
    | DESCENDING { $$ = pnstrdup($1, 10); }
    | DETACH     { $$ = pnstrdup($1, 6); }
    | DISTINCT   { $$ = pnstrdup($1, 8); }
    | ELSE       { $$ = pnstrdup($1, 4); }
    | ENDS       { $$ = pnstrdup($1, 4); }
    | EXISTS     { $$ = pnstrdup($1, 6); }
    | INTERVAL   { $$ = pnstrdup($1, 8); }
    | IN         { $$ = pnstrdup($1, 2); }
    | IS         { $$ = pnstrdup($1, 2); }
    | LIMIT      { $$ = pnstrdup($1, 6); }
    | MATCH      { $$ = pnstrdup($1, 6); }
    | MERGE      { $$ = pnstrdup($1, 6); }
    | NOT        { $$ = pnstrdup($1, 3); }
    | OPTIONAL   { $$ = pnstrdup($1, 8); }
    | OR         { $$ = pnstrdup($1, 2); }
    | ORDER      { $$ = pnstrdup($1, 5); }
    | REMOVE     { $$ = pnstrdup($1, 6); }
    | RETURN     { $$ = pnstrdup($1, 6); }
    | SET        { $$ = pnstrdup($1, 3); }
    | SKIP       { $$ = pnstrdup($1, 4); }
    | STARTS     { $$ = pnstrdup($1, 6); }
    | TIME       { $$ = pnstrdup($1, 4); }
    | TIMESTAMP  { $$ = pnstrdup($1, 9); }
    | THEN       { $$ = pnstrdup($1, 4); }
    | UNION      { $$ = pnstrdup($1, 5); }
    | WHEN       { $$ = pnstrdup($1, 4); }
    | WHERE      { $$ = pnstrdup($1, 5); }
    | XOR        { $$ = pnstrdup($1, 3); }
    | YIELD      { $$ = pnstrdup($1, 5); }
    ;

conflicted_keywords:
    END_P     { $$ = pnstrdup($1, 5); }
    | FALSE_P { $$ = pnstrdup($1, 7); }
    | NULL_P  { $$ = pnstrdup($1, 6); }
    | TRUE_P  { $$ = pnstrdup($1, 6); }
    | WITH       { $$ = pnstrdup($1, 4); }
    ;

%%

/*
 * logical operators
 */

static Node *make_or_expr(Node *lexpr, Node *rexpr, int location)
{
    // flatten "a OR b OR c ..." to a single BoolExpr on sight
    if (IsA(lexpr, BoolExpr))
    {
        BoolExpr *bexpr = (BoolExpr *)lexpr;

        if (bexpr->boolop == OR_EXPR)
        {
            bexpr->args = lappend(bexpr->args, rexpr);

            return (Node *)bexpr;
        }
    }

    return (Node *)makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *make_and_expr(Node *lexpr, Node *rexpr, int location)
{
    // flatten "a AND b AND c ..." to a single BoolExpr on sight
    if (IsA(lexpr, BoolExpr))
    {
        BoolExpr *bexpr = (BoolExpr *)lexpr;

        if (bexpr->boolop == AND_EXPR)
        {
            bexpr->args = lappend(bexpr->args, rexpr);

            return (Node *)bexpr;
        }
    }

    return (Node *)makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *make_xor_expr(Node *lexpr, Node *rexpr, int location)
{
    Expr *aorb;
    Expr *notaandb;

    // XOR is (A OR B) AND (NOT (A AND B))
    aorb = makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);

    notaandb = makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
    notaandb = makeBoolExpr(NOT_EXPR, list_make1(notaandb), location);

    return (Node *)makeBoolExpr(AND_EXPR, list_make2(aorb, notaandb), location);
}

static Node *make_not_expr(Node *expr, int location)
{
    return (Node *)makeBoolExpr(NOT_EXPR, list_make1(expr), location);
}

/*
 * arithmetic operators
 */

static Node *do_negate(Node *n, int location)
{
    if (IsA(n, A_Const))
    {
        A_Const *c = (A_Const *)n;

        // report the constant's location as that of the '-' sign
        c->location = location;

        if (c->val.type == T_Integer)
        {
            c->val.val.ival = -c->val.val.ival;
            return n;
        }
        else if (c->val.type == T_Float)
        {
            do_negate_float(&c->val);
            return n;
        }
    }

    return (Node *)makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

static void do_negate_float(Value *v)
{
    Assert(IsA(v, Float));

    if (v->val.str[0] == '-')
        v->val.str = v->val.str + 1; // just strip the '-'
    else
        v->val.str = psprintf("-%s", v->val.str);
}

/*
 * indirection
 */

static Node *append_indirection(Node *expr, Node *selector)
{
    A_Indirection *indir;

    if (IsA(expr, A_Indirection))
    {
        indir = (A_Indirection *)expr;
        indir->indirection = lappend(indir->indirection, selector);

        return expr;
    }
    else
    {
        indir = makeNode(A_Indirection);
        indir->arg = expr;
        indir->indirection = list_make1(selector);

        return (Node *)indir;
    }
}

/*
 * literals
 */

static Node *make_int_const(int i, int location)
{
    A_Const *n;

    n = makeNode(A_Const);
    n->val.type = T_Integer;
    n->val.val.ival = i;
    n->location = location;

    return (Node *)n;
}

static Node *make_float_const(char *s, int location)
{
    A_Const *n;

    n = makeNode(A_Const);
    n->val.type = T_Float;
    n->val.val.str = s;
    n->location = location;

    return (Node *)n;
}

static Node *make_string_const(char *s, int location)
{
    A_Const *n;

    n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = s;
    n->location = location;

    return (Node *)n;
}

static Node *make_bool_const(bool b, int location)
{
    cypher_bool_const *n;

    n = make_ag_node(cypher_bool_const);
    n->boolean = b;
    n->location = location;

    return (Node *)n;
}

static Node *make_inet_const(char *s, int location)
{
    cypher_inet_const *n;

    n = make_ag_node(cypher_inet_const);
    n->inet = s;
    n->location = location;

    return (Node *)n;
}


static Node *make_null_const(int location)
{
    A_Const *n;

    n = makeNode(A_Const);
    n->val.type = T_Null;
    n->location = location;

    return (Node *)n;
}

static Node *make_typecast_expr(Node *expr, char *typecast, int location)
{
    cypher_typecast *node;

    node = make_ag_node(cypher_typecast);
    node->expr = expr;
    node->typecast = typecast;
    node->location = location;

    return (Node *)node;
}

static Node *
makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod, int location)
{
        SQLValueFunction *svf = makeNode(SQLValueFunction);

        svf->op = op;
        /* svf->type will be filled during parse analysis */
        svf->typmod = typmod;
        svf->location = location;
        return (Node *) svf;
}

/*set operation function node to make a set op node*/
static Node *make_set_op(SetOperation op, bool all_or_distinct, List *larg, List *rarg)
{
    cypher_return *n = make_ag_node(cypher_return);

    n->op = op;
    n->all_or_distinct = all_or_distinct;
    n->where = NULL;
    n->larg = (List *) larg;
    n->rarg = (List *) rarg;
    return (Node *) n;
}
