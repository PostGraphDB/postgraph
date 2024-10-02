%{
/*
 * Copyright (C) 2023-2024 PostGraphDB
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
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */

#include "postgraph.h"

#include "catalog/index.h"
#include "catalog/pg_class.h"
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



/* Private struct for the result of privilege_target production */
typedef struct PrivTarget
{
	GrantTargetType targtype;
	ObjectType	objtype;
	List	   *objs;
} PrivTarget;

/* Private struct for the result of import_qualification production */
typedef struct ImportQual
{
	ImportForeignSchemaType type;
	List	   *table_names;
} ImportQual;

/* Private struct for the result of opt_select_limit production */
typedef struct SelectLimit
{
	Node *limitOffset;
	Node *limitCount;
	LimitOption limitOption;
} SelectLimit;

/* Private struct for the result of group_clause production */
typedef struct GroupClause
{
	bool	distinct;
	List   *list;
} GroupClause;

/* ConstraintAttributeSpec yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20

static Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
static void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								SelectLimit *limitClause,
								WithClause *withClause,
								ag_scanner_t yyscanner);
static void updateRawStmtEnd(RawStmt *rs, int end_location);
static RawStmt *makeRawStmt(Node *stmt, int stmt_location);
static Node *makeStringConst(char *str, int location);
static Node *makeStringConstCast(char *str, int location, TypeName *typename);
static Node *makeIntConst(int val, int location);
static Node *makeFloatConst(char *str, int location);
static Node *makeBitStringConst(char *str, int location);
static Node *makeBoolAConst(bool state, int location);
static Node *makeNullAConst(int location);
static Node *makeTypeCast(Node *arg, TypeName *typename, int location);
static Node *makeAndExpr(Node *lexpr, Node *rexpr, int location);
static Node *makeOrExpr(Node *lexpr, Node *rexpr, int location);
static Node *makeNotExpr(Node *expr, int location);
static Node *makeAConst(Value *v, int location);
static RoleSpec *makeRoleSpec(RoleSpecType type, int location);
static Node *makeAArrayExpr(List *elements, int location);
static Node *makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args, int location);
static List *extractArgTypes(List *parameters);
static List *mergeTableFuncParameters(List *func_args, List *columns);
static TypeName *TableFuncTypeName(List *columns);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, ag_scanner_t yyscanner);

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
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
	struct FunctionParameter   *fun_param;
	struct ObjectWithArgs		*objwithargs;
    struct WindowDef *windef;
    struct DefElem *defelt;

    struct TypeName *typnam;
	struct PartitionElem *partelem;
	struct PartitionSpec *partspec;
	struct PartitionBoundSpec *partboundspec;
    struct Value *value;
    struct ResTarget *target;
    struct Alias *alias;
    struct RangeVar *range;
	struct IntoClause *into;
	struct WithClause *with;
	struct PrivTarget	*privtarget;
	struct AccessPriv			*accesspriv;
    struct SortBy *sortby;
    struct InsertStmt *istmt;
    struct VariableSetStmt *vsetstmt;
	struct JoinExpr *jexpr;
	struct IndexElem *ielem;
	struct RoleSpec *rolespec;
	struct SelectLimit	*selectlimit;
    struct GroupClause  *groupclause;
}

%token <integer> INTEGER
%token <string> DECIMAL STRING

%token <string> IDENTIFIER
%token <string> INET
%token <string> PARAMETER
%token <string> OPERATOR
%token <string> XCONST

/* operators that have more than 1 character */
%token NOT_EQ LT_EQ GT_EQ DOT_DOT TYPECAST PLUS_EQ

/* keywords in alphabetical order */
%token <keyword> ACCESS ACTION ADMIN ALL ALTER AND ANY ALWAYS ARRAY AS ASC ASCENDING ASSIGNMENT ASYMMETRIC AT ATOMIC AUTHORIZATION

                 BIGINT BEGIN_P BETWEEN BOOLEAN_P BOTH BREADTH BY

                 CACHE CALL CALLED CASE CAST CASCADE CHECK CROSS COALESCE COLLATE COLLATION COMMENTS COMPRESSION CONNECTION
				 CONCURRENTLY CONTAINS CONSTRAINT CONSTRAINTS COST CREATE CUBE CURRENT 
                 CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME 
                 CURRENT_TIMESTAMP CURRENT_USER CYCLE CYPHER

                 DATA_P DATABASE DECADE DEC DECIMAL_P DEFAULT DEFAULTS DEFERRABLE DEFERRED DEFINER DELETE DEPTH DESC DESCENDING DETACH DISTINCT 
				 DOMAIN_P DOCUMENT_P DOUBLE_P DROP

                 ENCODING ENCRYPTED ELSE END_P ENDS ESCAPE EXCEPT EXCLUDE EXCLUDING EXISTS EXTENSION EXTRACT EXTERNAL
                 EVENT

                 GENERATED GLOBAL GRANT GRANTED GRAPH GREATEST GROUP GROUPS GROUPING

                 FALSE_P FETCH FILTER FIRST_P FLOAT_P FOLLOWING FOR FORCE FOREIGN FROM FULL FUNCTION FUNCTIONS

                 HAVING

                 IDENTITY_P IF ILIKE IN INCLUDING INDEX INDEXES IMMEDIATE IMMUTABLE IMPLICIT_P INCLUDE INCREMENT INHERIT INHERITS INITIALLY INNER 
				 INOUT INPUT_P INT_P INTEGER_P INTERSECT INSERT INTERVAL INTO INVOKER IS ISNULL ISOLATION

                 JOIN

                 KEY

                 LANGUAGE LARGE_P LAST_P LATERAL_P LEADING LEAKPROOF LEAST LEFT LEVEL LIKE LIMIT LOCATION LOCAL LOCALTIME LOCALTIMESTAMP
				 LOCK_P LOCKED

                 MATERIALIZED MATCH MAXVALUE MERGE METHOD MINVALUE

                 NAME_P NATURAL NEXT NFC NFD NFKC NFKD NO NORMALIZE NORMALIZED NOT NOTNULL NOWAIT NULL_P NULLIF NULLS_LA NUMERIC

                 OBJECT_P OF OFFSET ON ONLY OPTION OPTIONS OPTIONAL OTHERS OR ORDER OUT_P OUTER OVER OVERRIDING OVERLAPS OVERLAY OWNED OWNER

                 PARALLEL PARTIAL PARTITION PASSWORD PLACING POLICY POSITION PUBLICATION PRECEDING PRECISION PRIMARY PRIVILEGES PROCEDURAL PROCEDURE PROCEDURES

                 RANGE READ RIGHT REAL RECURSIVE REFERENCES REMOVE RESET RESTART RESTRICT REPLACE RETURN RETURNS RULE ROLE ROLLUP ROUTINE ROUTINES ROW ROWS

                 SCHEMA SEARCH SECURITY SERVER SELECT SEQUENCE SEQUENCES SESSION SESSION_USER SET SETOF SETS SHARE
				 SIMPLE SKIP SMALLINT SOME STABLE START STARTS STATEMENTS STATISTICS
				 STORED STORAGE STRICT_P SUBSCRIPTION SUBSTRING SUPPORT SYMMETRIC SYSID SYSTEM_P

                 TABLE TABLES TABLESPACE TEMP TEMPLATE TEMPORARY  TIME TIES THEN TIMESTAMP TO TRAILING TRANSACTION TRANSFORM TREAT TRIGGER TRIM TRUE_P
				 TYPE_P

                 UNBOUNDED UNENCRYPTED UNION UNIQUE UNKNOWN UNLOGGED UNTIL UPDATE UNWIND USE USER USING

                 VALID VALUE_P VALUES VARIADIC VERSION_P VOLATILE

                 WHEN WHERE WINDOW WITH WITHIN WITHOUT WRAPPER

                 XOR

                 YIELD

                 ZONE

/* query */
%type <node> stmt
%type <list> single_query  cypher_stmt

%type <node> parse_toplevel stmtmulti schema_stmt routine_body_stmt
             AlterDatabaseStmt AlterDatabaseSetStmt
             CreateCastStmt CreatedbStmt CreateSchemaStmt
             CreateExtensionStmt CreateFunctionStmt CreateGraphStmt 
			 CreateTableStmt CreateTableSpaceStmt
			 CreateUserStmt
             DefineStmt DeleteStmt DropdbStmt DropGraphStmt
			 DropStmt
             GrantStmt
			 InsertStmt
             UseGraphStmt
			 ReturnStmt
             SelectStmt
             UpdateStmt
			 PreparableStmt
             VariableResetStmt VariableSetStmt

%type <node> select_no_parens select_with_parens select_clause
             simple_select

%type <node> where_clause where_or_current_clause
             a_expr b_expr c_expr AexprConst indirection_el opt_slice_bound
             columnref in_expr having_clause array_expr
             OptWhereClause

%type <integer> set_quantifier
%type <target>	target_el set_target insert_column_item
%type <range>	relation_expr
%type <range>	relation_expr_opt_alias
%type <node>	case_expr case_arg when_clause case_default
%type <list>	when_clause_list 
%type <alias>	alias_clause opt_alias_clause opt_alias_clause_for_join_using
%type <node> clause
%type <groupclause> group_clause
%type <list>	group_by_list
%type <node>	group_by_item 
%type <node>	grouping_sets_clause

%type <node>	TableConstraint TableLikeClause ConstraintElem
%type <integer>	TableLikeOptionList TableLikeOption
%type <string>		column_compression opt_column_compression
%type <list>	ColQualList
%type <node>	ColConstraint ColConstraintElem ConstraintAttr
%type <integer>	key_actions key_delete key_match key_update key_action
%type <integer>	ConstraintAttributeSpec ConstraintAttributeElem
%type <string>	ExistingIndex
%type <integer>	generated_when override_kind
%type <integer> opt_materialized

%type <integer> cast_context
%type <node>	opt_search_clause opt_cycle_clause

%type <node>	func_application func_expr_common_subexpr
%type <node>	func_expr func_expr_windowless
%type <node>	common_table_expr
%type <with>	with_clause opt_with_clause
%type <list>	cte_list


%type <list>	 SeqOptList OptParenthesizedSeqOptList
%type <defelt>	SeqOptElem

%type <node>	TableElement 
%type <node>	columnDef columnOptions

%type <list>	ExclusionConstraintList ExclusionConstraintElem
%type <list>	func_arg_list func_arg_list_opt
%type <node>	func_arg_expr
%type <list>	row explicit_row implicit_row array_expr_list
%type <node>	table_ref
%type <jexpr>	joined_table
%type <ielem>	index_elem index_elem_options

%type <integer>	opt_nowait_or_skip

%type <integer>	for_locking_strength
%type <node>	for_locking_item
%type <list>	for_locking_clause opt_for_locking_clause for_locking_items
%type <list>	locked_rels_list

%type <node>	join_qual
%type <integer>	join_type

%type <string>		generic_option_name
%type <node>	generic_option_arg
%type <defelt>	generic_option_elem 
%type <list>	generic_option_list 

%type <list>	createdb_opt_list createdb_opt_items
%type <defelt>	createdb_opt_item 
%type <string>		createdb_opt_name 

%type <integer>	opt_drop_behavior

%type <integer>	object_type_any_name object_type_name object_type_name_on_any_name
				drop_type_name

%type <list>	OptRoleList 
%type <defelt>	CreateOptRoleElem AlterOptRoleElem

%type <string>		OptSchemaName
%type <list>	OptSchemaEltList

%type <string>		RoleId 
%type <string>		unicode_normal_form
%type <rolespec> RoleSpec 
%type <string>	access_method_clause

%type <node> cypher_query_start
%type <list> cypher_query_body
%type <vsetstmt> generic_set set_rest set_rest_more generic_reset reset_rest
                 SetResetClause 
%type <string> var_name type_function_name param_name
%type <node>	var_value 
%type <string>	 opt_boolean_or_string

%type <rolespec> grantee
%type <list>	grantee_list
%type <accesspriv> privilege
%type <list>	privileges privilege_list
%type <privtarget> privilege_target

%type <rolespec> opt_granted_by

%type <string> unreserved_keyword 
%type <string> reserved_keyword 
%type <string> bare_label_keyword

/* RETURN and WITH clause */
%type <node> empty_grouping_set cube_clause rollup_clause group_item having_opt return return_item sort_item skip_opt limit_opt with
%type <list> group_item_list return_item_list order_by_opt sort_item_list group_by_opt within_group_clause
%type <integer> order_opt opt_nulls_order 

/* MATCH clause */
%type <node> match cypher_varlen_opt cypher_range_opt cypher_range_idx
             cypher_range_idx_opt
%type <integer> Iconst
%type <boolean> optional_opt opt_or_replace
                opt_grant_grant_option 

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
%type <node> cypher_where_opt

/* pattern */
%type <list> pattern simple_path_opt_parens simple_path
%type <node> path anonymous_path
             path_node path_relationship path_relationship_body
             properties_opt
%type <string> label_opt 

/* expression */
%type <node> cypher_a_expr expr_opt expr_atom expr_literal map list cypher_in_expr
             

%type <node> expr_case expr_case_when expr_case_default values_clause
%type <list> expr_case_when_list

%type <node> cypher_expr_func cypher_expr_func_norm cypher_expr_func_subexpr
%type <list> cypher_expr_list cypher_expr_list_opt map_keyval_list_opt map_keyval_list
        

%type <node> filter_clause
%type <list> window_clause window_definition_list opt_partition_clause
%type <windef> window_definition over_clause window_specification opt_frame_clause
               frame_extent frame_bound
%type <integer>    opt_window_exclusion_clause
%type <string> all_op
/* names */

%type <defelt>	drop_option

%type <string> property_key_name cypher_var_name var_name_opt label_name
%type <string> symbolic_name schema_name temporal_cast attr_name table_access_method_clause
%type <keyword> cypher_reserved_keyword safe_keywords conflicted_keywords

%type <node>	select_limit_value
				offset_clause select_offset_value
				select_fetch_first_value I_or_F_const
%type <integer>	row_or_rows first_or_next

%type <selectlimit> opt_select_limit select_limit limit_clause

%type <list> routine_body_stmt_list
             cypher_func_name expr_list
             TableElementList OptTableElementList OptInherit definition
             reloptions opt_reloptions
             name_list role_list opt_array_bounds
             OptWith opt_definition func_args func_args_list
			 func_args_with_defaults func_args_with_defaults_list
			 func_as createfunc_opt_list opt_createfunc_opt_list
			 opt_column_list
			 opt_c_include
             qualified_name_list any_name any_name_list type_name_list
			 any_operator
             reloption_list
			 columnList opt_name_list
             sort_clause opt_sort_clause sortby_list
             from_clause from_list
             target_list opt_target_list insert_column_list set_target_list
             set_clause_list set_clause
			 opt_type_modifiers
			 def_list
             opt_collate
             using_clause
             indirection opt_indirection
			 attrs opt_class
             var_list
			 create_generic_options
			 table_func_column_list
			 transform_type_list
			 drop_option_list

%type <node>	opt_routine_body

%type <objwithargs> function_with_argtypes 
%type <list>	function_with_argtypes_list 

%type <list>	func_name qual_Op qual_all_Op subquery_Op
%type <string>		all_Op MathOp
%type <string>	 OptConsTableSpace
%type <rolespec> OptTableSpaceOwner
%type <integer>	sub_type 

%type <defelt>	def_elem reloption_elem
%type <string> Sconst 
%type <string> ColId  ColLabel BareColLabel
%type <string> NonReservedWord NonReservedWord_or_Sconst name
%type <list> create_extension_opt_list
%type <defelt> create_extension_opt_item
%type <sortby>	sortby
%type <integer>	 OptTemp opt_asc_desc

%type <typnam>	Typename SimpleTypename ConstTypename
                GenericType Numeric opt_float
				ConstDatetime
%type <boolean> opt_timezone opt_no_inherit


%type <defelt>	createfunc_opt_item common_func_opt_item 
%type <fun_param> func_arg func_arg_with_default table_func_column
%type <integer> arg_class
%type <typnam>	func_return func_type

%type <range>	qualified_name insert_target
%type <istmt>	insert_rest

%type <list>	extract_list overlay_list position_list
%type <list>	substr_list trim_list
%type <string>	extract_arg

/*set operations*/
%type <boolean> all_or_distinct

%type <integer> SignedIconst
%type <node>	def_arg columnElem
%type <partspec> PartitionSpec OptPartitionSpec
%type <partelem> part_elem
%type <list> part_params
%type <value>	NumericOnly
%type <list>	NumericOnly_list

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
%nonassoc CONTAINS ENDS EQ_TILDE STARTS LIKE ILIKE SIMILAR
%nonassoc ESCAPE
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
static bool is_cypher_a_expr_a_comparison_operation(A_Expr *a);
static void
doNegateFloat(Value *v);


static Node *
makeColumnRef(char *colname, List *indirection,
			  int location, ag_scanner_t yyscanner);
static List *
check_indirection(List *indirection, ag_scanner_t yyscanner);

static Node *
doNegate(Node *n, int location);
static List *
check_func_name(List *names, ag_scanner_t yyscanner);

%}
%%


/*
 *	The target production for the whole parse.
 *
 * Ordinarily we parse a list of statements, but if we see one of the
 * special MODE_XXX symbols as first token, we parse something else.
 * The options here correspond to enum RawParseMode, which see for details.
 */
parse_toplevel:
			stmtmulti
			{

            if (yychar != YYEOF)
                yyerror(&yylloc, scanner, extra, "syntax error");				
				//pg_yyget_extra(yyscanner)->parsetree = $1;
				extra->result= $1;
				(void) yynerrs;		/* suppress compiler warning */
			}/*
			| MODE_TYPE_NAME Typename
			{
				pg_yyget_extra(yyscanner)->parsetree = list_make1($2);
			}
			| MODE_PLPGSQL_EXPR PLpgSQL_Expr
			{
				pg_yyget_extra(yyscanner)->parsetree =
					list_make1(makeRawStmt($2, 0));
			}
			| MODE_PLPGSQL_ASSIGN1 PLAssignStmt
			{
				PLAssignStmt *n = (PLAssignStmt *) $2;
				n->nnames = 1;
				pg_yyget_extra(yyscanner)->parsetree =
					list_make1(makeRawStmt((Node *) n, 0));
			}
			| MODE_PLPGSQL_ASSIGN2 PLAssignStmt
			{
				PLAssignStmt *n = (PLAssignStmt *) $2;
				n->nnames = 2;
				pg_yyget_extra(yyscanner)->parsetree =
					list_make1(makeRawStmt((Node *) n, 0));
			}
			| MODE_PLPGSQL_ASSIGN3 PLAssignStmt
			{
				PLAssignStmt *n = (PLAssignStmt *) $2;
				n->nnames = 3;
				pg_yyget_extra(yyscanner)->parsetree =
					list_make1(makeRawStmt((Node *) n, 0));
			}*/
		;

/*
 * At top level, we wrap each stmt with a RawStmt node carrying start location
 * and length of the stmt's text.  Notice that the start loc/len are driven
 * entirely from semicolon locations (@2).  It would seem natural to use
 * @1 or @3 to get the true start location of a stmt, but that doesn't work
 * for statements that can start with empty nonterminals (opt_with_clause is
 * the main offender here); as noted in the comments for YYLLOC_DEFAULT,
 * we'd get -1 for the location in such cases.
 * We also take care to discard empty statements entirely.
 */
stmtmulti:	stmtmulti ';' stmt
				{
					//ereport(WARNING, errmsg("There"));
					if ($1 != NIL)
					{
						/* update length of previous stmt */
						updateRawStmtEnd(llast_node(RawStmt, $1), @2);
					}
					if ($3 != NULL)
						$$= lappend($1, makeRawStmt($3, @2 + 1));
					else
						$$ = $1;
				}
			| stmt 
				{
					//ereport(WARNING, errmsg("Here"));
					if ($1 != NULL)
						$$ = list_make1(makeRawStmt($1, 0));
					else
						$$ = NIL;
				}
		;

/*
 * query
 */
stmt:
    cypher_stmt 
	| AlterDatabaseSetStmt
    | AlterDatabaseStmt 
	| CreateCastStmt
	| CreatedbStmt
    | CreateGraphStmt
    | CreateExtensionStmt 
	| CreateFunctionStmt
	| CreateSchemaStmt
    | CreateTableStmt
	| CreateTableSpaceStmt
	| CreateUserStmt
    | DefineStmt 
    | DeleteStmt
	| DropdbStmt
    | DropGraphStmt 
	| DropStmt 
	| GrantStmt 
    | InsertStmt 
    | SelectStmt 
    | UseGraphStmt 
    | UpdateStmt 
	| VariableResetStmt
    | VariableSetStmt
	| /*EMPTY*/
		{ $$ = NULL; }
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

/*****************************************************************************
 *
 *		QUERY:
 *				SELECT STATEMENTS
 *
 *****************************************************************************/

/* A complete SELECT statement looks like this.
 *
 * The rule returns either a single SelectStmt node or a tree of them,
 * representing a set-operation tree.
 *
 * There is an ambiguity when a sub-SELECT is within an a_expr and there
 * are excess parentheses: do the parentheses belong to the sub-SELECT or
 * to the surrounding a_expr?  We don't really care, but bison wants to know.
 * To resolve the ambiguity, we are careful to define the grammar so that
 * the decision is staved off as long as possible: as long as we can keep
 * absorbing parentheses into the sub-SELECT, we will do so, and only when
 * it's no longer possible to do that will we decide that parens belong to
 * the expression.	For example, in "SELECT (((SELECT 2)) + 3)" the extra
 * parentheses are treated as part of the sub-select.  The necessity of doing
 * it that way is shown by "SELECT (((SELECT 2)) UNION SELECT 2)".	Had we
 * parsed "((SELECT 2))" as an a_expr, it'd be too late to go back to the
 * SELECT viewpoint when we see the UNION.
 *
 * This approach is implemented by defining a nonterminal select_with_parens,
 * which represents a SELECT with at least one outer layer of parentheses,
 * and being careful to use select_with_parens, never '(' SelectStmt ')',
 * in the expression grammar.  We will then have shift-reduce conflicts
 * which we can resolve in favor of always treating '(' <select> ')' as
 * a select_with_parens.  To resolve the conflicts, the productions that
 * conflict with the select_with_parens productions are manually given
 * precedences lower than the precedence of ')', thereby ensuring that we
 * shift ')' (and then reduce to select_with_parens) rather than trying to
 * reduce the inner <select> nonterminal to something else.  We use UNARY_MINUS
 * precedence for this, which is a fairly arbitrary choice.
 *
 * To be able to define select_with_parens itself without ambiguity, we need
 * a nonterminal select_no_parens that represents a SELECT structure with no
 * outermost parentheses.  This is a little bit tedious, but it works.
 *
 * In non-expression contexts, we use SelectStmt which can represent a SELECT
 * with or without outer parentheses.
 */

SelectStmt: select_no_parens			%prec '.'
			| select_with_parens		%prec '.'
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2; }
			| '(' select_with_parens ')'			{ $$ = $2; }
		;


/*
 * This rule parses the equivalent of the standard's <query expression>.
 * The duplicative productions are annoying, but hard to get rid of without
 * creating shift/reduce conflicts.
 *
 *	The locking clause (FOR UPDATE etc) may be before or after LIMIT/OFFSET.
 *	In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE
 *	We now support both orderings, but prefer LIMIT/OFFSET before the locking
 * clause.
 *	2002-08-28 bjm
 */
select_no_parens:
			simple_select						{ $$ = $1; }
			| select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, NIL,
										NULL, NULL,
										scanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $1, $2, $3,
										$4,
										NULL,
										scanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, $4,
										$3,
										NULL,
										scanner);
					$$ = $1;
				}
			| with_clause select_clause
				{
					insertSelectOptions((SelectStmt *) $2, NULL, NIL,
										NULL,
										$1,
										scanner);
					$$ = $2;
				}
			| with_clause select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, NIL, NULL, $1, scanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $2, $3, $4,
										$5,
										$1,
										scanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, $5,
										$4,
										$1,
										scanner);
					$$ = $2;
				}
		;

select_clause:
			simple_select							{ $$ = $1; }
			| select_with_parens					{ $$ = $1; }
		;

/*
 * This rule parses SELECT statements that can appear within set operations,
 * including UNION, INTERSECT and EXCEPT.  '(' and ')' can be used to specify
 * the ordering of the set operations.	Without '(' and ')' we want the
 * operations to be ordered per the precedence specs at the head of this file.
 *
 * As with select_no_parens, simple_select cannot have outer parentheses,
 * but can have parenthesized subclauses.
 *
 * It might appear that we could fold the first two alternatives into one
 * by using opt_distinct_clause.  However, that causes a shift/reduce conflict
 * against INSERT ... SELECT ... ON CONFLICT.  We avoid the ambiguity by
 * requiring SELECT DISTINCT [ON] to be followed by a non-empty target_list.
 *
 * Note that sort clauses cannot be included at this level --- SQL requires
 *		SELECT foo UNION SELECT bar ORDER BY baz
 * to be parsed as
 *		(SELECT foo UNION SELECT bar) ORDER BY baz
 * not
 *		SELECT foo UNION (SELECT bar ORDER BY baz)
 * Likewise for WITH, FOR UPDATE and LIMIT.  Therefore, those clauses are
 * described as part of the select_no_parens production, not simple_select.
 * This does not limit functionality, because you can reintroduce these
 * clauses inside parentheses.
 *
 * NOTE: only the leftmost component SelectStmt should have INTO.
 * However, this is not checked by the grammar; parse analysis must check it.
 */
simple_select:
			SELECT opt_all_clause opt_target_list
			//into_clause 
            from_clause where_clause group_clause
			 having_clause window_clause
				{
					SelectStmt *n = makeNode(SelectStmt);
					/*n->targetList = $3;
					n->intoClause = $4;
					n->fromClause = $5;
					n->whereClause = $6;
					n->groupClause = ($7)->list;
					n->groupDistinct = ($7)->distinct;
					n->havingClause = $8;
					n->windowClause = $9;*/
                    n->targetList = $3;
					n->intoClause = NULL;
					n->fromClause = $4;
					n->whereClause = $5;
					n->groupClause = ($6)->list;
					n->groupDistinct = ($6)->distinct;
					n->havingClause = $7;
					n->windowClause = $8;
					$$ = (Node *)n;
				}

			/*| SELECT distinct_clause target_list
			into_clause from_clause where_clause
			group_clause having_clause window_clause
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->distinctClause = $2;
					n->targetList = $3;
					n->intoClause = $4;
					n->fromClause = $5;
					n->whereClause = $6;
					n->groupClause = ($7)->list;
					n->groupDistinct = ($7)->distinct;
					n->havingClause = $8;
					n->windowClause = $9;
					$$ = (Node *)n;
				}
*/
			| values_clause							{ $$ = $1; }
			| TABLE relation_expr
				{
					// same as SELECT * FROM relation_expr 
					ColumnRef *cr = makeNode(ColumnRef);
					ResTarget *rt = makeNode(ResTarget);
					SelectStmt *n = makeNode(SelectStmt);

					cr->fields = list_make1(makeNode(A_Star));
					cr->location = -1;

					rt->name = NULL;
					rt->indirection = NIL;
					rt->val = (Node *)cr;
					rt->location = -1;

					n->targetList = list_make1(rt);
					n->fromClause = list_make1($2);
					$$ = (Node *)n;
				}
			| select_clause UNION set_quantifier select_clause
				{
					$$ = makeSetOp(SETOP_UNION, $3 == SET_QUANTIFIER_ALL, $1, $4);
				}
			| select_clause INTERSECT set_quantifier select_clause
				{
					$$ = makeSetOp(SETOP_INTERSECT, $3 == SET_QUANTIFIER_ALL, $1, $4);
				}
			| select_clause EXCEPT set_quantifier select_clause
				{
					$$ = makeSetOp(SETOP_EXCEPT, $3 == SET_QUANTIFIER_ALL, $1, $4);
				}
		;

/*
 * SQL standard WITH clause looks like:
 *
 * WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
 *		AS (query) [ SEARCH or CYCLE clause ]
 *
 * Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
 */
with_clause:
		WITH cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH RECURSIVE cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $3;
				$$->recursive = true;
				$$->location = @1;
			}
		;

cte_list:
		common_table_expr						{ $$ = list_make1($1); }
		| cte_list ',' common_table_expr		{ $$ = lappend($1, $3); }
		;

common_table_expr:  name opt_name_list AS opt_materialized '(' PreparableStmt ')' opt_search_clause opt_cycle_clause
			{
				CommonTableExpr *n = makeNode(CommonTableExpr);
				n->ctename = $1;
				n->aliascolnames = $2;
				n->ctematerialized = $4;
				n->ctequery = $6;
				n->search_clause = castNode(CTESearchClause, $8);
				n->cycle_clause = castNode(CTECycleClause, $9);
				n->location = @1;
				$$ = (Node *) n;
			}
		;

opt_materialized:
		MATERIALIZED							{ $$ = CTEMaterializeAlways; }
		| NOT MATERIALIZED						{ $$ = CTEMaterializeNever; }
		| /*EMPTY*/								{ $$ = CTEMaterializeDefault; }
		;

opt_search_clause:
		SEARCH DEPTH FIRST_P BY columnList SET ColId
			{
				CTESearchClause *n = makeNode(CTESearchClause);
				n->search_col_list = $5;
				n->search_breadth_first = false;
				n->search_seq_column = $7;
				n->location = @1;
				$$ = (Node *) n;
			}
		| SEARCH BREADTH FIRST_P BY columnList SET ColId
			{
				CTESearchClause *n = makeNode(CTESearchClause);
				n->search_col_list = $5;
				n->search_breadth_first = true;
				n->search_seq_column = $7;
				n->location = @1;
				$$ = (Node *) n;
			}
		| /*EMPTY*/
			{
				$$ = NULL;
			}
		;

opt_cycle_clause:
		CYCLE columnList SET ColId TO AexprConst DEFAULT AexprConst USING ColId
			{
				CTECycleClause *n = makeNode(CTECycleClause);
				n->cycle_col_list = $2;
				n->cycle_mark_column = $4;
				n->cycle_mark_value = $6;
				n->cycle_mark_default = $8;
				n->cycle_path_column = $10;
				n->location = @1;
				$$ = (Node *) n;
			}
		| CYCLE columnList SET ColId USING ColId
			{
				CTECycleClause *n = makeNode(CTECycleClause);
				n->cycle_col_list = $2;
				n->cycle_mark_column = $4;
				n->cycle_mark_value = makeBoolAConst(true, -1);
				n->cycle_mark_default = makeBoolAConst(false, -1);
				n->cycle_path_column = $6;
				n->location = @1;
				$$ = (Node *) n;
			}
		| /*EMPTY*/
			{
				$$ = NULL;
			}
		;

opt_with_clause:
		with_clause								{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NULL; }
		;

opt_all_clause:
			ALL
			| /*EMPTY*/
		;

PreparableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt					/* by default all are $$=$1 */
		;


opt_name_list:
			'(' name_list ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
/*****************************************************************************
 *
 *	target list for SELECT
 *
 *****************************************************************************/

opt_target_list: target_list						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

target_list:
			target_el								{ $$ = list_make1($1); }
			| target_list ',' target_el				{ $$ = lappend($1, $3); }
		;

target_el:	a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr BareColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $2;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| '*'
				{
					ColumnRef *n = makeNode(ColumnRef);
					n->fields = list_make1(makeNode(A_Star));
					n->location = @1;

					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)n;
					$$->location = @1;
				}
		;

/*****************************************************************************
 *
 *	clauses common to all Optimizable Stmts:
 *		from_clause		- allow list of both JOIN expressions and table names
 *		where_clause	- qualifications for joins or restrictions
 *
 *****************************************************************************/
from_clause:
			FROM from_list							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

from_list:
			table_ref								{ $$ = list_make1($1); }
			| from_list ',' table_ref				{ $$ = lappend($1, $3); }
		;

/*
 * table_ref is where an alias clause can be attached.
 */
table_ref:	relation_expr opt_alias_clause
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			/*| relation_expr opt_alias_clause tablesample_clause
				{
					RangeTableSample *n = (RangeTableSample *) $3;
					$1->alias = $2;
					// relation_expr goes inside the RangeTableSample node 
					n->relation = (Node *) $1;
					$$ = (Node *) n;
				}
			| func_table func_alias_clause
				{
					RangeFunction *n = (RangeFunction *) $1;
					n->alias = linitial($2);
					n->coldeflist = lsecond($2);
					$$ = (Node *) n;
				}
			| LATERAL_P func_table func_alias_clause
				{
					RangeFunction *n = (RangeFunction *) $2;
					n->lateral = true;
					n->alias = linitial($3);
					n->coldeflist = lsecond($3);
					$$ = (Node *) n;
				}
			| xmltable opt_alias_clause
				{
					RangeTableFunc *n = (RangeTableFunc *) $1;
					n->alias = $2;
					$$ = (Node *) n;
				}
			| LATERAL_P xmltable opt_alias_clause
				{
					RangeTableFunc *n = (RangeTableFunc *) $2;
					n->lateral = true;
					n->alias = $3;
					$$ = (Node *) n;
				}*/
			| select_with_parens opt_alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = false;
					n->subquery = $1;
					n->alias = $2;
					 // The SQL spec does not permit a subselect
					 // (<derived_table>) without an alias clause,
					 // so we don't either.  This avoids the problem
					 // of needing to invent a unique refname for it.
					 // That could be surmounted if there's sufficient
					 // popular demand, but for now let's just implement
					 // the spec and see if anyone complains.
					 // However, it does seem like a good idea to emit
					 // an error message that's better than "syntax error".
					 
					if ($2 == NULL)
					{
						if (IsA($1, SelectStmt) &&
							((SelectStmt *) $1)->valuesLists)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("VALUES in FROM must have an alias"),
									 errhint("For example, FROM (VALUES ...) [AS] foo.")));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("subquery in FROM must have an alias"),
									 errhint("For example, FROM (SELECT ...) [AS] foo.")));
					}
					$$ = (Node *) n;
				}
			| LATERAL_P select_with_parens opt_alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = true;
					n->subquery = $2;
					n->alias = $3;
					// same comment as above 
					if ($3 == NULL)
					{
						if (IsA($2, SelectStmt) &&
							((SelectStmt *) $2)->valuesLists)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("VALUES in FROM must have an alias"),
									 errhint("For example, FROM (VALUES ...) [AS] foo.")));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("subquery in FROM must have an alias"),
									 errhint("For example, FROM (SELECT ...) [AS] foo.")));
					}
					$$ = (Node *) n;
				}
			| joined_table
				{
					$$ = (Node *) $1;
				}
			| '(' joined_table ')' alias_clause
				{
					$2->alias = $4;
					$$ = (Node *) $2;
				}
		;


/*
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.  For the same reason we must
 * treat 'JOIN' and 'join_type JOIN' separately, rather than allowing
 * join_type to expand to empty; if we try it, the parser generator can't
 * figure out when to reduce an empty join_type right after table_ref.
 *
 * Note that a CROSS JOIN is the same as an unqualified
 * INNER JOIN, and an INNER JOIN/ON has the same shape
 * but a qualification expression to limit membership.
 * A NATURAL JOIN implicitly matches column names between
 * tables and the shape is determined by which columns are
 * in common. We'll collect columns during the later transformations.
 */

joined_table:
			'(' joined_table ')'
				{
					$$ = $2;
				}
			| table_ref CROSS JOIN table_ref
				{
					// CROSS JOIN is same as unqualified inner join 
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL;
					n->join_using_alias = NULL;
					n->quals = NULL;
					$$ = n;
				}
			| table_ref join_type JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
					{
						 // USING clause
						n->usingClause = linitial_node(List, castNode(List, $5));
						n->join_using_alias = lsecond_node(Alias, castNode(List, $5));
					}
					else
					{
						// ON clause 
						n->quals = $5;
					}
					$$ = n;
				}
			| table_ref JOIN table_ref join_qual
				{
					// letting join_type reduce to empty doesn't work 
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $3;
					if ($4 != NULL && IsA($4, List))
					{
						// USING clause 
						n->usingClause = linitial_node(List, castNode(List, $4));
						n->join_using_alias = lsecond_node(Alias, castNode(List, $4));
					}
					else
					{
						// ON clause
						n->quals = $4;
					}
					$$ = n;
				}
			| table_ref NATURAL join_type JOIN table_ref
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $3;
					n->isNatural = true;
					n->larg = $1;
					n->rarg = $5;
					n->usingClause = NIL; 
					n->join_using_alias = NULL;
					n->quals = NULL; 
					$$ = n;
				}
			| table_ref NATURAL JOIN table_ref
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = true;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL; 
					n->join_using_alias = NULL;
					n->quals = NULL;
					$$ = n;
				}
		;

join_type:	FULL opt_outer							{ $$ = JOIN_FULL; }
			| LEFT opt_outer						{ $$ = JOIN_LEFT; }
			| RIGHT opt_outer						{ $$ = JOIN_RIGHT; }
			| INNER								{ $$ = JOIN_INNER; }
		;

/* OUTER is just noise... */
opt_outer: OUTER
			| /*EMPTY*/
		;

/* JOIN qualification clauses
 * Possibilities are:
 *	USING ( column list ) [ AS alias ]
 *						  allows only unqualified column names,
 *						  which must match between tables.
 *	ON expr allows more general qualifications.
 *
 * We return USING as a two-element List (the first item being a sub-List
 * of the common column names, and the second either an Alias item or NULL).
 * An ON-expr will not be a List, so it can be told apart that way.
 */

join_qual: USING '(' name_list ')' opt_alias_clause_for_join_using
				{
					$$ = (Node *) list_make2($3, $5);
				}
			| ON a_expr
				{
					$$ = $2;
				}
		;


relation_expr:
			qualified_name
				{
					// inheritance query, implicitly
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;
				}
			| qualified_name '*'
				{
					// inheritance query, explicitly
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;
				}
			| ONLY qualified_name
				{
					// no inheritance 
					$$ = $2;
					$$->inh = false;
					$$->alias = NULL;
				}
			| ONLY '(' qualified_name ')'
				{
					// no inheritance, SQL99-style syntax
					$$ = $3;
					$$->inh = false;
					$$->alias = NULL;
				}
		;

/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UNARY_MINUS
				{
					$$ = $1;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ = $1;
				}
		;

columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, scanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, scanner);
				}
		;

name_list:	name
					{ $$ = list_make1(makeString($1)); }
			| name_list ',' name
					{ $$ = lappend($1, makeString($3)); }
		;


alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
		;

opt_alias_clause: alias_clause						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
        
/*
 * The alias clause after JOIN ... USING only accepts the AS ColId spelling,
 * per SQL standard.  (The grammar could parse the other variants, but they
 * don't seem to be useful, and it might lead to parser problems in the
 * future.)
 */
opt_alias_clause_for_join_using:
			AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					/* the column name list will be inserted later */
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


where_clause:
			WHERE a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


/* variant for UPDATE and DELETE */
where_or_current_clause:
			WHERE a_expr							{ $$ = $2; }
			/*| WHERE CURRENT_P OF cursor_name
				{
					CurrentOfExpr *n = makeNode(CurrentOfExpr);
					
					n->cursor_name = $4;
					n->cursor_param = 0;
					$$ = (Node *) n;
				}*/
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_sort_clause:
			sort_clause								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

sort_clause:
			ORDER BY sortby_list					{ $$ = $3; }
		;

sortby_list:
			sortby									{ $$ = list_make1($1); }
			| sortby_list ',' sortby				{ $$ = lappend($1, $3); }
		;

sortby:		a_expr USING all_op opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = SORTBY_USING;
					$$->sortby_nulls = $4;
					$$->useOp = $3;
					$$->location = @3;
				}
			| a_expr opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = $2;
					$$->sortby_nulls = $3;
					$$->useOp = NIL;
					$$->location = -1;		/* no operator */
				}
		;



select_limit:
			limit_clause offset_clause
				{
					$$ = $1;
					($$)->limitOffset = $2;
				}
			| offset_clause limit_clause
				{
					$$ = $2;
					($$)->limitOffset = $1;
				}
			| limit_clause
				{
					$$ = $1;
				}
			| offset_clause
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = $1;
					n->limitCount = NULL;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
		;

opt_select_limit:
			select_limit						{ $$ = $1; }
			| /* EMPTY */						{ $$ = NULL; }
		;

limit_clause:
			LIMIT select_limit_value
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $2;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| LIMIT select_limit_value ',' select_offset_value
				{
					/* Disabled because it was too confusing, bjm 2002-02-18 */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("LIMIT #,# syntax is not supported"),
							 errhint("Use separate LIMIT and OFFSET clauses.")));
				}
			/* SQL:2008 syntax */
			/* to avoid shift/reduce conflicts, handle the optional value with
			 * a separate production rather than an opt_ expression.  The fact
			 * that ONLY is fully reserved means that this way, we defer any
			 * decision about what rule reduces ROW or ROWS to the point where
			 * we can see the ONLY token in the lookahead slot.
			 */
			| FETCH first_or_next select_fetch_first_value row_or_rows ONLY
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| FETCH first_or_next select_fetch_first_value row_or_rows WITH TIES
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_WITH_TIES;
					$$ = n;
				}
			| FETCH first_or_next row_or_rows ONLY
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = makeIntConst(1, -1);
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| FETCH first_or_next row_or_rows WITH TIES
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = makeIntConst(1, -1);
					n->limitOption = LIMIT_OPTION_WITH_TIES;
					$$ = n;
				}
		;

offset_clause:
			OFFSET select_offset_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			| OFFSET select_fetch_first_value row_or_rows
				{ $$ = $2; }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is represented as a NULL constant */
					$$ = makeNullAConst(@1);
				}
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

/*
 * Allowing full expressions without parentheses causes various parsing
 * problems with the trailing ROW/ROWS key words.  SQL spec only calls for
 * <simple value specification>, which is either a literal or a parameter (but
 * an <SQL parameter reference> could be an identifier, bringing up conflicts
 * with ROW/ROWS). We solve this by leveraging the presence of ONLY (see above)
 * to determine whether the expression is missing rather than trying to make it
 * optional in this rule.
 *
 * c_expr covers almost all the spec-required cases (and more), but it doesn't
 * cover signed numeric literals, which are allowed by the spec. So we include
 * those here explicitly. We need FCONST as well as ICONST because values that
 * don't fit in the platform's "long", but do fit in bigint, should still be
 * accepted here. (This is possible in 64-bit Windows as well as all 32-bit
 * builds.)
 */
select_fetch_first_value:
			c_expr									{ $$ = $1; }
			| '+' I_or_F_const
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' I_or_F_const
				{ $$ = doNegate($2, @1); }
		;

I_or_F_const:
			Iconst									{ $$ = makeIntConst($1,@1); }
			| DECIMAL								{ $$ = makeFloatConst($1,@1); }
		;

/* noise words */
row_or_rows: ROW									{ $$ = 0; }
			| ROWS									{ $$ = 0; }
		;

first_or_next: FIRST_P								{ $$ = 0; }
			| NEXT									{ $$ = 0; }
		;



/*
 * Supporting nonterminals for expressions.
 */

/* Explicit row production.
 *
 * SQL99 allows an optional ROW keyword, so we can now do single-element rows
 * without conflicting with the parenthesized a_expr production.  Without the
 * ROW keyword, there must be more than one a_expr inside the parens.
 */
row:		ROW '(' expr_list ')'					{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
			| '(' expr_list ',' a_expr ')'			{ $$ = lappend($2, $4); }
		;

explicit_row:	ROW '(' expr_list ')'				{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
		;

implicit_row:	'(' expr_list ',' a_expr ')'		{ $$ = lappend($2, $4); }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			| SOME									{ $$ = ANY_SUBLINK; }
			| ALL									{ $$ = ALL_SUBLINK; }
		;

all_Op:		OPERATOR										{ $$ = $1; }
			| MathOp								{ $$ = $1; }
		;

MathOp:		 '+'									{ $$ = "+"; }
			| '-'									{ $$ = "-"; }
			| '*'									{ $$ = "*"; }
			| '/'									{ $$ = "/"; }
			| '%'									{ $$ = "%"; }
			| '^'									{ $$ = "^"; }
			| '<'									{ $$ = "<"; }
			| '>'									{ $$ = ">"; }
			| '='									{ $$ = "="; }
			| LT_EQ							{ $$ = "<="; }
			| GT_EQ						{ $$ = ">="; }
			| NOT_EQ							{ $$ = "<>"; }
		;

qual_Op:	OPERATOR
					{ $$ = list_make1(makeString($1)); }
			//| OPERATOR '(' any_operator ')'
			//		{ $$ = $3; }
		;

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			//| OPERATOR '(' any_operator ')'
			//		{ $$ = $3; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			//| OPERATOR '(' any_operator ')'
			//		{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT LIKE
					{ $$ = list_make1(makeString("!~~")); }
			| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT ILIKE
					{ $$ = list_make1(makeString("!~~*")); }    

/*****************************************************************************
 *
 *		QUERY:
 *				INSERT STATEMENTS
 *
 *****************************************************************************/

InsertStmt:
/*			opt_with_clause INSERT INTO insert_target insert_rest
			opt_on_conflict returning_clause
				{
					$5->relation = $4;
					$5->onConflictClause = $6;
					$5->returningList = $7;
					$5->withClause = $1;
					$$ = (Node *) $5;
				}
*/
	INSERT INTO insert_target insert_rest
			//opt_on_conflict returning_clause
				{
					$4->relation = $3;
					$4->onConflictClause = NULL;
					$4->returningList = NULL;
					$4->withClause = NULL;

					$$ = (Node *) $4;
				}
		;

/*
 * Can't easily make AS optional here, because VALUES in insert_rest would
 * have a shift/reduce conflict with VALUES as an optional alias.  We could
 * easily allow unreserved_keywords as optional aliases, but that'd be an odd
 * divergence from other places.  So just require AS for now.
 */
insert_target:
			qualified_name
				{
					$$ = $1;
				}
			| qualified_name AS ColId
				{
					$1->alias = makeAlias($3, NIL);
					$$ = $1;
				}
		;

insert_rest:
			SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = $1;
				}
			| OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->override = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->override = $5;
					$$->selectStmt = $7;
				}
			| DEFAULT VALUES
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = NULL;
				}
		;        

override_kind:
			USER		{ $$ = OVERRIDING_USER_VALUE; }
			| SYSTEM_P	{ $$ = OVERRIDING_SYSTEM_VALUE; }
		;

insert_column_list:
			insert_column_item
					{ $$ = list_make1($1); }
			| insert_column_list ',' insert_column_item
					{ $$ = lappend($1, $3); }
		;

insert_column_item:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, scanner);
					$$->val = NULL;
					$$->location = @1;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				UpdateStmt (UPDATE)
 *
 *****************************************************************************/

UpdateStmt: //opt_with_clause
            UPDATE relation_expr_opt_alias
			SET set_clause_list
			from_clause
			where_or_current_clause
			//returning_clause
				{
					UpdateStmt *n = makeNode(UpdateStmt);
					n->relation = $2;
					n->targetList = $4;
					n->fromClause = $5;
					n->whereClause = $6;//$7;
					n->returningList = NULL;//$8;
					n->withClause = NULL;//$1;
					$$ = (Node *)n;
				}
/*
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.  For the same reason we must
 * treat 'JOIN' and 'join_type JOIN' separately, rather than allowing
 * join_type to expand to empty; if we try it, the parser generator can't
 * figure out when to reduce an empty join_type right after table_ref.
 *
 * Note that a CROSS JOIN is the same as an unqualified
 * INNER JOIN, and an INNER JOIN/ON has the same shape
 * but a qualification expression to limit membership.
 * A NATURAL JOIN implicitly matches column names between
 * tables and the shape is determined by which columns are
 * in common. We'll collect columns during the later transformations.
 */

		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			set_target '=' a_expr
				{
					$1->val = (Node *) $3;
					$$ = list_make1($1);
				}
			| '(' set_target_list ')' '=' a_expr
				{
					int ncolumns = list_length($2);
					int i = 1;
					ListCell *col_cell;

					/* Create a MultiAssignRef source for each target */
					foreach(col_cell, $2)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						MultiAssignRef *r = makeNode(MultiAssignRef);

						r->source = (Node *) $5;
						r->colno = i;
						r->ncolumns = ncolumns;
						res_col->val = (Node *) r;
						i++;
					}

					$$ = $2;
				}
		;

set_target:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, scanner);
					$$->val = NULL;	/* upper production sets this */
					$$->location = @1;
				}
		;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				DELETE STATEMENTS
 *
 *****************************************************************************/

DeleteStmt: //opt_with_clause 
            DELETE FROM relation_expr_opt_alias
			using_clause //where_or_current_clause returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $3;
					n->usingClause = $4;
					n->whereClause = NULL;//$5;
					n->returningList = NULL;//$6;
					n->withClause = NULL;//$1;
					$$ = (Node *)n;
				}
		;

using_clause:
				USING from_list						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

CreateGraphStmt:
    CREATE GRAPH IDENTIFIER
        {

            cypher_create_graph *n;
            n = make_ag_node(cypher_create_graph);
            n->graph_name = $3;

            $$ = (Node *)n;
        }
        ;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE TABLESPACE tablespace LOCATION '/path/to/tablespace/'
 *
 *****************************************************************************/

CreateTableSpaceStmt: CREATE TABLESPACE name OptTableSpaceOwner LOCATION Sconst opt_reloptions
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = $4;
					n->location = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

OptTableSpaceOwner: OWNER RoleSpec		{ $$ = $2; }
			| /*EMPTY */				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE EXTENSION extension
 *             [ WITH ] [ SCHEMA schema ] [ VERSION version ]
 *
 *****************************************************************************/

CreateExtensionStmt:
    CREATE EXTENSION IDENTIFIER create_extension_opt_list
        {
            CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
            
            n->extname = $3;
            n->if_not_exists = false;
            n->options = $4;
            
            $$ = (Node *) n;
        }
	| CREATE EXTENSION IF NOT EXISTS IDENTIFIER  create_extension_opt_list
		{
			CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
		
        	n->extname = $6;
			n->if_not_exists = true;
			n->options = $7;
		
        	$$ = (Node *) n;
		}
    ;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *****************************************************************************/
CreateTableStmt:
    CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpec table_access_method_clause OptWith
			//OnCommitOption  OptTableSpace
		{
			CreateStmt *n = makeNode(CreateStmt);
			/*
			 n->oncommit = $12;
			 n->tablespacename = $13;
            */
            $4->relpersistence = $2;
			n->relation = $4;
			n->tableElts = $6;
			n->inhRelations = $8;
			n->partspec = $9;
			n->ofTypename = NULL;
			n->constraints = NIL;
			n->accessMethod = $10;
			n->options = $11;
			n->oncommit = ONCOMMIT_NOOP;
			n->tablespacename = NULL;
			n->if_not_exists = false;

			$$ = (Node *)n;
		}
    ;


/*****************************************************************************
 *
 *		DROP DATABASE [ IF EXISTS ] dbname [ [ WITH ] ( options ) ]
 *
 * This is implicitly CASCADE, no need for drop behavior
 *****************************************************************************/

DropdbStmt: DROP DATABASE name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $3;
					n->missing_ok = false;
					n->options = NULL;
					$$ = (Node *)n;
				}
			| DROP DATABASE IF EXISTS name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $5;
					n->missing_ok = true;
					n->options = NULL;
					$$ = (Node *)n;
				}
			| DROP DATABASE name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $3;
					n->missing_ok = false;
					n->options = $6;
					$$ = (Node *)n;
				}
			| DROP DATABASE IF EXISTS name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $5;
					n->missing_ok = true;
					n->options = $8;
					$$ = (Node *)n;
				}
		;

drop_option_list:
			drop_option
				{
					$$ = list_make1((Node *) $1);
				}
			| drop_option_list ',' drop_option
				{
					$$ = lappend($1, (Node *) $3);
				}
		;

/*
 * Currently only the FORCE option is supported, but the syntax is designed
 * to be extensible so that we can add more options in the future if required.
 */
drop_option:
			FORCE
				{
					$$ = makeDefElem("force", NULL, @1);
				}
		;

call_stmt:
    CALL cypher_expr_func_norm AS cypher_var_name cypher_where_opt
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
    | CALL cypher_expr_func_norm YIELD yield_item_list cypher_where_opt
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
    cypher_a_expr AS cypher_var_name
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = $3;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | cypher_a_expr
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
    cypher_query_start 
    {
        $$ = list_make1($1);
    }
    | cypher_query_start cypher_query_body
    {
        $$ = lcons($1, $2);
    }
    ;
    
cypher_query_start:
    create
    | match
    | CYPHER with { $$ = $2; }
    | merge
    | call_stmt
    | return
    | unwind
;

cypher_query_body:
    clause 
    {
        $$ = list_make1($1);
    }
    | cypher_query_body clause
    {
        $$ = lappend($1, $2);
    }
    | // Empty 
    { 
        $$ = NIL; 
    }
    ;

clause:
    create
    | set
    | remove
    | match
    | with
    | delete
    | merge
    | call_stmt
    | return
    | unwind
    ;


UseGraphStmt:
    USE GRAPH IDENTIFIER
    {
        cypher_use_graph *n = make_ag_node(cypher_use_graph);
        n->graph_name = $3;

        $$ = n;
    };

DropGraphStmt:
    DROP GRAPH IDENTIFIER CASCADE
    {

        cypher_drop_graph *n = make_ag_node(cypher_drop_graph);
        n->graph_name = $3;
        n->cascade = true;

        $$ = n;
    };

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

Iconst: INTEGER | INTEGER_P
;
/*
 * RETURN and WITH clause
 */
having_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | HAVING cypher_a_expr
        {
            $$ = $2;
        }
;

/*
 * This syntax for group_clause tries to follow the spec quite closely.
 * However, the spec allows only column references, not expressions,
 * which introduces an ambiguity between implicit row constructors
 * (a,b) and lists of column references.
 *
 * We handle this by using the a_expr production for what the spec calls
 * <ordinary grouping set>, which in the spec represents either one column
 * reference or a parenthesized list of column references. Then, we check the
 * top node of the a_expr to see if it's an implicit RowExpr, and if so, just
 * grab and use the list, discarding the node. (this is done in parse analysis,
 * not here)
 *
 * (we abuse the row_format field of RowExpr to distinguish implicit and
 * explicit row constructors; it's debatable if anyone sanely wants to use them
 * in a group clause, but if they have a reason to, we make it possible.)
 *
 * Each item in the group_clause list is either an expression tree or a
 * GroupingSet node of some type.
 */
group_clause:
			GROUP BY set_quantifier group_by_list
				{
					GroupClause *n = (GroupClause *) palloc(sizeof(GroupClause));
					n->distinct = $3 == SET_QUANTIFIER_DISTINCT;
					n->list = $4;
					$$ = n;
				}
			| /*EMPTY*/
				{
					GroupClause *n = (GroupClause *) palloc(sizeof(GroupClause));
					n->distinct = false;
					n->list = NIL;
					$$ = n;
				}
		;

set_quantifier:
			ALL										{ $$ = SET_QUANTIFIER_ALL; }
			| DISTINCT								{ $$ = SET_QUANTIFIER_DISTINCT; }
			| /*EMPTY*/								{ $$ = SET_QUANTIFIER_DEFAULT; }
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
group_by_list:
			group_by_item							{ $$ = list_make1($1); }
			| group_by_list ',' group_by_item		{ $$ = lappend($1,$3); }
		;

group_by_item:
			a_expr									{ $$ = $1; }
			| empty_grouping_set					{ $$ = $1; }
			| cube_clause							{ $$ = $1; }
			| rollup_clause							{ $$ = $1; }
			| grouping_sets_clause					{ $$ = $1; }
		;
grouping_sets_clause:
			GROUPING SETS '(' group_by_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_SETS, $4, @1);
				}
		;
group_item_list:
    group_item { $$ = list_make1($1); }
        | group_item_list ',' group_item { $$ = lappend($1, $3); }
;

group_item:
    cypher_a_expr { $$ = $1; }
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

having_clause:
			HAVING a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			for_locking_strength locked_rels_list opt_nowait_or_skip
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $2;
					n->strength = $1;
					n->waitPolicy = $3;
					$$ = (Node *) n;
				}
		;

for_locking_strength:
			FOR UPDATE							{ $$ = LCS_FORUPDATE; }
			| FOR NO KEY UPDATE					{ $$ = LCS_FORNOKEYUPDATE; }
			| FOR SHARE							{ $$ = LCS_FORSHARE; }
			| FOR KEY SHARE						{ $$ = LCS_FORKEYSHARE; }
		;

locked_rels_list:
			OF qualified_name_list					{ $$ = $2; }
			| /* EMPTY */							{ $$ = NIL; }
		;



opt_nowait_or_skip:
			NOWAIT							{ $$ = LockWaitError; }
			| SKIP LOCKED					{ $$ = LockWaitSkip; }
			| /*EMPTY*/						{ $$ = LockWaitBlock; }
		;


/*****************************************************************************
 *
 *		CREATE CAST / DROP CAST
 *
 *****************************************************************************/

CreateCastStmt: CREATE CAST '(' Typename AS Typename ')'
					WITH FUNCTION function_with_argtypes cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = $10;
					n->context = (CoercionContext) $11;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITHOUT FUNCTION cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITH INOUT cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = true;
					$$ = (Node *)n;
				}
		;

cast_context:  AS IMPLICIT_P					{ $$ = COERCION_IMPLICIT; }
		| AS ASSIGNMENT							{ $$ = COERCION_ASSIGNMENT; }
		| /*EMPTY*/								{ $$ = COERCION_EXPLICIT; }
		;



/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/

CreateSchemaStmt:
			CREATE SCHEMA OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* One can omit the schema name or the authorization id. */
					n->schemaname = $3;
					n->authrole = $5;
					n->schemaElts = $6;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not both */
					n->schemaname = $3;
					n->authrole = NULL;
					n->schemaElts = $4;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA IF NOT EXISTS OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* schema name can be omitted here, too */
					n->schemaname = $6;
					n->authrole = $8;
					if ($9 != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements")));
					n->schemaElts = $9;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA IF NOT EXISTS ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not here */
					n->schemaname = $6;
					n->authrole = NULL;
					if ($7 != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements")));
					n->schemaElts = $7;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;

OptSchemaName:
			ColId									{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSchemaEltList:
			OptSchemaEltList schema_stmt
				{
					if (@$ < 0)			
						@$ = @2;
					$$ = lappend($1, $2);
				}
			| /* EMPTY */ 
				{ $$ = NIL; }
		;

/*
 *	schema_stmt are the ones that can show up inside a CREATE SCHEMA
 *	statement (in addition to by themselves).
 */
schema_stmt:
			CreateTableStmt
			/*| IndexStmt
			| CreateSeqStmt
			| CreateTrigStmt
			| GrantStmt
			| ViewStmt*/
		;

/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

set_rest:/*
			TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| */set_rest_more
			;

generic_set:
			var_name TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
		;

/* SetResetClause allows SET or RESET without LOCAL */
SetResetClause:
			SET set_rest					{ $$ = $2; }
			| VariableResetStmt				{ $$ = (VariableSetStmt *) $1; }
		;


set_rest_more:	/* Generic SET syntaxes: */
			generic_set							{$$ = $1;}
			/*| var_name FROM CURRENT_P
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			// Special syntaxes mandated by SQL standard:
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| CATALOG_P Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("current database cannot be changed"),
							 parser_errposition(@2)));
					$$ = NULL; 
				}
			| SCHEMA Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, @2));
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| ROLE NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "role";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| SESSION AUTHORIZATION NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "session_authorization";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "session_authorization";
					$$ = n;
				}
			| XML_P OPTION document_or_content
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "xmloption";
					n->args = list_make1(makeStringConst($3 == XMLOPTION_DOCUMENT ? "DOCUMENT" : "CONTENT", @3));
					$$ = n;
				}
			// Special syntaxes invented by PostgreSQL:
			| TRANSACTION SNAPSHOT Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION SNAPSHOT";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}*/
		;


var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			//| ON									{ $$ = "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| NonReservedWord_or_Sconst				{ $$ = $1; }
		;
/*
 * We should allow ROW '(' expr_list ')' too, but that seems to require
 * making VALUES a fully reserved word, which will probably break more apps
 * than allowing the noise-word is worth.
 */
values_clause:
			VALUES '(' expr_list ')'
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->valuesLists = list_make1($3);
					$$ = (Node *) n;
				}
			| values_clause ',' '(' expr_list ')'
				{
					SelectStmt *n = (SelectStmt *) $1;
					n->valuesLists = lappend(n->valuesLists, $4);
					$$ = (Node *) n;
				}
		;

expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
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
    cypher_a_expr AS cypher_var_name
        {
            ResTarget *n;

            n = makeNode(ResTarget);
            n->name = $3;
            n->indirection = NIL;
            n->val = $1;
            n->location = @1;

            $$ = (Node *)n;
        }
    | cypher_a_expr
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

opt_asc_desc: ASC							{ $$ = SORTBY_ASC; }
	| DESC							{ $$ = SORTBY_DESC; }
	| /*EMPTY*/						{ $$ = SORTBY_DEFAULT; }
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
    cypher_a_expr USING all_op opt_nulls_order
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
    | cypher_a_expr order_opt opt_nulls_order
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
    | SKIP cypher_a_expr
        {
            $$ = $2;
        }
    ;

limit_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | LIMIT cypher_a_expr
        {
            $$ = $2;
        }
    ;

with:
    WITH DISTINCT return_item_list cypher_where_opt group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
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
    | WITH return_item_list cypher_where_opt group_by_opt having_opt window_clause order_by_opt skip_opt limit_opt
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
    optional_opt MATCH pattern cypher_where_opt order_by_opt
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
    UNWIND cypher_a_expr AS cypher_var_name
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

table_access_method_clause:
			USING name							{ $$ = $2; }
			| /*EMPTY*/					{ $$ = NULL; }

/* WITHOUT OIDS is legacy only */
OptWith:
			WITH reloptions				{ $$ = $2; }
			/*| WITHOUT OIDS				{ $$ = NIL; }*/
			| /*EMPTY*/					{ $$ = NIL; }
		;

OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


ExistingIndex:   USING INDEX name					{ $$ = $3; }
		;		

reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;

opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;
			
reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;

/* This should match def_elem and also allow qualified names */
reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
			| ColLabel '.' ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (Node *) $5,
											 DEFELEM_UNSPEC, @1);
				}
			| ColLabel '.' ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, DEFELEM_UNSPEC, @1);
				}
		;  


OptParenthesizedSeqOptList: '(' SeqOptList ')'		{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: AS SimpleTypename
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2, @1);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(true), @1);
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(false), @1);
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3, @1);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2, @1);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2, @1);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3, @1);
				}
			| SEQUENCE NAME_P any_name
				{
					/* not documented, only used by pg_dump */
					$$ = makeDefElem("sequence_name", (Node *)$3, @1);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3, @1);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3, @1);
				}
		;

opt_by:		BY
			| /* EMPTY */
	  ;

NumericOnly:
			DECIMAL								{ $$ = makeFloat($1); }
			| '+' DECIMAL						{ $$ = makeFloat($2); }
			| '-' DECIMAL
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;

NumericOnly_list:	NumericOnly						{ $$ = list_make1($1); }
				| NumericOnly_list ',' NumericOnly	{ $$ = lappend($1, $3); }
		;

 SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;
Iconst:		INTEGER									{ $$ = $1; }
 | INTEGER_P
;




/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| all_op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
			//| NONE							{ $$ = (Node *)makeString(pstrdup($1)); }
			// XXX Temporary
			| TRUE_P	{ $$ = (Node *)makeBoolAConst(true, @1); }
			| FALSE_P	{ $$ = (Node *)makeBoolAConst(false, @1); }
		;

OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
        

/* Optional partition key specification */
OptPartitionSpec: PartitionSpec	{ $$ = $1; }
			| /*EMPTY*/			{ $$ = NULL; }
		;

PartitionSpec: PARTITION BY ColId '(' part_params ')'
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->partParams = $5;
					n->location = @1;

					$$ = n;
				}
		;

part_params:	part_elem						{ $$ = list_make1($1); }
			| part_params ',' part_elem			{ $$ = lappend($1, $3); }
		;

 opt_class:	any_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
        
part_elem: ColId opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = $1;
					n->expr = NULL;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;

					$$ = n;
				}
			| func_expr_windowless opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $1;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;
					$$ = n;
				}
			| '(' a_expr ')' opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $2;
					n->collation = $4;
					n->opclass = $5;
					n->location = @1;
					$$ = n;
				}
		;       


/*****************************************************************************
 *
 *		QUERY:
 *				create [or replace] function <fname>
 *						[(<type-1> { , <type-n>})]
 *						returns <type-r>
 *						as <filename or code in language as appropriate>
 *						language <lang> [with parameters]
 *
 *****************************************************************************/

CreateFunctionStmt:
			CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			RETURNS func_return opt_createfunc_opt_list opt_routine_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->is_procedure = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $7;
					n->options = $8;
					n->sql_body = $9;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			  RETURNS TABLE '(' table_func_column_list ')' opt_createfunc_opt_list opt_routine_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->is_procedure = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = mergeTableFuncParameters($5, $9);
					n->returnType = TableFuncTypeName($9);
					n->returnType->location = @7;
					n->options = $11;
					n->sql_body = $12;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			  opt_createfunc_opt_list opt_routine_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->is_procedure = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->options = $6;
					n->sql_body = $7;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace PROCEDURE func_name func_args_with_defaults
			  opt_createfunc_opt_list opt_routine_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->is_procedure = true;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->options = $6;
					n->sql_body = $7;
					$$ = (Node *)n;
				}
		;

opt_or_replace:
			OR REPLACE								{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

func_args:	'(' func_args_list ')'					{ $$ = $2; }
			| '(' ')'								{ $$ = NIL; }
		;

func_args_list:
			func_arg								{ $$ = list_make1($1); }
			| func_args_list ',' func_arg			{ $$ = lappend($1, $3); }
		;

function_with_argtypes_list:
			function_with_argtypes					{ $$ = list_make1($1); }
			| function_with_argtypes_list ',' function_with_argtypes
													{ $$ = lappend($1, $3); }
		;

function_with_argtypes:
			func_name func_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractArgTypes($2);
					n->objfuncargs = $2;
					$$ = n;
				}
			/*
			 * Because of reduce/reduce conflicts, we can't use func_name
			 * below, but we can write it out the long way, which actually
			 * allows more cases.
			 *//*
			| type_func_name_keyword
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString(pstrdup($1)));
					n->args_unspecified = true;
					$$ = n;
				}*/
			| ColId
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString($1));
					n->args_unspecified = true;
					$$ = n;
				}
			| ColId indirection
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = check_func_name(lcons(makeString($1), $2),
												  scanner);
					n->args_unspecified = true;
					$$ = n;
				}
		;

/*
 * func_args_with_defaults is separate because we only want to accept
 * defaults in CREATE FUNCTION, not in ALTER etc.
 */
func_args_with_defaults:
		'(' func_args_with_defaults_list ')'		{ $$ = $2; }
		| '(' ')'									{ $$ = NIL; }
		;

func_args_with_defaults_list:
		func_arg_with_default						{ $$ = list_make1($1); }
		| func_args_with_defaults_list ',' func_arg_with_default
													{ $$ = lappend($1, $3); }
		;

/*
 * The style with arg_class first is SQL99 standard, but Oracle puts
 * param_name first; accept both since it's likely people will try both
 * anyway.  Don't bother trying to save productions by letting arg_class
 * have an empty alternative ... you'll get shift/reduce conflicts.
 *
 * We can catch over-specified arguments here if we want to,
 * but for now better to silently swallow typmod, etc.
 * - thomas 2000-03-22
 */
func_arg:
			arg_class param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $2;
					n->argType = $3;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $3;
					n->mode = $2;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_DEFAULT;
					n->defexpr = NULL;
					$$ = n;
				}
			| arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $2;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $1;
					n->mode = FUNC_PARAM_DEFAULT;
					n->defexpr = NULL;
					$$ = n;
				}
		;

/* INOUT is SQL99 standard, IN OUT is for Oracle compatibility */
arg_class:	IN								{ $$ = FUNC_PARAM_IN; }
			| OUT_P								{ $$ = FUNC_PARAM_OUT; }
			| INOUT								{ $$ = FUNC_PARAM_INOUT; }
			| IN OUT_P						{ $$ = FUNC_PARAM_INOUT; }
			| VARIADIC							{ $$ = FUNC_PARAM_VARIADIC; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	type_function_name
		;

func_return:
			func_type
				{
					/* We can catch over-specified results here if we want to,
					 * but for now better to silently swallow typmod, etc.
					 * - thomas 2000-03-22
					 */
					$$ = $1;
				}
		;


/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_function_name
 * is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			/*| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->setof = true;
					$$->location = @2;
				}*/
		;

func_arg_with_default:
		func_arg
				{
					$$ = $1;
				}
		| func_arg DEFAULT a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg '=' a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		;


opt_createfunc_opt_list:
			createfunc_opt_list
			| /*EMPTY*/ { $$ = NIL; }
	;

createfunc_opt_list:
			/* Must be at least one to prevent conflict */
			createfunc_opt_item						{ $$ = list_make1($1); }
			| createfunc_opt_list createfunc_opt_item { $$ = lappend($1, $2); }
	;

/*
 * Options common to both CREATE FUNCTION and ALTER FUNCTION
 */
common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(false), @1);
				}
			| RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(true), @1);
				}
			| STRICT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(true), @1);
				}
			| IMMUTABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("immutable"), @1);
				}
			| STABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("stable"), @1);
				}
			| VOLATILE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("volatile"), @1);
				}
			| EXTERNAL SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(true), @1);
				}
			| EXTERNAL SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(false), @1);
				}
			| SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(true), @1);
				}
			| SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(false), @1);
				}
			| LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(true), @1);
				}
			| NOT LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(false), @1);
				}
			| COST NumericOnly
				{
					$$ = makeDefElem("cost", (Node *)$2, @1);
				}
			| ROWS NumericOnly
				{
					$$ = makeDefElem("rows", (Node *)$2, @1);
				}
			| SUPPORT any_name
				{
					$$ = makeDefElem("support", (Node *)$2, @1);
				}
			/*| FunctionSetResetClause
				{
					// we abuse the normal content of a DefElem here 
					$$ = makeDefElem("set", (Node *)$1, @1);
				}*/
			| PARALLEL ColId
				{
					$$ = makeDefElem("parallel", (Node *)makeString($2), @1);
				}
		;

createfunc_opt_item:
			AS func_as
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| LANGUAGE NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2), @1);
				}
			| TRANSFORM transform_type_list
				{
					$$ = makeDefElem("transform", (Node *)$2, @1);
				}
			| WINDOW
				{
					$$ = makeDefElem("window", (Node *)makeInteger(true), @1);
				}
			| common_func_opt_item
				{
					$$ = $1;
				}
		;

func_as:	Sconst						{ $$ = list_make1(makeString($1)); }
			| Sconst ',' Sconst
				{
					$$ = list_make2(makeString($1), makeString($3));
				}
		;

ReturnStmt:	RETURN a_expr
				{
					ReturnStmt *r = makeNode(ReturnStmt);
					r->returnval = (Node *) $2;
					$$ = (Node *) r;
				}
		;

opt_routine_body:
			ReturnStmt
				{
					$$ = $1;
				}
			| BEGIN_P ATOMIC routine_body_stmt_list END_P
				{
					/*
					 * A compound statement is stored as a single-item list
					 * containing the list of statements as its member.  That
					 * way, the parse analysis code can tell apart an empty
					 * body from no body at all.
					 */
					$$ = (Node *) list_make1($3);
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;

routine_body_stmt_list:
			routine_body_stmt_list routine_body_stmt ';'
				{
					/* As in stmtmulti, discard empty statements */
					if ($2 != NULL)
						$$ = lappend($1, $2);
					else
						$$ = $1;
				}
			| /*EMPTY*/
				{
					$$ = NIL;
				}
		;

routine_body_stmt:
			stmt
			| ReturnStmt
		;


transform_type_list:
			FOR TYPE_P Typename { $$ = list_make1($3); }
			| transform_type_list ',' FOR TYPE_P Typename { $$ = lappend($1, $5); }
		;

def_elem:	ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;

table_access_method_clause:
			USING name							{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NULL; }
		;

opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;



/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...]
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

DropStmt:	DROP object_type_any_name IF EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP object_type_any_name any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name IF EXISTS name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP object_type_name_on_any_name name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP object_type_name_on_any_name IF EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TYPE_P type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TYPE;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TYPE_P IF EXISTS type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TYPE;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP DOMAIN_P type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DOMAIN;
					n->missing_ok = false;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP DOMAIN_P IF EXISTS type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DOMAIN;
					n->missing_ok = true;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = false;
					n->objects = $4;
					n->behavior = $5;
					n->concurrent = true;
					$$ = (Node *)n;
				}
			| DROP INDEX CONCURRENTLY IF EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = true;
					n->objects = $6;
					n->behavior = $7;
					n->concurrent = true;
					$$ = (Node *)n;
				}
		;

/* object types taking any_name/any_name_list */
object_type_any_name:
			TABLE									{ $$ = OBJECT_TABLE; }
			/*| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| COLLATION								{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| STATISTICS							{ $$ = OBJECT_STATISTIC_EXT; }
			| TEXT_P SEARCH PARSER					{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = OBJECT_TSCONFIGURATION; }*/
		;

/*
 * object types taking name/name_list
 *
 * DROP handles some of them separately
 */

object_type_name:
			drop_type_name							{ $$ = $1; }
			| DATABASE								{ $$ = OBJECT_DATABASE; }
			| ROLE									{ $$ = OBJECT_ROLE; }
			| SUBSCRIPTION							{ $$ = OBJECT_SUBSCRIPTION; }
			| TABLESPACE							{ $$ = OBJECT_TABLESPACE; }
		;

drop_type_name:
			ACCESS METHOD							{ $$ = OBJECT_ACCESS_METHOD; }
			| EVENT TRIGGER							{ $$ = OBJECT_EVENT_TRIGGER; }
			| EXTENSION								{ $$ = OBJECT_EXTENSION; }
			| FOREIGN DATA_P WRAPPER				{ $$ = OBJECT_FDW; }
			| opt_procedural LANGUAGE				{ $$ = OBJECT_LANGUAGE; }
			| PUBLICATION							{ $$ = OBJECT_PUBLICATION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| SERVER								{ $$ = OBJECT_FOREIGN_SERVER; }
		;

/* object types attached to a table */
object_type_name_on_any_name:
			POLICY									{ $$ = OBJECT_POLICY; }
			| RULE									{ $$ = OBJECT_RULE; }
			| TRIGGER								{ $$ = OBJECT_TRIGGER; }
		;


any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;

type_name_list:
			Typename								{ $$ = list_make1($1); }
			| type_name_list ',' Typename			{ $$ = lappend($1, $3); }
		;

opt_procedural:
			PROCEDURAL
			| /*EMPTY*/
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
OptTemp:	TEMPORARY					{ $$ = RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMP				{ $$ = RELPERSISTENCE_TEMP; }
			| GLOBAL TEMPORARY
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation")));
					$$ = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation")));
					$$ = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;
        
OptTableElementList:
			TableElementList					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;

TableElement:
			columnDef							{ $$ = $1; }
			| TableLikeClause					{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;



columnDef:	ColId Typename opt_column_compression create_generic_options ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
                    n->compression = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = NULL;
					n->location = @1;
					/*n->compression = $3;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $4;
					SplitColQualList($5, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;*/
					$$ = (Node *)n;
				}
		;


columnOptions:	ColId ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($2, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
				| ColId WITH OPTIONS ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($4, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
		;

column_compression:
			COMPRESSION ColId						{ $$ = $2; }
			| COMPRESSION DEFAULT					{ $$ = pstrdup("default"); }
		;

opt_column_compression:
			column_compression						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = castNode(Constraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
		;

/* DEFAULT NULL is already the default for Postgres.
 * But define it here and carry it forward into the system
 * to make it explicit.
 * - thomas 1998-09-13
 *
 * WITH NULL and NULL are not SQL-standard syntax elements,
 * so leave them out. Use DEFAULT NULL to explicitly indicate
 * that a column may have that value. WITH NULL leads to
 * shift/reduce conflicts with WITH TIME ZONE anyway.
 * - thomas 1999-01-08
 *
 * DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
 * conflict on NOT (since NOT might start a subsequent NOT NULL constraint,
 * or be part of a_expr NOT LIKE or similar constructs).
 */
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->skip_validation = false;
					n->initially_valid = true;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_IDENTITY;
					n->generated_when = $2;
					n->options = $5;
					n->location = @1;
					$$ = (Node *)n;
				}
			| GENERATED generated_when AS '(' a_expr ')' STORED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_GENERATED;
					n->generated_when = $2;
					n->raw_expr = $5;
					n->cooked_expr = NULL;
					n->location = @1;

					/*
					 * Can't do this in the grammar because of shift/reduce
					 * conflicts.  (IDENTITY allows both ALWAYS and BY
					 * DEFAULT, but generated columns only allow ALWAYS.)  We
					 * can also give a more useful error message and location.
					 */
					if ($2 != ATTRIBUTE_IDENTITY_ALWAYS)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("for a generated column, GENERATED ALWAYS must be specified")));

					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (Node *)n;
				}
		;

generated_when:
			ALWAYS			{ $$ = ATTRIBUTE_IDENTITY_ALWAYS; }
			| BY DEFAULT	{ $$ = ATTRIBUTE_IDENTITY_BY_DEFAULT; }
		;

/*
 * ConstraintAttr represents constraint attributes, which we parse as if
 * they were independent constraint clauses, in order to avoid shift/reduce
 * conflicts (since NOT might start either an independent NOT NULL clause
 * or an attribute).  parse_utilcmd.c is responsible for attaching the
 * attribute information to the preceding "real" constraint node, and for
 * complaining if attribute clauses appear in the wrong place or wrong
 * combinations.
 *
 * See also ConstraintAttributeSpec, which can be used in places where
 * there is no parsing conflict.  (Note: currently, NOT VALID and NO INHERIT
 * are allowed clauses in ConstraintAttributeSpec, but not here.  Someday we
 * might need to allow them here too, but for the moment it doesn't seem
 * useful in the statements that use ConstraintAttr.)
 */
ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					Constraint *n = castNode(Constraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ConstraintElem						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, scanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->including = $5;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					processCASbits($8, @8, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, scanner);
					$$ = (Node *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->including = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, scanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->including = $6;
					n->options = $7;
					n->indexname = NULL;
					n->indexspace = $8;
					processCASbits($9, @9, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, scanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->including = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, scanner);
					$$ = (Node *)n;
				}
			| EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
				opt_c_include opt_definition OptConsTableSpace OptWhereClause
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_EXCLUSION;
					n->location = @1;
					n->access_method	= $2;
					n->exclusions		= $4;
					n->including		= $6;
					n->options			= $7;
					n->indexname		= NULL;
					n->indexspace		= $8;
					n->where_clause		= $9;
					processCASbits($10, @10, "EXCLUDE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, scanner);
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   scanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
		;

opt_no_inherit:	NO INHERIT							{  $$ = true; }
			| /* EMPTY */							{  $$ = false; }
		;

opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;

opt_c_include:	INCLUDE '(' columnList ')'			{ $$ = $3; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented")));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		;


ExclusionConstraintList:
			ExclusionConstraintElem					{ $$ = list_make1($1); }
			| ExclusionConstraintList ',' ExclusionConstraintElem
													{ $$ = lappend($1, $3); }
		;

ExclusionConstraintElem: index_elem WITH any_operator
			{
				$$ = list_make2($1, $3);
			}
			/* allow OPERATOR() decoration for the benefit of ruleutils.c */
			/*| index_elem WITH OPERATOR '(' any_operator ')'
			{
				$$ = list_make2($1, $5);
			}*/
		;

OptWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

key_update: ON UPDATE key_action		{ $$ = $3; }
		;

key_delete: ON DELETE key_action		{ $$ = $3; }
		;

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }

ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE")));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties")));
					$$ = newspec;
				}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;


TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = $3;
					n->relationOid = InvalidOid;
					$$ = (Node *)n;
				}
		;

TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = 0; }
		;

TableLikeOption:
				COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| COMPRESSION		{ $$ = CREATE_TABLE_LIKE_COMPRESSION; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS; }
				| IDENTITY_P		{ $$ = CREATE_TABLE_LIKE_IDENTITY; }
				| GENERATED			{ $$ = CREATE_TABLE_LIKE_GENERATED; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STATISTICS		{ $$ = CREATE_TABLE_LIKE_STATISTICS; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| ALL				{ $$ = CREATE_TABLE_LIKE_ALL; }
		;

access_method_clause:
			USING name								{ $$ = $2; }
			| /*EMPTY*/								{ $$ = DEFAULT_INDEX_TYPE; }
		;

index_elem_options:
	opt_collate opt_class opt_asc_desc opt_nulls_order
		{
			$$ = makeNode(IndexElem);
			$$->name = NULL;
			$$->expr = NULL;
			$$->indexcolname = NULL;
			$$->collation = $1;
			$$->opclass = $2;
			$$->opclassopts = NIL;
			$$->ordering = $3;
			$$->nulls_ordering = $4;
		}
	| opt_collate any_name reloptions opt_asc_desc opt_nulls_order
		{
			$$ = makeNode(IndexElem);
			$$->name = NULL;
			$$->expr = NULL;
			$$->indexcolname = NULL;
			$$->collation = $1;
			$$->opclass = $2;
			$$->opclassopts = $3;
			$$->ordering = $4;
			$$->nulls_ordering = $5;
		}
	;

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem: ColId index_elem_options
				{
					$$ = $2;
					$$->name = $1;
				}
			| func_expr_windowless index_elem_options
				{
					$$ = $2;
					$$->expr = $1;
				}
			| '(' a_expr ')' index_elem_options
				{
					$$ = $4;
					$$->expr = $2;
				}
		;

any_operator:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| ColId '.' any_operator
					{ $$ = lcons(makeString($1), $3); }
		;


opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

table_func_column:	param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_TABLE;
					n->defexpr = NULL;
					$$ = n;
				}
		;

table_func_column_list:
			table_func_column
				{
					$$ = list_make1($1);
				}
			| table_func_column_list ',' table_func_column
				{
					$$ = lappend($1, $3);
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				define (aggregate,operator,type)
 *
 *****************************************************************************/

DefineStmt:
			/*CREATE opt_or_replace AGGREGATE func_name aggr_args definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = false;
					n->replace = $2;
					n->defnames = $4;
					n->args = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace AGGREGATE func_name old_aggr_definition
				{
					// old-style (pre-8.2) syntax for CREATE AGGREGATE 
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = true;
					n->replace = $2;
					n->defnames = $4;
					n->args = NIL;
					n->definition = $5;
					$$ = (Node *)n;
				}
			| CREATE OPERATOR any_operator definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_OPERATOR;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| */CREATE TYPE_P any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name
				{
					// Shell type (identified by lack of definition) 
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = NIL;
					$$ = (Node *)n;
				}
			/*| CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);

					// can't use qualified_name, sigh 
					n->typevar = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->coldeflist = $6;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
				{
					CreateEnumStmt *n = makeNode(CreateEnumStmt);
					n->typeName = $3;
					n->vals = $7;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS RANGE definition
				{
					CreateRangeStmt *n = makeNode(CreateRangeStmt);
					n->typeName = $3;
					n->params	= $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH PARSER any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSPARSER;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH DICTIONARY any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSDICTIONARY;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH TEMPLATE any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSTEMPLATE;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH CONFIGURATION any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSCONFIGURATION;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE COLLATION any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE COLLATION IF_P NOT EXISTS any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $6;
					n->definition = $7;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			| CREATE COLLATION any_name FROM any_name
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = list_make1(makeDefElem("from", (Node *) $5, @5));
					$$ = (Node *)n;
				}
			| CREATE COLLATION IF_P NOT EXISTS any_name FROM any_name
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $6;
					n->definition = list_make1(makeDefElem("from", (Node *) $8, @8));
					n->if_not_exists = true;
					$$ = (Node *)n;
				}*/
		;
		
definition: '(' def_list ')'						{ $$ = $2; }
		;

def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:	ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;



/* Role specifications */
RoleId:		RoleSpec
				{
					RoleSpec *spc = (RoleSpec *) $1;
					switch (spc->roletype)
					{
						case ROLESPEC_CSTRING:
							$$ = spc->rolename;
							break;
						case ROLESPEC_PUBLIC:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("role name \"%s\" is reserved",
											"public")));
							break;
						case ROLESPEC_SESSION_USER:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"SESSION_USER")));
							break;
						case ROLESPEC_CURRENT_USER:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"CURRENT_USER")));
							break;
						case ROLESPEC_CURRENT_ROLE:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"CURRENT_ROLE")));
							break;
					}
				}
			;

RoleSpec:	NonReservedWord
					{
						/*
						 * "public" and "none" are not keywords, but they must
						 * be treated specially here.
						 */
						RoleSpec *n;
						if (strcmp($1, "public") == 0)
						{
							n = (RoleSpec *) makeRoleSpec(ROLESPEC_PUBLIC, @1);
							n->roletype = ROLESPEC_PUBLIC;
						}
						else if (strcmp($1, "none") == 0)
						{
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("role name \"%s\" is reserved",
											"none")));
						}
						else
						{
							n = makeRoleSpec(ROLESPEC_CSTRING, @1);
							n->rolename = pstrdup($1);
						}
						$$ = n;
					}
			| CURRENT_ROLE
					{
						$$ = makeRoleSpec(ROLESPEC_CURRENT_ROLE, @1);
					}
			| CURRENT_USER
					{
						$$ = makeRoleSpec(ROLESPEC_CURRENT_USER, @1);
					}
			| SESSION_USER
					{
						$$ = makeRoleSpec(ROLESPEC_SESSION_USER, @1);
					}
		;

role_list:	RoleSpec
					{ $$ = list_make1($1); }
			| role_list ',' RoleSpec
					{ $$ = lappend($1, $3); }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS user (role with implied login ability)
 *
 *****************************************************************************/

CreateUserStmt:
			CREATE USER RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_USER;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

opt_with:	WITH
			| /*EMPTY*/
		;


/*
 * Options for CREATE ROLE and ALTER ROLE (also used by CREATE/ALTER USER
 * for backwards compatibility).  Note: the only option required by SQL99
 * is "WITH ADMIN name".
 */
OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;



AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (Node *)makeString($2), @1);
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL, @1);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					/*
					 * These days, passwords are always stored in encrypted
					 * form, so there is no difference between PASSWORD and
					 * ENCRYPTED PASSWORD.
					 */
					$$ = makeDefElem("password",
									 (Node *)makeString($3), @1);
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNENCRYPTED PASSWORD is no longer supported"),
							 errhint("Remove UNENCRYPTED to store the password in encrypted form instead.")));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(true), @1);
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3), @1);
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3), @1);
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
			| IDENTIFIER
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "superuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nosuperuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "createrole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nocreaterole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "replication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "noreplication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "createdb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nocreatedb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "login") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nologin") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "bypassrls") == 0)
						$$ = makeDefElem("bypassrls", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nobypassrls") == 0)
						$$ = makeDefElem("bypassrls", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "noinherit") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (Node *)makeInteger(false), @1);
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1)));
				}
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2), @1);
				}
			| ADMIN role_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2, @1);
				}
			| ROLE role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
			| IN ROLE role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
			| IN GROUP role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
		;

/* Options definition for CREATE FDW, SERVER and USER MAPPING */
create_generic_options:
			OPTIONS '(' generic_option_list ')'			{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NIL; }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = list_make1($1);
				}
			| generic_option_list ',' generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;

generic_option_name:
				ColLabel			{ $$ = $1; }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;



/*****************************************************************************
 *
 *		CREATE DATABASE
 *
 *****************************************************************************/

CreatedbStmt:
			CREATE DATABASE name opt_with createdb_opt_list
				{
					CreatedbStmt *n = makeNode(CreatedbStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

createdb_opt_list:
			createdb_opt_items						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

createdb_opt_items:
			createdb_opt_item						{ $$ = list_make1($1); }
			| createdb_opt_items createdb_opt_item	{ $$ = lappend($1, $2); }
		;

createdb_opt_item:
			createdb_opt_name opt_equal SignedIconst
				{
					$$ = makeDefElem($1, (Node *)makeInteger($3), @1);
				}
			| createdb_opt_name opt_equal opt_boolean_or_string
				{
					$$ = makeDefElem($1, (Node *)makeString($3), @1);
				}
			| createdb_opt_name opt_equal DEFAULT
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;

/*
 * Ideally we'd use ColId here, but that causes shift/reduce conflicts against
 * the ALTER DATABASE SET/RESET syntaxes.  Instead call out specific keywords
 * we need, and allow IDENT so that database option names don't have to be
 * parser keywords unless they are already keywords for other reasons.
 *
 * XXX this coding technique is fragile since if someone makes a formerly
 * non-keyword option name into a keyword and forgets to add it here, the
 * option will silently break.  Best defense is to provide a regression test
 * exercising every such option, at least at the syntax level.
 */
createdb_opt_name:
			IDENTIFIER							{ $$ = $1; }
			| CONNECTION LIMIT				{ $$ = pstrdup("connection_limit"); }
			| ENCODING						{ $$ = pstrdup($1); }
			| LOCATION						{ $$ = pstrdup($1); }
			| OWNER							{ $$ = pstrdup($1); }
			| TABLESPACE					{ $$ = pstrdup($1); }
			| TEMPLATE						{ $$ = pstrdup($1); }
		;

/*
 *	Though the equals sign doesn't match other WITH options, pg_dump uses
 *	equals for backward compatibility, and it doesn't seem worth removing it.
 */
opt_equal:	'='
			| /*EMPTY*/
		;


/*****************************************************************************
 *
 *		ALTER DATABASE
 *
 *****************************************************************************/

AlterDatabaseStmt:
			ALTER DATABASE name WITH createdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				 }
			| ALTER DATABASE name createdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $4;
					$$ = (Node *)n;
				 }
			| ALTER DATABASE name SET TABLESPACE name
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = list_make1(makeDefElem("tablespace",
														(Node *)makeString($6), @6));
					$$ = (Node *)n;
				 }
		;

AlterDatabaseSetStmt:
			ALTER DATABASE name SetResetClause
				{
					AlterDatabaseSetStmt *n = makeNode(AlterDatabaseSetStmt);
					n->dbname = $3;
					n->setstmt = $4;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * GRANT and REVOKE statements
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option opt_granted_by
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grant_option = $7;
					n->grantor = $8;
					$$ = (Node*)n;
				}
		;


opt_granted_by: GRANTED BY RoleSpec						{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NULL; }
		;

/*
 * Privilege names are represented as strings; the validity of the privilege
 * names gets checked at execution.  This is a bit annoying but we have little
 * choice because of the syntactic conflict with lists of role names in
 * GRANT/REVOKE.  What's more, we have to call out in the "privilege"
 * production any reserved keywords that need to be usable as privilege names.
 */

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1; }
			| ALL
				{ $$ = NIL; }
			| ALL PRIVILEGES
				{ $$ = NIL; }
			| ALL '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $3;
					$$ = list_make1(n);
				}
			| ALL PRIVILEGES '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $4;
					$$ = list_make1(n);
				}
		;

privilege_list:	privilege							{ $$ = list_make1($1); }
			| privilege_list ',' privilege			{ $$ = lappend($1, $3); }
		;

privilege:	SELECT opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| REFERENCES opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| CREATE opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| ColId opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = $1;
				n->cols = $2;
				$$ = n;
			}
		;


/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_TABLE;
					n->objs = $1;
					$$ = n;
				}
			| TABLE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_TABLE;
					n->objs = $2;
					$$ = n;
				}
			| SEQUENCE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_SEQUENCE;
					n->objs = $2;
					$$ = n;
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_FDW;
					n->objs = $4;
					$$ = n;
				}
			| FOREIGN SERVER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_FOREIGN_SERVER;
					n->objs = $3;
					$$ = n;
				}
			| FUNCTION function_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_FUNCTION;
					n->objs = $2;
					$$ = n;
				}
			| PROCEDURE function_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_PROCEDURE;
					n->objs = $2;
					$$ = n;
				}
			| ROUTINE function_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_ROUTINE;
					n->objs = $2;
					$$ = n;
				}
			| DATABASE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_DATABASE;
					n->objs = $2;
					$$ = n;
				}
			| DOMAIN_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_DOMAIN;
					n->objs = $2;
					$$ = n;
				}
			| LANGUAGE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_LANGUAGE;
					n->objs = $2;
					$$ = n;
				}
			| LARGE_P OBJECT_P NumericOnly_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_LARGEOBJECT;
					n->objs = $3;
					$$ = n;
				}
			| SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_SCHEMA;
					n->objs = $2;
					$$ = n;
				}
			| TABLESPACE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_TABLESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TYPE_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = OBJECT_TYPE;
					n->objs = $2;
					$$ = n;
				}
			| ALL TABLES IN SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = OBJECT_TABLE;
					n->objs = $5;
					$$ = n;
				}
			| ALL SEQUENCES IN SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = OBJECT_SEQUENCE;
					n->objs = $5;
					$$ = n;
				}
			| ALL FUNCTIONS IN SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = OBJECT_FUNCTION;
					n->objs = $5;
					$$ = n;
				}
			| ALL PROCEDURES IN SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = OBJECT_PROCEDURE;
					n->objs = $5;
					$$ = n;
				}
			| ALL ROUTINES IN SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = OBJECT_ROUTINE;
					n->objs = $5;
					$$ = n;
				}
		;


grantee_list:
			grantee									{ $$ = list_make1($1); }
			| grantee_list ',' grantee				{ $$ = lappend($1, $3); }
		;

grantee:
			RoleSpec								{ $$ = $1; }
			| GROUP RoleSpec						{ $$ = $2; }
		;

opt_grant_grant_option:
			WITH GRANT OPTION { $$ = true; }
			| /*EMPTY*/ { $$ = false; }
		;



opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;


/* Any not-fully-reserved word --- these names can be, eg, role names.
 */
NonReservedWord:	IDENTIFIER							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			//| col_name_keyword						{ $$ = pstrdup($1); }
			//| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENTIFIER									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			/*| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }*/
			| reserved_keyword						{ $$ = pstrdup($1); }
		;


/* Bare column label --- names that can be column labels without writing "AS".
 * This classification is orthogonal to the other keyword categories.
 */
BareColLabel:	IDENTIFIER								{ $$ = $1; }
			| bare_label_keyword					{ $$ = pstrdup($1); }
		;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| '.' '*'
				{
					$$ = (Node *) makeNode(A_Star);
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = false;
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' opt_slice_bound ':' opt_slice_bound ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = true;
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

opt_slice_bound:
			a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

/*
 * General expressions
 * This is the heart of the expression syntax.
 *
 * We have two expression types: a_expr is the unrestricted kind, and
 * b_expr is a subset that must be used in some places to avoid shift/reduce
 * conflicts.  For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr"
 * because that use of AND conflicts with AND as a boolean operator.  So,
 * b_expr is used in BETWEEN and we remove boolean keywords from b_expr.
 *
 * Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
 * always be used by surrounding it with parens.
 *
 * c_expr is all the productions that are common to a_expr and b_expr;
 * it's factored out just to eliminate redundant coding.
 *
 * Be careful of productions involving more than one terminal token.
 * By default, bison will assign such productions the precedence of their
 * last terminal, but in nearly all cases you want it to be the precedence
 * of the first terminal instead; otherwise you will not get the behavior
 * you expect!  So we use %prec annotations freely to set precedences.
 */
a_expr:		c_expr									{ $$ = $1; }
			| a_expr TYPECAST Typename
					{ $$ = makeTypeCast($1, $3, @2); }
			| a_expr COLLATE any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = $1;
					n->collname = $3;
					n->location = @2;
					$$ = (Node *) n;
				}
			| a_expr AT TIME ZONE a_expr			//%prec AT
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("timezone"),
											   list_make2($5, $1),
											   COERCE_SQL_SYNTAX,
											   @2);
				}
			| '+' a_expr					%prec UNARY_MINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' a_expr					%prec UNARY_MINUS
				{ $$ = doNegate($2, @1); }
			| a_expr '+' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| a_expr '-' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| a_expr '*' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| a_expr '/' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| a_expr '%' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr '^' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| a_expr '<' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| a_expr '>' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| a_expr '=' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| a_expr LT_EQ a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| a_expr GT_EQ a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| a_expr NOT_EQ a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| a_expr qual_Op a_expr				%prec OPERATOR
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op a_expr					%prec OPERATOR
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| a_expr AND a_expr
				{ $$ = makeAndExpr($1, $3, @2); }
			| a_expr OR a_expr
				{ $$ = makeOrExpr($1, $3, @2); }
			| NOT a_expr
				{ $$ = makeNotExpr($2, @1); }
			| a_expr LIKE a_expr
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, $3, @2);
				}
			| a_expr LIKE a_expr ESCAPE a_expr					%prec LIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT LIKE a_expr							%prec NOT
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, $4, @2);
				}
			| a_expr NOT LIKE a_expr ESCAPE a_expr			%prec NOT
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, (Node *) n, @2);
				}
			| a_expr ILIKE a_expr
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, $3, @2);
				}
			| a_expr ILIKE a_expr ESCAPE a_expr					%prec ILIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT ILIKE a_expr						%prec NOT
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, $4, @2);
				}
			| a_expr NOT ILIKE a_expr ESCAPE a_expr			%prec NOT
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr							%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_to_escape"),
											   list_make1($4),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr ESCAPE a_expr			%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_to_escape"),
											   list_make2($4, $6),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr					%prec NOT
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_to_escape"),
											   list_make1($5),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr ESCAPE a_expr		%prec NOT
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_to_escape"),
											   list_make2($5, $7),
											   COERCE_EXPLICIT_CALL,
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}
			| a_expr IS NULL_P							%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr ISNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr IS NOT NULL_P						%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr NOTNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			 | row OVERLAPS row
				{
					if (list_length($1) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on left side of OVERLAPS expression")));
					if (list_length($3) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on right side of OVERLAPS expression")));
					$$ = (Node *) makeFuncCall(SystemFuncName("overlaps"),
											   list_concat($1, $3),
											   COERCE_SQL_SYNTAX,
											   @2);
				}
			| a_expr IS TRUE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT TRUE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS FALSE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT FALSE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS UNKNOWN							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT UNKNOWN						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS DISTINCT FROM a_expr			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| a_expr IS NOT DISTINCT FROM a_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| a_expr BETWEEN opt_asymmetric b_expr AND a_expr		//%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN,
												   "BETWEEN",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT BETWEEN opt_asymmetric b_expr AND a_expr //%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN,
												   "NOT BETWEEN",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr BETWEEN SYMMETRIC b_expr AND a_expr			//%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN_SYM,
												   "BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT BETWEEN SYMMETRIC b_expr AND a_expr		//%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN_SYM,
												   "NOT BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr IN in_expr
				{
					if (IsA($3, SubLink))
					{
						SubLink *n = (SubLink *) $3;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		
						n->location = @2;
						$$ = (Node *)n;
					}
					else
					{
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| a_expr NOT IN in_expr						%prec NOT
				{
					if (IsA($4, SubLink))
					{
						SubLink *n = (SubLink *) $4;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;	
						n->location = @2;
						$$ = makeNotExpr((Node *) n, @2);
					}
					else
					{
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, $4, @2);
					}
				}
			| a_expr subquery_Op sub_type select_with_parens	%prec OPERATOR
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = $3;
					n->subLinkId = 0;
					n->testexpr = $1;
					n->operName = $2;
					n->subselect = $4;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr subquery_Op sub_type '(' a_expr ')'		%prec OPERATOR
				{
					if ($3 == ANY_SUBLINK)
						$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
					else
						$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, $5, @2);
				}
			| UNIQUE select_with_parens
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNIQUE predicate is not yet implemented")));
				}
			| a_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| a_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
			| a_expr IS NORMALIZED								%prec IS
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("is_normalized"),
											   list_make1($1),
											   COERCE_SQL_SYNTAX,
											   @2);
				}
			| a_expr IS unicode_normal_form NORMALIZED			%prec IS
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("is_normalized"),
											   list_make2($1, makeStringConst($3, @3)),
											   COERCE_SQL_SYNTAX,
											   @2);
				}
			| a_expr IS NOT NORMALIZED							%prec IS
				{
					$$ = makeNotExpr((Node *) makeFuncCall(SystemFuncName("is_normalized"),
														   list_make1($1),
														   COERCE_SQL_SYNTAX,
														   @2),
									 @2);
				}
			| a_expr IS NOT unicode_normal_form NORMALIZED		%prec IS
				{
					$$ = makeNotExpr((Node *) makeFuncCall(SystemFuncName("is_normalized"),
														   list_make2($1, makeStringConst($4, @4)),
														   COERCE_SQL_SYNTAX,
														   @2),
									 @2);
				}
			| DEFAULT
				{

					SetToDefault *n = makeNode(SetToDefault);
					n->location = @1;
					$$ = (Node *)n;
				}
		;


opt_asymmetric: ASYMMETRIC
			| /*EMPTY*/
		;
/*
 * Restricted expressions
 *
 * b_expr is a subset of the complete expression syntax defined by a_expr.
 *
 * Presently, AND, NOT, IS, and IN are the a_expr keywords that would
 * cause trouble in the places where b_expr is used.  For simplicity, we
 * just eliminate all the boolean-keyword-operator productions from b_expr.
 */
b_expr:		c_expr
				{ $$ = $1; }
			| b_expr TYPECAST Typename
				{ $$ = makeTypeCast($1, $3, @2); }
			| '+' b_expr					%prec UNARY_MINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' b_expr					%prec UNARY_MINUS
				{ $$ = doNegate($2, @1); }
			| b_expr '+' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| b_expr '-' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| b_expr '*' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| b_expr '/' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| b_expr '%' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr '^' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| b_expr '<' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| b_expr '>' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| b_expr '=' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| b_expr LT_EQ b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| b_expr GT_EQ b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| b_expr NOT_EQ b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| b_expr qual_Op b_expr				%prec OPERATOR
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op b_expr					%prec OPERATOR
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| b_expr IS DISTINCT FROM b_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| b_expr IS NOT DISTINCT FROM b_expr	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| b_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| b_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
		;

/*
 * Productions that can be used in both a_expr and b_expr.
 *
 * Note: productions that refer recursively to a_expr or b_expr mostly
 * cannot appear here.	However, it's OK to refer to a_exprs that occur
 * inside parentheses, such as function arguments; that cannot introduce
 * ambiguity to the b_expr syntax.
 */
c_expr:		columnref								{ $$ = $1; }
			| AexprConst							{ $$ = $1; }
			| PARAMETER opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, scanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = check_indirection($4, scanner);
						$$ = (Node *)n;
					}
					else
						$$ = $2;
				} 
			| case_expr
				{ $$ = $1; }
			| func_expr 
				{ $$ = $1; }
			| select_with_parens			%prec '-'
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| select_with_parens indirection
				{

					SubLink *n = makeNode(SubLink);
					A_Indirection *a = makeNode(A_Indirection);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					a->arg = (Node *)n;
					a->indirection = check_indirection($2, scanner);
					$$ = (Node *)a;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{
					A_ArrayExpr *n = castNode(A_ArrayExpr, $2);
				
					n->location = @1;
					$$ = (Node *)n;
				}
			| explicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	
					r->colnames = NIL;
					r->row_format = COERCE_EXPLICIT_CALL;
					r->location = @1;
					$$ = (Node *)r;
				}
			| implicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	
					r->colnames = NIL;
					r->row_format = COERCE_IMPLICIT_CAST; 
					r->location = @1;
					$$ = (Node *)r;
				}
			| GROUPING '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  $$ = (Node *)g;
			  }
		;

in_expr:	select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subselect = $1;
					/* other fields will be filled later */
					$$ = (Node *)n;
				}
			| '(' expr_list ')'						{ $$ = (Node *)$2; }
		;


func_application: func_name '(' ')'
				{
					$$ = (Node *) makeFuncCall($1, NIL,
											   COERCE_EXPLICIT_CALL,
											   @1);
				}
			| func_name '(' func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, $3,
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->agg_order = $4;
					$$ = (Node *)n;
				}
			| func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, list_make1($4),
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->func_variadic = true;
					n->agg_order = $5;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, lappend($3, $6),
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->func_variadic = true;
					n->agg_order = $7;
					$$ = (Node *)n;
				}
			| func_name '(' ALL func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, $4,
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->agg_order = $5;
					/* Ideally we'd mark the FuncCall node to indicate
					 * "must be an aggregate", but there's no provision
					 * for that in FuncCall at the moment.
					 */
					$$ = (Node *)n;
				}
			| func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, $4,
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->agg_order = $5;
					n->agg_distinct = true;
					$$ = (Node *)n;
				}
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
					FuncCall *n = makeFuncCall($1, NIL,
											   COERCE_EXPLICIT_CALL,
											   @1);
					n->agg_star = true;
					$$ = (Node *)n;
				}
		;


/*
 * func_expr and its cousin func_expr_windowless are split out from c_expr just
 * so that we have classifications for "everything that is a function call or
 * looks like one".  This isn't very important, but it saves us having to
 * document which variants are legal in places like "FROM function()" or the
 * backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr: func_application within_group_clause filter_clause over_clause
				{
					FuncCall *n = (FuncCall *) $1;
					
					 // The order clause for WITHIN GROUP and the one for
					 // plain-aggregate ORDER BY share a field, so we have to
					 // check here that at most one is present.  We also check
					 // for DISTINCT and VARIADIC here to give a better error
					 // location.  Other consistency checks are deferred to
					 // parse analysis.
					 
					if ($2 != NIL)
					{
						if (n->agg_order != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP")));
						if (n->agg_distinct)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use DISTINCT with WITHIN GROUP")));
						if (n->func_variadic)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use VARIADIC with WITHIN GROUP")));
						n->agg_order = $2;
						n->agg_within_group = true;
					}
					n->agg_filter = $3;
					n->over = $4;
					$$ = (Node *) n;
				}
			| func_expr_common_subexpr
				{ $$ = $1; }
		;


/*
 * As func_expr but does not accept WINDOW functions directly
 * (but they can still be contained in arguments for functions etc).
 * Use this when window expressions are not allowed, where needed to
 * disambiguate the grammar (e.g. in CREATE INDEX).
 */
func_expr_windowless:
			func_application						{ $$ = $1; }
			| func_expr_common_subexpr				{ $$ = $1; }
		;


/*
 * Special expressions that are considered to be functions.
 */
func_expr_common_subexpr:
			COLLATION FOR '(' a_expr ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("pg_collation_for"),
											   list_make1($4),
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| CURRENT_DATE
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1);
				}
			| CURRENT_TIME
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIME, -1, @1);
				}
			| CURRENT_TIME '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIME_N, $3, @1);
				}
			| CURRENT_TIMESTAMP
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1);
				}
			| CURRENT_TIMESTAMP '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP_N, $3, @1);
				}
			| LOCALTIME
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIME, -1, @1);
				}
			| LOCALTIME '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIME_N, $3, @1);
				}
			| LOCALTIMESTAMP
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, -1, @1);
				}
			| LOCALTIMESTAMP '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP_N, $3, @1);
				}
			| CURRENT_ROLE
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_ROLE, -1, @1);
				}
			| CURRENT_USER
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_USER, -1, @1);
				}
			| SESSION_USER
				{
					$$ = makeSQLValueFunction(SVFOP_SESSION_USER, -1, @1);
				}
			| USER
				{
					$$ = makeSQLValueFunction(SVFOP_USER, -1, @1);
				}
			| CURRENT_CATALOG
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_CATALOG, -1, @1);
				}
			| CURRENT_SCHEMA
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_SCHEMA, -1, @1);
				}
			| CAST '(' a_expr AS Typename ')'
				{ $$ = makeTypeCast($3, $5, @1); }
			| EXTRACT '(' extract_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("extract"),
											   $3,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| NORMALIZE '(' a_expr ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("normalize"),
											   list_make1($3),
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| NORMALIZE '(' a_expr ',' unicode_normal_form ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("normalize"),
											   list_make2($3, makeStringConst($5, @5)),
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| OVERLAY '(' overlay_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("overlay"),
											   $3,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| OVERLAY '(' func_arg_list_opt ')'
				{

					$$ = (Node *) makeFuncCall(list_make1(makeString("overlay")),
											   $3,
											   COERCE_EXPLICIT_CALL,
											   @1);
				}
			| POSITION '(' position_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("position"),
											   $3,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| SUBSTRING '(' substr_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("substring"),
											   $3,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| SUBSTRING '(' func_arg_list_opt ')'
				{
					$$ = (Node *) makeFuncCall(list_make1(makeString("substring")),
											   $3,
											   COERCE_EXPLICIT_CALL,
											   @1);
				}
			| TREAT '(' a_expr AS Typename ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName(strVal(llast($5->names))),
											   list_make1($3),
											   COERCE_EXPLICIT_CALL,
											   @1);
				}
			| TRIM '(' BOTH trim_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("btrim"),
											   $4,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| TRIM '(' LEADING trim_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("ltrim"),
											   $4,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| TRIM '(' TRAILING trim_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("rtrim"),
											   $4,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| TRIM '(' trim_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("btrim"),
											   $3,
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| NULLIF '(' a_expr ',' a_expr ')'
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
				}
			| COALESCE '(' expr_list ')'
				{
					CoalesceExpr *c = makeNode(CoalesceExpr);
					c->args = $3;
					c->location = @1;
					$$ = (Node *)c;
				}
			| GREATEST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_GREATEST;
					v->location = @1;
					$$ = (Node *)v;
				}
			| LEAST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_LEAST;
					v->location = @1;
					$$ = (Node *)v;
				}/*
			| XMLCONCAT '(' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLCONCAT, NULL, NIL, $3, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, $6, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, $8, @1);
				}
			| XMLEXISTS '(' c_expr xmlexists_argument ')'
				{
					// xmlexists(A PASSING [BY REF] B [BY REF]) is
					// converted to xmlexists(A, B)
					$$ = (Node *) makeFuncCall(SystemFuncName("xmlexists"),
											   list_make2($3, $4),
											   COERCE_SQL_SYNTAX,
											   @1);
				}
			| XMLFOREST '(' xml_attribute_list ')'
				{
					$$ = makeXmlExpr(IS_XMLFOREST, NULL, $3, NIL, @1);
				}
			| XMLPARSE '(' document_or_content a_expr xml_whitespace_option ')'
				{
					XmlExpr *x = (XmlExpr *)
						makeXmlExpr(IS_XMLPARSE, NULL, NIL,
									list_make2($4, makeBoolAConst($5, -1)),
									@1);
					x->xmloption = $3;
					$$ = (Node *)x;
				}
			| XMLPI '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, NIL, @1);
				}
			| XMLPI '(' NAME_P ColLabel ',' a_expr ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, list_make1($6), @1);
				}
			| XMLROOT '(' a_expr ',' xml_root_version opt_xml_root_standalone ')'
				{
					$$ = makeXmlExpr(IS_XMLROOT, NULL, NIL,
									 list_make3($3, $5, $6), @1);
				}
			| XMLSERIALIZE '(' document_or_content a_expr AS SimpleTypename ')'
				{
					XmlSerialize *n = makeNode(XmlSerialize);
					n->xmloption = $3;
					n->expr = $4;
					n->typeName = $6;
					n->location = @1;
					$$ = (Node *)n;
				}*/
		;


/* function arguments can have names */
func_arg_list:  func_arg_expr
				{
					$$ = list_make1($1);
				}
			| func_arg_list ',' func_arg_expr
				{
					$$ = lappend($1, $3);
				}
		;

func_arg_list_opt:	func_arg_list					{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	type_function_name
		;


func_arg_expr:  a_expr
				{
					$$ = $1;
				}
			| param_name ':' '=' a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $4;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
			| param_name GT_EQ a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $3;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
		;

array_expr: '[' expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' array_expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' ']'
				{
					$$ = makeAArrayExpr(NIL, @1);
				}
		;

array_expr_list: array_expr							{ $$ = list_make1($1); }
			| array_expr_list ',' array_expr		{ $$ = lappend($1, $3); }
		;

extract_list:
			extract_arg FROM a_expr
				{
					$$ = list_make2(makeStringConst($1, @1), $3);
				}
		;



/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
			IDENTIFIER									{ $$ = $1; }
			/*| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }*/
			| Sconst								{ $$ = $1; }
		;

unicode_normal_form:
			NFC										{ $$ = "NFC"; }
			| NFD									{ $$ = "NFD"; }
			| NFKC									{ $$ = "NFKC"; }
			| NFKD									{ $$ = "NFKD"; }
		;

/* OVERLAY() arguments */
overlay_list:
			a_expr PLACING a_expr FROM a_expr FOR a_expr
				{
					/* overlay(A PLACING B FROM C FOR D) is converted to overlay(A, B, C, D) */
					$$ = list_make4($1, $3, $5, $7);
				}
			| a_expr PLACING a_expr FROM a_expr
				{
					/* overlay(A PLACING B FROM C) is converted to overlay(A, B, C) */
					$$ = list_make3($1, $3, $5);
				}
		;


/* position_list uses b_expr not a_expr to avoid conflict with general IN */
position_list:
			b_expr IN b_expr						{ $$ = list_make2($3, $1); }
		;


/*
 * SUBSTRING() arguments
 *
 * Note that SQL:1999 has both
 *     text FROM int FOR int
 * and
 *     text FROM pattern FOR escape
 *
 * In the parser we map them both to a call to the substring() function and
 * rely on type resolution to pick the right one.
 *
 * In SQL:2003, the second variant was changed to
 *     text SIMILAR pattern ESCAPE escape
 * We could in theory map that to a different function internally, but
 * since we still support the SQL:1999 version, we don't.  However,
 * ruleutils.c will reverse-list the call in the newer style.
 */
substr_list:
			a_expr FROM a_expr FOR a_expr
				{
					$$ = list_make3($1, $3, $5);
				}
			| a_expr FOR a_expr FROM a_expr
				{
					/* not legal per SQL, but might as well allow it */
					$$ = list_make3($1, $5, $3);
				}
			| a_expr FROM a_expr
				{
					/*
					 * Because we aren't restricting data types here, this
					 * syntax can end up resolving to textregexsubstr().
					 * We've historically allowed that to happen, so continue
					 * to accept it.  However, ruleutils.c will reverse-list
					 * such a call in regular function call syntax.
					 */
					$$ = list_make2($1, $3);
				}
			| a_expr FOR a_expr
				{
					/* not legal per SQL */

					/*
					 * Since there are no cases where this syntax allows
					 * a textual FOR value, we forcibly cast the argument
					 * to int4.  The possible matches in pg_proc are
					 * substring(text,int4) and substring(text,text),
					 * and we don't want the parser to choose the latter,
					 * which it is likely to do if the second argument
					 * is unknown or doesn't have an implicit cast to int4.
					 */
					$$ = list_make3($1, makeIntConst(1, -1),
									makeTypeCast($3,
												 SystemTypeName("int4"), -1));
				}
			| a_expr SIMILAR a_expr ESCAPE a_expr
				{
					$$ = list_make3($1, $3, $5);
				}
		;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
			| FROM expr_list						{ $$ = $2; }
			| expr_list								{ $$ = $1; }
		;

/*
 * Define SQL-style CASE clause.
 * - Full specification
 *	CASE WHEN a = b THEN c ... ELSE d END
 * - Implicit argument
 *	CASE a WHEN b THEN c ... ELSE d END
 */
case_expr:	CASE case_arg when_clause_list case_default END_P
				{
					CaseExpr *c = makeNode(CaseExpr);
					c->casetype = InvalidOid; /* not analyzed yet */
					c->arg = (Expr *) $2;
					c->args = $3;
					c->defresult = (Expr *) $4;
					c->location = @1;
					$$ = (Node *)c;
				}
		;

when_clause_list:
			/* There must be at least one */
			when_clause								{ $$ = list_make1($1); }
			| when_clause_list when_clause			{ $$ = lappend($1, $2); }
		;

when_clause:
			WHEN a_expr THEN a_expr
				{
					CaseWhen *w = makeNode(CaseWhen);
					w->expr = (Expr *) $2;
					w->result = (Expr *) $4;
					w->location = @1;
					$$ = (Node *)w;
				}
		;

case_default:
			ELSE a_expr								{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

case_arg:	a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		; 

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' or Sconst for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	type_function_name
					{ $$ = list_make1(makeString($1)); }
			| ColId indirection
					{
						$$ = check_func_name(lcons(makeString($1), $2),
											 scanner);
					}
		;

/*
 * Constants
 */
AexprConst: Iconst
				{
					$$ = makeIntConst($1, @1);
				}
			| DECIMAL
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			/*| BCONST
				{
					$$ = makeBitStringConst($1, @1);
				}*/
			| XCONST
				{

					$$ = makeBitStringConst($1, @1);
				}
			| func_name Sconst
				{
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = makeStringConstCast($2, @2, t);
				}
			/*| func_name '(' func_arg_list opt_sort_clause ')' Sconst
				{
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}
					if ($4 != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have ORDER BY"),
									 parser_errposition(@4)));

					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($6, @6, t);
				}*/
			| ConstTypename Sconst
				{
					$$ = makeStringConstCast($2, @2, $1);
				}
			/*| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}*/
			| TRUE_P
				{
					$$ = makeBoolAConst(true, @1);
				}
			| FALSE_P
				{
					$$ = makeBoolAConst(false, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
		;

/*****************************************************************************
 *
 *	Names and constants
 *
 *****************************************************************************/
qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

/*
 * The production for a qualified relation name has to exactly match the
 * production for a qualified func_name, because in a FROM clause we cannot
 * tell which we are parsing until we see what comes after it ('(' for a
 * func_name, something else for a relation). Therefore we allow 'indirection'
 * which may contain subscripts, and reject that case in the C code.
 */
qualified_name:
			ColId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					//check_qualified_name($2, scanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))) ));//,
									 //parser_errposition(@1)));
							break;
					}
				}
		;

/*****************************************************************************
 *
 *	Type syntax
 *		SQL introduces a large amount of type-specific syntax.
 *		Define individual clauses to handle these cases, and use
 *		 the generic case to handle regular type-extensible Postgres syntax.
 *		- thomas 1997-10-10
 *
 *****************************************************************************/

Typename:	SimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
			| SETOF SimpleTypename opt_array_bounds
				{
					$$ = $2;
					$$->arrayBounds = $3;
					$$->setof = true;
				}
			// SQL standard syntax, currently only one-dimensional
			| SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger($4));
				}
			| SETOF SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger($5));
					$$->setof = true;
				}
			| SimpleTypename ARRAY
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger(-1));
				}
			| SETOF SimpleTypename ARRAY
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger(-1));
					$$->setof = true;
				}
		;

opt_array_bounds:
			opt_array_bounds '[' ']'
					{  $$ = lappend($1, makeInteger(-1)); }
			| opt_array_bounds '[' Iconst ']'
					{  $$ = lappend($1, makeInteger($3)); }
			| /*EMPTY*/
					{  $$ = NIL; }
		;


SimpleTypename:
			GenericType								{ $$ = $1; }
			| Numeric								{ $$ = $1; }
			/*| Bit									{ $$ = $1; }
			| Character								{ $$ = $1; }*/
			| ConstDatetime							{ $$ = $1; }
			/*| ConstInterval opt_interval
				{
					$$ = $1;
					$$->typmods = $2;
				}
			| ConstInterval '(' Iconst ')'
				{
					$$ = $1;
					$$->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											 makeIntConst($3, @3));
				}*/
		;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accommodate the postfix
 * options (e.g. INTERVAL '1' YEAR). Likewise, we have to handle
 * the generic-type-name case in AexprConst to avoid premature
 * reduce/reduce conflicts against function names.
 */
ConstTypename:
			Numeric									{ $$ = $1; }
		/*	| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }*/
			| ConstDatetime							{ $$ = $1; }
		;


/*
 * GenericType covers all type names that don't have special syntax mandated
 * by the standard, including qualified names.  We also allow type modifiers.
 * To avoid parsing conflicts against function invocations, the modifiers
 * have to be shown as expr_list here, but parse analysis will only accept
 * constants for them.
 */
GenericType:
			type_function_name opt_type_modifiers
				{
					$$ = makeTypeName($1);
					$$->typmods = $2;
					$$->location = @1;
				}
			/*| type_function_name attrs opt_type_modifiers
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->typmods = $3;
					$$->location = @1;
				}*/
		;

opt_type_modifiers: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }

/*
 * SQL numeric data types
 */
Numeric:	INT_P
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| INTEGER
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
				| INTEGER_P
								{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| SMALLINT
				{
					$$ = SystemTypeName("int2");
					$$->location = @1;
				}
			| BIGINT
				{
					$$ = SystemTypeName("int8");
					$$->location = @1;
				}
			| REAL
				{
					$$ = SystemTypeName("float4");
					$$->location = @1;
				}
			| FLOAT_P opt_float
				{
					$$ = $2;
					$$->location = @1;
				}
			| DOUBLE_P PRECISION
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| DECIMAL_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| DEC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| NUMERIC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| BOOLEAN_P
				{
					$$ = SystemTypeName("bool");
					$$->location = @1;
				}
		;

opt_float:	'(' Iconst ')'
				{
					/*
					 * Check FLOAT() precision limits assuming IEEE floating
					 * types - thomas 1997-09-18
					 */
					if ($2 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be at least 1 bit")));
					else if ($2 <= 24)
						$$ = SystemTypeName("float4");
					else if ($2 <= 53)
						$$ = SystemTypeName("float8");
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be less than 54 bits")));
				}
			| /*EMPTY*/
				{
					$$ = SystemTypeName("float8");
				}
		;


/*
 * SQL date/time types
 */
ConstDatetime:
			TIMESTAMP '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIMESTAMP opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->location = @1;
				}
			| TIME '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIME opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->location = @1;
				}
		;

opt_timezone:
			WITH TIME ZONE						{ $$ = true; }
			| WITHOUT TIME ZONE						{ $$ = false; }
			| /*EMPTY*/								{ $$ = false; }
		;

create_extension_opt_list:
	create_extension_opt_list create_extension_opt_item
		{ $$ = lappend($1, $2); }
	| /* EMPTY */
		{ $$ = NIL; }
	;

create_extension_opt_item:
	SCHEMA name
		{
			$$ = makeDefElem("schema", (Node *)makeString($2), @1);
		}
	| VERSION_P NonReservedWord_or_Sconst
		{
			$$ = makeDefElem("new_version", (Node *)makeString($2), @1);
		}
	| CASCADE
		{
			$$ = makeDefElem("cascade", (Node *)makeInteger(true), @1);
		}
	;

/*
 * Constants
 */
Sconst:		STRING									{ $$ = $1; };

/*
 * Name classification hierarchy.
 *
 * IDENT is the lexeme returned by the lexer for identifiers that match
 * no known keyword.  In most cases, we can accept certain keywords as
 * names, not only IDENTs.	We prefer to accept as many such keywords
 * as possible to minimize the impact of "reserved words" on programmers.
 * So, we divide names into several possible classes.  The classification
 * is chosen in part to make keywords acceptable as names wherever possible.
 */

attr_name:	ColLabel								{ $$ = $1; };
name:		ColId									{ $$ = $1; }
            | Sconst { $$ = $1;}
			;
ColId:		IDENTIFIER									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			/*| col_name_keyword						{ $$ = pstrdup($1); }
		*/;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENTIFIER							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			//| type_func_name_keyword				{ $$ = pstrdup($1); }
		;
NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; } 
			| Sconst								{ $$ = $1; }
		;


VariableResetStmt:
			RESET reset_rest						{ $$ = (Node *) $2; }
		;

reset_rest:
			generic_reset							{ $$ = $1; }
			| TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "timezone";
					$$ = n;
				}
			| TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "transaction_isolation";
					$$ = n;
				}
			| SESSION AUTHORIZATION
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "session_authorization";
					$$ = n;
				}
		;

generic_reset:
			var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $1;
					$$ = n;
				}
			| ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = n;
				}
		;

/*
 * Keyword category lists.  Generally, every keyword present in
 * the Postgres grammar should appear in exactly one of these lists.
 *
 * Put a new keyword into the first list that it can go into without causing
 * shift or reduce conflicts.  The earlier lists define "less reserved"
 * categories of keywords.
 *
 * Make sure that each keyword's category in kwlist.h matches where
 * it is listed here.  (Someday we may be able to generate these lists and
 * kwlist.h's table from one source of truth.)
 */

/* "Unreserved" keywords --- available for use as any kind of name.
 */
unreserved_keyword:
			INPUT_P
			| KEY
			| LIKE
			| NAME_P
			| TYPE_P
;

/*
 * While all keywords can be used as column labels when preceded by AS,
 * not all of them can be used as a "bare" column label without AS.
 * Those that can be used as a bare label must be listed here,
 * in addition to appearing in one of the category lists above.
 *
 * Always add a new keyword to this list if possible.  Mark it BARE_LABEL
 * in kwlist.h if it is included here, or AS_LABEL if it is not.
 */
bare_label_keyword:
        FALSE_P
		| INPUT_P
		| KEY
		| LIKE
		| NAME_P
		| TRUE_P
		| TYPE_P
;

reserved_keyword:
        FALSE_P
		| TRUE_P

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
    cypher_a_expr '=' cypher_a_expr
        {
            cypher_set_item *n;

            n = make_ag_node(cypher_set_item);
            n->prop = $1;
            n->expr = $3;
            n->is_add = false;
            n->location = @1;

            $$ = (Node *)n;
        }
   | cypher_a_expr PLUS_EQ cypher_a_expr
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
    cypher_a_expr
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
    detach_opt DELETE cypher_expr_list
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

cypher_where_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | WHERE cypher_a_expr
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
    | cypher_var_name '=' anonymous_path /* named path */
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
cypher_a_expr:
    cypher_a_expr OR cypher_a_expr
        {
            $$ = make_or_expr($1, $3, @2);
        }
    | cypher_a_expr AND cypher_a_expr
        {
            $$ = make_and_expr($1, $3, @2);
        }
    | cypher_a_expr XOR cypher_a_expr
        {
            $$ = make_xor_expr($1, $3, @2);
        }
    | NOT cypher_a_expr
        {
            $$ = make_not_expr($2, @1);
        }
    | cypher_a_expr '=' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2);
        }
    | cypher_a_expr LIKE cypher_a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~~~", $1, $3, @2);
        } 
    | cypher_a_expr NOT LIKE cypher_a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2);
        }  
    | cypher_a_expr ILIKE cypher_a_expr
        {   
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~~*", $1, $3, @2);
        } 
    | cypher_a_expr NOT ILIKE cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, $4, @2);
        }  
    | cypher_a_expr NOT_EQ cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2);
        }
    | cypher_a_expr '<' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2);
        }
    | cypher_a_expr LT_EQ cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2);
        }
    | cypher_a_expr '>' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2);
        }
    | cypher_a_expr GT_EQ cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2);
        }
    | cypher_a_expr '+' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2);
        }
    | cypher_a_expr '-' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2);
        }
    | cypher_a_expr '*' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2);
        }
    | cypher_a_expr '/' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2);
        }
    | cypher_a_expr '%' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2);
        }
    | cypher_a_expr '^' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2);
        }
    | cypher_a_expr OPERATOR cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, $2, $1, $3, @2);
        }
    | OPERATOR cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, $1, NULL, $2, @1);
        }
    | cypher_a_expr '<' '-' '>' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "<->", $1, $5, @3);
        }
    | cypher_a_expr IN cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "@=", $1, $3, @2);
        }
    | cypher_a_expr IS NULL_P %prec IS
        {
            NullTest *n;

            n = makeNode(NullTest);
            n->arg = (Expr *)$1;
            n->nulltesttype = IS_NULL;
            n->location = @2;

            $$ = (Node *)n;
        }
    | cypher_a_expr IS NOT NULL_P %prec IS
        {
            NullTest *n;

            n = makeNode(NullTest);
            n->arg = (Expr *)$1;
            n->nulltesttype = IS_NOT_NULL;
            n->location = @2;

            $$ = (Node *)n;
        }
    | '-' cypher_a_expr %prec UNARY_MINUS
        {
            $$ = do_negate($2, @1);
        
        }
    | '~' cypher_a_expr %prec UNARY_MINUS
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~", NULL, $2, @1);
        }
    | cypher_a_expr STARTS WITH cypher_a_expr %prec STARTS
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_STARTS_WITH;
            n->lhs = $1;
            n->rhs = $4;
            n->location = @2;

            $$ = (Node *)n;
        }
    | cypher_a_expr ENDS WITH cypher_a_expr %prec ENDS
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_ENDS_WITH;
            n->lhs = $1;
            n->rhs = $4;
            n->location = @2;

            $$ = (Node *)n;
        }
    | cypher_a_expr CONTAINS cypher_a_expr
        {
            cypher_string_match *n;

            n = make_ag_node(cypher_string_match);
            n->operation = CSMO_CONTAINS;
            n->lhs = $1;
            n->rhs = $3;
            n->location = @2;

            $$ = (Node *)n;
        }
    | cypher_a_expr '~' cypher_a_expr
        {
            $$ = (Node *)makeSimpleA_Expr(AEXPR_OP, "~", $1, $3, @2);
        }
    | cypher_a_expr '[' cypher_a_expr ']'  %prec '.'
        {
            A_Indices *i;

            i = makeNode(A_Indices);
            i->is_slice = false;
            i->lidx = NULL;
            i->uidx = $3;

            $$ = append_indirection($1, (Node *)i);
        }
    | cypher_a_expr '[' expr_opt DOT_DOT expr_opt ']'
        {
            A_Indices *i;

            i = makeNode(A_Indices);
            i->is_slice = true;
            i->lidx = $3;
            i->uidx = $5;

            $$ = append_indirection($1, (Node *)i);
        }
    | cypher_a_expr '.' cypher_a_expr
        {
            $$ = append_indirection($1, $3);
        }
    | cypher_a_expr TYPECAST schema_name
        {
            $$ = make_typecast_expr($1, $3, @2);
        }
    | temporal_cast cypher_a_expr %prec TYPECAST
        {
            $$ = make_typecast_expr($2, $1, @1);
        }
    | cypher_a_expr all_op ALL cypher_in_expr //%prec OPERATOR
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
    | cypher_a_expr all_op ANY cypher_in_expr //%prec OPERATOR
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
    | cypher_a_expr all_op SOME cypher_in_expr //%prec OPERATOR
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
    | cypher_a_expr
    ;

cypher_expr_list:
    cypher_a_expr
        {
            $$ = list_make1($1);
        }
    | cypher_expr_list ',' cypher_a_expr
        {
            $$ = lappend($1, $3);
        }
    ;

cypher_expr_list_opt:
    /* empty */
        {
            $$ = NIL;
        }
    | cypher_expr_list
    ;

cypher_expr_func:
    cypher_expr_func_norm within_group_clause filter_clause over_clause 
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
    | cypher_expr_func_subexpr
    ;

/*
 * Aggregate decoration clauses
 */
within_group_clause:
    WITHIN GROUP '(' order_by_opt ')' { $$ = $4; }
    | /*EMPTY*/                        { $$ = NIL; }
; 

filter_clause:
     FILTER '(' WHERE cypher_a_expr ')' { $$ = $4; }
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
    '(' opt_partition_clause opt_sort_clause order_by_opt opt_frame_clause ')'
     {
         WindowDef *n = makeNode(WindowDef);
         n->name = NULL;
         n->refname = $2;
         n->partitionClause = $3;
         n->orderClause = $4;
         /* copy relevant fields of opt_frame_clause */
         n->frameOptions = $5->frameOptions;
         n->startOffset = $5->startOffset;
         n->endOffset = $5->endOffset;
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


cypher_expr_func_norm:
    cypher_func_name '(' ')'
        {
            $$ = (Node *)makeFuncCall($1, NIL, COERCE_SQL_SYNTAX, @1);
        }
    | cypher_func_name '(' expr_list ')'
        {
            $$ = (Node *)makeFuncCall($1, $3, COERCE_SQL_SYNTAX, @1);
        }
    | cypher_func_name '(' '*' ')'
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
    | cypher_func_name '(' DISTINCT  expr_list ')'
        {
            FuncCall *n = (FuncCall *)makeFuncCall($1, $4, COERCE_SQL_SYNTAX, @1);
            n->agg_order = NIL;
            n->agg_distinct = true;
            $$ = (Node *)n;
        }
    ;

cypher_expr_func_subexpr:
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
    | EXTRACT '(' IDENTIFIER FROM cypher_a_expr ')'
        {
            $$ = (Node *)makeFuncCall(list_make1(makeString($1)),
                                      list_make2(make_string_const($3, @3), $5),
                                      COERCE_SQL_SYNTAX, @1);
        }
    | '(' cypher_a_expr ',' cypher_a_expr ')' OVERLAPS '(' cypher_a_expr ',' cypher_a_expr ')'
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

                                        
cypher_in_expr:
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
    | '(' cypher_a_expr ')'
        {
            $$ = $2;
        }
    | expr_case
    | cypher_var_name 
        {
            ColumnRef *n;
            
            n = makeNode(ColumnRef);
            n->fields = list_make1(makeString($1));
            n->location = @1;
            
            $$ = (Node *)n;
        }   
    | cypher_expr_func
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
    property_key_name ':' cypher_a_expr
        {
            $$ = list_make2(makeString($1), $3);
        }
    | map_keyval_list ',' property_key_name ':' cypher_a_expr
        {
            $$ = lappend(lappend($1, makeString($3)), $5);
        }
    ;

list:
    '[' cypher_expr_list_opt ']'
        {
            cypher_list *n;

            n = make_ag_node(cypher_list);
            n->elems = $2;

            $$ = (Node *)n;
        }
    ;

expr_case:
    CASE cypher_a_expr expr_case_when_list expr_case_default END_P
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
    WHEN cypher_a_expr THEN cypher_a_expr
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
    ELSE cypher_a_expr
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
cypher_func_name:
    symbolic_name
        {
            $$ = list_make1(makeString($1));
        }
    /*
     * symbolic_name '.' symbolic_name is already covered with the
     * rule expr '.' expr above. This rule is to allow most reserved
     * keywords to be used as well. So, it essentially makes the
     * rule schema_name '.' symbolic_name for func_name
     * NOTE: This rule is incredibly stupid and needs to go
     */
    | safe_keywords '.' symbolic_name
        {
            $$ = list_make2(makeString((char *)$1), makeString($3));
        }
    ;

property_key_name:
    schema_name
    ;

cypher_var_name:
    symbolic_name
    ;

var_name_opt:
    /* empty */
        {
            $$ = NULL;
        }
    | cypher_var_name
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
    | cypher_reserved_keyword
        {
            /* we don't need to copy it, as it already has been */
            $$ = (char *) $1;
        }
    ;

cypher_reserved_keyword:
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
  //  | WITH       { $$ = pnstrdup($1, 4); }
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

static void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
		v->val.str = psprintf("-%s", oldval);
}


static Node *
makeColumnRef(char *colname, List *indirection,
			  int location, ag_scanner_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}


/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
static List *
check_indirection(List *indirection, ag_scanner_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
static Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
static void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					SelectLimit *limitClause,
					WithClause *withClause,
					ag_scanner_t yyscanner)
{
	Assert(IsA(stmt, SelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed")));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitClause && limitClause->limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed")));
		stmt->limitOffset = limitClause->limitOffset;
	}
	if (limitClause && limitClause->limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed")));
		stmt->limitCount = limitClause->limitCount;
	}
	if (limitClause && limitClause->limitOption != LIMIT_OPTION_DEFAULT)
	{
		if (stmt->limitOption)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple limit options not allowed")));
		if (!stmt->sortClause && limitClause->limitOption == LIMIT_OPTION_WITH_TIES)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("WITH TIES cannot be specified without ORDER BY clause")));
		if (limitClause->limitOption == LIMIT_OPTION_WITH_TIES && stmt->lockingClause)
		{
			ListCell   *lc;

			foreach(lc, stmt->lockingClause)
			{
				LockingClause *lock = lfirst_node(LockingClause, lc);

				if (lock->waitPolicy == LockWaitSkip)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("%s and %s options cannot be used together",
									"SKIP LOCKED", "WITH TIES")));
			}
		}
		stmt->limitOption = limitClause->limitOption;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed")));
		stmt->withClause = withClause;
	}
}

static Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}

static Node *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typeName = typename;
	n->location = location;
	return (Node *) n;
}

static Node *
makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

static Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *)n;
}

static Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

static Node *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = (state ? "t" : "f");
	n->location = location;

	return makeTypeCast((Node *)n, SystemTypeName("bool"), -1);
}

static Node *
makeNullAConst(int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Null;
	n->location = location;

	return (Node *)n;
}


static Node *
makeAndExpr(Node *lexpr, Node *rexpr, int location)
{
	/* Flatten "a AND b AND c ..." to a single BoolExpr on sight */
	if (IsA(lexpr, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexpr;

		if (blexpr->boolop == AND_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *
makeOrExpr(Node *lexpr, Node *rexpr, int location)
{
	/* Flatten "a OR b OR c ..." to a single BoolExpr on sight */
	if (IsA(lexpr, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexpr;

		if (blexpr->boolop == OR_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *
makeNotExpr(Node *expr, int location)
{
	return (Node *) makeBoolExpr(NOT_EXPR, list_make1(expr), location);
}

static Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}


/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
static List *
check_func_name(List *names, ag_scanner_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
	return names;
}

static Node *
makeAArrayExpr(List *elements, int location)
{
	A_ArrayExpr *n = makeNode(A_ArrayExpr);

	n->elements = elements;
	n->location = location;
	return (Node *) n;
}


static Node *
makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args,
			int location)
{
	XmlExpr		*x = makeNode(XmlExpr);

	x->op = op;
	x->name = name;
	/*
	 * named_args is a list of ResTarget; it'll be split apart into separate
	 * expression and name lists in transformXmlExpr().
	 */
	x->named_args = named_args;
	x->arg_names = NIL;
	x->args = args;
	/* xmloption, if relevant, must be filled in by caller */
	/* type and typmod will be filled in during parse analysis */
	x->type = InvalidOid;			/* marks the node as not analyzed */
	x->location = location;
	return (Node *) x;
}


/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, ag_scanner_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

/* makeRoleSpec
 * Create a RoleSpec with the given type
 */
static RoleSpec *
makeRoleSpec(RoleSpecType type, int location)
{
	RoleSpec *spec = makeNode(RoleSpec);

	spec->roletype = type;
	spec->location = location;

	return spec;
}


/* extractArgTypes()
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for input parameters only.  This is what
 * is needed to look up an existing function, which is what is wanted by
 * the productions that use this call.
 */
static List *
extractArgTypes(List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		if (p->mode != FUNC_PARAM_OUT && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
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

/* Adjust a RawStmt to reflect that it doesn't run to the end of the string */
static void
updateRawStmtEnd(RawStmt *rs, int end_location)
{
	/*
	 * If we already set the length, don't change it.  This is for situations
	 * like "select foo ;; select bar" where the same statement will be last
	 * in the string for more than one semicolon.
	 */
	if (rs->stmt_len > 0)
		return;

	/* OK, update length of RawStmt */
	rs->stmt_len = end_location - rs->stmt_location;
}


/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
static TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

/*
 * Merge the input and output parameters of a table function.
 */
static List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		if (p->mode != FUNC_PARAM_DEFAULT &&
			p->mode != FUNC_PARAM_IN &&
			p->mode != FUNC_PARAM_VARIADIC)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("OUT and INOUT arguments aren't allowed in TABLE functions")));
	}

	return list_concat(func_args, columns);
}

static Node *
makeStringConstCast(char *str, int location, TypeName *typename)
{
	Node *s = makeStringConst(str, location);

	return makeTypeCast(s, typename, -1);
}


static Node *
makeBitStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_BitString;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}
