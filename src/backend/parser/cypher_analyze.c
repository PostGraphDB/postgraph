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

#include "access/skey.h"
#include "access/relscan.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "utils/rel.h"
#include "access/table.h"

#include "utils/ag_cache.h"
#include "catalog/ag_graph.h"
#include "nodes/ag_nodes.h"
#include "parser/cypher_analyze.h"
#include "parser/cypher_clause.h"
#include "parser/cypher_item.h"
#include "parser/cypher_parse_node.h"
#include "parser/cypher_parser.h"
#include "utils/ag_func.h"
#include "utils/gtype.h"

static void post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static Name expr_get_const_name(Node *expr);
static const char *expr_get_const_cstring(Node *expr, const char *source_str);
static int get_query_location(const int location, const char *source_str);
static Query *analyze_cypher(List *stmt, ParseState *parent_pstate, const char *query_str, int query_loc, char *graph_name, uint32 graph_oid, Param *params);
static List *cypher_parse(char *string);
static List * cypher_parse_analyze_hook (RawStmt *parsetree, const char *query_string,
                                         Oid *paramTypes, int numParams,
                                         QueryEnvironment *queryEnv);


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
    rs->stmt_len = 0;            /* might get changed later */
    return rs;
}

static List *cypher_parse(char *string){
    List       *raw_parsetree_list;

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
        List       *new_list = copyObject(raw_parsetree_list);

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


/*
 * AlterObjectTypeCommandTag
 *        helper function for CreateCommandTag
 *
 * This covers most cases where ALTER is used with an ObjectType enum.
 */
static CommandTag
AlterObjectTypeCommandTag(ObjectType objtype)
{
    CommandTag    tag;

    switch (objtype)
    {
        case OBJECT_AGGREGATE:
            tag = CMDTAG_ALTER_AGGREGATE;
            break;
        case OBJECT_ATTRIBUTE:
            tag = CMDTAG_ALTER_TYPE;
            break;
        case OBJECT_CAST:
            tag = CMDTAG_ALTER_CAST;
            break;
        case OBJECT_COLLATION:
            tag = CMDTAG_ALTER_COLLATION;
            break;
        case OBJECT_COLUMN:
            tag = CMDTAG_ALTER_TABLE;
            break;
        case OBJECT_CONVERSION:
            tag = CMDTAG_ALTER_CONVERSION;
            break;
        case OBJECT_DATABASE:
            tag = CMDTAG_ALTER_DATABASE;
            break;
        case OBJECT_DOMAIN:
        case OBJECT_DOMCONSTRAINT:
            tag = CMDTAG_ALTER_DOMAIN;
            break;
        case OBJECT_EXTENSION:
            tag = CMDTAG_ALTER_EXTENSION;
            break;
        case OBJECT_FDW:
            tag = CMDTAG_ALTER_FOREIGN_DATA_WRAPPER;
            break;
        case OBJECT_FOREIGN_SERVER:
            tag = CMDTAG_ALTER_SERVER;
            break;
        case OBJECT_FOREIGN_TABLE:
            tag = CMDTAG_ALTER_FOREIGN_TABLE;
            break;
        case OBJECT_FUNCTION:
            tag = CMDTAG_ALTER_FUNCTION;
            break;
        case OBJECT_INDEX:
            tag = CMDTAG_ALTER_INDEX;
            break;
        case OBJECT_LANGUAGE:
            tag = CMDTAG_ALTER_LANGUAGE;
            break;
        case OBJECT_LARGEOBJECT:
            tag = CMDTAG_ALTER_LARGE_OBJECT;
            break;
        case OBJECT_OPCLASS:
            tag = CMDTAG_ALTER_OPERATOR_CLASS;
            break;
        case OBJECT_OPERATOR:
            tag = CMDTAG_ALTER_OPERATOR;
            break;
        case OBJECT_OPFAMILY:
            tag = CMDTAG_ALTER_OPERATOR_FAMILY;
            break;
        case OBJECT_POLICY:
            tag = CMDTAG_ALTER_POLICY;
            break;
        case OBJECT_PROCEDURE:
            tag = CMDTAG_ALTER_PROCEDURE;
            break;
        case OBJECT_ROLE:
            tag = CMDTAG_ALTER_ROLE;
            break;
        case OBJECT_ROUTINE:
            tag = CMDTAG_ALTER_ROUTINE;
            break;
        case OBJECT_RULE:
            tag = CMDTAG_ALTER_RULE;
            break;
        case OBJECT_SCHEMA:
            tag = CMDTAG_ALTER_SCHEMA;
            break;
        case OBJECT_SEQUENCE:
            tag = CMDTAG_ALTER_SEQUENCE;
            break;
        case OBJECT_TABLE:
        case OBJECT_TABCONSTRAINT:
            tag = CMDTAG_ALTER_TABLE;
            break;
        case OBJECT_TABLESPACE:
            tag = CMDTAG_ALTER_TABLESPACE;
            break;
        case OBJECT_TRIGGER:
            tag = CMDTAG_ALTER_TRIGGER;
            break;
        case OBJECT_EVENT_TRIGGER:
            tag = CMDTAG_ALTER_EVENT_TRIGGER;
            break;
        case OBJECT_TSCONFIGURATION:
            tag = CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION;
            break;
        case OBJECT_TSDICTIONARY:
            tag = CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY;
            break;
        case OBJECT_TSPARSER:
            tag = CMDTAG_ALTER_TEXT_SEARCH_PARSER;
            break;
        case OBJECT_TSTEMPLATE:
            tag = CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE;
            break;
        case OBJECT_TYPE:
            tag = CMDTAG_ALTER_TYPE;
            break;
        case OBJECT_VIEW:
            tag = CMDTAG_ALTER_VIEW;
            break;
        case OBJECT_MATVIEW:
            tag = CMDTAG_ALTER_MATERIALIZED_VIEW;
            break;
        case OBJECT_PUBLICATION:
            tag = CMDTAG_ALTER_PUBLICATION;
            break;
        case OBJECT_SUBSCRIPTION:
            tag = CMDTAG_ALTER_SUBSCRIPTION;
            break;
        case OBJECT_STATISTIC_EXT:
            tag = CMDTAG_ALTER_STATISTICS;
            break;
        default:
            tag = CMDTAG_UNKNOWN;
            break;
    }

    return tag;
}

static CommandTag
CypherCreateCommandTag(Node *parsetree)
{
    CommandTag    tag;

    switch (nodeTag(parsetree))
    {
            /* recurse if we're given a RawStmt */
        case T_RawStmt:
            tag = CreateCommandTag(((RawStmt *) parsetree)->stmt);
            break;

            /* raw plannable queries */
        case T_InsertStmt:
            tag = CMDTAG_INSERT;
            break;

        case T_DeleteStmt:
            tag = CMDTAG_DELETE;
            break;

        case T_UpdateStmt:
            tag = CMDTAG_UPDATE;
            break;

        case T_SelectStmt:
            tag = CMDTAG_SELECT;
            break;

        case T_PLAssignStmt:
            tag = CMDTAG_SELECT;
            break;

            /* utility statements --- same whether raw or cooked */
        case T_TransactionStmt:
            {
                TransactionStmt *stmt = (TransactionStmt *) parsetree;

                switch (stmt->kind)
                {
                    case TRANS_STMT_BEGIN:
                        tag = CMDTAG_BEGIN;
                        break;

                    case TRANS_STMT_START:
                        tag = CMDTAG_START_TRANSACTION;
                        break;

                    case TRANS_STMT_COMMIT:
                        tag = CMDTAG_COMMIT;
                        break;

                    case TRANS_STMT_ROLLBACK:
                    case TRANS_STMT_ROLLBACK_TO:
                        tag = CMDTAG_ROLLBACK;
                        break;

                    case TRANS_STMT_SAVEPOINT:
                        tag = CMDTAG_SAVEPOINT;
                        break;

                    case TRANS_STMT_RELEASE:
                        tag = CMDTAG_RELEASE;
                        break;

                    case TRANS_STMT_PREPARE:
                        tag = CMDTAG_PREPARE_TRANSACTION;
                        break;

                    case TRANS_STMT_COMMIT_PREPARED:
                        tag = CMDTAG_COMMIT_PREPARED;
                        break;

                    case TRANS_STMT_ROLLBACK_PREPARED:
                        tag = CMDTAG_ROLLBACK_PREPARED;
                        break;

                    default:
                        tag = CMDTAG_UNKNOWN;
                        break;
                }
            }
            break;

        case T_DeclareCursorStmt:
            tag = CMDTAG_DECLARE_CURSOR;
            break;

        case T_ClosePortalStmt:
            {
                ClosePortalStmt *stmt = (ClosePortalStmt *) parsetree;

                if (stmt->portalname == NULL)
                    tag = CMDTAG_CLOSE_CURSOR_ALL;
                else
                    tag = CMDTAG_CLOSE_CURSOR;
            }
            break;

        case T_FetchStmt:
            {
                FetchStmt  *stmt = (FetchStmt *) parsetree;

                tag = (stmt->ismove) ? CMDTAG_MOVE : CMDTAG_FETCH;
            }
            break;

        case T_CreateDomainStmt:
            tag = CMDTAG_CREATE_DOMAIN;
            break;

        case T_CreateSchemaStmt:
            tag = CMDTAG_CREATE_SCHEMA;
            break;

        case T_CreateStmt:
            tag = CMDTAG_CREATE_TABLE;
            break;

        case T_CreateTableSpaceStmt:
            tag = CMDTAG_CREATE_TABLESPACE;
            break;

        case T_DropTableSpaceStmt:
            tag = CMDTAG_DROP_TABLESPACE;
            break;

        case T_AlterTableSpaceOptionsStmt:
            tag = CMDTAG_ALTER_TABLESPACE;
            break;

        case T_CreateExtensionStmt:
            tag = CMDTAG_CREATE_EXTENSION;
            break;

        case T_AlterExtensionStmt:
            tag = CMDTAG_ALTER_EXTENSION;
            break;

        case T_AlterExtensionContentsStmt:
            tag = CMDTAG_ALTER_EXTENSION;
            break;

        case T_CreateFdwStmt:
            tag = CMDTAG_CREATE_FOREIGN_DATA_WRAPPER;
            break;

        case T_AlterFdwStmt:
            tag = CMDTAG_ALTER_FOREIGN_DATA_WRAPPER;
            break;

        case T_CreateForeignServerStmt:
            tag = CMDTAG_CREATE_SERVER;
            break;

        case T_AlterForeignServerStmt:
            tag = CMDTAG_ALTER_SERVER;
            break;

        case T_CreateUserMappingStmt:
            tag = CMDTAG_CREATE_USER_MAPPING;
            break;

        case T_AlterUserMappingStmt:
            tag = CMDTAG_ALTER_USER_MAPPING;
            break;

        case T_DropUserMappingStmt:
            tag = CMDTAG_DROP_USER_MAPPING;
            break;

        case T_CreateForeignTableStmt:
            tag = CMDTAG_CREATE_FOREIGN_TABLE;
            break;

        case T_ImportForeignSchemaStmt:
            tag = CMDTAG_IMPORT_FOREIGN_SCHEMA;
            break;

        case T_DropStmt:
            switch (((DropStmt *) parsetree)->removeType)
            {
                case OBJECT_TABLE:
                    tag = CMDTAG_DROP_TABLE;
                    break;
                case OBJECT_SEQUENCE:
                    tag = CMDTAG_DROP_SEQUENCE;
                    break;
                case OBJECT_VIEW:
                    tag = CMDTAG_DROP_VIEW;
                    break;
                case OBJECT_MATVIEW:
                    tag = CMDTAG_DROP_MATERIALIZED_VIEW;
                    break;
                case OBJECT_INDEX:
                    tag = CMDTAG_DROP_INDEX;
                    break;
                case OBJECT_TYPE:
                    tag = CMDTAG_DROP_TYPE;
                    break;
                case OBJECT_DOMAIN:
                    tag = CMDTAG_DROP_DOMAIN;
                    break;
                case OBJECT_COLLATION:
                    tag = CMDTAG_DROP_COLLATION;
                    break;
                case OBJECT_CONVERSION:
                    tag = CMDTAG_DROP_CONVERSION;
                    break;
                case OBJECT_SCHEMA:
                    tag = CMDTAG_DROP_SCHEMA;
                    break;
                case OBJECT_TSPARSER:
                    tag = CMDTAG_DROP_TEXT_SEARCH_PARSER;
                    break;
                case OBJECT_TSDICTIONARY:
                    tag = CMDTAG_DROP_TEXT_SEARCH_DICTIONARY;
                    break;
                case OBJECT_TSTEMPLATE:
                    tag = CMDTAG_DROP_TEXT_SEARCH_TEMPLATE;
                    break;
                case OBJECT_TSCONFIGURATION:
                    tag = CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION;
                    break;
                case OBJECT_FOREIGN_TABLE:
                    tag = CMDTAG_DROP_FOREIGN_TABLE;
                    break;
                case OBJECT_EXTENSION:
                    tag = CMDTAG_DROP_EXTENSION;
                    break;
                case OBJECT_FUNCTION:
                    tag = CMDTAG_DROP_FUNCTION;
                    break;
                case OBJECT_PROCEDURE:
                    tag = CMDTAG_DROP_PROCEDURE;
                    break;
                case OBJECT_ROUTINE:
                    tag = CMDTAG_DROP_ROUTINE;
                    break;
                case OBJECT_AGGREGATE:
                    tag = CMDTAG_DROP_AGGREGATE;
                    break;
                case OBJECT_OPERATOR:
                    tag = CMDTAG_DROP_OPERATOR;
                    break;
                case OBJECT_LANGUAGE:
                    tag = CMDTAG_DROP_LANGUAGE;
                    break;
                case OBJECT_CAST:
                    tag = CMDTAG_DROP_CAST;
                    break;
                case OBJECT_TRIGGER:
                    tag = CMDTAG_DROP_TRIGGER;
                    break;
                case OBJECT_EVENT_TRIGGER:
                    tag = CMDTAG_DROP_EVENT_TRIGGER;
                    break;
                case OBJECT_RULE:
                    tag = CMDTAG_DROP_RULE;
                    break;
                case OBJECT_FDW:
                    tag = CMDTAG_DROP_FOREIGN_DATA_WRAPPER;
                    break;
                case OBJECT_FOREIGN_SERVER:
                    tag = CMDTAG_DROP_SERVER;
                    break;
                case OBJECT_OPCLASS:
                    tag = CMDTAG_DROP_OPERATOR_CLASS;
                    break;
                case OBJECT_OPFAMILY:
                    tag = CMDTAG_DROP_OPERATOR_FAMILY;
                    break;
                case OBJECT_POLICY:
                    tag = CMDTAG_DROP_POLICY;
                    break;
                case OBJECT_TRANSFORM:
                    tag = CMDTAG_DROP_TRANSFORM;
                    break;
                case OBJECT_ACCESS_METHOD:
                    tag = CMDTAG_DROP_ACCESS_METHOD;
                    break;
                case OBJECT_PUBLICATION:
                    tag = CMDTAG_DROP_PUBLICATION;
                    break;
                case OBJECT_STATISTIC_EXT:
                    tag = CMDTAG_DROP_STATISTICS;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_TruncateStmt:
            tag = CMDTAG_TRUNCATE_TABLE;
            break;

        case T_CommentStmt:
            tag = CMDTAG_COMMENT;
            break;

        case T_SecLabelStmt:
            tag = CMDTAG_SECURITY_LABEL;
            break;

        case T_CopyStmt:
            tag = CMDTAG_COPY;
            break;

        case T_RenameStmt:

            /*
             * When the column is renamed, the command tag is created from its
             * relation type
             */
            tag = AlterObjectTypeCommandTag(((RenameStmt *) parsetree)->renameType == OBJECT_COLUMN ?
                                            ((RenameStmt *) parsetree)->relationType :
                                            ((RenameStmt *) parsetree)->renameType);
            break;

        case T_AlterObjectDependsStmt:
            tag = AlterObjectTypeCommandTag(((AlterObjectDependsStmt *) parsetree)->objectType);
            break;

        case T_AlterObjectSchemaStmt:
            tag = AlterObjectTypeCommandTag(((AlterObjectSchemaStmt *) parsetree)->objectType);
            break;

        case T_AlterOwnerStmt:
            tag = AlterObjectTypeCommandTag(((AlterOwnerStmt *) parsetree)->objectType);
            break;

        case T_AlterTableMoveAllStmt:
            tag = AlterObjectTypeCommandTag(((AlterTableMoveAllStmt *) parsetree)->objtype);
            break;

        case T_AlterTableStmt:
            tag = AlterObjectTypeCommandTag(((AlterTableStmt *) parsetree)->objtype);
            break;

        case T_AlterDomainStmt:
            tag = CMDTAG_ALTER_DOMAIN;
            break;

        case T_AlterFunctionStmt:
            switch (((AlterFunctionStmt *) parsetree)->objtype)
            {
                case OBJECT_FUNCTION:
                    tag = CMDTAG_ALTER_FUNCTION;
                    break;
                case OBJECT_PROCEDURE:
                    tag = CMDTAG_ALTER_PROCEDURE;
                    break;
                case OBJECT_ROUTINE:
                    tag = CMDTAG_ALTER_ROUTINE;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_GrantStmt:
            {
                GrantStmt  *stmt = (GrantStmt *) parsetree;

                tag = (stmt->is_grant) ? CMDTAG_GRANT : CMDTAG_REVOKE;
            }
            break;

        case T_GrantRoleStmt:
            {
                GrantRoleStmt *stmt = (GrantRoleStmt *) parsetree;

                tag = (stmt->is_grant) ? CMDTAG_GRANT_ROLE : CMDTAG_REVOKE_ROLE;
            }
            break;

        case T_AlterDefaultPrivilegesStmt:
            tag = CMDTAG_ALTER_DEFAULT_PRIVILEGES;
            break;

        case T_DefineStmt:
            switch (((DefineStmt *) parsetree)->kind)
            {
                case OBJECT_AGGREGATE:
                    tag = CMDTAG_CREATE_AGGREGATE;
                    break;
                case OBJECT_OPERATOR:
                    tag = CMDTAG_CREATE_OPERATOR;
                    break;
                case OBJECT_TYPE:
                    tag = CMDTAG_CREATE_TYPE;
                    break;
                case OBJECT_TSPARSER:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_PARSER;
                    break;
                case OBJECT_TSDICTIONARY:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY;
                    break;
                case OBJECT_TSTEMPLATE:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE;
                    break;
                case OBJECT_TSCONFIGURATION:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION;
                    break;
                case OBJECT_COLLATION:
                    tag = CMDTAG_CREATE_COLLATION;
                    break;
                case OBJECT_ACCESS_METHOD:
                    tag = CMDTAG_CREATE_ACCESS_METHOD;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_CompositeTypeStmt:
            tag = CMDTAG_CREATE_TYPE;
            break;

        case T_CreateEnumStmt:
            tag = CMDTAG_CREATE_TYPE;
            break;

        case T_CreateRangeStmt:
            tag = CMDTAG_CREATE_TYPE;
            break;

        case T_AlterEnumStmt:
            tag = CMDTAG_ALTER_TYPE;
            break;

        case T_ViewStmt:
            tag = CMDTAG_CREATE_VIEW;
            break;

        case T_CreateFunctionStmt:
            if (((CreateFunctionStmt *) parsetree)->is_procedure)
                tag = CMDTAG_CREATE_PROCEDURE;
            else
                tag = CMDTAG_CREATE_FUNCTION;
            break;

        case T_IndexStmt:
            tag = CMDTAG_CREATE_INDEX;
            break;

        case T_RuleStmt:
            tag = CMDTAG_CREATE_RULE;
            break;

        case T_CreateSeqStmt:
            tag = CMDTAG_CREATE_SEQUENCE;
            break;

        case T_AlterSeqStmt:
            tag = CMDTAG_ALTER_SEQUENCE;
            break;

        case T_DoStmt:
            tag = CMDTAG_DO;
            break;

        case T_CreatedbStmt:
            tag = CMDTAG_CREATE_DATABASE;
            break;

        case T_AlterDatabaseStmt:
            tag = CMDTAG_ALTER_DATABASE;
            break;

        case T_AlterDatabaseSetStmt:
            tag = CMDTAG_ALTER_DATABASE;
            break;

        case T_DropdbStmt:
            tag = CMDTAG_DROP_DATABASE;
            break;

        case T_NotifyStmt:
            tag = CMDTAG_NOTIFY;
            break;

        case T_ListenStmt:
            tag = CMDTAG_LISTEN;
            break;

        case T_UnlistenStmt:
            tag = CMDTAG_UNLISTEN;
            break;

        case T_LoadStmt:
            tag = CMDTAG_LOAD;
            break;

        case T_CallStmt:
            tag = CMDTAG_CALL;
            break;

        case T_ClusterStmt:
            tag = CMDTAG_CLUSTER;
            break;

        case T_VacuumStmt:
            if (((VacuumStmt *) parsetree)->is_vacuumcmd)
                tag = CMDTAG_VACUUM;
            else
                tag = CMDTAG_ANALYZE;
            break;

        case T_ExplainStmt:
            tag = CMDTAG_EXPLAIN;
            break;

        case T_CreateTableAsStmt:
            switch (((CreateTableAsStmt *) parsetree)->objtype)
            {
                case OBJECT_TABLE:
                    if (((CreateTableAsStmt *) parsetree)->is_select_into)
                        tag = CMDTAG_SELECT_INTO;
                    else
                        tag = CMDTAG_CREATE_TABLE_AS;
                    break;
                case OBJECT_MATVIEW:
                    tag = CMDTAG_CREATE_MATERIALIZED_VIEW;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_RefreshMatViewStmt:
            tag = CMDTAG_REFRESH_MATERIALIZED_VIEW;
            break;

        case T_AlterSystemStmt:
            tag = CMDTAG_ALTER_SYSTEM;
            break;

        case T_VariableSetStmt:
            switch (((VariableSetStmt *) parsetree)->kind)
            {
                case VAR_SET_VALUE:
                case VAR_SET_CURRENT:
                case VAR_SET_DEFAULT:
                case VAR_SET_MULTI:
                    tag = CMDTAG_SET;
                    break;
                case VAR_RESET:
                case VAR_RESET_ALL:
                    tag = CMDTAG_RESET;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_VariableShowStmt:
            tag = CMDTAG_SHOW;
            break;

        case T_DiscardStmt:
            switch (((DiscardStmt *) parsetree)->target)
            {
                case DISCARD_ALL:
                    tag = CMDTAG_DISCARD_ALL;
                    break;
                case DISCARD_PLANS:
                    tag = CMDTAG_DISCARD_PLANS;
                    break;
                case DISCARD_TEMP:
                    tag = CMDTAG_DISCARD_TEMP;
                    break;
                case DISCARD_SEQUENCES:
                    tag = CMDTAG_DISCARD_SEQUENCES;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

        case T_CreateTransformStmt:
            tag = CMDTAG_CREATE_TRANSFORM;
            break;

        case T_CreateTrigStmt:
            tag = CMDTAG_CREATE_TRIGGER;
            break;

        case T_CreateEventTrigStmt:
            tag = CMDTAG_CREATE_EVENT_TRIGGER;
            break;

        case T_AlterEventTrigStmt:
            tag = CMDTAG_ALTER_EVENT_TRIGGER;
            break;

        case T_CreatePLangStmt:
            tag = CMDTAG_CREATE_LANGUAGE;
            break;

        case T_CreateRoleStmt:
            tag = CMDTAG_CREATE_ROLE;
            break;

        case T_AlterRoleStmt:
            tag = CMDTAG_ALTER_ROLE;
            break;

        case T_AlterRoleSetStmt:
            tag = CMDTAG_ALTER_ROLE;
            break;

        case T_DropRoleStmt:
            tag = CMDTAG_DROP_ROLE;
            break;

        case T_DropOwnedStmt:
            tag = CMDTAG_DROP_OWNED;
            break;

        case T_ReassignOwnedStmt:
            tag = CMDTAG_REASSIGN_OWNED;
            break;

        case T_LockStmt:
            tag = CMDTAG_LOCK_TABLE;
            break;

        case T_ConstraintsSetStmt:
            tag = CMDTAG_SET_CONSTRAINTS;
            break;

        case T_CheckPointStmt:
            tag = CMDTAG_CHECKPOINT;
            break;

        case T_ReindexStmt:
            tag = CMDTAG_REINDEX;
            break;

        case T_CreateConversionStmt:
            tag = CMDTAG_CREATE_CONVERSION;
            break;

        case T_CreateCastStmt:
            tag = CMDTAG_CREATE_CAST;
            break;

        case T_CreateOpClassStmt:
            tag = CMDTAG_CREATE_OPERATOR_CLASS;
            break;

        case T_CreateOpFamilyStmt:
            tag = CMDTAG_CREATE_OPERATOR_FAMILY;
            break;

        case T_AlterOpFamilyStmt:
            tag = CMDTAG_ALTER_OPERATOR_FAMILY;
            break;

        case T_AlterOperatorStmt:
            tag = CMDTAG_ALTER_OPERATOR;
            break;

        case T_AlterTypeStmt:
            tag = CMDTAG_ALTER_TYPE;
            break;

        case T_AlterTSDictionaryStmt:
            tag = CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY;
            break;

        case T_AlterTSConfigurationStmt:
            tag = CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION;
            break;

        case T_CreatePolicyStmt:
            tag = CMDTAG_CREATE_POLICY;
            break;

        case T_AlterPolicyStmt:
            tag = CMDTAG_ALTER_POLICY;
            break;

        case T_CreateAmStmt:
            tag = CMDTAG_CREATE_ACCESS_METHOD;
            break;

        case T_CreatePublicationStmt:
            tag = CMDTAG_CREATE_PUBLICATION;
            break;

        case T_AlterPublicationStmt:
            tag = CMDTAG_ALTER_PUBLICATION;
            break;

        case T_CreateSubscriptionStmt:
            tag = CMDTAG_CREATE_SUBSCRIPTION;
            break;

        case T_AlterSubscriptionStmt:
            tag = CMDTAG_ALTER_SUBSCRIPTION;
            break;

        case T_DropSubscriptionStmt:
            tag = CMDTAG_DROP_SUBSCRIPTION;
            break;

        case T_AlterCollationStmt:
            tag = CMDTAG_ALTER_COLLATION;
            break;

        case T_PrepareStmt:
            tag = CMDTAG_PREPARE;
            break;

        case T_ExecuteStmt:
            tag = CMDTAG_EXECUTE;
            break;

        case T_CreateStatsStmt:
            tag = CMDTAG_CREATE_STATISTICS;
            break;

        case T_AlterStatsStmt:
            tag = CMDTAG_ALTER_STATISTICS;
            break;

        case T_DeallocateStmt:
            {
                DeallocateStmt *stmt = (DeallocateStmt *) parsetree;

                if (stmt->name == NULL)
                    tag = CMDTAG_DEALLOCATE_ALL;
                else
                    tag = CMDTAG_DEALLOCATE;
            }
            break;

            /* already-planned queries */
        case T_PlannedStmt:
            {
                PlannedStmt *stmt = (PlannedStmt *) parsetree;

                switch (stmt->commandType)
                {
                    case CMD_SELECT:

                        /*
                         * We take a little extra care here so that the result
                         * will be useful for complaints about read-only
                         * statements
                         */
                        if (stmt->rowMarks != NIL)
                        {
                            /* not 100% but probably close enough */
                            switch (((PlanRowMark *) linitial(stmt->rowMarks))->strength)
                            {
                                case LCS_FORKEYSHARE:
                                    tag = CMDTAG_SELECT_FOR_KEY_SHARE;
                                    break;
                                case LCS_FORSHARE:
                                    tag = CMDTAG_SELECT_FOR_SHARE;
                                    break;
                                case LCS_FORNOKEYUPDATE:
                                    tag = CMDTAG_SELECT_FOR_NO_KEY_UPDATE;
                                    break;
                                case LCS_FORUPDATE:
                                    tag = CMDTAG_SELECT_FOR_UPDATE;
                                    break;
                                default:
                                    tag = CMDTAG_SELECT;
                                    break;
                            }
                        }
                        else
                            tag = CMDTAG_SELECT;
                        break;
                    case CMD_UPDATE:
                        tag = CMDTAG_UPDATE;
                        break;
                    case CMD_INSERT:
                        tag = CMDTAG_INSERT;
                        break;
                    case CMD_DELETE:
                        tag = CMDTAG_DELETE;
                        break;
                    case CMD_UTILITY:
                        tag = CreateCommandTag(stmt->utilityStmt);
                        break;
                    default:
                        elog(WARNING, "unrecognized commandType: %d",
                             (int) stmt->commandType);
                        tag = CMDTAG_UNKNOWN;
                        break;
                }
            }
            break;

            /* parsed-and-rewritten-but-not-planned queries */
        case T_Query:
            {
                Query       *stmt = (Query *) parsetree;

                switch (stmt->commandType)
                {
                    case CMD_SELECT:

                        /*
                         * We take a little extra care here so that the result
                         * will be useful for complaints about read-only
                         * statements
                         */
                        if (stmt->rowMarks != NIL)
                        {
                            /* not 100% but probably close enough */
                            switch (((RowMarkClause *) linitial(stmt->rowMarks))->strength)
                            {
                                case LCS_FORKEYSHARE:
                                    tag = CMDTAG_SELECT_FOR_KEY_SHARE;
                                    break;
                                case LCS_FORSHARE:
                                    tag = CMDTAG_SELECT_FOR_SHARE;
                                    break;
                                case LCS_FORNOKEYUPDATE:
                                    tag = CMDTAG_SELECT_FOR_NO_KEY_UPDATE;
                                    break;
                                case LCS_FORUPDATE:
                                    tag = CMDTAG_SELECT_FOR_UPDATE;
                                    break;
                                default:
                                    tag = CMDTAG_UNKNOWN;
                                    break;
                            }
                        }
                        else
                            tag = CMDTAG_SELECT;
                        break;
                    case CMD_UPDATE:
                        tag = CMDTAG_UPDATE;
                        break;
                    case CMD_INSERT:
                        tag = CMDTAG_INSERT;
                        break;
                    case CMD_DELETE:
                        tag = CMDTAG_DELETE;
                        break;
                    case CMD_UTILITY:
                        tag = CreateCommandTag(stmt->utilityStmt);
                        break;
                    default:
                        elog(WARNING, "unrecognized commandType: %d",
                             (int) stmt->commandType);
                        tag = CMDTAG_UNKNOWN;
                        break;
                }
            }
            break;

        default:
        {
            if (IsA(parsetree, List)) {

                List *lst = parsetree;
                if (list_length(lst) == 1) {
                    Node *n = linitial(lst);

                    if (IsA(n, CreateExtensionStmt)) {
	                    tag = CMDTAG_CREATE_EXTENSION;
                        break;
                    } else if (IsA(n, CreateStmt)) {
	                    tag = CMDTAG_CREATE_TABLE;
                        break;
                    } else if (IsA(n, InsertStmt)) {
                        tag = CMDTAG_INSERT;
                        break;
                    } else if (IsA(n, DeleteStmt)) {
                        tag = CMDTAG_DELETE;
                        break;
                    } else if (IsA(n, CreateFunctionStmt)) {
            if (((CreateFunctionStmt *) parsetree)->is_procedure)
                tag = CMDTAG_CREATE_PROCEDURE;
            else
                tag = CMDTAG_CREATE_FUNCTION;
            break;

                    } else if (IsA(n, DefineStmt)) {
            switch (((DefineStmt *) parsetree)->kind)
            {
                case OBJECT_AGGREGATE:
                    tag = CMDTAG_CREATE_AGGREGATE;
                    break;
                case OBJECT_OPERATOR:
                    tag = CMDTAG_CREATE_OPERATOR;
                    break;
                case OBJECT_TYPE:
                    tag = CMDTAG_CREATE_TYPE;
                    break;
                case OBJECT_TSPARSER:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_PARSER;
                    break;
                case OBJECT_TSDICTIONARY:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY;
                    break;
                case OBJECT_TSTEMPLATE:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE;
                    break;
                case OBJECT_TSCONFIGURATION:
                    tag = CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION;
                    break;
                case OBJECT_COLLATION:
                    tag = CMDTAG_CREATE_COLLATION;
                    break;
                case OBJECT_ACCESS_METHOD:
                    tag = CMDTAG_CREATE_ACCESS_METHOD;
                    break;
                default:
                    tag = CMDTAG_UNKNOWN;
            }
            break;

                    } else if (IsA(n, VariableSetStmt)) {
                        switch (((VariableSetStmt *) parsetree)->kind)
                        {
                            case VAR_SET_VALUE:
                            case VAR_SET_CURRENT:
                            case VAR_SET_DEFAULT:
                            case VAR_SET_MULTI:
                                tag = CMDTAG_SET;
                                break;
                            case VAR_RESET:
                            case VAR_RESET_ALL:
                                tag = CMDTAG_RESET;
                                break;
                            default:
                                tag = CMDTAG_UNKNOWN;
                        }
                        break;
                    }
                }
            }

            tag = CMDTAG_SELECT;
            break;
        }
    }

    return tag;
}

void parse_analyze_init(void){
   parse_analyze_hook = cypher_parse_analyze_hook;
   create_command_tag_hook = CypherCreateCommandTag;
}

void parse_analyze_fini(void){
   parse_analyze_hook = NULL;
   create_command_tag_hook = NULL;
}


static FuncExpr *make_utility_func_expr(char *graph_name, char *function_name) {
    Const *c = makeConst(TEXTOID, -1, InvalidOid, strlen(graph_name), CStringGetTextDatum(graph_name), false, false);

    Oid func_oid = get_ag_func_oid("create_graph", 1, TEXTOID);

    return makeFuncExpr(func_oid, VOIDOID, list_make1(c), InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
}

static Query *
cypher_graph_utility(ParseState *pstate, const char *graph_name, char *function_name) {
    Query *query;
    TargetEntry *tle;
    FuncExpr *func_expr;

    query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->targetList = NIL;

    func_expr = make_utility_func_expr(graph_name, function_name);

    // Create the target entry
    tle = makeTargetEntry((Expr *)func_expr, pstate->p_next_resno++, "create_graph", false);
    query->targetList = lappend(query->targetList, tle);

    query->rtable = pstate->p_rtable;
    query->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    return query;
}

/*
 * Creates the function expression that represents the clause. Adds the
 * extensible node that represents the metadata that the clause needs to
 * handle the clause in the execution phase.
 */
static FuncExpr *make_clause_drop_graph_func_expr(char *graph_name) {
    Const *c = makeConst(NAMEOID, -1, InvalidOid, strlen(graph_name), CStringGetTextDatum(graph_name), false, false);
    Const *c1 = makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(true), false, false);
    
    Oid func_oid = get_ag_func_oid("drop_graph", 2, NAMEOID, BOOLOID);

    return makeFuncExpr(func_oid, VOIDOID, list_make2(c, c1), InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
}

static Query *
cypher_drop_graph_utility(ParseState *pstate, const char *graph_name) {
    Query *query;
    TargetEntry *tle;
    FuncExpr *func_expr;

    query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->targetList = NIL;

    func_expr = make_clause_drop_graph_func_expr(graph_name);

    // Create the target entry
    tle = makeTargetEntry((Expr *)func_expr, pstate->p_next_resno++, "drop_graph", false);
    query->targetList = lappend(query->targetList, tle);

    query->rtable = pstate->p_rtable;
    query->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    return query;
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



// Function updates graph name in ag_graph table.
Oid get_session_graph_oid()
{
    ScanKeyData scan_keys[1];
    Relation ag_graph;
    TableScanDesc scan_desc;
    HeapTuple cur_tuple;
    Datum repl_values[2];
    bool repl_isnull[2];
    bool do_replace[2];
    HeapTuple new_tuple;
    bool is_null;

    // open and scan ag_graph for graph name
    ScanKeyInit(&scan_keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(MyProcPid));

    ag_graph = table_open(session_graph_use(), RowExclusiveLock);
    scan_desc = systable_beginscan(ag_graph, session_graph_use_index(), true,
                                   NULL, 1, scan_keys);


    cur_tuple = systable_getnext(scan_desc);

    if (!HeapTupleIsValid(cur_tuple))
        ereport(ERROR, errmsg("Graph is not set", errhint("Run 'USE graph' to choose a graph to run cypher commands")));


    Oid graph_oid = heap_getattr(cur_tuple, 2, RelationGetDescr(ag_graph), &is_null);
    // end scan and close ag_graph
    systable_endscan(scan_desc);
    table_close(ag_graph, RowExclusiveLock);

    return graph_oid;
}

/*
 * parse_analyze
 *        Analyze a raw parse tree and transform it to Query form.
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
    cypher_parsestate *cpstate = pstate;
    Query       *query;
    JumbleState *jstate = NULL;

    Assert(sourceText != NULL); /* required as of 8.4 */

    pstate->p_sourcetext = sourceText;

    if (numParams > 0)
        parse_fixed_parameters(pstate, paramTypes, numParams);

    pstate->p_queryEnv = queryEnv;

    //query = transformTopLevelStmt(pstate, parseTree);
    if (list_length(parseTree->stmt) == 1) {
      Node *n = linitial(parseTree->stmt);

      if (IsA(n, CreateExtensionStmt) || IsA(n,CreateStmt) || IsA(n, SelectStmt) || IsA(n, InsertStmt) ||
          IsA(n, UpdateStmt) || IsA(n, DeleteStmt) || IsA(n, VariableSetStmt) || IsA(n, DefineStmt) ||
          IsA(n, CreateFunctionStmt)) {
	        query = parse_analyze((makeRawStmt(n, 0)), sourceText, paramTypes, numParams, queryEnv);

            return query;
      } else if (is_ag_node(n, cypher_create_graph)) {
        cypher_create_graph *ccg = n;

        query = cypher_create_graph_utility(pstate, ccg->graph_name);

        query->canSetTag = true;

        if (IsQueryIdEnabled())
          jstate = JumbleQuery(query, sourceText);

        free_parsestate(pstate);

        pgstat_report_query_id(query->queryId, false);

        return query;

      } else if (is_ag_node(n, cypher_use_graph)) {
        cypher_use_graph *ccg = n;

        query = cypher_use_graph_utility(pstate, ccg->graph_name);

        query->canSetTag = true;

        if (IsQueryIdEnabled())
          jstate = JumbleQuery(query, sourceText);

        free_parsestate(pstate);

        pgstat_report_query_id(query->queryId, false);

        return query;
      } else if (is_ag_node(n, cypher_drop_graph)) {
        cypher_drop_graph *ccg = n;

        query = cypher_drop_graph_utility(pstate, ccg->graph_name);

        query->canSetTag = true;

        if (IsQueryIdEnabled())
          jstate = JumbleQuery(query, sourceText);

        free_parsestate(pstate);

        pgstat_report_query_id(query->queryId, false);

        return query;
        
      }
    }


    Oid graph_oid = get_session_graph_oid();
    graph_cache_data *gcd = search_graph_namespace_cache(graph_oid);

cpstate->graph_name = gcd->name.data;
cpstate->graph_oid = graph_oid;

    query = analyze_cypher(parseTree->stmt, pstate, sourceText, 0, gcd->name.data, graph_oid, NULL);
    //PushActiveSnapshot(GetTransactionSnapshot());
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
    Query       *query;
    List       *querytree_list;

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

    Query *query = transform_cypher_clause(cpstate, clause);

    free_cypher_parsestate(cpstate);

    return query;
}