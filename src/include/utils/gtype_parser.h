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
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 1996-2010, Bitnine Global
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 */

/*
 * Declarations for gtype parser.
 */

#ifndef AG_GTYPE_PARSER_H
#define AG_GTYPE_PARSER_H

#include "lib/stringinfo.h"

typedef enum
{
    GTYPE_TOKEN_INVALID,
    GTYPE_TOKEN_STRING,
    GTYPE_TOKEN_INTEGER,
    GTYPE_TOKEN_FLOAT,
    GTYPE_TOKEN_NUMERIC,
    GTYPE_TOKEN_TIMESTAMP,
    GTYPE_TOKEN_TIMESTAMPTZ,
    GTYPE_TOKEN_DATE,
    GTYPE_TOKEN_TIME,
    GTYPE_TOKEN_INTERVAL,
    GTYPE_TOKEN_OBJECT_START,
    GTYPE_TOKEN_OBJECT_END,
    GTYPE_TOKEN_ARRAY_START,
    GTYPE_TOKEN_ARRAY_END,
    GTYPE_TOKEN_COMMA,
    GTYPE_TOKEN_COLON,
    GTYPE_TOKEN_ANNOTATION,
    GTYPE_TOKEN_IDENTIFIER,
    GTYPE_TOKEN_TRUE,
    GTYPE_TOKEN_FALSE,
    GTYPE_TOKEN_NULL,
    GTYPE_TOKEN_END
} gtype_token_type;

/*
 * All the fields in this structure should be treated as read-only.
 *
 * If strval is not null, then it should contain the de-escaped value
 * of the lexeme if it's a string. Otherwise most of these field names
 * should be self-explanatory.
 *
 * line_number and line_start are principally for use by the parser's
 * error reporting routines.
 * token_terminator and prev_token_terminator point to the character
 * AFTER the end of the token, i.e. where there would be a nul byte
 * if we were using nul-terminated strings.
 */
typedef struct gtype_lex_context
{
    char *input;
    int input_length;
    char *token_start;
    char *token_terminator;
    char *prev_token_terminator;
    gtype_token_type token_type;
    int lex_level;
    int line_number;
    char *line_start;
    StringInfo strval;
} gtype_lex_context;

typedef void (*gtype_struct_action)(void *state);
typedef void (*gtype_ofield_action)(void *state, char *fname, bool isnull);
typedef void (*gtype_aelem_action)(void *state, bool isnull);
typedef void (*gtype_scalar_action)(void *state, char *token,
                                     gtype_token_type tokentype,
                                     char *annotation);

/*
 * Semantic Action structure for use in parsing gtype.
 * Any of these actions can be NULL, in which case nothing is done at that
 * point, Likewise, semstate can be NULL. Using an all-NULL structure amounts
 * to doing a pure parse with no side-effects, and is therefore exactly
 * what the gtype input routines do.
 *
 * The 'fname' and 'token' strings passed to these actions are palloc'd.
 * They are not free'd or used further by the parser, so the action function
 * is free to do what it wishes with them.
 */
typedef struct gtype_sem_action
{
    void *semstate;
    gtype_struct_action object_start;
    gtype_struct_action object_end;
    gtype_struct_action array_start;
    gtype_struct_action array_end;
    gtype_ofield_action object_field_start;
    gtype_ofield_action object_field_end;
    gtype_aelem_action array_element_start;
    gtype_aelem_action array_element_end;
    gtype_scalar_action scalar;
} gtype_sem_action;

/*
 * parse_gtype will parse the string in the lex calling the
 * action functions in sem at the appropriate points. It is
 * up to them to keep what state they need in semstate. If they
 * need access to the state of the lexer, then its pointer
 * should be passed to them as a member of whatever semstate
 * points to. If the action pointers are NULL the parser
 * does nothing and just continues.
 */
void parse_gtype(gtype_lex_context *lex, gtype_sem_action *sem);

/*
 * constructors for gtype_lex_context, with or without strval element.
 * If supplied, the strval element will contain a de-escaped version of
 * the lexeme. However, doing this imposes a performance penalty, so
 * it should be avoided if the de-escaped lexeme is not required.
 *
 * If you already have the gtype as a text* value, use the first of these
 * functions, otherwise use ag_make_gtype_lex_context_cstring_len().
 */
gtype_lex_context *make_gtype_lex_context(text *t, bool need_escapes);
gtype_lex_context *make_gtype_lex_context_cstring_len(char *str, int len,
                                                        bool need_escapes);

/*
 * Utility function to check if a string is a valid gtype number.
 *
 * str argument does not need to be null-terminated.
 */
extern bool is_valid_gtype_number(const char *str, int len);

extern char *gtype_encode_date_time(char *buf, Datum value, Oid typid);

#endif
