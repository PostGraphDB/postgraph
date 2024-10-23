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
 */

#ifndef AG_AG_SCANNER_H
#define AG_AG_SCANNER_H

#include "common/kwlookup.h"
#include "mb/pg_wchar.h"

#define YYLTYPE  int
/*
 * Set the type of YYSTYPE.
 */
#define YYSTYPE cypher_YYSTYPE

typedef union cypher_YYSTYPE
{
    int integer;
    char *string;
    const char *keyword;
	char chr;
    bool boolean;
    Node *node;
    List *list;
	struct FunctionParameter   *fun_param;
	struct ObjectWithArgs		*objwithargs;
    struct WindowDef *windef;
    struct DefElem *defelt;

    struct TypeName *typnam;
	struct PartitionElem *partelem;
    struct Value *value;
    struct ResTarget *target;
    struct Alias *alias;
    struct RangeVar *range;
	struct IntoClause *into;
	struct WithClause *with;
	struct InferClause			*infer;
	struct PrivTarget	*privtarget;
	struct AccessPriv			*accesspriv;
    struct SortBy *sortby;
	struct OnConflictClause	*onconflict;
    struct InsertStmt *istmt;
    struct VariableSetStmt *vsetstmt;
	struct JoinExpr *jexpr;
	struct IndexElem *ielem;
	struct StatsElem			*selem;
	struct PartitionSpec		*partspec;
	struct PartitionBoundSpec	*partboundspec;
	struct RoleSpec *rolespec;
	struct SelectLimit	*selectlimit;
    struct GroupClause  *groupclause;
	struct ImportQual	*importqual;

} cypher_YYSTYPE;

/*
 * AG_TOKEN_NULL indicates the end of a scan. The name came from YY_NULL.
 *
 * AG_TOKEN_DECIMAL can be a decimal integer literal that does not fit in "int"
 * type.
 */
typedef enum ag_token_type
{
    AG_TOKEN_NULL,
    AG_TOKEN_INTEGER,
    AG_TOKEN_DECIMAL,
    AG_TOKEN_STRING,
    AG_TOKEN_XCONST,
    AG_TOKEN_BCONST,
    AG_TOKEN_IDENTIFIER,
    AG_TOKEN_CS_IDENTIFIER,
    AG_TOKEN_PARAMETER,
    AG_TOKEN_EQ_GT,
    AG_TOKEN_LT_GT,
    AG_TOKEN_LT_EQ,
    AG_TOKEN_GT_EQ,
    AG_TOKEN_DOT_DOT,
    AG_TOKEN_TYPECAST,
    AG_TOKEN_COLON_EQUALS,
    AG_TOKEN_OPERATOR,
    AG_TOKEN_RIGHT_ARROW,
    AG_TOKEN_PLUS_EQ,
    AG_TOKEN_INET,
    AG_TOKEN_CHAR
} ag_token_type;

/*
 * Fields in value field are used with the following types.
 *
 *     * c: AG_TOKEN_CHAR
 *     * i: AG_TOKEN_INTEGER
 *     * s: all other types except the types for c and i, and AG_TOKEN_NULL
 *
 * "int" type is chosen for value.i to line it up with Value in PostgreSQL.
 *
 * value.s is read-only because it points at an internal buffer and it changes
 * for every ag_scanner_next_token() call. So, users who want to keep or modify
 * the value need to copy it first.
 */
typedef struct ag_token
{
    ag_token_type type;
    union
    {
        char c;
        int i;
        const char *s;
    } value;
    int location;
} ag_token;

// an opaque data structure encapsulating the current state of the scanner
typedef void *ag_scanner_t;

typedef struct strbuf
{
    char *buffer;
    int capacity;
    int length;
} strbuf;

typedef struct ag_yy_extra
{
	/*
	 * The keyword list to use, and the associated grammar token codes.
	 */
	const ScanKeywordList *keywordlist;
	const uint16 *keyword_tokens;

	/*
	 * Scanner settings to use.  These are initialized from the corresponding
	 * GUC variables by scanner_init().  Callers can modify them after
	 * scanner_init() if they don't want the scanner's behavior to follow the
	 * prevailing GUC settings.
	 */
	int			backslash_quote;
	bool		escape_string_warning;
	bool		standard_conforming_strings;


    /*
     * accumulate matched strings to build a complete literal if multiple rules
     * are needed to scan it, or keep a decimal integer literal that is
     * converted from a hexadecimal or an octal integer literal if it is too
     * large to fit in "int" type
     */
    strbuf literal_buf;
	int			literallen;		/* actual current string length */
	int			literalalloc;	/* current allocated buffer size */

    // for Unicode surrogate pair
    pg_wchar high_surrogate;
    int start_cond;

    // for the location of the current token and the actual position of it
    const char *scan_buf;
    int last_loc;

	/*
	 * Random assorted scanner state.
	 */
	int			state_before_str_stop;	/* start cond. before end quote */
	int			xcdepth;		/* depth of nesting in slash-star comments */
	char	   *dolqstart;		/* current $foo$ quote start string */
	YYLTYPE		save_yylloc;	/* one-element stack for PUSH_YYLLOC() */

	/* first part of UTF16 surrogate pair for Unicode escapes */
	int32		utf16_first_part;

	/*
	 * State variables for base_yylex().
	 */
	bool		have_lookahead; /* is lookahead info valid? */
	ag_token lookahead_token;	/* one-token lookahead */
	YYLTYPE		lookahead_yylloc;	/* yylloc for lookahead token */
	char	   *lookahead_end;	/* end of current token */
	char		lookahead_hold_char;	/* to be put back at *lookahead_end */

	bool		warn_on_first_escape;
	bool		saw_non_ascii;
    List *result;
} ag_yy_extra;

#define cypher_yyget_extra(scanner) (*((ag_yy_extra **) (scanner)))

ag_scanner_t ag_scanner_create(const char *s, const ScanKeywordList *keywordlist, const uint16 *keyword_tokens, ag_yy_extra *extra);
void ag_scanner_destroy(ag_scanner_t scanner);
ag_token ag_scanner_next_token(YYSTYPE * yylval_param, YYLTYPE * yylloc_param, ag_scanner_t yyscanner);

int ag_scanner_errmsg(const char *msg, ag_scanner_t *scanner);
int ag_scanner_errposition(const int location, ag_scanner_t *scanner);

#endif
