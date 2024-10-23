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

#include "nodes/pg_list.h"
#include "parser/scansup.h"

#include "parser/ag_scanner.h"
#include "parser/cypher_gram.h"
#include "parser/cypher_keywords.h"
#include "parser/cypher_parser.h"

/*
 * ecpg_isspace() --- return true if flex scanner considers char whitespace
 */
static bool
ecpg_isspace(char ch)
{
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\f')
		return true;
	return false;
}

/* is Unicode code point acceptable? */
static void
check_unicode_value(pg_wchar c)
{
	if (!is_valid_unicode_codepoint(c))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid Unicode escape value")));
}
/* convert hex digit (caller should have verified that) to value */
static unsigned int
hexval(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 0xA;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 0xA;
	elog(ERROR, "invalid hexadecimal digit");
	return 0;					/* not reached */
}
/*
 * check_uescapechar() and ecpg_isspace() should match their equivalents
 * in pgc.l.
 */

/* is 'escape' acceptable as Unicode escape character (UESCAPE syntax) ? */
static bool
check_uescapechar(unsigned char escape)
{
	if (isxdigit(escape)
		|| escape == '+'
		|| escape == '\''
		|| escape == '"'
		|| ecpg_isspace(escape))
		return false;
	else
		return true;
}

/* Support for scanner_errposition_callback function */
typedef struct ScannerCallbackState
{
	ag_scanner_t yyscanner;
	int			location;
	ErrorContextCallback errcallback;
} ScannerCallbackState;
/*
 * Process Unicode escapes in "str", producing a palloc'd plain string
 *
 * escape: the escape character to use
 * position: start position of U&'' or U&"" string token
 * yyscanner: context information needed for error reports
 */
static char *
str_udeescape(const char *str, char escape,
			  int position, ag_scanner_t yyscanner)
{
	const char *in;
	char	   *new,
			   *out;
	size_t		new_len;
	pg_wchar	pair_first = 0;
	ScannerCallbackState scbstate;

	/*
	 * Guesstimate that result will be no longer than input, but allow enough
	 * padding for Unicode conversion.
	 */
	new_len = strlen(str) + MAX_UNICODE_EQUIVALENT_STRING + 1;
	new = palloc(new_len);

	in = str;
	out = new;
	while (*in)
	{
		/* Enlarge string if needed */
		size_t		out_dist = out - new;

		if (out_dist > new_len - (MAX_UNICODE_EQUIVALENT_STRING + 1))
		{
			new_len *= 2;
			new = repalloc(new, new_len);
			out = new + out_dist;
		}

		if (in[0] == escape)
		{
			/*
			 * Any errors reported while processing this escape sequence will
			 * have an error cursor pointing at the escape.
			 */
			setup_scanner_errposition_callback(&scbstate, yyscanner,
											   in - str + position + 3);	/* 3 for U&" */
			if (in[1] == escape)
			{
				if (pair_first)
					goto invalid_pair;
				*out++ = escape;
				in += 2;
			}
			else if (isxdigit((unsigned char) in[1]) &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]))
			{
				pg_wchar	unicode;

				unicode = (hexval(in[1]) << 12) +
					(hexval(in[2]) << 8) +
					(hexval(in[3]) << 4) +
					hexval(in[4]);
				check_unicode_value(unicode);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) out);
					out += strlen(out);
				}
				in += 5;
			}
			else if (in[1] == '+' &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]) &&
					 isxdigit((unsigned char) in[5]) &&
					 isxdigit((unsigned char) in[6]) &&
					 isxdigit((unsigned char) in[7]))
			{
				pg_wchar	unicode;

				unicode = (hexval(in[2]) << 20) +
					(hexval(in[3]) << 16) +
					(hexval(in[4]) << 12) +
					(hexval(in[5]) << 8) +
					(hexval(in[6]) << 4) +
					hexval(in[7]);
				check_unicode_value(unicode);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) out);
					out += strlen(out);
				}
				in += 8;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid Unicode escape"),
						 errhint("Unicode escapes must be \\XXXX or \\+XXXXXX.")));

			cancel_scanner_errposition_callback(&scbstate);
		}
		else
		{
			if (pair_first)
				goto invalid_pair;

			*out++ = *in++;
		}
	}

	/* unfinished surrogate pair? */
	if (pair_first)
		goto invalid_pair;

	*out = '\0';
	return new;

	/*
	 * We might get here with the error callback active, or not.  Call
	 * scanner_errposition to make sure an error cursor appears; if the
	 * callback is active, this is duplicative but harmless.
	 */
invalid_pair:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid Unicode surrogate pair"),
			 scanner_errposition(in - str + position + 3,	/* 3 for U&" */
								 yyscanner)));
	return NULL;				/* keep compiler quiet */
}


int cypher_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, ag_scanner_t scanner, ag_yy_extra *extra)
{

    /*
     * This list must match ag_token_type.
     * 0 means end-of-input.
     */
    const int type_map[] = {
        0,
        INTEGER,
        DECIMAL,
        STRING,
        XCONST,
        BCONST,
        IDENTIFIER,
        CS_IDENTIFIER,
        PARAMETER,
        EQ_GT, 
        NOT_EQ,
        LT_EQ,
        GT_EQ,
        DOT_DOT,
        TYPECAST,
        COLON_EQUALS,
	    OPERATOR,
        RIGHT_ARROW,
        PLUS_EQ,
	    INET,
    };
   // ag_yy_extra *extra = ag_yyget_extra(scanner);
    ag_token token;

	int			cur_token;
	int			next_token;
	int			cur_token_length;
	YYLTYPE		cur_yylloc;

	// Get next token --- we might already have it
	if (extra->have_lookahead)
	{
		token = extra->lookahead_token;
		//lvalp->core_yystype = extra->lookahead_yylval;
		//*llocp = extra->lookahead_yylloc;
		//if (extra->lookahead_end) 
		//	*(extra->lookahead_end) = extra->lookahead_hold_char;
		extra->have_lookahead = false;
	}
	else
        token = ag_scanner_next_token(lvalp, llocp, scanner);

	//cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

    // token = ag_scanner_next_token(scanner);

   // token = ag_scanner_next_token(scanner);

    switch (token.type)
    {
    case AG_TOKEN_NULL:
        break;
    case AG_TOKEN_INTEGER:
        lvalp->integer = token.value.i;
        break;
        case AG_TOKEN_STRING:
        {
            ag_token next_token = ag_scanner_next_token(lvalp, llocp, scanner);
            if (next_token.type == AG_TOKEN_IDENTIFIER)
                truncate_identifier(next_token.value.s, strlen(next_token.value.s), true);
            if (next_token.type == AG_TOKEN_IDENTIFIER && !strcmp("uescape", next_token.value.s))
            {
				/* Yup, so get third token, which had better be SCONST */
				const char *escstr;

				/* Again save and restore *llocp */
				cur_yylloc = *llocp;
          
                //*(extra->lookahead_end) = extra->lookahead_hold_char;

				/* Get third token */
				next_token = ag_scanner_next_token(lvalp, llocp, scanner);
                /* If we throw error here, it will point to third token */
				if (next_token.type != AG_TOKEN_STRING)
					scanner_yyerror("UESCAPE must be followed by a simple string literal",
									scanner);

				escstr = next_token.value.s;
				if (strlen(escstr) != 1 || !check_uescapechar(escstr[0]))
					scanner_yyerror("invalid Unicode escape character",
									scanner);

				/* Now restore *llocp; errors will point to first token */
				*llocp = cur_yylloc;

				/* Apply Unicode conversion */
				lvalp->string  =
					str_udeescape(pstrdup(token.value.s),
								  escstr[0],
								  *llocp,
								  scanner);

				/*
				 * We don't need to revert the un-truncation of UESCAPE.  What
				 * we do want to do is clear have_lookahead, thereby consuming
				 * all three tokens.
				 */
				extra->have_lookahead = false;                              
            }
            else {
		        extra->lookahead_token = next_token;
                extra->have_lookahead = true;
        	    lvalp->string = 
									/*str_udeescape(pstrdup(token.value.s),
								  '\\',
								  *llocp,
								  yscanner);
				*/
				pstrdup(token.value.s);
            }
            break;
        }


    case AG_TOKEN_DECIMAL:

    case AG_TOKEN_INET:
    case AG_TOKEN_OPERATOR:
    case AG_TOKEN_XCONST:
    case AG_TOKEN_BCONST:
    case AG_TOKEN_RIGHT_ARROW:
	lvalp->string = pstrdup(token.value.s);
        break;
    case AG_TOKEN_CS_IDENTIFIER: {
        token.type = AG_TOKEN_IDENTIFIER;
	    lvalp->string = pstrdup(token.value.s);
        break;
    }
    case AG_TOKEN_IDENTIFIER:
    {
        int kwnum;
        char *ident;

        kwnum = ScanKeywordLookup(token.value.s, &CypherKeyword);
        if (kwnum >= 0)
        {
            /*
             * use token.value.s instead of keyword->name to preserve
             * case sensitivity
             */
            lvalp->keyword = GetScanKeyword(kwnum, &CypherKeyword);
            *llocp = token.location;
            if (CypherKeywordTokens[kwnum] == WITH) {
                ag_token next_token = ag_scanner_next_token(lvalp, llocp, scanner);
		        extra->lookahead_token = next_token;
                extra->have_lookahead = true;
                if (next_token.type == AG_TOKEN_IDENTIFIER)
                    truncate_identifier(next_token.value.s, strlen(next_token.value.s), true);
                if (next_token.type == AG_TOKEN_IDENTIFIER && (!strcmp("time", next_token.value.s) || !strcmp("ordinality", next_token.value.s)))
                {
                    return WITH_LA;
                }
            }
            return CypherKeywordTokens[kwnum];
        }

        
        ident = pstrdup(token.value.s);
        truncate_identifier(ident, strlen(ident), true);
        lvalp->string = ident;
        break;
    }
    case AG_TOKEN_PARAMETER:
        lvalp->string = pstrdup(token.value.s);
        break;
    case AG_TOKEN_EQ_GT:
    case AG_TOKEN_LT_GT:
    case AG_TOKEN_LT_EQ:
    case AG_TOKEN_GT_EQ:
    case AG_TOKEN_DOT_DOT:
    case AG_TOKEN_PLUS_EQ:
    case AG_TOKEN_COLON_EQUALS:
        break;
    case AG_TOKEN_TYPECAST:
        break;
    case AG_TOKEN_CHAR:
        *llocp = token.location;
        return token.value.c;
    default:
        ereport(ERROR, (errmsg("unexpected ag_token_type: %d", token.type)));
        break;
    }

    *llocp = token.location;
    return type_map[token.type];
}

void cypher_yyerror(YYLTYPE *llocp, ag_scanner_t scanner,
                    cypher_yy_extra *extra, const char *msg)
{
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    ag_scanner_errmsg(msg, scanner),
                    ag_scanner_errposition(*llocp, scanner)));
}

/* declaration to make mac os x compiler happy */
int cypher_yyparse(ag_scanner_t scanner, ag_yy_extra *extra);


List *parse_cypher(const char *s)
{
    ag_scanner_t scanner;
    ag_yy_extra extra;
    int yyresult;

    scanner = ag_scanner_create(pstrdup(s), &CypherKeyword, &CypherKeywordTokens, &extra);
    extra.result = NIL;
    extra.have_lookahead = false;

    yyresult = cypher_yyparse(scanner, &extra);

    ag_scanner_destroy(scanner);

    /*
     * cypher_yyparse() returns 0 if parsing was successful.
     * Otherwise, it returns 1 (invalid input) or 2 (memory exhaustion).
     */
    if (yyresult)
        return NIL;

    /*
     * Append the extra node node regardless of its value. Currently the extra
     * node is only used by EXPLAIN
    */
    return extra.result;
}
