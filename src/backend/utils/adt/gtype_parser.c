/*
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its documentation for any purpose,
 * without fee, and without a written agreement is hereby granted, provided that the above copyright notice
 * and this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
 * ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY
 * OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 *
 * THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA
 * HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

/*
 * gtype parser.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/date.h"
#include "utils/datetime.h"

#include "utils/gtype_parser.h"

/*
 * The context of the parser is maintained by the recursive descent
 * mechanism, but is passed explicitly to the error reporting routine
 * for better diagnostics.
 */
typedef enum /* contexts of gtype parser */
{
    GTYPE_PARSE_VALUE, /* expecting a value */
    GTYPE_PARSE_STRING, /* expecting a string (for a field name) */
    GTYPE_PARSE_ARRAY_START, /* saw '[', expecting value or ']' */
    GTYPE_PARSE_ARRAY_NEXT, /* saw array element, expecting ',' or ']' */
    GTYPE_PARSE_OBJECT_START, /* saw '{', expecting label or '}' */
    GTYPE_PARSE_OBJECT_LABEL, /* saw object label, expecting ':' */
    GTYPE_PARSE_OBJECT_NEXT, /* saw object value, expecting ',' or '}' */
    GTYPE_PARSE_OBJECT_COMMA, /* saw object ',', expecting next label */
    GTYPE_PARSE_END /* saw the end of a document, expect nothing */
} gtype_parse_context;

static inline void gtype_lex(gtype_lex_context *lex);
static inline void gtype_lex_string(gtype_lex_context *lex);
static inline void gtype_lex_number(gtype_lex_context *lex, char *s, bool *num_err, int *total_len);
static void parse_scalar_annotation(gtype_lex_context *lex, void *func, char **annotation);
static inline void parse_scalar(gtype_lex_context *lex, gtype_sem_action *sem);
static void parse_object_field(gtype_lex_context *lex, gtype_sem_action *sem);
static void parse_object(gtype_lex_context *lex, gtype_sem_action *sem);
static void parse_array_element(gtype_lex_context *lex, gtype_sem_action *sem);
static void parse_array(gtype_lex_context *lex, gtype_sem_action *sem);
static void report_parse_error(gtype_parse_context ctx, gtype_lex_context *lex) pg_attribute_noreturn();
static void report_invalid_token(gtype_lex_context *lex) pg_attribute_noreturn();
static int report_gtype_context(gtype_lex_context *lex);
static char *extract_mb_char(char *s);

/* Recursive Descent parser support routines */

/*
 * lex_peek
 *
 * what is the current look_ahead token?
*/
static inline gtype_token_type lex_peek(gtype_lex_context *lex) {
    return lex->token_type;
}

/*
 * lex_accept
 *
 * accept the look_ahead token and move the lexer to the next token if the
 * look_ahead token matches the token parameter. In that case, and if required,
 * also hand back the de-escaped lexeme.
 *
 * returns true if the token matched, false otherwise.
 */
static inline bool lex_accept(gtype_lex_context *lex, gtype_token_type token, char **lexeme) {
    if (lex->token_type == token) {
        if (lexeme != NULL) {
            if (lex->token_type == GTYPE_TOKEN_STRING) {
                if (lex->strval != NULL)
                    *lexeme = pstrdup(lex->strval->data);
            } else {
                int len = (lex->token_terminator - lex->token_start);
                char *tokstr = palloc(len + 1);

                memcpy(tokstr, lex->token_start, len);
                tokstr[len] = '\0';
                *lexeme = tokstr;
            }
        }
        gtype_lex(lex);
        return true;
    }
    return false;
}

/*
 * lex_accept
 *
 * move the lexer to the next token if the current look_ahead token matches
 * the parameter token. Otherwise, report an error.
 */
static inline void lex_expect(gtype_parse_context ctx, gtype_lex_context *lex, gtype_token_type token) {
    if (!lex_accept(lex, token, NULL))
        report_parse_error(ctx, lex);
}

/* chars to consider as part of an alphanumeric token */
#define GTYPE_ALPHANUMERIC_CHAR(c) \
    (((c) >= 'a' && (c) <= 'z') || ((c) >= 'A' && (c) <= 'Z') || \
     ((c) >= '0' && (c) <= '9') || (c) == '_' || IS_HIGHBIT_SET(c))

/*
 * Utility function to check if a string is a valid gtype number.
 *
 * str is of length len, and need not be null-terminated.
 */
bool is_valid_gtype_number(const char *str, int len) {
    bool numeric_error;
    int total_len;
    gtype_lex_context dummy_lex;

    if (len <= 0)
        return false;

    /*
     * gtype_lex_number expects a leading  '-' to have been eaten already.
     *
     * having to cast away the constness of str is ugly, but there's not much
     * easy alternative.
     */
    if (*str == '-') {
        dummy_lex.input = (char *)str + 1;
        dummy_lex.input_length = len - 1;
    } else {
        dummy_lex.input = (char *)str;
        dummy_lex.input_length = len;
    }

    gtype_lex_number(&dummy_lex, dummy_lex.input, &numeric_error, &total_len);

    return (!numeric_error) && (total_len == dummy_lex.input_length);
}

/*
 * make_gtype_lex_context
 *
 * lex constructor, with or without StringInfo object
 * for de-escaped lexemes.
 *
 * Without is better as it makes the processing faster, so only make one
 * if really required.
 *
 * If you already have the gtype as a text* value, use the first of these
 * functions, otherwise use gtype_lex_context_cstring_len().
 */
gtype_lex_context *make_gtype_lex_context(text *t, bool need_escapes) {
    return make_gtype_lex_context_cstring_len( VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t), need_escapes);
}

gtype_lex_context *make_gtype_lex_context_cstring_len(char *str, int len, bool need_escapes) {
    gtype_lex_context *lex = palloc0(sizeof(gtype_lex_context));

    lex->input = lex->token_terminator = lex->line_start = str;
    lex->line_number = 1;
    lex->input_length = len;
    if (need_escapes)
        lex->strval = makeStringInfo();
    return lex;
}

/*
 * parse_gtype
 *
 * Publicly visible entry point for the gtype parser.
 *
 * lex is a lexing context, set up for the gtype to be processed by calling
 * make_gtype_lex_context(). sem is a structure of function pointers to
 * semantic action routines to be called at appropriate spots during parsing,
 * and a pointer to a state object to be passed to those routines.
 */
void parse_gtype(gtype_lex_context *lex, gtype_sem_action *sem) {
    gtype_token_type tok;
    char **valaddr;

    /* get the initial token */
    gtype_lex(lex);

    tok = lex_peek(lex);

    /* parse by recursive descent */
    switch (tok)
    {
    case GTYPE_TOKEN_OBJECT_START:
        parse_object(lex, sem);
        break;
    case GTYPE_TOKEN_ARRAY_START:
        parse_array(lex, sem);
        break;
    default:
        parse_scalar(lex, sem); /* gtype can be a bare scalar */
    }

    //parse_annotation(lex, &annotation);

    lex_expect(GTYPE_PARSE_END, lex, GTYPE_TOKEN_END);
}

static void parse_annotation(gtype_lex_context *lex, void *func, char **annotation) {
    /* check next token for annotations (typecasts, etc.) */
    if (lex_peek(lex) == GTYPE_TOKEN_ANNOTATION) {
        /* eat the annotation token */
        lex_accept(lex, GTYPE_TOKEN_ANNOTATION, NULL);
        if (lex_peek(lex) == GTYPE_TOKEN_IDENTIFIER) {
            /* eat the identifier token and get the annotation value */
            if (func != NULL)
                lex_accept(lex, GTYPE_TOKEN_IDENTIFIER, annotation);
            else
                lex_accept(lex, GTYPE_TOKEN_IDENTIFIER, NULL);
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid value for annotation")));
    }
}


static void parse_scalar_annotation(gtype_lex_context *lex, void *func, char **annotation) {
    /* check next token for annotations (typecasts, etc.) */
    if (lex_peek(lex) == GTYPE_TOKEN_ANNOTATION) {
        /* eat the annotation token */
        lex_accept(lex, GTYPE_TOKEN_ANNOTATION, NULL);
        if (lex_peek(lex) == GTYPE_TOKEN_IDENTIFIER) {
            /* eat the identifier token and get the annotation value */
            if (func != NULL)
                lex_accept(lex, GTYPE_TOKEN_IDENTIFIER, annotation);
            else
                lex_accept(lex, GTYPE_TOKEN_IDENTIFIER, NULL);
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid value for annotation")));
    }
}

/*
 *  Recursive Descent parse routines. There is one for each structural
 *  element in an gtype document:
 *    - scalar (string, number, true, false, null)
 *    - array  ( [ ] )
 *    - array element
 *    - object ( { } )
 *    - object field
 */
static inline void parse_scalar(gtype_lex_context *lex, gtype_sem_action *sem) {
    char *val = NULL;
    char *annotation = NULL;
    gtype_scalar_action sfunc = sem->scalar;
    char **valaddr;
    gtype_token_type tok = lex_peek(lex);

    valaddr = sfunc == NULL ? NULL : &val;

    /* a scalar must be a string, a number, true, false, or null */
    switch (tok)
    {
    case GTYPE_TOKEN_TRUE:
        lex_accept(lex, GTYPE_TOKEN_TRUE, valaddr);
        break;
    case GTYPE_TOKEN_FALSE:
        lex_accept(lex, GTYPE_TOKEN_FALSE, valaddr);
        break;
    case GTYPE_TOKEN_NULL:
        lex_accept(lex, GTYPE_TOKEN_NULL, valaddr);
        break;
    case GTYPE_TOKEN_INTEGER:
        lex_accept(lex, GTYPE_TOKEN_INTEGER, valaddr);
        break;
    case GTYPE_TOKEN_FLOAT:
        lex_accept(lex, GTYPE_TOKEN_FLOAT, valaddr);
        break;
    case GTYPE_TOKEN_STRING:
        lex_accept(lex, GTYPE_TOKEN_STRING, valaddr);
        break;
    case GTYPE_TOKEN_INET:
        lex_accept(lex, GTYPE_TOKEN_INET, valaddr);
        break;
    default:
        report_parse_error(GTYPE_PARSE_VALUE, lex);
    }

    parse_scalar_annotation(lex, sfunc, &annotation);

    if (sfunc != NULL)
        (*sfunc)(sem->semstate, val, tok, annotation);
}

static void parse_object_field(gtype_lex_context *lex, gtype_sem_action *sem) {
    /*
     * An object field is "fieldname" : value where value can be a scalar,
     * object or array.  Note: in user-facing docs and error messages, we
     * generally call a field name a "key".
     */
    char *fname = NULL; /* keep compiler quiet */
    gtype_ofield_action ostart = sem->object_field_start;
    gtype_ofield_action oend = sem->object_field_end;
    bool isnull;
    char **fnameaddr = NULL;
    gtype_token_type tok;

    if (ostart != NULL || oend != NULL)
        fnameaddr = &fname;

    if (!lex_accept(lex, GTYPE_TOKEN_STRING, fnameaddr))
        report_parse_error(GTYPE_PARSE_STRING, lex);

    lex_expect(GTYPE_PARSE_OBJECT_LABEL, lex, GTYPE_TOKEN_COLON);

    tok = lex_peek(lex);
    isnull = tok == GTYPE_TOKEN_NULL;

    if (ostart != NULL)
        (*ostart)(sem->semstate, fname, isnull);

    switch (tok)
    {
    case GTYPE_TOKEN_OBJECT_START:
        parse_object(lex, sem);
        break;
    case GTYPE_TOKEN_ARRAY_START:
        parse_array(lex, sem);
        break;
    default:
        parse_scalar(lex, sem);
    }

    if (oend != NULL)
        (*oend)(sem->semstate, fname, isnull);
}

static void parse_object(gtype_lex_context *lex, gtype_sem_action *sem) {
    /*
     * an object is a possibly empty sequence of object fields, separated by
     * commas and surrounded by curly braces.
     */
    gtype_struct_action ostart = sem->object_start;
    gtype_struct_action oend = sem->object_end;
    gtype_token_type tok;

    check_stack_depth();

    if (ostart != NULL)
        (*ostart)(sem->semstate);

    /*
     * Data inside an object is at a higher nesting level than the object
     * itself. Note that we increment this after we call the semantic routine
     * for the object start and restore it before we call the routine for the
     * object end.
     */
    lex->lex_level++;

    /* we know this will succeed, just clearing the token */
    lex_expect(GTYPE_PARSE_OBJECT_START, lex, GTYPE_TOKEN_OBJECT_START);

    tok = lex_peek(lex);
    switch (tok)
    {
    case GTYPE_TOKEN_STRING:
        parse_object_field(lex, sem);
        while (lex_accept(lex, GTYPE_TOKEN_COMMA, NULL))
            parse_object_field(lex, sem);
        break;
    case GTYPE_TOKEN_OBJECT_END:
        break;
    default:
        /* case of an invalid initial token inside the object */
        report_parse_error(GTYPE_PARSE_OBJECT_START, lex);
    }

    lex_expect(GTYPE_PARSE_OBJECT_NEXT, lex, GTYPE_TOKEN_OBJECT_END);

    lex->lex_level--;

    if (oend != NULL)
        (*oend)(sem->semstate);

}

static void parse_array_element(gtype_lex_context *lex, gtype_sem_action *sem) {
    gtype_aelem_action astart = sem->array_element_start;
    gtype_aelem_action aend = sem->array_element_end;
    gtype_token_type tok = lex_peek(lex);

    bool isnull;

    isnull = tok == GTYPE_TOKEN_NULL;

    if (astart != NULL)
        (*astart)(sem->semstate, isnull);

    /* an array element is any object, array or scalar */
    switch (tok)
    {
    case GTYPE_TOKEN_OBJECT_START:
        parse_object(lex, sem);
        break;
    case GTYPE_TOKEN_ARRAY_START:
        parse_array(lex, sem);
        break;
    default:
        parse_scalar(lex, sem);
    }

    if (aend != NULL)
        (*aend)(sem->semstate, isnull);
}

static void parse_array(gtype_lex_context *lex, gtype_sem_action *sem) {
    /*
     * an array is a possibly empty sequence of array elements, separated by
     * commas and surrounded by square brackets.
     */
    gtype_struct_action astart = sem->array_start;
    gtype_struct_action aend = sem->array_end;

    check_stack_depth();

    if (astart != NULL)
        (*astart)(sem->semstate);

    /*
     * Data inside an array is at a higher nesting level than the array
     * itself. Note that we increment this after we call the semantic routine
     * for the array start and restore it before we call the routine for the
     * array end.
     */
    lex->lex_level++;

    lex_expect(GTYPE_PARSE_ARRAY_START, lex, GTYPE_TOKEN_ARRAY_START);
    if (lex_peek(lex) != GTYPE_TOKEN_ARRAY_END) {
        parse_array_element(lex, sem);

        while (lex_accept(lex, GTYPE_TOKEN_COMMA, NULL))
            parse_array_element(lex, sem);
    }

    lex_expect(GTYPE_PARSE_ARRAY_NEXT, lex, GTYPE_TOKEN_ARRAY_END);

    lex->lex_level--;

    if (aend != NULL)
        (*aend)(sem->semstate);

}

/*
 * Lex one token from the input stream.
 */
static inline void gtype_lex(gtype_lex_context *lex) {
    char *s;
    int len;

    /* Skip leading whitespace. */
    s = lex->token_terminator;
    len = s - lex->input;
    while (len < lex->input_length && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) {
        if (*s == '\n')
            ++lex->line_number;
        ++s;
        ++len;
    }
    lex->token_start = s;

    /* Determine token type. */
    if (len >= lex->input_length) {
        lex->token_start = NULL;
        lex->prev_token_terminator = lex->token_terminator;
        lex->token_terminator = s;
        lex->token_type = GTYPE_TOKEN_END;
    } else {
        switch (*s)
        {
            /* Single-character token, some kind of punctuation mark. */
        case '{':
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
            lex->token_type = GTYPE_TOKEN_OBJECT_START;
            break;
        case '}':
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
            lex->token_type = GTYPE_TOKEN_OBJECT_END;
            break;
        case '[':
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
            lex->token_type = GTYPE_TOKEN_ARRAY_START;
            break;
        case ']':
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
            lex->token_type = GTYPE_TOKEN_ARRAY_END;
            break;
        case ',':
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = s + 1;
            lex->token_type = GTYPE_TOKEN_COMMA;
            break;
        case ':':
            /* if this is an annotation '::' */
            if ((len < lex->input_length - 1) && *(s + 1) == ':') {
                s += 2;
                lex->prev_token_terminator = lex->token_terminator;
                lex->token_terminator = s;
                lex->token_type = GTYPE_TOKEN_ANNOTATION;
            } else {
                lex->prev_token_terminator = lex->token_terminator;
                lex->token_terminator = s + 1;
                lex->token_type = GTYPE_TOKEN_COLON;
            }
            break;
        case '"':
            /* string */
            gtype_lex_string(lex);
            lex->token_type = GTYPE_TOKEN_STRING;
            break;
        case '-':
            /* Negative numbers and special float values. */
            if (*(s + 1) == 'i' || *(s + 1) == 'I') {
                char *s1 = s + 1;
                char *p = s1;

                /* advance p to the end of the token */
                while (p - s < lex->input_length - len && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z')))
                    p++;

                /* update the terminators */
                lex->prev_token_terminator = lex->token_terminator;
                lex->token_terminator = p;

                lex->token_type = GTYPE_TOKEN_INVALID;
                len = p - s1;
                switch (len)
                {
                case 3:
                    if (pg_strncasecmp(s1, "inf", len) == 0)
                        lex->token_type = GTYPE_TOKEN_FLOAT;
                    break;
                case 8:
                    if (pg_strncasecmp(s1, "Infinity", len) == 0)
                        lex->token_type = GTYPE_TOKEN_FLOAT;
                    break;
                }
                if (lex->token_type == GTYPE_TOKEN_INVALID)
                    report_invalid_token(lex);
            } else {
                gtype_lex_number(lex, s + 1, NULL, NULL);
            }
            /* token is assigned in gtype_lex_number */
            break;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            /* Positive number. */
            gtype_lex_number(lex, s, NULL, NULL);
            /* token is assigned in gtype_lex_number */
            break;
        default:
        {
            char *p;

            /*
             * We're not dealing with a string, number, legal
             * punctuation mark, or end of string.  The only legal
             * tokens we might find here are true, false, and null,
             * but for error reporting purposes we scan until we see a
             * non-alphanumeric character.  That way, we can report
             * the whole word as an unexpected token, rather than just
             * some unintuitive prefix thereof.
             */
            for (p = s; p - s < lex->input_length - len && GTYPE_ALPHANUMERIC_CHAR(*p); p++)
                /* skip */;

            /*
             * We got some sort of unexpected punctuation or an
             * otherwise unexpected character, so just complain about
             * that one character.
             */
            if (p == s) {
                lex->prev_token_terminator = lex->token_terminator;
                lex->token_terminator = s + 1;
                report_invalid_token(lex);
            }

            /*
             * We've got a real alphanumeric token here.  If it
             * happens to be true, false, or null, all is well.  If
             * not, error out.
             */
            lex->prev_token_terminator = lex->token_terminator;
            lex->token_terminator = p;

            /* it is an identifier, unless proven otherwise */
            lex->token_type = GTYPE_TOKEN_IDENTIFIER;
            len = p - s;
            switch (len)
            {
            /* A note about the mixture of case and case insensitivity -
             * The original code adheres to the JSON spec where true,
             * false, and null are strictly lower case. The Postgres float
             * logic, on the other hand, is case insensitive, allowing for
             * possibly many different input sources for float values. Hence,
             * the mixture of the two.
             */
            case 3:
                if ((pg_strncasecmp(s, "NaN", len) == 0) || (pg_strncasecmp(s, "inf", len) == 0))
                    lex->token_type = GTYPE_TOKEN_FLOAT;
                break;
            case 4:
                if (memcmp(s, "true", len) == 0)
                    lex->token_type = GTYPE_TOKEN_TRUE;
                else if (memcmp(s, "null", len) == 0)
                    lex->token_type = GTYPE_TOKEN_NULL;
                break;
            case 5:
                if (memcmp(s, "false", len) == 0)
                    lex->token_type = GTYPE_TOKEN_FALSE;
                break;
            case 8:
                if (pg_strncasecmp(s, "Infinity", len) == 0)
                    lex->token_type = GTYPE_TOKEN_FLOAT;
                break;
            }
        } /* end of default case */
        } /* end of switch */
    }
}

/*
 * The next token in the input stream is known to be a string; lex it.
 */
static inline void gtype_lex_string(gtype_lex_context *lex) {
    char *s;
    int len;
    int hi_surrogate = -1;

    if (lex->strval != NULL)
        resetStringInfo(lex->strval);

    Assert(lex->input_length > 0);
    s = lex->token_start;
    len = lex->token_start - lex->input;
    for (;;) {
        s++;
        len++;
        /* Premature end of the string. */
        if (len >= lex->input_length) {
            lex->token_terminator = s;
            report_invalid_token(lex);
        } else if (*s == '"') {
            break;
        } else if ((unsigned char)*s < 32) {
            /* Per RFC4627, these characters MUST be escaped. */
            /* Since *s isn't printable, exclude it from the context string */
            lex->token_terminator = s;
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Character with value 0x%02x must be escaped.",
                               (unsigned char)*s),
                     report_gtype_context(lex)));
        } else if (*s == '\\') {
            /* OK, we have an escape character. */
            s++;
            len++;
            if (len >= lex->input_length) {
                lex->token_terminator = s;
                report_invalid_token(lex);
            } else if (*s == 'u') {
                int i;
                int ch = 0;

                for (i = 1; i <= 4; i++) {
                    s++;
                    len++;
                    if (len >= lex->input_length) {
                        lex->token_terminator = s;
                        report_invalid_token(lex);
                    } else if (*s >= '0' && *s <= '9') {
                        ch = (ch * 16) + (*s - '0');
                    } else if (*s >= 'a' && *s <= 'f') {
                        ch = (ch * 16) + (*s - 'a') + 10;
                    } else if (*s >= 'A' && *s <= 'F') {
                        ch = (ch * 16) + (*s - 'A') + 10;
                    } else {
                        lex->token_terminator = s + pg_mblen(s);
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                             errmsg("invalid input syntax for type %s",
                                    "gtype"),
                             errdetail(
                                 "\"\\u\" must be followed by four hexadecimal digits."),
                             report_gtype_context(lex)));
                    }
                }
                if (lex->strval != NULL) {
                    char utf8str[5];
                    int utf8len;

                    if (ch >= 0xd800 && ch <= 0xdbff) {
                        if (hi_surrogate != -1) {
                            ereport( ERROR,
                                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                 errmsg("invalid input syntax for type %s", "gtype"),
                                 errdetail( "Unicode high surrogate must not follow a high surrogate."),
                                 report_gtype_context(lex)));
                        }
                        hi_surrogate = (ch & 0x3ff) << 10;
                        continue;
                    } else if (ch >= 0xdc00 && ch <= 0xdfff) {
                        if (hi_surrogate == -1) {
                            ereport( ERROR,
                                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                 errmsg("invalid input syntax for type %s", "gtype"),
                                 errdetail( "Unicode low surrogate must follow a high surrogate."),
                                 report_gtype_context(lex)));
                        }
                        ch = 0x10000 + hi_surrogate + (ch & 0x3ff);
                        hi_surrogate = -1;
                    }

                    if (hi_surrogate != -1) {
                        ereport( ERROR,
                            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                             errmsg("invalid input syntax for type %s", "gtype"),
                             errdetail( "Unicode low surrogate must follow a high surrogate."),
                             report_gtype_context(lex)));
                    }

                    /*
                     * For UTF8, replace the escape sequence by the actual
                     * utf8 character in lex->strval. Do this also for other
                     * encodings if the escape designates an ASCII character,
                     * otherwise raise an error.
                     */
                    if (ch == 0) {
                        /* We can't allow this, since our TEXT type doesn't */
                        ereport( ERROR,
                            (errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
                             errmsg("unsupported Unicode escape sequence"),
                             errdetail("\\u0000 cannot be converted to text."),
                             report_gtype_context(lex)));
                    } else if (GetDatabaseEncoding() == PG_UTF8) {
                        unicode_to_utf8(ch, (unsigned char *)utf8str);
                        utf8len = pg_utf_mblen((unsigned char *)utf8str);
                        appendBinaryStringInfo(lex->strval, utf8str, utf8len);
                    } else if (ch <= 0x007f) {
                        /*
                         * This is the only way to designate things like a
                         * form feed character in gtype, so it's useful in all
                         * encodings.
                         */
                        appendStringInfoChar(lex->strval, (char)ch);
                    } else {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
                             errmsg("unsupported Unicode escape sequence"),
                             errdetail( "Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8."),
                             report_gtype_context(lex)));
                    }
                }
            } else if (lex->strval != NULL) {
                if (hi_surrogate != -1)
                    ereport( ERROR,
                        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                         errmsg("invalid input syntax for type %s", "gtype"),
                         errdetail( "Unicode low surrogate must follow a high surrogate."),
                         report_gtype_context(lex)));

                switch (*s)
                {
                case '"':
                case '\\':
                case '/':
                    appendStringInfoChar(lex->strval, *s);
                    break;
                case 'b':
                    appendStringInfoChar(lex->strval, '\b');
                    break;
                case 'f':
                    appendStringInfoChar(lex->strval, '\f');
                    break;
                case 'n':
                    appendStringInfoChar(lex->strval, '\n');
                    break;
                case 'r':
                    appendStringInfoChar(lex->strval, '\r');
                    break;
                case 't':
                    appendStringInfoChar(lex->strval, '\t');
                    break;
                default:
                    /* Not a valid string escape, so error out. */
                    lex->token_terminator = s + pg_mblen(s);
                    ereport( ERROR,
                        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                         errmsg("invalid input syntax for type %s", "gtype"),
                         errdetail("Escape sequence \"\\%s\" is invalid.", extract_mb_char(s)),
                         report_gtype_context(lex)));
                }
            } else if (strchr("\"\\/bfnrt", *s) == NULL) {
                /*
                 * Simpler processing if we're not bothered about de-escaping
                 *
                 * It's very tempting to remove the strchr() call here and
                 * replace it with a switch statement, but testing so far has
                 * shown it's not a performance win.
                 */
                lex->token_terminator = s + pg_mblen(s);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                         errmsg("invalid input syntax for type %s", "gtype"),
                         errdetail("Escape sequence \"\\%s\" is invalid.", extract_mb_char(s)),
                         report_gtype_context(lex)));
            }
        } else if (lex->strval != NULL) {
            if (hi_surrogate != -1)
                ereport( ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail( "Unicode low surrogate must follow a high surrogate."),
                     report_gtype_context(lex)));

            appendStringInfoChar(lex->strval, *s);
        }
    }

    if (hi_surrogate != -1)
        ereport( ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s", "gtype"),
             errdetail("Unicode low surrogate must follow a high surrogate."),
             report_gtype_context(lex)));

    /* Hooray, we found the end of the string! */
    lex->prev_token_terminator = lex->token_terminator;
    lex->token_terminator = s + 1;
}

/*
 * The next token in the input stream is known to be a number; lex it.
 *
 * In gtype, a number consists of four parts:
 *
 * (1) An optional minus sign ('-').
 *
 * (2) Either a single '0', or a string of one or more digits that does not
 *     begin with a '0'.
 *
 * (3) An optional decimal part, consisting of a period ('.') followed by
 *     one or more digits.  (Note: While this part can be omitted
 *     completely, it's not OK to have only the decimal point without
 *     any digits afterwards.)
 *
 * (4) An optional exponent part, consisting of 'e' or 'E', optionally
 *     followed by '+' or '-', followed by one or more digits.  (Note:
 *     As with the decimal part, if 'e' or 'E' is present, it must be
 *     followed by at least one digit.)
 *
 * The 's' argument to this function points to the ostensible beginning
 * of part 2 - i.e. the character after any optional minus sign, or the
 * first character of the string if there is none.
 *
 * If num_err is not NULL, we return an error flag to *num_err rather than
 * raising an error for a badly-formed number.  Also, if total_len is not NULL
 * the distance from lex->input to the token end+1 is returned to *total_len.
 */
static inline void gtype_lex_number(gtype_lex_context *lex, char *s, bool *num_err, int *total_len) {
    bool error = false;
    int len = s - lex->input;

    /* assume we have an integer until proven otherwise */
    lex->token_type = GTYPE_TOKEN_INTEGER;

    /* Part (1): leading sign indicator. */
    /* Caller already did this for us; so do nothing. */

    /* Part (2): parse main digit string. */
    if (len < lex->input_length && *s == '0') {
        s++;
        len++;
    } else if (len < lex->input_length && *s >= '1' && *s <= '9') {
        do {
            s++;
            len++;
        } while (len < lex->input_length && *s >= '0' && *s <= '9');
    } else {
        error = true;
    }

    /* Part (3): parse optional decimal portion. */
    if (len < lex->input_length && *s == '.') {
        /* since we have a decimal point, we have a float */
        lex->token_type = GTYPE_TOKEN_FLOAT;

        s++;
        len++;
        if (len == lex->input_length || *s < '0' || *s > '9') {
            error = true;
        } else {
            do {
                s++;
                len++;
            } while (len < lex->input_length && *s >= '0' && *s <= '9');
        }
    }

    /* Part (4A) Houston, we discovered the internet! ... well IP4 internet*/
    if (len < lex->input_length && *s == '.') {
        lex->token_type = GTYPE_TOKEN_INET;

        s++;
        len++;
        if (len == lex->input_length || *s < '0' || *s > '9') {
            error = true;
        } else {
            do {
    		s++;
                len++;
            } while (len < lex->input_length && ((*s >= '0' && *s <= '9') || *s == '.') );

	}

	// Optional netmask
        if (len < lex->input_length && *s == '/')  {
            s++;
            len++;
            if (len == lex->input_length || *s < '0' || *s > '9') {
                error = true;
            } else {
                do {
                    s++;
                    len++;
                } while (len < lex->input_length && *s >= '0' && *s <= '9');
            }
	}
    }

    /* Part (4b): parse optional exponent. */
    else if (len < lex->input_length && (*s == 'e' || *s == 'E')) {
        /* since we have an exponent, we have a float */
        lex->token_type = GTYPE_TOKEN_FLOAT;

        s++;
        len++;
        if (len < lex->input_length && (*s == '+' || *s == '-')) {
            s++;
            len++;
        }
        if (len == lex->input_length || *s < '0' || *s > '9') {
            error = true;
        } else {
            do {
                s++;
                len++;
            } while (len < lex->input_length && *s >= '0' && *s <= '9');
        }
    }

    /*
     * Check for trailing garbage.  As in gtype_lex(), any alphanumeric stuff
     * here should be considered part of the token for error-reporting
     * purposes.
     */
    for (; len < lex->input_length && GTYPE_ALPHANUMERIC_CHAR(*s); s++, len++)
        error = true;

    if (total_len != NULL)
        *total_len = len;

    if (num_err != NULL) {
        /* let the caller handle any error */
        *num_err = error;
    } else {
        /* return token endpoint */
        lex->prev_token_terminator = lex->token_terminator;
        lex->token_terminator = s;
        /* handle error if any */
        if (error)
            report_invalid_token(lex);
    }
}

/*
 * Report a parse error.
 *
 * lex->token_start and lex->token_terminator must identify the current token.
 */
static void report_parse_error(gtype_parse_context ctx, gtype_lex_context *lex) {
    char *token;
    int toklen;

    /* Handle case where the input ended prematurely. */
    if (lex->token_start == NULL || lex->token_type == GTYPE_TOKEN_END)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type %s", "gtype"),
                        errdetail("The input string ended unexpectedly."),
                        report_gtype_context(lex)));

    /* Separate out the current token. */
    toklen = lex->token_terminator - lex->token_start;
    token = palloc(toklen + 1);
    memcpy(token, lex->token_start, toklen);
    token[toklen] = '\0';

    /* Complain, with the appropriate detail message. */
    if (ctx == GTYPE_PARSE_END) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s", "gtype"),
                 errdetail("Expected end of input, but found \"%s\".", token),
                 report_gtype_context(lex)));
    } else {
        switch (ctx)
        {
        case GTYPE_PARSE_VALUE:
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s", "gtype"),
                 errdetail("Expected gtype value, but found \"%s\".", token),
                 report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_STRING:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected string, but found \"%s\".", token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_ARRAY_START:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail(
                         "Expected array element or \"]\", but found \"%s\".",
                         token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_ARRAY_NEXT:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected \",\" or \"]\", but found \"%s\".",
                               token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_OBJECT_START:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected string or \"}\", but found \"%s\".",
                               token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_OBJECT_LABEL:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected \":\", but found \"%s\".", token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_OBJECT_NEXT:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected \",\" or \"}\", but found \"%s\".",
                               token),
                     report_gtype_context(lex)));
            break;
        case GTYPE_PARSE_OBJECT_COMMA:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s", "gtype"),
                     errdetail("Expected string, but found \"%s\".", token),
                     report_gtype_context(lex)));
            break;
        default:
            elog(ERROR, "unexpected gtype parse state: %d", ctx);
        }
    }
}

/*
 * Report an invalid input token.
 *
 * lex->token_start and lex->token_terminator must identify the token.
 */
static void report_invalid_token(gtype_lex_context *lex) {
    char *token;
    int toklen;

    /* Separate out the offending token. */
    toklen = lex->token_terminator - lex->token_start;
    token = palloc(toklen + 1);
    memcpy(token, lex->token_start, toklen);
    token[toklen] = '\0';

    ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s", "gtype"),
                    errdetail("Token \"%s\" is invalid.", token),
                    report_gtype_context(lex)));
}

/*
 * Report a CONTEXT line for bogus gtype input.
 *
 * lex->token_terminator must be set to identify the spot where we detected
 * the error.  Note that lex->token_start might be NULL, in case we recognized
 * error at EOF.
 *
 * The return value isn't meaningful, but we make it non-void so that this
 * can be invoked inside ereport().
 */
static int report_gtype_context(gtype_lex_context *lex)
{
    const char *context_start;
    const char *context_end;
    const char *line_start;
    int line_number;
    char *ctxt;
    int ctxtlen;
    const char *prefix;
    const char *suffix;

    /* Choose boundaries for the part of the input we will display */
    context_start = lex->input;
    context_end = lex->token_terminator;
    line_start = context_start;
    line_number = 1;
    for (;;) {
        /* Always advance over newlines */
        if (context_start < context_end && *context_start == '\n') {
            context_start++;
            line_start = context_start;
            line_number++;
            continue;
        }
        /* Otherwise, done as soon as we are close enough to context_end */
        if (context_end - context_start < 50)
            break;
        /* Advance to next multibyte character */
        if (IS_HIGHBIT_SET(*context_start))
            context_start += pg_mblen(context_start);
        else
            context_start++;
    }

    /*
     * We add "..." to indicate that the excerpt doesn't start at the
     * beginning of the line ... but if we're within 3 characters of the
     * beginning of the line, we might as well just show the whole line.
     */
    if (context_start - line_start <= 3)
        context_start = line_start;

    /* Get a null-terminated copy of the data to present */
    ctxtlen = context_end - context_start;
    ctxt = palloc(ctxtlen + 1);
    memcpy(ctxt, context_start, ctxtlen);
    ctxt[ctxtlen] = '\0';

    /*
     * Show the context, prefixing "..." if not starting at start of line, and
     * suffixing "..." if not ending at end of line.
     */
    prefix = (context_start > line_start) ? "..." : "";
    if (lex->token_type != GTYPE_TOKEN_END &&
        context_end - lex->input < lex->input_length && *context_end != '\n' &&
        *context_end != '\r')
        suffix = "...";
    else
        suffix = "";

    return errcontext("gtype data, line %d: %s%s%s", line_number, prefix,
                      ctxt, suffix);
}

/*
 * Extract a single, possibly multi-byte char from the input string.
 */
static char *extract_mb_char(char *s) {
    char *res;
    int len;

    len = pg_mblen(s);
    res = palloc(len + 1);
    memcpy(res, s, len);
    res[len] = '\0';

    return res;
}

/*
 * Encode 'value' of datetime type 'typid' into gtype string in ISO format
 * using optionally preallocated buffer 'buf'.
 */
char *gtype_encode_date_time(char *buf, Datum value, Oid typid) {
    if (!buf)
        buf = palloc(MAXDATELEN + 1);

    switch (typid)
    {
    case DATEOID:
    {
        DateADT date;
        struct pg_tm tm;

        date = DatumGetDateADT(value);

        /* Same as date_out(), but forcing DateStyle */
        if (DATE_NOT_FINITE(date)) {
            EncodeSpecialDate(date, buf);
        } else {
            j2date(date + POSTGRES_EPOCH_JDATE, &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
            EncodeDateOnly(&tm, USE_XSD_DATES, buf);
        }
    }
    break;
    case TIMEOID:
    {
        TimeADT time = DatumGetTimeADT(value);
        struct pg_tm tt, *tm = &tt;
        fsec_t fsec;

        /* Same as time_out(), but forcing DateStyle */
        time2tm(time, tm, &fsec);
        EncodeTimeOnly(tm, fsec, false, 0, USE_XSD_DATES, buf);
    }
    break;
    case TIMETZOID:
    {
        TimeTzADT *time = DatumGetTimeTzADTP(value);
        struct pg_tm tt, *tm = &tt;
        fsec_t fsec;
        int tz;

        /* Same as timetz_out(), but forcing DateStyle */
        timetz2tm(time, tm, &fsec, &tz);
        EncodeTimeOnly(tm, fsec, true, tz, USE_XSD_DATES, buf);
    }
    break;
    case TIMESTAMPOID:
    {
        Timestamp timestamp;
        struct pg_tm tm;
        fsec_t fsec;

        timestamp = DatumGetTimestamp(value);
        /* Same as timestamp_out(), but forcing DateStyle */
        if (TIMESTAMP_NOT_FINITE(timestamp))
            EncodeSpecialTimestamp(timestamp, buf);
        else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
            EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
        else
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                            errmsg("timestamp out of range")));
    }
    break;
    case TIMESTAMPTZOID:
    {
        TimestampTz timestamp;
        struct pg_tm tm;
        int tz;
        fsec_t fsec;
        const char *tzn = NULL;

        timestamp = DatumGetTimestampTz(value);
        /* Same as timestamptz_out(), but forcing DateStyle */
        if (TIMESTAMP_NOT_FINITE(timestamp))
            EncodeSpecialTimestamp(timestamp, buf);
        else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
            EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
        else
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                            errmsg("timestamp out of range")));
    }
    break;
    default:
        elog(ERROR, "unknown gtype value datetime type oid %d", typid);
        return NULL;
    }

    return buf;
}
