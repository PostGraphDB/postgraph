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

LOAD 'postgraph';
SET search_path TO postgraph;

CREATE GRAPH tsearch;
USE GRAPH tsearch;

RETURN totsvector('a fat cat sat on a mat and ate a fat rat') ;
RETURN totsvector('1') ;
RETURN totsvector('1 2') ;
RETURN totsvector('''w'':4A,3B,2C,1D,5 a:8') ;

--
-- TSVector Operators
--
-- TSVector = TSVector
RETURN totsvector('1') = totsvector('1') ;
RETURN totsvector('1') = totsvector('2') ;
RETURN totsvector('2') = totsvector('1') ;
-- TSVector <> TSVector
RETURN totsvector('1') <> totsvector('1') ;
RETURN totsvector('1') <> totsvector('2') ;
RETURN totsvector('2') <> totsvector('1') ;
-- TSVector < TSVector
RETURN totsvector('1') < totsvector('1') ;
RETURN totsvector('1') < totsvector('2') ;
RETURN totsvector('2') < totsvector('1') ;
-- TSVector <= TSVector
RETURN totsvector('1') <= totsvector('1') ;
RETURN totsvector('1') <= totsvector('2') ;
RETURN totsvector('2') <= totsvector('1') ;
-- TSVector > TSVector
RETURN totsvector('1') > totsvector('1') ;
RETURN totsvector('1') > totsvector('2') ;
RETURN totsvector('2') > totsvector('1') ;
-- TSVector >= TSVector
RETURN totsvector('1') >= totsvector('1') ;
RETURN totsvector('1') >= totsvector('2') ;
RETURN totsvector('2') >= totsvector('1') ;
-- TSVector || TSVector
RETURN totsvector('1') || totsvector('1') ;
RETURN totsvector('1') || totsvector('2') ;
RETURN totsvector('2') || totsvector('1') ;

--
-- TS_Delete
--
    RETURN ts_delete(totsvector('a fat cat sat on a mat and ate a fat rat'), 'rat');

--
-- Strip
--
RETURN strip('fat:2,4 cat:3 rat:5A'::tsvector);

RETURN totsquery('1') ;
RETURN totsquery('''1 2''') ;
RETURN totsquery('!1') ;
RETURN totsquery('1|2') ;
RETURN totsquery('!(!1|!2)') ;
RETURN totsquery('!(!1|2)') ;
RETURN totsquery('!(1|2)') ;
RETURN totsquery('!1&2') ;
RETURN totsquery('1&!2') ;
RETURN totsquery('!(1)&2') ;
RETURN totsquery('!(1&2)') ;
RETURN totsquery('1|!2&3') ;
RETURN totsquery('!1|2&3') ;
RETURN totsquery('(!1|2)&3') ;
RETURN totsquery('1|(2|(4|(5|6)))') ;
RETURN totsquery('1|2|4|5|6') ;
RETURN totsquery('1&(2&(4&(5|!6)))') ;
RETURN totsquery('a:* & nbb:*ac | doo:a* | goo') ;
RETURN totsquery('!!!b') ;
RETURN totsquery('!!a & b') ;

--
-- PlainTo_TSQuery
--
RETURN PlainTo_TSQuery('"fat rat" or cat dog') ;
RETURN PlainTo_TSQuery('The Fat Rats') ;
RETURN PlainTo_TSQuery('The Cat and Rats') ;

--
-- PhraseTo_TSQuery
--
RETURN PhraseTo_TSQuery('The Fat Rats') ;
RETURN PhraseTo_TSQuery('The Cat and Rats') ;

--
-- Websearch_To_TSQuery
--
RETURN Websearch_To_TSQuery('"fat rat" or cat dog') ;
RETURN Websearch_To_TSQuery('The Fat Rats') ;
RETURN Websearch_To_TSQuery('The Cat and Rats') ;

--
-- TSQuery Operators
--
-- TSQuery = TSQuery
RETURN totsquery('1') = totsquery('1') ;
RETURN totsquery('1') = totsquery('2') ;
RETURN totsquery('2') = totsquery('1') ;
-- TSQuery <> TSQuery
RETURN totsquery('1') <> totsquery('1') ;
RETURN totsquery('1') <> totsquery('2') ;
RETURN totsquery('2') <> totsquery('1') ;
-- TSQuery < TSQuery
RETURN totsquery('1') < totsquery('1') ;
RETURN totsquery('1') < totsquery('2') ;
RETURN totsquery('2') < totsquery('1') ;
-- TSQuery <= TSQuery
RETURN totsquery('1') <= totsquery('1') ;
RETURN totsquery('1') <= totsquery('2') ;
RETURN totsquery('2') <= totsquery('1') ;
-- TSQuery > TSQuery
RETURN totsquery('1') > totsquery('1') ;
RETURN totsquery('1') > totsquery('2') ;
RETURN totsquery('2') > totsquery('1') ;
-- TSQuery >= TSQuery
RETURN totsquery('1') >= totsquery('1') ;
RETURN totsquery('1') >= totsquery('2') ;
RETURN totsquery('2') >= totsquery('1') ;
-- TSQuery && TSQuery
RETURN '1'::tsquery & '2'::tsquery ;
RETURN '2'::tsquery & '1'::tsquery ;
-- TSQuery || TSQuery
RETURN '1'::tsquery || '2'::tsquery ;
RETURN '2'::tsquery || '1'::tsquery ;
-- TSQuery_Phrase and <-> Operator
RETURN tsquery_phrase('fat'::tsquery,'cat'::tsquery) ;
RETURN 'fat'::tsquery <-> 'cat'::tsquery ;
RETURN tsquery_phrase('fat'::tsquery,'cat'::tsquery, 10) ;
-- TSQuery @> TSQuery
RETURN 'cat & rat'::tsquery @> 'rat'::tsquery ;
RETURN 'cat'::tsquery @> 'cat & rat'::tsquery ;
-- TSQuery <@ TSQuery
RETURN 'cat & rat'::tsquery <@ 'rat'::tsquery ;
RETURN 'cat'::tsquery <@ 'cat & rat'::tsquery ;
-- !! TSQuery
RETURN !! totsquery('!1|2&3') ;

--
-- Cleanup
--
DROP GRAPH tsearch CASCADE;
