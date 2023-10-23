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

SELECT * FROM create_graph('tsearch');


SELECT totsvector('"a fat cat sat on a mat and ate a fat rat"');
SELECT totsvector('"1"');
SELECT totsvector('"1 "');
SELECT totsvector('" 1"');
SELECT totsvector('" 1 "');
SELECT totsvector('"1 2"');
SELECT totsvector('"''1 2''"');
SELECT totsvector('"''w'':4A,3B,2C,1D,5 a:8"');

SELECT totsquery('"1"');
SELECT totsquery('"1 "');
SELECT totsquery('" 1"');
SELECT totsquery('" 1 "');
SELECT totsquery('"''1 2''"');
SELECT totsquery('"!1"');
SELECT totsquery('"1|2"');
SELECT totsquery('"1|!2"');
SELECT totsquery('"!1|2"');
SELECT totsquery('"!1|!2"');
SELECT totsquery('"!(!1|!2)"');
SELECT totsquery('"!(!1|2)"');
SELECT totsquery('"!(1|!2)"');
SELECT totsquery('"!(1|2)"');
SELECT totsquery('"1&2"');
SELECT totsquery('"!1&2"');
SELECT totsquery('"1&!2"');
SELECT totsquery('"!1&!2"');
SELECT totsquery('"(1&2)"');
SELECT totsquery('"1&(2)"');
SELECT totsquery('"!(1)&2"');
SELECT totsquery('"!(1&2)"');
SELECT totsquery('"1|2&3"');
SELECT totsquery('"1|(2&3)"');
SELECT totsquery('"(1|2)&3"');
SELECT totsquery('"1|2&!3"');
SELECT totsquery('"1|!2&3"');
SELECT totsquery('"!1|2&3"');
SELECT totsquery('"!1|(2&3)"');
SELECT totsquery('"!(1|2)&3"');
SELECT totsquery('"(!1|2)&3"');
SELECT totsquery('"1|(2|(4|(5|6)))"');
SELECT totsquery('"1|2|4|5|6"');
SELECT totsquery('"1&(2&(4&(5&6)))"');
SELECT totsquery('"1&2&4&5&6"');
SELECT totsquery('"1&(2&(4&(5|6)))"');
SELECT totsquery('"1&(2&(4&(5|!6)))"');
SELECT totsquery('"a:* & nbb:*ac | doo:a* | goo"');
SELECT totsquery('"!!b"');
SELECT totsquery('"!!!b"');
SELECT totsquery('"!(!b)"');
SELECT totsquery('"a & !!b"');
SELECT totsquery('"!!a & b"');

SELECT * FROM cypher('tsearch', $$ RETURN totsvector('a fat cat sat on a mat and ate a fat rat') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1 ') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector(' 1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector(' 1 ') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1 2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('''1 2''') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('''w'':4A,3B,2C,1D,5 a:8') $$) as (a gtype);

--
-- TSVector = TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') = totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') = totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') = totsvector('1') $$) as (a gtype);

--
-- TSVector <> TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') <> totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') <> totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') <> totsvector('1') $$) as (a gtype);

--
-- TSVector < TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') < totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') < totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') < totsvector('1') $$) as (a gtype);

--
-- TSVector <= TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') <= totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') <= totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') <= totsvector('1') $$) as (a gtype);

--
-- TSVector > TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') > totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') > totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') > totsvector('1') $$) as (a gtype);

--
-- TSVector >= TSVector
--
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') >= totsvector('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('1') >= totsvector('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsvector('2') >= totsvector('1') $$) as (a gtype);

--
-- TS_Delete
--
SELECT * FROM cypher('tsearch', $$
    RETURN ts_delete(totsvector('a fat cat sat on a mat and ate a fat rat'), 'rat')
$$) as (a gtype);

--
-- Strip
--
SELECT * FROM cypher('tsearch', $$
     RETURN strip('fat:2,4 cat:3 rat:5A'::tsvector)
$$) as (a gtype);

SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1 ') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery(' 1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery(' 1 ') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('''1 2''') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|!2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1|2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1|!2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(!1|!2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(!1|2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(1|!2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(1|2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1&2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&!2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1&!2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('(1&2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&(2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(1)&2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(1&2)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|2&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|(2&3)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('(1|2)&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|2&!3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|!2&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1|2&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!1|(2&3)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(1|2)&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('(!1|2)&3') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|(2|(4|(5|6)))') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1|2|4|5|6') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&(2&(4&(5&6)))') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&2&4&5&6') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&(2&(4&(5|6)))') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1&(2&(4&(5|!6)))') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('a:* & nbb:*ac | doo:a* | goo') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!!b') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!!!b') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!(!b)') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('a & !!b') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('!!a & b') $$) as (a gtype);

--
-- TSQuery = TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') = totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') = totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') = totsquery('1') $$) as (a gtype);

--
-- TSQuery <> TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') <> totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') <> totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') <> totsquery('1') $$) as (a gtype);

--
-- TSQuery < TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') < totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') < totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') < totsquery('1') $$) as (a gtype);

--
-- TSQuery <= TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') <= totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') <= totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') <= totsquery('1') $$) as (a gtype);

--
-- TSQuery > TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') > totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') > totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') > totsquery('1') $$) as (a gtype);

--
-- TSQuery >= TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') >= totsquery('1') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('1') >= totsquery('2') $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN totsquery('2') >= totsquery('1') $$) as (a gtype);

--
-- TSQuery && TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN '1'::tsquery & '2'::tsquery $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN '2'::tsquery & '1'::tsquery $$) as (a gtype);

--
-- TSQuery_Phrase and <-> Operator
--
SELECT * FROM cypher('tsearch', $$ RETURN tsquery_phrase('fat'::tsquery,'cat'::tsquery) $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN 'fat'::tsquery <-> 'cat'::tsquery $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN tsquery_phrase('fat'::tsquery,'cat'::tsquery, 10) $$) as (a gtype);

--
-- TSQuery @> TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN 'cat & rat'::tsquery @> 'rat'::tsquery $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN 'cat'::tsquery @> 'cat & rat'::tsquery $$) as (a gtype);

--
-- TSQuery <@ TSQuery
--
SELECT * FROM cypher('tsearch', $$ RETURN 'cat & rat'::tsquery <@ 'rat'::tsquery $$) as (a gtype);
SELECT * FROM cypher('tsearch', $$ RETURN 'cat'::tsquery <@ 'cat & rat'::tsquery $$) as (a gtype);
--
-- Cleanup
--
SELECT drop_graph('tsearch', true);
