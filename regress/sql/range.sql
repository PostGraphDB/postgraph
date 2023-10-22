/*
 * PostGraph
 * Copyright (C) 2023 PostGraphDB
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

LOAD 'postgraph';
SET search_path TO postgraph;
set timezone TO 'GMT';

SELECT * FROM create_graph('range');

SELECT tointrange('"[0, 1]"');
SELECT tointrange('"[0, 1)"');
SELECT tointrange('"(0, 1]"');
SELECT tointrange('"(0, 1)"');

SELECT * FROM cypher('range', $$ RETURN intrange(0, 1) $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') $$) as (a gtype);

--
-- intrange = intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') = intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') = intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') = intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') = intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') = intrange(3, 4, '[]') $$) as (a gtype);

--
-- intrange <> intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') <> intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') <> intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') <> intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') <> intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') <> intrange(3, 4, '[]') $$) as (a gtype);


--
-- intrange > intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') > intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') > intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') > intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') > intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') > intrange(3, 4, '[]') $$) as (a gtype);

--
-- intrange >= intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') >= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') >= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') >= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') >= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') >= intrange(3, 4, '[]') $$) as (a gtype);


--
-- intrange < intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') < intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') < intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') < intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') < intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') < intrange(3, 4, '[]') $$) as (a gtype);


--
-- intrange <= intrange
--
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[]') <= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '()') <= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '(]') <= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') <= intrange(0, 1, '[]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN intrange(0, 1, '[)') <= intrange(3, 4, '[]') $$) as (a gtype);


SELECT tointmultirange('"{[0, 1]}"');

SELECT tonumrange('"[0.5, 1]"');
SELECT tonumrange('"[0, 1.5)"');
SELECT tonumrange('"(0.5, 1]"');
SELECT tonumrange('"(0, 1.5)"');

SELECT * FROM cypher('range', $$ RETURN numrange(0, 1) $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN numrange(0, 1, '()') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN numrange(0, 1, '(]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN numrange(0, 1, '[)') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN numrange(0, 1, '[]') $$) as (a gtype);


SELECT tonummultirange('"{(0, 1.5)}"');

SELECT totsrange('"[''1/1/2000 12:00:00'', ''1/1/2000 4:00:00 PM'']"');

SELECT * FROM cypher('range', $$ RETURN tsrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tsrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '()') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tsrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '(]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tsrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '[)') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tsrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '[]') $$) as (a gtype);


SELECT totsmultirange('"{[''1/1/2000 12:00:00'', ''1/1/2000 4:00:00 PM'']}"');

SELECT totstzrange('"[''1/1/2000 12:00:00 GMT'', ''1/1/2000 4:00:00 PM GMT'']"');

SELECT * FROM cypher('range', $$ RETURN tstzrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tstzrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '()') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tstzrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '(]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tstzrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '[)') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN tstzrange('1/1/2000 12:00:00', '1/1/2000 4:00:00 PM', '[]') $$) as (a gtype);

SELECT totstzmultirange('"{[''1/1/2000 12:00:00 GMT'', ''1/1/2000 4:00:00 PM GMT'']}"');

SELECT todaterange('"[''1/1/2000'', ''1/1/2001'')"');

SELECT * FROM cypher('range', $$ RETURN daterange('1/1/2000', '1/1/2001') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN daterange('1/1/2000', '1/1/2001', '()') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN daterange('1/1/2000', '1/1/2001', '(]') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN daterange('1/1/2000', '1/1/2001', '[)') $$) as (a gtype);
SELECT * FROM cypher('range', $$ RETURN daterange('1/1/2000', '1/1/2001', '[]') $$) as (a gtype);


SELECT todatemultirange('"{[''1/1/2000'', ''1/1/2001'')}"');

--
-- Clean up
--
SELECT drop_graph('range', true);

