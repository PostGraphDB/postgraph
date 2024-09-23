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

SELECT * FROM create_graph('lists');

--
-- list literal
--
-- empty list
SELECT * FROM cypher('lists', $$RETURN []$$) AS r(c gtype);

-- list of scalar values
SELECT * FROM cypher('lists', $$ RETURN ['str', 1, 1.0, true, null] $$) AS r(c gtype);

-- nested lists
SELECT * FROM cypher('lists', $$ RETURN [['str'], [1, [1.0], [[true]]], null] $$) AS r(c gtype);

--list concatenation
SELECT * FROM cypher('lists', $$RETURN ['str', 1, 1.0] + [true, null]$$) AS r(c gtype);

-- IN
SELECT * FROM cypher('lists', $$RETURN 1 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 'str' IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 1.0 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN true IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN [1,3,5,[2,4,6]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN {bool: true, int: 1} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN null IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN null IN ['str', 1, 1.0, true]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 'str' IN null $$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 0 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 1.1 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 'Str' IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN [1,3,5,[2,4,5]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN {bool: true, int: 2} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN null IN 'str' $$) AS r(c boolean);
SELECT * FROM cypher('lists', $$RETURN 'str' IN 'str' $$) AS r(c boolean);

-- TODO ALL, ANY/SOME, NONE

-- list access
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][5]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3]$$) AS r(c gtype);
-- should return null
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][11]$$) AS r(c gtype);

-- list slice
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..11]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..0]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10..10]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..1]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][9..10]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..11]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3..11]$$) AS r(c gtype);
SELECT * FROM cypher('lists', $$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..10]$$) AS r(c gtype);
SELECT * from cypher('lists', $$RETURN 0[0..1]$$) as r(a gtype);
SELECT * from cypher('lists', $$RETURN 0[[0]..[1]]$$) as r(a gtype);
SELECT * from cypher('lists', $$RETURN [0][0..-2147483649]$$) as r(a gtype);

-- access and slice operators nested
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[0] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-1] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2..] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1..] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][0] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-2..-1] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][0] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..][0] $$) as (results gtype);

-- empty list
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2..-2] $$) as (results gtype);

-- should return null
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][3] $$) as (results gtype);
SELECT * from cypher('lists', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2] $$) as (results gtype);

-- size() of a string
SELECT * FROM cypher('lists', $$ RETURN size('12345') $$) AS (size gtype);
SELECT * FROM cypher('lists', $$ RETURN size("1234567890") $$) AS (size gtype);
-- size() of an array
SELECT * FROM cypher('lists', $$ RETURN size([1, 2, 3, 4, 5]) $$) AS (size gtype);
SELECT * FROM cypher('lists', $$ RETURN size([]) $$) AS (size gtype);
SELECT * FROM cypher('lists', $$ RETURN size(null) $$) AS (size gtype);
-- head
SELECT * FROM cypher('lists', $$ RETURN head([1, 2, 3, 4, 5]) $$) AS (head gtype);
SELECT * FROM cypher('lists', $$ RETURN head([1]) $$) AS (head gtype);
SELECT * FROM cypher('lists', $$ RETURN head([]) $$) AS (head gtype);
SELECT * FROM cypher('lists', $$ RETURN head(null) $$) AS (head gtype);
-- last
SELECT * FROM cypher('lists', $$ RETURN last([1, 2, 3, 4, 5]) $$) AS (last gtype);
SELECT * FROM cypher('lists', $$ RETURN last([1]) $$) AS (last gtype);
SELECT * FROM cypher('lists', $$ RETURN last([]) $$) AS (last gtype);
SELECT * FROM cypher('lists', $$ RETURN last(null) $$) AS (last gtype);

-- range()
SELECT * from cypher('lists', $$RETURN range(0, 10)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, 10, 1)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, -10, -3)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, 10, 11)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(-20, 10, 5)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, 10, -1)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(null, -10, -3)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, null, -3)$$) as (range gtype);
SELECT * from cypher('lists', $$RETURN range(0, -10.0, -3.0)$$) as (range gtype);


--
-- Cleanup
--
SELECT * FROM drop_graph('lists', true);

--
-- End of tests
--
