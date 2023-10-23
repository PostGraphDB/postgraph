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

SET extra_float_digits = 0;
LOAD 'postgraph';
SET search_path TO postgraph;
set timezone TO 'GMT';

SELECT * FROM create_graph('expr');

SET bytea_output = 'hex';
SELECT tobytea('"abc \153\154\155 \052\251\124"');
SELECT tobytea('"\xDEADBEEF"');

SET bytea_output = 'escape';
SELECT tobytea('"\xDEADBEEF"');
SELECT tobytea('"abc \153\154\155 \052\251\124"');

SELECT topoint('"(1,1)"');
SELECT '(1,1)'::point::gtype;
select * FROM cypher('expr', $$
    RETURN 'POINT(1 1)'::geometry
$$) AS (c point);
SELECT * FROM cypher('expr', $$RETURN '(1,1)'::point $$) AS r(c gtype);
SELECT * FROM cypher('expr', $$RETURN '(1,1)'::point $$) AS r(c point);




SELECT tolseg('"(1,1), (2,2)"');
SELECT * FROM cypher('expr', $$RETURN tolseg('(1,1), (2,2)') $$) AS r(c gtype);
SELECT * FROM cypher('expr', $$RETURN '(1,1), (2,2)'::lseg $$) AS r(c gtype);


SELECT toline('"{1,1,2}"');
SELECT * FROM cypher('expr', $$RETURN '{1,1,2}'::line $$) AS r(c gtype);

SELECT topath('"[(1,1), (2,2)]"');
SELECT topath('"((1,1), (2,2))"');
SELECT '"[(1,1), (2,2)]'::path::gtype;
SELECT topath('"((1,1), (2,2))"')::path;
SELECT * FROM cypher('expr', $$RETURN '[(1,1), (2,2)]'::path $$) AS r(c gtype);
SELECT * FROM cypher('expr', $$RETURN '[(1,1), (2,2)]'::path $$) AS r(c path);

SELECT topolygon('"(1,1), (2,2), (3, 3), (4, 4)"');
SELECT topolygon('"(1,1), (2,2), (3, 3), (4, 4)"')::polygon;
SELECT '(1,1), (2,2), (3, 3), (4, 4)'::polygon::gtype;
SELECT * FROM cypher('expr', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c gtype);
SELECT * FROM cypher('expr', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c polygon);


SELECT tocircle('"(1,1), 3"');

SELECT tobox('"(1,1), (2,2)"');
SELECT tobox('"(1,1), (2,2)"')::box;
SELECT '(1,1), (2,2)'::box::gtype;
SELECT * FROM cypher('expr', $$RETURN '(1,1), (2,2)'::box $$) AS r(c gtype);
SELECT * FROM cypher('expr', $$RETURN '(1,1), (2,2)'::box $$) AS r(c box);

--
-- map literal
--

-- empty map
SELECT * FROM cypher('expr', $$RETURN {}$$) AS r(c gtype);

-- map of scalar values
SELECT * FROM cypher('expr', $$
RETURN {s: 's', i: 1, f: 1.0, b: true, z: null}
$$) AS r(c gtype);

-- nested maps
SELECT * FROM cypher('expr', $$
RETURN {s: {s: 's'}, t: {i: 1, e: {f: 1.0}, s: {a: {b: true}}}, z: null}
$$) AS r(c gtype);

--
-- list literal
--

-- empty list
SELECT * FROM cypher('expr', $$RETURN []$$) AS r(c gtype);

-- list of scalar values
SELECT * FROM cypher('expr', $$
RETURN ['str', 1, 1.0, true, null]
$$) AS r(c gtype);

-- nested lists
SELECT * FROM cypher('expr', $$
RETURN [['str'], [1, [1.0], [[true]]], null]
$$) AS r(c gtype);

--
-- parameter
--

PREPARE cypher_parameter(gtype) AS
SELECT * FROM cypher('expr', $$
RETURN $var
$$, $1) AS t(i gtype);
EXECUTE cypher_parameter('{"var": 1}');

PREPARE cypher_parameter_object(gtype) AS
SELECT * FROM cypher('expr', $$
RETURN $var.innervar
$$, $1) AS t(i gtype);
EXECUTE cypher_parameter_object('{"var": {"innervar": 1}}');

PREPARE cypher_parameter_array(gtype) AS
SELECT * FROM cypher('expr', $$
RETURN $var[$indexvar]
$$, $1) AS t(i gtype);
EXECUTE cypher_parameter_array('{"var": [1, 2, 3], "indexvar": 1}');

-- missing parameter
PREPARE cypher_parameter_missing_argument(gtype) AS
SELECT * FROM cypher('expr', $$
RETURN $var, $missingvar
$$, $1) AS t(i gtype, j gtype);
EXECUTE cypher_parameter_missing_argument('{"var": 1}');

-- invalid parameter
PREPARE cypher_parameter_invalid_argument(gtype) AS
SELECT * FROM cypher('expr', $$
RETURN $var
$$, $1) AS t(i gtype);
EXECUTE cypher_parameter_invalid_argument('[1]');

-- missing parameters argument

PREPARE cypher_missing_params_argument(int) AS
SELECT $1, * FROM cypher('expr', $$
RETURN $var
$$) AS t(i gtype);

SELECT * FROM cypher('expr', $$
RETURN $var
$$) AS t(i gtype);

--list concatenation
SELECT * FROM cypher('expr',
$$RETURN ['str', 1, 1.0] + [true, null]$$) AS r(c gtype);

--list IN (contains), should all be true
SELECT * FROM cypher('expr',
$$RETURN 1 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 'str' IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 1.0 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN true IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN [1,3,5,[2,4,6]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN {bool: true, int: 1} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
-- should return SQL null, nothing
SELECT * FROM cypher('expr',
$$RETURN null IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN null IN ['str', 1, 1.0, true]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 'str' IN null $$) AS r(c boolean);
-- should all return false
SELECT * FROM cypher('expr',
$$RETURN 0 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 1.1 IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 'Str' IN ['str', 1, 1.0, true, null]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN [1,3,5,[2,4,5]] IN ['str', 1, 1.0, true, null, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN {bool: true, int: 2} IN ['str', 1, 1.0, true, null, {bool: true, int: 1}, [1,3,5,[2,4,6]]]$$) AS r(c boolean);
-- should error - ERROR:  object of IN must be a list
SELECT * FROM cypher('expr',
$$RETURN null IN 'str' $$) AS r(c boolean);
SELECT * FROM cypher('expr',
$$RETURN 'str' IN 'str' $$) AS r(c boolean);

-- list access
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][5]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3]$$) AS r(c gtype);
-- should return null
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][11]$$) AS r(c gtype);

-- list slice
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][..11]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..0]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][10..10]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][0..1]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][9..10]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..11]$$) AS r(c gtype);
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-3..11]$$) AS r(c gtype);
-- this one should return null
SELECT * FROM cypher('expr',
$$RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10][-1..10]$$) AS r(c gtype);
SELECT gtype_access_slice('[0]'::gtype, 'null'::gtype, '1'::gtype);
SELECT gtype_access_slice('[0]'::gtype, '0'::gtype, 'null'::gtype);
-- should error - ERROR:  slice must access a list
SELECT * from cypher('expr',
$$RETURN 0[0..1]$$) as r(a gtype);
SELECT * from cypher('expr',
$$RETURN 0[[0]..[1]]$$) as r(a gtype);
-- should return nothing
SELECT * from cypher('expr',
$$RETURN [0][0..-2147483649]$$) as r(a gtype);

-- access and slice operators nested
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[0] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-1] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][-2..] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1..] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][0] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-1] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-2..-1] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][0] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-4..-2][-2][-2..][0] $$) as (results gtype);

-- empty list
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2..-2] $$) as (results gtype);

-- should return null
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[2][3] $$) as (results gtype);
SELECT * from cypher('expr', $$ WITH [0, 1, [2, 3, 4], 5, [6, 7, 8], 9] as l RETURN l[-2..][-1..][-2] $$) as (results gtype);

--
-- String operators
--

-- String LHS + String RHS
SELECT * FROM cypher('expr', $$RETURN 'str' + 'str'$$) AS r(c gtype);

-- String LHS + Integer RHS
SELECT * FROM cypher('expr', $$RETURN 'str' + 1$$) AS r(c gtype);

-- String LHS + Float RHS
SELECT * FROM cypher('expr', $$RETURN 'str' + 1.0$$) AS r(c gtype);

-- Integer LHS + String LHS
SELECT * FROM cypher('expr', $$RETURN 1 + 'str'$$) AS r(c gtype);

-- Float LHS + String RHS
SELECT * FROM cypher('expr', $$RETURN 1.0 + 'str'$$) AS r(c gtype);

--
-- Test transform logic for operators
--

SELECT * FROM cypher('expr', $$
RETURN (-(3 * 2 - 4.0) ^ ((10 / 5) + 1)) % -3
$$) AS r(result gtype);

--
-- comparison operators
--

SELECT * FROM cypher('expr', $$
RETURN 1 = 1.0
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN 1 > -1.0
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN -1.0 < 1
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN "aaa" < "z"
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN "z" > "aaa"
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false = false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN ("string" < true)
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true < 1
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN (1 + 1.0) = (7 % 5)
$$) AS r(result boolean);


--
-- IS NULL & IS NOT NULL
--
SELECT * FROM cypher('expr', $$ RETURN null IS NULL $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN 1 IS NULL $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN 1 IS NOT NULL $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN null IS NOT NULL $$) AS r(result boolean);

--
-- AND, OR, NOT and XOR
--
SELECT * FROM cypher('expr', $$ RETURN NOT false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN NOT true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true AND true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true AND false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false AND true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false AND false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true OR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true OR false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false OR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false OR false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN NOT ((true OR false) AND (false OR true)) $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true XOR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true XOR false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false XOR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false XOR false $$) AS r(result boolean);

--
-- Test indirection transform logic for object.property, object["property"],
-- and array[element]
--
SELECT * FROM cypher('expr', $$ RETURN [ 1, { bool: true, int: 3, array: [ 9, 11, { boom: false, float: 3.14 }, 13 ] }, 5, 7, 9 ][1].array[2]["float"] $$) AS r(result gtype);

--
-- Test STARTS WITH, ENDS WITH, and CONTAINS transform logic
--
-- true
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH "abcd" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH "wxyz" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS "klmn" $$) AS r(result gtype);

-- false
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH "bcde" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH "vwxy" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS "klmo" $$) AS r(result gtype);
-- these should return SQL NULL
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH NULL $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH NULL $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS NULL $$) AS r(result gtype);

--
--Coearce to Postgres 3 int types (smallint, int, bigint)
--
SELECT create_graph('type_coercion');
SELECT * FROM cypher('type_coercion', $$
	RETURN NULL
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN 1
$$) AS (i smallint);

SELECT * FROM cypher('type_coercion', $$
	RETURN 1
$$) AS (i int);

SELECT * FROM cypher('type_coercion', $$
	RETURN 1
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN 1.0
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN 1.0::numeric
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN '1'
$$) AS (i bigint);

--Invalid String Format
SELECT * FROM cypher('type_coercion', $$
	RETURN '1.0'
$$) AS (i bigint);

-- Casting to ints that will cause overflow
SELECT * FROM cypher('type_coercion', $$
	RETURN 10000000000000000000
$$) AS (i smallint);

SELECT * FROM cypher('type_coercion', $$
	RETURN 10000000000000000000
$$) AS (i int);

--Invalid types
SELECT * FROM cypher('type_coercion', $$ RETURN true $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN {key: 1} $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN [1] $$) AS (i bigint);


--
-- typecasting '::'
--

--
-- Test from an gtype value to gtype int
--
SELECT * FROM cypher('expr', $$ RETURN 0.0::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 0.0::integer $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '0'::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '0'::integer $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 0.0::numeric::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 2.71::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 2.71::numeric::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1.0, pie: 3.1415927, e: 2::numeric}, 2, null][1].one)::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1.0::int, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one) $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::int $$) AS r(result gtype);
-- these should fail
SELECT * FROM cypher('expr', $$
RETURN '0.0'::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '1.5'::int
$$) AS r(result gtype);
SELECT * FROM cypher('graph_name', $$
RETURN "15555555555555555555555555555"::int
$$) AS (string_result gtype);
SELECT * FROM cypher('expr', $$
RETURN 'NaN'::float::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 'infinity'::float::int
$$) AS r(result gtype);

-- Test from an gtype value to an gtype numeric
--
SELECT * FROM cypher('expr', $$ RETURN 0::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 2.71::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '2.71'::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ((1 + 2.71) * 3)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].pie)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e) $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2::numeric, null]) $$) AS r(result gtype);
-- these should fail
SELECT * FROM cypher('expr', $$
RETURN ('2:71'::numeric)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ('inf'::numeric)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ('infinity'::numeric)::numeric
$$) AS r(result gtype);
-- verify that output can be accepted and reproduced correctly via gtype_in
SELECT gtype_in('2.71::numeric');
SELECT gtype_in('[0, {"e": 2.718281::numeric, "one": 1, "pie": 3.1415927}, 2::numeric, null]');
SELECT * FROM cypher('expr', $$
RETURN (['NaN'::numeric, {one: 1, pie: 3.1415927, nan: 'nAn'::numeric}, 2::numeric, null])
$$) AS r(result gtype);
SELECT gtype_in('[NaN::numeric, {"nan": NaN::numeric, "one": 1, "pie": 3.1415927}, 2::numeric, null]');

--
-- Test from an gtype value to gtype float
--
SELECT * FROM cypher('expr', $$
RETURN 0::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '2.71'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 2.71::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2::numeric}, 2, null][1].one)::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::float
$$) AS r(result gtype);
-- test NaN, infinity, and -infinity
SELECT * FROM cypher('expr', $$
RETURN 'NaN'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 'inf'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '-inf'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 'infinity'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '-infinity'::float
$$) AS r(result gtype);
-- should return SQL null
SELECT * FROM cypher('expr', $$
RETURN null::float
$$) AS r(result gtype);
-- should return JSON null
SELECT gtype_in('null::float');
-- these should fail
SELECT * FROM cypher('expr', $$
RETURN '2:71'::float
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 'infi'::float
$$) AS r(result gtype);
-- verify that output can be accepted and reproduced correctly via gtype_in
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2::numeric, null])
$$) AS r(result gtype);
SELECT gtype_in('[0, {"e": 2.718281::numeric, "one": 1.0, "pie": 3.1415927}, 2::numeric, null]');
SELECT * FROM cypher('expr', $$
RETURN (['NaN'::float, {one: 'inf'::float, pie: 3.1415927, e: 2.718281::numeric}, 2::numeric, null])
$$) AS r(result gtype);
SELECT gtype_in('[NaN, {"e": 2.718281::numeric, "one": Infinity, "pie": 3.1415927}, 2::numeric, null]');

-- size() of a string
SELECT * FROM cypher('expr', $$
    RETURN size('12345')
$$) AS (size gtype);
SELECT * FROM cypher('expr', $$
    RETURN size("1234567890")
$$) AS (size gtype);
-- size() of an array
SELECT * FROM cypher('expr', $$
    RETURN size([1, 2, 3, 4, 5])
$$) AS (size gtype);
SELECT * FROM cypher('expr', $$
    RETURN size([])
$$) AS (size gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN size(null)
$$) AS (size gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN size(1234567890)
$$) AS (size gtype);
SELECT * FROM cypher('expr', $$
    RETURN size()
$$) AS (size gtype);
-- head() of an array
SELECT * FROM cypher('expr', $$
    RETURN head([1, 2, 3, 4, 5])
$$) AS (head gtype);
SELECT * FROM cypher('expr', $$
    RETURN head([1])
$$) AS (head gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN head([])
$$) AS (head gtype);
SELECT * FROM cypher('expr', $$
    RETURN head(null)
$$) AS (head gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN head(1234567890)
$$) AS (head gtype);
SELECT * FROM cypher('expr', $$
    RETURN head()
$$) AS (head gtype);
-- last()
SELECT * FROM cypher('expr', $$
    RETURN last([1, 2, 3, 4, 5])
$$) AS (last gtype);
SELECT * FROM cypher('expr', $$
    RETURN last([1])
$$) AS (last gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN last([])
$$) AS (last gtype);
SELECT * FROM cypher('expr', $$
    RETURN last(null)
$$) AS (last gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN last(1234567890)
$$) AS (last gtype);
SELECT * FROM cypher('expr', $$
    RETURN last()
$$) AS (last gtype);
-- properties()
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN properties(v)
$$) AS (properties gtype);
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN properties(e)
$$) AS (properties gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN properties(null)
$$) AS (properties gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN properties(1234)
$$) AS (properties gtype);
SELECT * FROM cypher('expr', $$
    RETURN properties()
$$) AS (properties gtype);
-- coalesce
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, 1, null, null)
$$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, -3.14, null, null)
$$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, "string", null, null)
$$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, null, null, [])
$$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, null, null, {})
$$) AS (coalesce gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null, id(null), null)
$$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$
    RETURN coalesce(null)
$$) AS (coalesce gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN coalesce()
$$) AS (coalesce gtype);
-- toBoolean()
SELECT * FROM cypher('expr', $$
    RETURN toBoolean(true)
$$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$
    RETURN toBoolean(false)
$$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$
    RETURN toBoolean("true")
$$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$
    RETURN toBoolean("false")
$$) AS (toBoolean gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN toBoolean("falze")
$$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$
    RETURN toBoolean(null)
$$) AS (toBoolean gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN toBoolean(1)
$$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$
    RETURN toBoolean()
$$) AS (toBoolean gtype);
-- toFloat()
SELECT * FROM cypher('expr', $$
    RETURN toFloat(1)
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat(1.2)
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat("1")
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat("1.2")
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat("1.2"::numeric)
$$) AS (toFloat gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN toFloat("falze")
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat(null)
$$) AS (toFloat gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN toFloat(true)
$$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$
    RETURN toFloat()
$$) AS (toFloat gtype);
-- toInteger()
SELECT * FROM cypher('expr', $$
    RETURN toInteger(1)
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger(1.2)
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger("1")
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger("1.2")
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger("1.2"::numeric)
$$) AS (toInteger gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN toInteger("falze")
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger(null)
$$) AS (toInteger gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN toInteger(true)
$$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$
    RETURN toInteger()
$$) AS (toInteger gtype);

--
-- toString()
--
SELECT * FROM toString(gtype_in('3'));
SELECT * FROM toString(gtype_in('3.14'));
SELECT * FROM toString(gtype_in('3.14::float'));
SELECT * FROM toString(gtype_in('3.14::numeric'));
SELECT * FROM toString(gtype_in('true'));
SELECT * FROM toString(gtype_in('false'));
SELECT * FROM toString(gtype_in('"a string"'));
SELECT * FROM cypher('expr', $$ RETURN toString(3.14::numeric) $$) AS (results gtype);
-- should return null
SELECT * FROM toString(NULL);
SELECT * FROM toString(gtype_in(null));
-- should fail
SELECT * FROM toString();
SELECT * FROM cypher('expr', $$ RETURN toString() $$) AS (results gtype);

--
-- reverse(string)
--
SELECT * FROM cypher('expr', $$
    RETURN reverse("gnirts a si siht")
$$) AS (results gtype);
SELECT * FROM reverse('"gnirts a si siht"'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN reverse(null)
$$) AS (results gtype);
SELECT * FROM reverse(null);
-- should return error
SELECT * FROM reverse([4923, 'abc', 521, NULL, 487]);
-- Should return the reversed list
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 'abc', 521, NULL, 487])
$$) AS (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923])
$$) AS (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 257])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 257, null])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 257, 'tea'])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([[1, 4, 7], 4923, [1, 2, 3], 'abc', 521, NULL, 487, ['fgt', 7, 10]])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 257, {test1: "key"}])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    RETURN reverse([4923, 257, {test2: [1, 2, 3]}])
$$) as (u gtype);
SELECT * FROM cypher('expr', $$
    CREATE ({test: [1, 2, 3]})
$$) as (u gtype);

-- should fail
SELECT * FROM cypher('expr', $$
    RETURN reverse(true)
$$) AS (results gtype);
SELECT * FROM reverse(true);
SELECT * FROM cypher('expr', $$
    RETURN reverse(3.14)
$$) AS (results gtype);
SELECT * FROM reverse(3.14);
SELECT * FROM cypher('expr', $$
    RETURN reverse()
$$) AS (results gtype);
SELECT * FROM reverse();

--
-- toUpper() and toLower()
--
SELECT * FROM cypher('expr', $$
    RETURN toUpper('to uppercase')
$$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$
    RETURN toUpper('')
$$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$
    RETURN toLower('TO LOWERCASE')
$$) AS (toLower gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN toUpper(null)
$$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$
    RETURN toLower(null)
$$) AS (toLower gtype);
SELECT * FROM toupper(null);
SELECT * FROM tolower(null);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN toUpper(true)
$$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$
    RETURN toUpper()
$$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$
    RETURN toLower(true)
$$) AS (toLower gtype);
SELECT * FROM cypher('expr', $$
    RETURN toLower()
$$) AS (toLower gtype);
SELECT * FROM toupper();
SELECT * FROM tolower();

--
-- lTrim(), rTrim(), trim()
--

SELECT * FROM cypher('expr', $$
    RETURN lTrim("  string   ")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN rTrim("  string   ")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN trim("  string   ")
$$) AS (results gtype);
SELECT * FROM ltrim('"  string   "'::gtype);
SELECT * FROM rtrim('"  string   "'::gtype);
SELECT * FROM trim('"  string   "'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN lTrim(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN rTrim(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN trim(null)
$$) AS (results gtype);
SELECT * FROM ltrim(null);
SELECT * FROM rtrim(null);
SELECT * FROM trim(null);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN lTrim(true)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN rTrim(true)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN trim(true)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lTrim()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN rTrim()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN trim()
$$) AS (results gtype);

SELECT * FROM ltrim();
SELECT * FROM rtrim();
SELECT * FROM trim();

--
-- left(), right(), & substring()
-- left()
SELECT * FROM cypher('expr', $$
    RETURN left("123456789", 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN left("123456789", 3)
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN left("123456789", 0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN left(null, 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN left(null, null)
$$) AS (results gtype);
SELECT * FROM left(null, '1'::gtype);
SELECT * FROM left(null, null);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN left("123456789", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN left("123456789", -1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN left()
$$) AS (results gtype);
SELECT * FROM left('123456789', null);
SELECT * FROM left('"123456789"'::gtype, '-1'::gtype);
SELECT * FROM left();
--right()
SELECT * FROM cypher('expr', $$
    RETURN right("123456789", 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN right("123456789", 3)
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN right("123456789", 0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN right(null, 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN right(null, null)
$$) AS (results gtype);
SELECT * FROM right(null, '1'::gtype);
SELECT * FROM right(null, null);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN right("123456789", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN right("123456789", -1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN right()
$$) AS (results gtype);
SELECT * FROM right('123456789', null);
SELECT * FROM right('"123456789"'::gtype, '-1'::gtype);
SELECT * FROM right();
-- substring()
SELECT * FROM cypher('expr', $$
    RETURN substring("0123456789", 0, 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("0123456789", 1, 3)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("0123456789", 3)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("0123456789", 0)
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN substring(null, null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring(null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring(null, 1)
$$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN substring("123456789", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("123456789", 0, -1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("123456789", -1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN substring("123456789")
$$) AS (results gtype);

--
-- split()
--
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", ",")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", "")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", " ")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,cd  e,f", " ")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,cd  e,f", "  ")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", "c,")
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN split(null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split(null, ",")
$$) AS (results gtype);
SELECT * FROM split(null, null);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype, null);
SELECT * FROM split(null, '","'::gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN split(123456789, ",")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f", -1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split("a,b,c,d,e,f")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN split()
$$) AS (results gtype);
SELECT * FROM split('123456789'::gtype, '","'::gtype);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype, '-1'::gtype);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype);
SELECT * FROM split();

--
-- replace()
--
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", "lo", "p")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", "hello", "Good bye")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("abcabcabc", "abc", "a")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("abcabcabc", "ab", "")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("ababab", "ab", "ab")
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN replace(null, null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", "", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("", "", "")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", "Hello", "")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("", "Hello", "Mellow")
$$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN replace()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", "e", 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN replace("Hello", 1, "e")
$$) AS (results gtype);

--
-- sin, cos, tan, cot
--
SELECT sin::gtype = results FROM cypher('expr', $$
    RETURN sin(3.1415)
$$) AS (results gtype), sin(3.1415);
SELECT cos::gtype = results FROM cypher('expr', $$
    RETURN cos(3.1415)
$$) AS (results gtype), cos(3.1415);
SELECT tan::gtype = results FROM cypher('expr', $$
    RETURN tan(3.1415)
$$) AS (results gtype), tan(3.1415);
SELECT cot::gtype = results FROM cypher('expr', $$
    RETURN cot(3.1415)
$$) AS (results gtype), cot(3.1415);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN sin(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cos(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN tan(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cot(null)
$$) AS (results gtype);
SELECT * FROM sin(null);
SELECT * FROM cos(null);
SELECT * FROM tan(null);
SELECT * FROM cot(null);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN sin("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cos("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN tan("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cot("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sin()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cos()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN tan()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cot()
$$) AS (results gtype);
SELECT * FROM sin('0'::gtype);
SELECT * FROM cos('0'::gtype);
SELECT * FROM tan('0'::gtype);
SELECT * FROM cot('0'::gtype);
SELECT * FROM sin();
SELECT * FROM cos();
SELECT * FROM tan();
SELECT * FROM cot();

--
-- Arc functions: asin, acos, atan, & atan2
--
SELECT * FROM cypher('expr', $$
    RETURN asin(1)*2
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos(0)*2
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan(1)*4
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(1, 1)*4
$$) AS (results gtype);
SELECT * FROM asin(1), asin('1'::gtype);
SELECT * FROM acos(0), acos('0'::gtype);
SELECT * FROM atan(1), atan('1'::gtype);
SELECT * FROM atan2(1, 1), atan2('1'::gtype, '1'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN asin(1.1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos(1.1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN asin(-1.1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos(-1.1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN asin(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(null, null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(null, 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(1, null)
$$) AS (results gtype);
SELECT * FROM asin(null);
SELECT * FROM acos(null);
SELECT * FROM atan(null);
SELECT * FROM atan2(null, null);
SELECT * FROM atan2('1'::gtype, null);
SELECT * FROM atan2(null, '1'::gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN asin("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan("0")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2("0", 1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(0, "1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN asin()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN acos()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN atan2(null)
$$) AS (results gtype);
SELECT * FROM asin('0'::gtype);
SELECT * FROM acos('0'::gtype);
SELECT * FROM atan('0'::gtype);
SELECT * FROM atan2('"0"'::gtype, '1'::gtype);
SELECT * FROM atan2('1'::gtype, '"0"'::gtype);
SELECT * FROM asin();
SELECT * FROM acos();
SELECT * FROM atan();
SELECT * FROM atan2();
SELECT * FROM atan2('1'::gtype);

--
-- pi
--
SELECT * FROM cypher('expr', $$
    RETURN pi()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sin(pi())
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sin(pi()/4)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cos(pi())
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN cos(pi()/2)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sin(pi()/2)
$$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN pi(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN pi(1)
$$) AS (results gtype);

--
-- radians() & degrees()
--
SELECT * FROM cypher('expr', $$
    RETURN radians(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN radians(360), 2*pi()
$$) AS (results gtype, Two_PI gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees(2*pi())
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN radians(180), pi()
$$) AS (results gtype, PI gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees(pi())
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN radians(90), pi()/2
$$) AS (results gtype, Half_PI gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees(pi()/2)
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN radians(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees(null)
$$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN radians()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN radians("1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN degrees("1")
$$) AS (results gtype);

--sinh
SELECT results FROM cypher('expr', $$
    RETURN sinh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN sinh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN sinh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN sinh(1)
$$) AS (results gtype);
--cosh
SELECT results FROM cypher('expr', $$
    RETURN cosh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN cosh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN cosh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN cosh(1)
$$) AS (results gtype);
--tanh
SELECT results FROM cypher('expr', $$
    RETURN tanh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN tanh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN tanh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN tanh(1)
$$) AS (results gtype);
--asinh
SELECT results FROM cypher('expr', $$
    RETURN asinh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN asinh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN asinh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN asinh(1)
$$) AS (results gtype);
--acosh
SELECT results FROM cypher('expr', $$
    RETURN acosh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN acosh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN acosh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN acosh(1)
$$) AS (results gtype);
--atanh
SELECT results FROM cypher('expr', $$
    RETURN atanh(3.1415)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN atanh(pi())
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN atanh(0)
$$) AS (results gtype);
SELECT results FROM cypher('expr', $$
    RETURN atanh(1)
$$) AS (results gtype);

--
-- abs(), ceil(), floor(), & round()
--
SELECT * FROM cypher('expr', $$
    RETURN abs(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN abs(10)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN abs(-10)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(-1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(1.01)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(-1.01)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(-1)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(1.01)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(-1.01)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(4.49999999)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(4.5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(-4.49999999)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(-4.5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(7.4163, 3)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(7.416343479, 8)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(7.416343479, NULL)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(NULL, 7)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(7, 2)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(7.4342, 2.1123)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(NULL, NULL)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign(10)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign(-10)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign(0)
$$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN abs(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round(null)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign(null)
$$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN abs()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign()
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN abs("1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN ceil("1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN floor("1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN round("1")
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN sign("1")
$$) AS (results gtype);

--
-- gcd
--
SELECT * FROM cypher('expr', $$
    RETURN gcd(10, 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN gcd(10.0, 5.0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN gcd(10.0, 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN gcd(10, 5::numeric)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN gcd('10', 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN gcd(10::numeric, '5')
$$) AS (results gtype);

--
-- lcm
--
SELECT * FROM cypher('expr', $$
    RETURN lcm(10, 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lcm(10.0, 5.0)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lcm(10.0, 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lcm(10, 5::numeric)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lcm('10', 5)
$$) AS (results gtype);
SELECT * FROM cypher('expr', $$
    RETURN lcm(10::numeric, '5')
$$) AS (results gtype);


--
-- rand()
--
-- should select 0 rows as rand() is in [0,1)
SELECT true FROM cypher('expr', $$
    RETURN rand()
$$) AS (result int)
WHERE result >= 1 or result < 0;
-- should select 0 rows as rand() should not return the same value
SELECT * FROM cypher('expr', $$
    RETURN rand()
$$) AS cypher_1(result gtype),
    cypher('expr', $$
    RETURN rand()
$$) AS cypher_2(result gtype)
WHERE cypher_1.result = cypher_2.result;

--
-- log (ln) and log10
--
SELECT * from cypher('expr', $$
    RETURN log(2.718281828459045)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log10(10)
$$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$
    RETURN log(null)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log10(null)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log(0)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log10(0)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log(-1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log10(-1)
$$) as (result gtype);
-- should fail
SELECT * from cypher('expr', $$
    RETURN log()
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log10()
$$) as (result gtype);

--
-- e()
--
SELECT * from cypher('expr', $$
    RETURN e()
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN log(e())
$$) as (result gtype);

--
-- exp() aka e^x
--
SELECT * from cypher('expr', $$
    RETURN exp(1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN exp(0)
$$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$
    RETURN exp(null)
$$) as (result gtype);
-- should fail
SELECT * from cypher('expr', $$
    RETURN exp()
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN exp("1")
$$) as (result gtype);

--
-- sqrt()
--
SELECT * from cypher('expr', $$
    RETURN sqrt(25)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN sqrt(1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN sqrt(0)
$$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$
    RETURN sqrt(-1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN sqrt(null)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN sqrt("1")
$$) as (result gtype);


--
-- cbrt()
--
SELECT * from cypher('expr', $$
    RETURN cbrt(125)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN cbrt(1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN cbrt(0)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN cbrt(-1)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN cbrt(null)
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN cbrt("1")
$$) as (result gtype);


--CASE
SELECT create_graph('case_statement');
SELECT * FROM cypher('case_statement', $$CREATE ({i: 1, j: null})$$) AS (result gtype);
SELECT * FROM cypher('case_statement', $$CREATE ({i: 'a', j: 'b'})$$) AS (result gtype);
SELECT * FROM cypher('case_statement', $$CREATE ({i: 0, j: 1})$$) AS (result gtype);
SELECT * FROM cypher('case_statement', $$CREATE ({i: true, j: false})$$) AS (result gtype);
SELECT * FROM cypher('case_statement', $$CREATE ({i: [], j: [0,1,2]})$$) AS (result gtype);
SELECT * FROM cypher('case_statement', $$CREATE ({i: {}, j: {i:1}})$$) AS (result gtype);

--CASE WHEN condition THEN result END
SELECT * FROM cypher('case_statement', $$
	MATCH (n)
	RETURN n.i, n.j, CASE
    WHEN null THEN 'should not return me'
		WHEN n.i = 1 THEN 'i is 1'
		WHEN n.j = 'b' THEN 'j is b'
    WHEN n.i = 0 AND n.j = 1 THEN '0 AND 1'
    WHEN n.i = true OR n.j = true THEN 'i or j true'
		ELSE 'default'
	END
$$ ) AS (i gtype, j gtype, case_statement gtype);

--CASE expression WHEN value THEN result END
SELECT * FROM cypher('case_statement', $$
	MATCH (n)
	RETURN n.j, CASE n.j
    WHEN null THEN 'should not return me'
    WHEN 'b' THEN 'b'
    WHEN 1 THEN 1
    WHEN false THEN false
    WHEN [0,1,2] THEN [0,1,2]
    WHEN {i:1} THEN {i:1}
		ELSE 'not a or b'
	END
$$ ) AS (j gtype, case_statement gtype);

-- list functions range(), keys()
SELECT create_graph('keys');
-- keys()
SELECT * FROM cypher('keys', $$CREATE ({name: 'hikaru utada', age: 38, job: 'singer'})-[:collaborated_with {song:"face my fears"}]->( {name: 'sonny moore', age: 33, stage_name: 'skrillex', job: 'producer'})$$) AS (result gtype);
SELECT * FROM cypher('keys', $$CREATE ({name: 'alexander guy cook', age: 31, stage_name:"a. g. cook", job: 'producer'})$$) AS (result gtype);
SELECT * FROM cypher('keys', $$CREATE ({name: 'keiko fuji', age: 62, job: 'singer'})$$) AS (result gtype);
SELECT * FROM cypher('keys', $$MATCH (a),(b) WHERE a.name = 'hikaru utada' AND b.name = 'alexander guy cook' CREATE (a)-[:collaborated_with {song:"one last kiss"}]->(b)$$) AS (result gtype);
SELECT * FROM cypher('keys', $$MATCH (a),(b) WHERE a.name = 'hikaru utada' AND b.name = 'keiko fuji' CREATE (a)-[:knows]->(b)$$) AS (result gtype);
SELECT * FROM cypher('keys', $$MATCH (v) RETURN keys(v)$$) AS (vertex_keys gtype);
SELECT * FROM cypher('keys', $$MATCH ()-[e]-() RETURN keys(e)$$) AS (edge_keys gtype);
SELECT * FROM cypher('keys', $$RETURN keys({a:1,b:'two',c:[1,2,3]})$$) AS (keys gtype);

--should return empty list
SELECT * FROM cypher('keys', $$RETURN keys({})$$) AS (keys gtype);
--should return sql null
SELECT * FROM cypher('keys', $$RETURN keys(null)$$) AS (keys gtype);
--should return error
SELECT * from cypher('keys', $$RETURN keys([1,2,3])$$) as (keys gtype);
SELECT * from cypher('keys', $$RETURN keys("string")$$) as (keys gtype);
SELECT * from cypher('keys', $$MATCH u=()-[]-() RETURN keys(u)$$) as (keys gtype);

SELECT create_graph('list');
SELECT * from cypher('list', $$CREATE p=({name:"rick"})-[:knows]->({name:"morty"}) RETURN p$$) as (path traversal);
SELECT * from cypher('list', $$CREATE p=({name:'rachael'})-[:knows]->({name:'monica'})-[:knows]->({name:'phoebe'}) RETURN p$$) as (path traversal);
-- range()
SELECT * from cypher('list', $$RETURN range(0, 10)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, 10, null)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, 10, 1)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, 10, 3)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, -10, -1)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, -10, -3)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, 10, 11)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(-20, 10, 5)$$) as (range gtype);
-- should return an empty list []
SELECT * from cypher('list', $$RETURN range(0, -10)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, 10, -1)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(-10, 10, -1)$$) as (range gtype);
-- should return an error
SELECT * from cypher('list', $$RETURN range(null, -10, -3)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, null, -3)$$) as (range gtype);
SELECT * from cypher('list', $$RETURN range(0, -10.0, -3.0)$$) as (range gtype);
-- labels()
SELECT * from cypher('list', $$CREATE (u:People {name: "John"}) RETURN u$$) as (Vertices vertex);
SELECT * from cypher('list', $$CREATE (u:People {name: "Larry"}) RETURN u$$) as (Vertices vertex);
SELECT * from cypher('list', $$CREATE (u:Cars {name: "G35"}) RETURN u$$) as (Vertices vertex);
SELECT * from cypher('list', $$CREATE (u:Cars {name: "MR2"}) RETURN u$$) as (Vertices vertex);
SELECT * from cypher('list', $$MATCH (u) RETURN labels(u), u$$) as (Labels gtype, Vertices vertex);
-- should return SQL NULL
SELECT * from cypher('list', $$RETURN labels(NULL)$$) as (Labels gtype);
-- should return an error
SELECT * from cypher('list', $$RETURN labels("string")$$) as (Labels gtype);

--
-- Cleanup
--
SELECT * FROM drop_graph('chained', true);
SELECT * FROM drop_graph('case_statement', true);
SELECT * FROM drop_graph('type_coercion', true);
SELECT * FROM drop_graph('expr', true);
SELECT * FROM drop_graph('regex', true);
SELECT * FROM drop_graph('keys', true);
SELECT * FROM drop_graph('list', true);

--
-- End of tests
--
