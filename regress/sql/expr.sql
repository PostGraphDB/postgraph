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

SELECT '{"Hello", "World"}'::text[]::gtype;

SELECT '{1, 2, 3, 4, 5, 6}'::int8[]::gtype;

SELECT '{1.0, 2, 3.0, 4, 5, 6}'::float4[]::gtype;
SELECT '{1.0, 2, 3.0, 4, 5, 6}'::float8[]::gtype;


--
-- map literal
--
-- empty map
SELECT * FROM cypher('expr', $$RETURN {}$$) AS r(c gtype);
-- map of scalar values
SELECT * FROM cypher('expr', $$ RETURN {s: 's', i: 1, f: 1.0, b: true, z: null} $$) AS r(c gtype);
-- nested maps
SELECT * FROM cypher('expr', $$ RETURN {s: {s: 's'}, t: {i: 1, e: {f: 1.0}, s: {a: {b: true}}}, z: null} $$) AS r(c gtype);

--
-- parameter
--
PREPARE cypher_parameter(gtype) AS SELECT * FROM cypher('expr', $$ RETURN $var $$, $1) AS t(i gtype);
EXECUTE cypher_parameter('{"var": 1}');

PREPARE cypher_parameter_object(gtype) AS SELECT * FROM cypher('expr', $$ RETURN $var.innervar $$, $1) AS t(i gtype);
EXECUTE cypher_parameter_object('{"var": {"innervar": 1}}');

PREPARE cypher_parameter_array(gtype) AS SELECT * FROM cypher('expr', $$ RETURN $var[$indexvar] $$, $1) AS t(i gtype);
EXECUTE cypher_parameter_array('{"var": [1, 2, 3], "indexvar": 1}');

-- missing parameter
PREPARE cypher_parameter_missing_argument(gtype) AS SELECT * FROM cypher('expr', $$ RETURN $var, $missingvar $$, $1) AS t(i gtype, j gtype);
EXECUTE cypher_parameter_missing_argument('{"var": 1}');

-- invalid parameter
PREPARE cypher_parameter_invalid_argument(gtype) AS SELECT * FROM cypher('expr', $$ RETURN $var $$, $1) AS t(i gtype);
EXECUTE cypher_parameter_invalid_argument('[1]');

-- missing parameters argument
PREPARE cypher_missing_params_argument(int) AS SELECT $1, * FROM cypher('expr', $$ RETURN $var $$) AS t(i gtype);
SELECT * FROM cypher('expr', $$ RETURN $var $$) AS t(i gtype);

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
SELECT * FROM cypher('expr', $$ RETURN (-(3 * 2 - 4.0) ^ ((10 / 5) + 1)) % -3 $$) AS r(result gtype);

--
-- a bunch of comparison operators
--
SELECT * FROM cypher('expr', $$ RETURN 1 = 1.0 $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN 1 > -1.0 $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN -1.0 < 1 $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN "aaa" < "z" $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN "z" > "aaa" $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false = false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN ("string" < true) $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true < 1 $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN (1 + 1.0) = (7 % 5) $$) AS r(result boolean);

-- IS NULL
SELECT * FROM cypher('expr', $$ RETURN null IS NULL $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN 1 IS NULL $$) AS r(result boolean);
-- IS NOT NULL
SELECT * FROM cypher('expr', $$ RETURN 1 IS NOT NULL $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN null IS NOT NULL $$) AS r(result boolean);
-- NOT
SELECT * FROM cypher('expr', $$ RETURN NOT false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN NOT true $$) AS r(result boolean);
-- AND
SELECT * FROM cypher('expr', $$ RETURN true AND true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true AND false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false AND true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false AND false $$) AS r(result boolean);
-- OR
SELECT * FROM cypher('expr', $$ RETURN true OR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN true OR false $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false OR true $$) AS r(result boolean);
SELECT * FROM cypher('expr', $$ RETURN false OR false $$) AS r(result boolean);
-- The ONE test on operator precedence...
SELECT * FROM cypher('expr', $$ RETURN NOT ((true OR false) AND (false OR true)) $$) AS r(result boolean);
-- XOR
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
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH "bcde" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH "vwxy" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS "klmo" $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH NULL $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH NULL $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS NULL $$) AS r(result gtype);

--
--Coearce to Postgres 3 int types (smallint, int, bigint)
--
SELECT create_graph('type_coercion');
SELECT * FROM cypher('type_coercion', $$ RETURN NULL $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN 1 $$) AS (i smallint);
SELECT * FROM cypher('type_coercion', $$ RETURN 1 $$) AS (i int);
SELECT * FROM cypher('type_coercion', $$ RETURN 1 $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN 1.0 $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN 1.0::numeric $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN '1' $$) AS (i bigint);
--Invalid String Format
SELECT * FROM cypher('type_coercion', $$ RETURN '1.0' $$) AS (i bigint);
-- Casting to ints that will cause overflow
SELECT * FROM cypher('type_coercion', $$ RETURN 10000000000000000000 $$) AS (i smallint);
SELECT * FROM cypher('type_coercion', $$ RETURN 10000000000000000000 $$) AS (i int);
--Invalid types
SELECT * FROM cypher('type_coercion', $$ RETURN true $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN {key: 1} $$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$ RETURN [1] $$) AS (i bigint);


--
-- typecasting '::'
--

-- gtype int
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
SELECT * FROM cypher('expr', $$ RETURN '0.0'::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '1.5'::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 'NaN'::float::int $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 'infinity'::float::int $$) AS r(result gtype);

-- to gtype numeric
SELECT * FROM cypher('expr', $$ RETURN 0::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 2.71::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '2.71'::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ((1 + 2.71) * 3)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].pie)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e) $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2::numeric, null]) $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ('2:71'::numeric)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ('inf'::numeric)::numeric $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ('infinity'::numeric)::numeric $$) AS r(result gtype);

-- to gtype float
SELECT * FROM cypher('expr', $$ RETURN 0::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '2.71'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 2.71::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2::numeric}, 2, null][1].one)::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one) $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::float $$) AS r(result gtype);

-- Float
SELECT * FROM cypher('expr', $$ RETURN 'NaN'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 'inf'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '-inf'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 'infinity'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '-infinity'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN null::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN '2:71'::float $$) AS r(result gtype);
SELECT * FROM cypher('expr', $$ RETURN 'infi'::float $$) AS r(result gtype);

-- size() of a string
SELECT * FROM cypher('expr', $$ RETURN size('12345') $$) AS (size gtype);
SELECT * FROM cypher('expr', $$ RETURN size("1234567890") $$) AS (size gtype);
-- coalesce
SELECT * FROM cypher('expr', $$ RETURN coalesce(null, 1, null, null) $$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$ RETURN coalesce(null, -3.14, null, null) $$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$ RETURN coalesce(null, "string", null, null) $$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$ RETURN coalesce(null, null, null, []) $$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$ RETURN coalesce(null, null, null, {}) $$) AS (coalesce gtype);
SELECT * FROM cypher('expr', $$ RETURN coalesce(null) $$) AS (coalesce gtype);
-- toBoolean
SELECT * FROM cypher('expr', $$ RETURN toBoolean(true) $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean(false) $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean("true") $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean("false") $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean("falze") $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean(null) $$) AS (toBoolean gtype);
SELECT * FROM cypher('expr', $$ RETURN toBoolean(1) $$) AS (toBoolean gtype);
-- toFloat
SELECT * FROM cypher('expr', $$ RETURN toFloat(1) $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat(1.2) $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat("1") $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat("1.2") $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat("1.2"::numeric) $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat("falze") $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat(null) $$) AS (toFloat gtype);
SELECT * FROM cypher('expr', $$ RETURN toFloat(true) $$) AS (toFloat gtype);
-- toInteger
SELECT * FROM cypher('expr', $$ RETURN toInteger(1) $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger(1.2) $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger("1") $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger("1.2") $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger("1.2"::numeric) $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger("falze") $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger(null) $$) AS (toInteger gtype);
SELECT * FROM cypher('expr', $$ RETURN toInteger(true) $$) AS (toInteger gtype);
-- toString
SELECT * FROM cypher('expr', $$ RETURN toString(3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(3.14) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(3.14::float) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(3.14::numeric) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(true) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(false) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString('a string') $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN toString(null) $$) AS (results gtype);

-- reverse(string)
SELECT * FROM cypher('expr', $$ RETURN reverse("gnirts a si siht") $$) AS (results gtype);
SELECT * FROM reverse('"gnirts a si siht"'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$ RETURN reverse(null) $$) AS (results gtype);
SELECT * FROM reverse(null);
SELECT * FROM reverse([4923, 'abc', 521, NULL, 487]);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 'abc', 521, NULL, 487]) $$) AS (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923]) $$) AS (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 257]) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 257, null]) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 257, 'tea']) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([[1, 4, 7], 4923, [1, 2, 3], 'abc', 521, NULL, 487, ['fgt', 7, 10]]) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 257, {test1: "key"}]) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse([4923, 257, {test2: [1, 2, 3]}]) $$) as (u gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse(true) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN reverse(3.14) $$) AS (results gtype);
-- toUpper
SELECT * FROM cypher('expr', $$ RETURN toUpper('to uppercase') $$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$ RETURN toUpper(null) $$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$ RETURN toUpper('') $$) AS (toUpper gtype);
SELECT * FROM cypher('expr', $$ RETURN toUpper(true) $$) AS (toUpper gtype);
-- toLower
SELECT * FROM cypher('expr', $$ RETURN toLower('TO LOWERCASE') $$) AS (toLower gtype);
SELECT * FROM cypher('expr', $$ RETURN toLower(null) $$) AS (toLower gtype);
SELECT * FROM cypher('expr', $$ RETURN toLower(true) $$) AS (toLower gtype);
-- lTrim
SELECT * FROM cypher('expr', $$ RETURN lTrim("  string   ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lTrim(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lTrim(true) $$) AS (results gtype);
-- rtrim
SELECT * FROM cypher('expr', $$ RETURN rTrim("  string   ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN rTrim(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN rTrim(true) $$) AS (results gtype);
-- trim
SELECT * FROM cypher('expr', $$ RETURN trim("  string   ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN trim(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN trim(true) $$) AS (results gtype);
-- left
SELECT * FROM cypher('expr', $$ RETURN left("123456789", 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left("123456789", 3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left("123456789", 0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left(null, 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left(null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left("123456789", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left("123456789", -1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN left() $$) AS (results gtype);
--right
SELECT * FROM cypher('expr', $$ RETURN right("123456789", 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right("123456789", 3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right("123456789", 0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right(null, 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right(null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right("123456789", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right("123456789", -1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN right() $$) AS (results gtype);
-- substring
SELECT * FROM cypher('expr', $$ RETURN substring("0123456789", 0, 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("0123456789", 1, 3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("0123456789", 3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("0123456789", 0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring(null, null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring(null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring(null, 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("123456789", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("123456789", 0, -1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("123456789", -1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN substring("123456789") $$) AS (results gtype);
-- split
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", ",") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", "") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", " ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,cd  e,f", " ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,cd  e,f", "  ") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", "c,") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split(null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split(null, ",") $$) AS (results gtype);
SELECT * FROM split(null, null);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype, null);
SELECT * FROM split(null, '","'::gtype);
SELECT * FROM cypher('expr', $$ RETURN split(123456789, ",") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f", -1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split("a,b,c,d,e,f") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN split() $$) AS (results gtype);
SELECT * FROM split('123456789'::gtype, '","'::gtype);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype, '-1'::gtype);
SELECT * FROM split('"a,b,c,d,e,f"'::gtype);

-- replace
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", "lo", "p") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", "hello", "Good bye") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("abcabcabc", "abc", "a") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("abcabcabc", "ab", "") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("ababab", "ab", "ab") $$) AS (results gtype);
-- should return null
SELECT * FROM cypher('expr', $$ RETURN replace(null, null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", "", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("", "", "") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", "Hello", "") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("", "Hello", "Mellow") $$) AS (results gtype);
-- should fail
SELECT * FROM cypher('expr', $$ RETURN replace("Hello") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", "e", 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN replace("Hello", 1, "e") $$) AS (results gtype);

--
-- sin, cos, tan, cot
--
SELECT sin::gtype = results FROM cypher('expr', $$ RETURN sin(3.1415) $$) AS (results gtype), sin(3.1415);
SELECT cos::gtype = results FROM cypher('expr', $$ RETURN cos(3.1415) $$) AS (results gtype), cos(3.1415);
SELECT tan::gtype = results FROM cypher('expr', $$ RETURN tan(3.1415) $$) AS (results gtype), tan(3.1415);
SELECT cot::gtype = results FROM cypher('expr', $$ RETURN cot(3.1415) $$) AS (results gtype), cot(3.1415);
-- should return null
SELECT * FROM cypher('expr', $$ RETURN sin(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cos(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN tan(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cot(null) $$) AS (results gtype);
SELECT * FROM sin(null);
SELECT * FROM cos(null);
SELECT * FROM tan(null);
SELECT * FROM cot(null);
-- should fail
SELECT * FROM cypher('expr', $$ RETURN sin("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cos("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN tan("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cot("0") $$) AS (results gtype);

--
-- Arc functions: asin, acos, atan, & atan2
--
SELECT * FROM cypher('expr', $$ RETURN asin(1)*2 $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN acos(0)*2 $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan(1)*4 $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2(1, 1)*4 $$) AS (results gtype);
SELECT * FROM asin(1), asin('1'::gtype);
SELECT * FROM acos(0), acos('0'::gtype);
SELECT * FROM atan(1), atan('1'::gtype);
SELECT * FROM atan2(1, 1), atan2('1'::gtype, '1'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$ RETURN asin(1.1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN acos(1.1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN asin(-1.1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN acos(-1.1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN asin(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN acos(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2(null, null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2(null, 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2(1, null) $$) AS (results gtype);
SELECT * FROM asin(null);
SELECT * FROM acos(null);
SELECT * FROM atan(null);
SELECT * FROM atan2(null, null);
SELECT * FROM atan2('1'::gtype, null);
SELECT * FROM atan2(null, '1'::gtype);
SELECT * FROM cypher('expr', $$ RETURN asin("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN acos("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan("0") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2("0", 1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN atan2(0, "1") $$) AS (results gtype);
-- pi
SELECT * FROM cypher('expr', $$ RETURN pi() $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sin(pi()) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sin(pi()/4) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cos(pi()) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN cos(pi()/2) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sin(pi()/2) $$) AS (results gtype);
--
-- radians() & degrees()
--
SELECT * FROM cypher('expr', $$ RETURN radians(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN radians(360), 2*pi() $$) AS (results gtype, Two_PI gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees(2*pi()) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN radians(180), pi() $$) AS (results gtype, PI gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees(pi()) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN radians(90), pi()/2 $$) AS (results gtype, Half_PI gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees(pi()/2) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN radians(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN radians("1") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN degrees("1") $$) AS (results gtype);

--sinh
SELECT results FROM cypher('expr', $$ RETURN sinh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN sinh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN sinh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN sinh(1) $$) AS (results gtype);
--cosh
SELECT results FROM cypher('expr', $$ RETURN cosh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN cosh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN cosh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN cosh(1) $$) AS (results gtype);
--tanh
SELECT results FROM cypher('expr', $$ RETURN tanh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN tanh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN tanh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN tanh(1) $$) AS (results gtype);
--asinh
SELECT results FROM cypher('expr', $$ RETURN asinh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN asinh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN asinh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN asinh(1) $$) AS (results gtype);
--acosh
SELECT results FROM cypher('expr', $$ RETURN acosh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN acosh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN acosh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN acosh(1) $$) AS (results gtype);
--atanh
SELECT results FROM cypher('expr', $$ RETURN atanh(3.1415) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN atanh(pi()) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN atanh(0) $$) AS (results gtype);
SELECT results FROM cypher('expr', $$ RETURN atanh(1) $$) AS (results gtype);

-- abs
SELECT * FROM cypher('expr', $$ RETURN abs(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN abs(10) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN abs(-10) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN abs(null) $$) AS (results gtype);
-- ceil
SELECT * FROM cypher('expr', $$ RETURN ceil(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(-1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(1.01) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(-1.01) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceiling(-1.01) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(-1.01::numeric) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil("1") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN ceil(null) $$) AS (results gtype);
-- floor
SELECT * FROM cypher('expr', $$ RETURN floor(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor(1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor(-1) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor(1.01) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor(-1.01) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor(null) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN floor("1") $$) AS (results gtype);
-- round
SELECT * FROM cypher('expr', $$ RETURN round(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(4.49999999) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(4.5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(-4.49999999) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(-4.5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(7.4163, 3) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(7.416343479, 8) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(7.416343479, NULL) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round("1") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(NULL, 7) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(7, 2) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(7.4342, 2.1123) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN round(NULL, NULL) $$) AS (results gtype);
-- sign
SELECT * FROM cypher('expr', $$ RETURN sign(10) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sign(-10) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sign(0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sign("1") $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN sign(null) $$) AS (results gtype);
-- gcd
SELECT * FROM cypher('expr', $$ RETURN gcd(10, 5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN gcd(10.0, 5.0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN gcd(10.0, 5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN gcd(10, 5::numeric) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN gcd('10', 5) $$) AS (results gtype);
-- lcm
SELECT * FROM cypher('expr', $$ RETURN lcm(10, 5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lcm(10.0, 5.0) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lcm(10.0, 5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lcm('10', 5) $$) AS (results gtype);
SELECT * FROM cypher('expr', $$ RETURN lcm(10::numeric, '5') $$) AS (results gtype);

--
-- rand()
--
-- should select 0 rows as rand() is in [0,1)
SELECT true FROM cypher('expr', $$ RETURN rand() $$) AS (result int) WHERE result >= 1 or result < 0;

--
-- log (ln) and log10
--
SELECT * from cypher('expr', $$ RETURN log(2.718281828459045) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log10(10) $$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$ RETURN log(null) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log10(null) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log(0) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log10(0) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log(-1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log10(-1) $$) as (result gtype);

--
-- e()
--
SELECT * from cypher('expr', $$ RETURN e() $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN log(e()) $$) as (result gtype);

--
-- exp() aka e^x
--
SELECT * from cypher('expr', $$ RETURN exp(1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN exp(0) $$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$ RETURN exp(null) $$) as (result gtype);
-- should fail
SELECT * from cypher('expr', $$ RETURN exp("1") $$) as (result gtype);

--
-- sqrt()
--
SELECT * from cypher('expr', $$ RETURN sqrt(25) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN sqrt(1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN sqrt(0) $$) as (result gtype);
-- should return null
SELECT * from cypher('expr', $$ RETURN sqrt(-1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN sqrt(null) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN sqrt("1") $$) as (result gtype);

--
-- cbrt()
--
SELECT * from cypher('expr', $$ RETURN cbrt(125) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN cbrt(1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN cbrt(0) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN cbrt(-1) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN cbrt(null) $$) as (result gtype);
SELECT * from cypher('expr', $$ RETURN cbrt("1") $$) as (result gtype);

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

--
-- Cleanup
--
SELECT * FROM drop_graph('case_statement', true);
SELECT * FROM drop_graph('type_coercion', true);
SELECT * FROM drop_graph('expr', true);

--
-- End of tests
--
