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

SELECT * FROM create_graph('expr');

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
-- Timestamp
--
SELECT * FROM cypher('expr', $$
RETURN '2023-06-23 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('expr', $$
RETURN '06/23/2023 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('expr', $$
RETURN 'Fri Jun 23 13:39:40.00 2023"'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('expr', $$
RETURN '06/23/1970 13:39:40.00'::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('expr', $$
RETURN 0::timestamp
$$) AS r(c gtype);
SELECT * FROM cypher('expr', $$
RETURN NULL::timestamp
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
-- Test transform logic for comparison operators
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
-- Test chained comparisons
--

SELECT * FROM create_graph('chained');
SELECT * FROM cypher('chained', $$ CREATE (:people {name: "Jason", age:50}) $$) AS (result gtype);
SELECT * FROM cypher('chained', $$ CREATE (:people {name: "Amy", age:25}) $$) AS (result gtype);
SELECT * FROM cypher('chained', $$ CREATE (:people {name: "Samantha", age:35}) $$) AS (result gtype);
SELECT * FROM cypher('chained', $$ CREATE (:people {name: "Mark", age:40}) $$) AS (result gtype);
SELECT * FROM cypher('chained', $$ CREATE (:people {name: "David", age:15}) $$) AS (result gtype);
-- should return 1
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 35 < u.age <= 49  RETURN u $$) AS (result vertex);
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 25 <= u.age <= 25  RETURN u $$) AS (result vertex);
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 35 = u.age = 35  RETURN u $$) AS (result vertex);
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 50 > u.age > 35  RETURN u $$) AS (result vertex);
-- should return 3
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 40 <> u.age <> 35 RETURN u $$) AS (result vertex);
-- should return 2
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 30 <= u.age <= 49 > u.age RETURN u $$) AS (result vertex);
-- should return 0
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 30 <= u.age <= 49 = u.age RETURN u $$) AS (result vertex);
-- should return 2
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE 35 < u.age + 1 <= 50 RETURN u $$) AS (result vertex);
-- should return 3
SELECT * FROM cypher('chained', $$ MATCH (u:people) WHERE NOT 35 < u.age + 1 <= 50 RETURN u $$) AS (result vertex);

--
-- Test transform logic for IS NULL & IS NOT NULL
--

SELECT * FROM cypher('expr', $$
RETURN null IS NULL
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN 1 IS NULL
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN 1 IS NOT NULL
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN null IS NOT NULL
$$) AS r(result boolean);

--
-- Test transform logic for AND, OR, NOT and XOR
--

SELECT * FROM cypher('expr', $$
RETURN NOT false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN NOT true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true AND true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true AND false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false AND true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false AND false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true OR true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true OR false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false OR true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false OR false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN NOT ((true OR false) AND (false OR true))
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true XOR true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN true XOR false
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false XOR true
$$) AS r(result boolean);

SELECT * FROM cypher('expr', $$
RETURN false XOR false
$$) AS r(result boolean);

--
-- Test indirection transform logic for object.property, object["property"],
-- and array[element]
--

SELECT * FROM cypher('expr', $$
RETURN [
  1,
  {
    bool: true,
    int: 3,
    array: [
      9,
      11,
      {
        boom: false,
        float: 3.14
      },
      13
    ]
  },
  5,
  7,
  9
][1].array[2]["float"]
$$) AS r(result gtype);

--
-- Test STARTS WITH, ENDS WITH, and CONTAINS transform logic
--

SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH "abcd"
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH "wxyz"
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS "klmn"
$$) AS r(result gtype);

-- these should return false
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH "bcde"
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH "vwxy"
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS "klmo"
$$) AS r(result gtype);
-- these should return SQL NULL
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" STARTS WITH NULL
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" ENDS WITH NULL
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN "abcdefghijklmnopqrstuvwxyz" CONTAINS NULL
$$) AS r(result gtype);

--
-- Test =~ aka regular expression comparisons
--
SELECT create_graph('regex');
SELECT * FROM cypher('regex', $$
CREATE (n:Person {name: 'John'}) RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
CREATE (n:Person {name: 'Jeff'}) RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
CREATE (n:Person {name: 'Joan'}) RETURN n
$$) AS r(result vertex);

SELECT * FROM cypher('regex', $$
MATCH (n:Person) WHERE n.name =~ 'JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
MATCH (n:Person) WHERE n.name =~ '(?i)JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
MATCH (n:Person) WHERE n.name =~ 'Jo.n' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
MATCH (n:Person) WHERE n.name =~ 'J.*' RETURN n
$$) AS r(result vertex);

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
SELECT * FROM cypher('type_coercion', $$
	RETURN true
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN {key: 1}
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$
	RETURN [1]
$$) AS (i bigint);

SELECT * FROM cypher('type_coercion', $$CREATE ()-[:edge]->()$$) AS (result gtype);
SELECT * FROM cypher('type_coercion', $$
	MATCH (v)
	RETURN v
$$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$
	MATCH ()-[e]-()
	RETURN e
$$) AS (i bigint);
SELECT * FROM cypher('type_coercion', $$
	MATCH p=()-[]-()
	RETURN p
$$) AS (i bigint);

--
-- Test typecasting '::' transform and execution logic
--

--
-- Test from an gtype value to gtype int
--
SELECT * FROM cypher('expr', $$
RETURN 0.0::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 0.0::integer
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '0'::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '0'::integer
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 0.0::numeric::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 2.71::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 2.71::numeric::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1.0, pie: 3.1415927, e: 2::numeric}, 2, null][1].one)::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1.0::int, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1::float, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].one)::int
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::int
$$) AS r(result gtype);
-- should return SQL null
SELECT * FROM cypher('expr', $$
RETURN null::int
$$) AS r(result gtype);
-- should return JSON null
SELECT gtype_in('null::int');
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
SELECT * FROM cypher('expr', $$
RETURN 0::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN 2.71::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN '2.71'::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN (2.71::numeric)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ('2.71'::numeric)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ('NaN'::numeric)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ((1 + 2.71) * 3)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].pie)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e)
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][1].e)::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2, null][3])::numeric
$$) AS r(result gtype);
SELECT * FROM cypher('expr', $$
RETURN ([0, {one: 1, pie: 3.1415927, e: 2.718281::numeric}, 2::numeric, null])
$$) AS r(result gtype);
-- should return SQL null
SELECT age_tonumeric('null'::gtype);
SELECT age_tonumeric(null);
SELECT * FROM cypher('expr', $$
RETURN null::numeric
$$) AS r(result gtype);
-- should return JSON null
SELECT gtype_in('null::numeric');
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

-- make sure that output can be read back in and reproduce the output
SELECT gtype_in('{"name": "container 0", "vertices": [{"vertex_0": {"id": 0, "label": "vertex 0", "properties": {}}::vertex}, {"vertex_0": {"id": 0, "label": "vertex 0", "properties": {}}::vertex}]}');
SELECT gtype_in('{"name": "container 1", "edges": [{"id": 3, "label": "edge 0", "end_id": 1, "start_id": 0, "properties": {}}::edge, {"id": 4, "label": "edge 1", "end_id": 0, "start_id": 1, "properties": {}}::edge]}');
SELECT gtype_in('{"name": "path 1", "path": [{"id": 0, "label": "vertex 0", "properties": {}}::vertex, {"id": 2, "label": "edge 0", "end_id": 1, "start_id": 0, "properties": {}}::edge, {"id": 1, "label": "vertex 1", "properties": {}}::vertex]}');

-- test functions
-- create some vertices and edges
-- XXX: Move after the CREATE regression tests...

SELECT * FROM cypher('expr', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('expr', $$CREATE (:v {i: 0})$$) AS (a gtype);
SELECT * FROM cypher('expr', $$CREATE (:v {i: 1})$$) AS (a gtype);
SELECT * FROM cypher('expr', $$
    CREATE (:v1 {id:'initial'})-[:e1]->(:v1 {id:'middle'})-[:e1]->(:v1 {id:'end'})
$$) AS (a gtype);
-- id()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN id(e)
$$) AS (id gtype);
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN id(v)
$$) AS (id gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN id(null)
$$) AS (id gtype);
-- should error
SELECT * FROM cypher('expr', $$
    RETURN id()
$$) AS (id gtype);
-- start_id()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN start_id(e)
$$) AS (start_id gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN start_id(null)
$$) AS (start_id gtype);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN start_id(v)
$$) AS (start_id gtype);
SELECT * FROM cypher('expr', $$
    RETURN start_id()
$$) AS (start_id gtype);
-- end_id()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN end_id(e)
$$) AS (end_id gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN end_id(null)
$$) AS (end_id gtype);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN end_id(v)
$$) AS (end_id gtype);
SELECT * FROM cypher('expr', $$
    RETURN end_id()
$$) AS (end_id gtype);
-- startNode()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN id(e), start_id(e), startNode(e)
$$) AS (id gtype, start_id gtype, startNode vertex);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN startNode(null)
$$) AS (startNode vertex);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN startNode(v)
$$) AS (startNode vertex);
SELECT * FROM cypher('expr', $$
    RETURN startNode()
$$) AS (startNode vertex);
-- endNode()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN id(e), end_id(e), endNode(e)
$$) AS (id gtype, end_id gtype, endNode vertex);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN endNode(null)
$$) AS (endNode vertex);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN endNode(v)
$$) AS (endNode vertex);
SELECT * FROM cypher('expr', $$
    RETURN endNode()
$$) AS (endNode vertex);
-- type()
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]-() RETURN type(e)
$$) AS (type gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN type(null)
$$) AS (type gtype);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN type(v)
$$) AS (type gtype);
SELECT * FROM cypher('expr', $$
    RETURN type()
$$) AS (type gtype);
-- label ()
SELECT * FROM cypher('expr', $$
    MATCH (v) RETURN label(v)
$$) AS (label gtype);
SELECT * FROM cypher('expr', $$
    MATCH ()-[e]->() RETURN label(e)
$$) AS (label gtype);
-- return NULL
SELECT * FROM cypher('expr', $$
    RETURN label(NULL)
$$) AS (label gtype);
SELECT postgraph.age_label(NULL);
-- should error
SELECT * FROM cypher('expr', $$
    MATCH p=()-[]->() RETURN label(p)
$$) AS (label gtype);
SELECT * FROM cypher('expr', $$
    RETURN label(1)
$$) AS (label gtype);
SELECT * FROM cypher('expr', $$
    MATCH (n) RETURN label([n])
$$) AS (label gtype);
SELECT * FROM cypher('expr', $$
    RETURN label({id: 0, label: 'failed', properties: {}})
$$) AS (label gtype);
-- timestamp() can't be done as it will always have a different value
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
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN length(null)
$$) AS (length gtype);
-- should fail
SELECT * FROM cypher('expr', $$
    RETURN length(true)
$$) AS (length gtype);
SELECT * FROM cypher('expr', $$
    RETURN length()
$$) AS (length gtype);

--
-- toString()
--
SELECT * FROM age_toString(gtype_in('3'));
SELECT * FROM age_toString(gtype_in('3.14'));
SELECT * FROM age_toString(gtype_in('3.14::float'));
SELECT * FROM age_toString(gtype_in('3.14::numeric'));
SELECT * FROM age_toString(gtype_in('true'));
SELECT * FROM age_toString(gtype_in('false'));
SELECT * FROM age_toString(gtype_in('"a string"'));
SELECT * FROM cypher('expr', $$ RETURN toString(3.14::numeric) $$) AS (results gtype);
-- should return null
SELECT * FROM age_toString(NULL);
SELECT * FROM age_toString(gtype_in(null));
-- should fail
SELECT * FROM age_toString();
SELECT * FROM cypher('expr', $$ RETURN toString() $$) AS (results gtype);

--
-- reverse(string)
--
SELECT * FROM cypher('expr', $$
    RETURN reverse("gnirts a si siht")
$$) AS (results gtype);
SELECT * FROM age_reverse('"gnirts a si siht"'::gtype);
-- should return null
SELECT * FROM cypher('expr', $$
    RETURN reverse(null)
$$) AS (results gtype);
SELECT * FROM age_reverse(null);
-- should return error
SELECT * FROM age_reverse([4923, 'abc', 521, NULL, 487]);
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
SELECT * FROM age_reverse(true);
SELECT * FROM cypher('expr', $$
    RETURN reverse(3.14)
$$) AS (results gtype);
SELECT * FROM age_reverse(3.14);
SELECT * FROM cypher('expr', $$
    RETURN reverse()
$$) AS (results gtype);
SELECT * FROM age_reverse();

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
SELECT * FROM age_toupper(null);
SELECT * FROM age_tolower(null);
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
SELECT * FROM age_toupper();
SELECT * FROM age_tolower();

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
SELECT * FROM age_ltrim('"  string   "'::gtype);
SELECT * FROM age_rtrim('"  string   "'::gtype);
SELECT * FROM age_trim('"  string   "'::gtype);
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
SELECT * FROM age_ltrim(null);
SELECT * FROM age_rtrim(null);
SELECT * FROM age_trim(null);
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

SELECT * FROM age_ltrim();
SELECT * FROM age_rtrim();
SELECT * FROM age_trim();

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
SELECT * FROM age_left(null, '1'::gtype);
SELECT * FROM age_left(null, null);
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
SELECT * FROM age_left('123456789', null);
SELECT * FROM age_left('"123456789"'::gtype, '-1'::gtype);
SELECT * FROM age_left();
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
SELECT * FROM age_right(null, '1'::gtype);
SELECT * FROM age_right(null, null);
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
SELECT * FROM age_right('123456789', null);
SELECT * FROM age_right('"123456789"'::gtype, '-1'::gtype);
SELECT * FROM age_right();
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
SELECT * FROM age_split(null, null);
SELECT * FROM age_split('"a,b,c,d,e,f"'::gtype, null);
SELECT * FROM age_split(null, '","'::gtype);
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
SELECT * FROM age_split('123456789'::gtype, '","'::gtype);
SELECT * FROM age_split('"a,b,c,d,e,f"'::gtype, '-1'::gtype);
SELECT * FROM age_split('"a,b,c,d,e,f"'::gtype);
SELECT * FROM age_split();

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
SELECT * FROM age_sin(null);
SELECT * FROM age_cos(null);
SELECT * FROM age_tan(null);
SELECT * FROM age_cot(null);
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
SELECT * FROM age_sin('0');
SELECT * FROM age_cos('0');
SELECT * FROM age_tan('0');
SELECT * FROM age_cot('0');
SELECT * FROM age_sin();
SELECT * FROM age_cos();
SELECT * FROM age_tan();
SELECT * FROM age_cot();

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
SELECT * FROM asin(1), age_asin('1'::gtype);
SELECT * FROM acos(0), age_acos('0'::gtype);
SELECT * FROM atan(1), age_atan('1'::gtype);
SELECT * FROM atan2(1, 1), age_atan2('1'::gtype, '1'::gtype);
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
SELECT * FROM age_asin(null);
SELECT * FROM age_acos(null);
SELECT * FROM age_atan(null);
SELECT * FROM age_atan2(null, null);
SELECT * FROM age_atan2('1'::gtype, null);
SELECT * FROM age_atan2(null, '1'::gtype);
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
SELECT * FROM age_asin('0');
SELECT * FROM age_acos('0');
SELECT * FROM age_atan('0');
SELECT * FROM age_atan2('"0"'::gtype, '1'::gtype);
SELECT * FROM age_atan2('1'::gtype, '"0"'::gtype);
SELECT * FROM age_asin();
SELECT * FROM age_acos();
SELECT * FROM age_atan();
SELECT * FROM age_atan2();
SELECT * FROM age_atan2('1'::gtype);

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
-- should fail
SELECT * from cypher('expr', $$
    RETURN sqrt()
$$) as (result gtype);
SELECT * from cypher('expr', $$
    RETURN sqrt("1")
$$) as (result gtype);

--
-- aggregate functions avg(), sum(), count(), & count(*)
--
SELECT create_graph('UCSC');
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Jack", gpa: 3.0, age: 21, zip: 94110})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Jill", gpa: 3.5, age: 27, zip: 95060})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Jim", gpa: 3.75, age: 32, zip: 96062})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Rick", gpa: 2.5, age: 24, zip: "95060"})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Ann", gpa: 3.8::numeric, age: 23})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Derek", gpa: 4.0, age: 19, zip: 90210})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Jessica", gpa: 3.9::numeric, age: 20})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN (u) $$) AS (vertex vertex);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN avg(u.gpa), sum(u.gpa), sum(u.gpa)/count(u.gpa), count(u.gpa), count(*) $$) 
AS (avg gtype, sum gtype, sum_divided_by_count gtype, count gtype, count_star gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Dave", age: 24})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Mike", age: 18})$$) AS (a gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN (u) $$) AS (vertex vertex);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN avg(u.gpa), sum(u.gpa), sum(u.gpa)/count(u.gpa), count(u.gpa), count(*) $$) 
AS (avg gtype, sum gtype, sum_divided_by_count gtype, count gtype, count_star gtype);
-- should return null
SELECT * FROM cypher('UCSC', $$ RETURN avg(NULL) $$) AS (avg gtype);
SELECT * FROM cypher('UCSC', $$ RETURN sum(NULL) $$) AS (sum gtype);
-- should return 0
SELECT * FROM cypher('UCSC', $$ RETURN count(NULL) $$) AS (count gtype);
-- should fail
SELECT * FROM cypher('UCSC', $$ RETURN avg() $$) AS (avg gtype);
SELECT * FROM cypher('UCSC', $$ RETURN sum() $$) AS (sum gtype);
SELECT * FROM cypher('UCSC', $$ RETURN count() $$) AS (count gtype);

--
-- aggregate functions min() & max()
--
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN min(u.gpa), max(u.gpa), count(u.gpa), count(*) $$)
AS (min gtype, max gtype, count gtype, count_star gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN min(u.gpa), max(u.gpa), count(u.gpa), count(*) $$)
AS (min gtype, max gtype, count gtype, count_star gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN min(u.name), max(u.name), count(u.name), count(*) $$)
AS (min gtype, max gtype, count gtype, count_star gtype);
-- check that min() & max() can work against mixed types
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN min(u.zip), max(u.zip), count(u.zip), count(*) $$)
AS (min gtype, max gtype, count gtype, count_star gtype);
CREATE TABLE min_max_tbl (id gtype);
insert into min_max_tbl VALUES ('16'::gtype), ('17188'::gtype), ('1000'::gtype), ('869'::gtype);

SELECT age_min(id), age_max(id) FROM min_max_tbl;
SELECT age_min(age_tofloat(id)), age_max(age_tofloat(id)) FROM min_max_tbl;
SELECT age_min(age_tonumeric(id)), age_max(age_tonumeric(id)) FROM min_max_tbl;
SELECT age_min(age_tostring(id)), age_max(age_tostring(id)) FROM min_max_tbl;

DROP TABLE min_max_tbl;
-- should return null
SELECT * FROM cypher('UCSC', $$ RETURN min(NULL) $$) AS (min gtype);
SELECT * FROM cypher('UCSC', $$ RETURN max(NULL) $$) AS (max gtype);
SELECT age_min(NULL);
SELECT age_min(gtype_in('null'));
SELECT age_max(NULL);
SELECT age_max(gtype_in('null'));
-- should fail
SELECT * FROM cypher('UCSC', $$ RETURN min() $$) AS (min gtype);
SELECT * FROM cypher('UCSC', $$ RETURN max() $$) AS (max gtype);
SELECT age_min();
SELECT age_min();
--
-- aggregate functions stDev() & stDevP()
--
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN stDev(u.gpa), stDevP(u.gpa) $$)
AS (stDev gtype, stDevP gtype);
-- should return 0
SELECT * FROM cypher('UCSC', $$ RETURN stDev(NULL) $$) AS (stDev gtype);
SELECT * FROM cypher('UCSC', $$ RETURN stDevP(NULL) $$) AS (stDevP gtype);
-- should fail
SELECT * FROM cypher('UCSC', $$ RETURN stDev() $$) AS (stDev gtype);
SELECT * FROM cypher('UCSC', $$ RETURN stDevP() $$) AS (stDevP gtype);

--
-- aggregate functions percentileCont() & percentileDisc()
--
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN percentileCont(u.gpa, .55), percentileDisc(u.gpa, .55), percentileCont(u.gpa, .9), percentileDisc(u.gpa, .9) $$)
AS (percentileCont1 gtype, percentileDisc1 gtype, percentileCont2 gtype, percentileDisc2 gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN percentileCont(u.gpa, .55) $$)
AS (percentileCont gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN percentileDisc(u.gpa, .55) $$)
AS (percentileDisc gtype);
-- should return null
SELECT * FROM cypher('UCSC', $$ RETURN percentileCont(NULL, .5) $$) AS (percentileCont gtype);
SELECT * FROM cypher('UCSC', $$ RETURN percentileDisc(NULL, .5) $$) AS (percentileDisc gtype);
-- should fail
SELECT * FROM cypher('UCSC', $$ RETURN percentileCont(.5, NULL) $$) AS (percentileCont gtype);
SELECT * FROM cypher('UCSC', $$ RETURN percentileDisc(.5, NULL) $$) AS (percentileDisc gtype);

--
-- aggregate function collect()
--
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN collect(u.name), collect(u.age), collect(u.gpa), collect(u.zip) $$)
AS (name gtype, age gtype, gqa gtype, zip gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN collect(u.gpa), collect(u.gpa) $$)
AS (gpa1 gtype, gpa2 gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN collect(u.zip), collect(u.zip) $$)
AS (zip1 gtype, zip2 gtype);
SELECT * FROM cypher('UCSC', $$ RETURN collect(5) $$) AS (result gtype);
-- should return an empty aray
SELECT * FROM cypher('UCSC', $$ RETURN collect(NULL) $$) AS (empty gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) WHERE u.name =~ "doesn't exist" RETURN collect(u.name) $$) AS (name gtype);

-- should fail
SELECT * FROM cypher('UCSC', $$ RETURN collect() $$) AS (collect gtype);

-- test DISTINCT inside aggregate functions
SELECT * FROM cypher('UCSC', $$CREATE (:students {name: "Sven", gpa: 3.2, age: 27, zip: 94110})$$)
AS (a gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN (u) $$) AS (vertex vertex);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN count(u.zip), count(DISTINCT u.zip) $$)
AS (zip gtype, distinct_zip gtype);
SELECT * FROM cypher('UCSC', $$ MATCH (u) RETURN count(u.age), count(DISTINCT u.age) $$)
AS (age gtype, distinct_age gtype);

-- test AUTO GROUP BY for aggregate functions
SELECT create_graph('group_by');
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 2, k:3})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 2, k:4})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 1, j: 3, k:5})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:row {i: 2, j: 3, k:6})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$MATCH (u:row) RETURN u.i, u.j, u.k$$) AS (i gtype, j gtype, k gtype);
SELECT * FROM cypher('group_by', $$MATCH (u:row) RETURN u.i, u.j, sum(u.k)$$) AS (i gtype, j gtype, sumk gtype);
SELECT * FROM cypher('group_by', $$CREATE (:L {a: 1, b: 2, c:3})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:L {a: 2, b: 3, c:1})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$CREATE (:L {a: 3, b: 1, c:2})$$) AS (result gtype);
SELECT * FROM cypher('group_by', $$MATCH (x:L) RETURN x.a, x.b, x.c, x.a + count(*) + x.b + count(*) + x.c$$)
AS (a gtype, b gtype, c gtype, result gtype);
SELECT * FROM cypher('group_by', $$MATCH (x:L) RETURN x.a + x.b + x.c, x.a + x.b + x.c + count(*) + count(*) $$)
AS (a_b_c gtype,  result gtype);
-- with WITH clause
SELECT * FROM cypher('group_by', $$MATCH(x:L) WITH x, count(x) AS c RETURN x.a + x.b + x.c + c$$)
AS (result gtype);
SELECT * FROM cypher('group_by', $$MATCH(x:L) WITH x, count(x) AS c RETURN x.a + x.b + x.c + c + c$$)
AS (result gtype);
SELECT * FROM cypher('group_by', $$MATCH(x:L) WITH x.a + x.b + x.c AS v, count(x) as c RETURN v + c + c $$)
AS (result gtype);
-- should fail
SELECT * FROM cypher('group_by', $$MATCH (x:L) RETURN x.a, x.a + count(*) + x.b + count(*) + x.c$$)
AS (a gtype, result gtype);
SELECT * FROM cypher('group_by', $$MATCH (x:L) RETURN x.a + count(*) + x.b + count(*) + x.c$$)
AS (result gtype);

--ORDER BY
SELECT create_graph('order_by');
SELECT * FROM cypher('order_by', $$CREATE ()$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: '1'})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1.0})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1::numeric})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: true})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: false})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: {key: 'value'}})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: [1]})$$) AS (result gtype);

SELECT * FROM cypher('order_by', $$
	MATCH (u)
	RETURN u.i
	ORDER BY u.i
$$) AS (i gtype);

SELECT * FROM cypher('order_by', $$
	MATCH (u)
	RETURN u.i
	ORDER BY u.i DESC
$$) AS (i gtype);

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

-- RETURN * and (u)--(v) optional forms
SELECT create_graph('opt_forms');
SELECT * FROM cypher('opt_forms', $$CREATE ({i:1})-[:KNOWS]->({i:2})<-[:KNOWS]-({i:3})$$)AS (result gtype);
SELECT * FROM cypher('opt_forms', $$MATCH (u) RETURN u$$) AS (result vertex);
SELECT * FROM cypher('opt_forms', $$MATCH (u) RETURN *$$) AS (result vertex);
SELECT * FROM cypher('opt_forms', $$MATCH (u)--(v) RETURN u.i, v.i$$) AS (u gtype, v gtype);
SELECT * FROM cypher('opt_forms', $$MATCH (u)-->(v) RETURN u.i, v.i$$) AS (u gtype, v gtype);
SELECT * FROM cypher('opt_forms', $$MATCH (u)<--(v) RETURN u.i, v.i$$) AS (u gtype, v gtype);
SELECT * FROM cypher('opt_forms', $$MATCH (u)-->()<--(v) RETURN u.i, v.i$$) AS (u gtype, v gtype);
SELECT * FROM cypher('opt_forms', $$MATCH (u) CREATE (u)-[:edge]->() RETURN *$$) AS (results vertex);
SELECT * FROM cypher('opt_forms', $$MATCH (u)-->()<--(v) RETURN *$$) AS (col1 vertex, col2 vertex);

-- list functions relationships(), range(), keys()
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
-- nodes()
-- XXX: Not Working
SELECT * from cypher('list', $$MATCH p=()-[]->() RETURN nodes(p)$$) as (nodes gtype);
SELECT * from cypher('list', $$MATCH p=()-[]->()-[]->() RETURN nodes(p)$$) as (nodes gtype);
-- should return nothing
SELECT * from cypher('list', $$MATCH p=()-[]->()-[]->()-[]->() RETURN nodes(p)$$) as (nodes gtype);
-- should return SQL NULL
SELECT * from cypher('list', $$RETURN nodes(NULL)$$) as (nodes gtype);
-- should return an error
SELECT * from cypher('list', $$MATCH (u) RETURN nodes([1,2,3])$$) as (nodes gtype);
SELECT * from cypher('list', $$MATCH (u) RETURN nodes("string")$$) as (nodes gtype);
SELECT * from cypher('list', $$MATCH (u) RETURN nodes(u)$$) as (nodes gtype);
SELECT * from cypher('list', $$MATCH (u)-[]->() RETURN nodes(u)$$) as (nodes gtype);
-- relationships()
-- XXX: Not Working
SELECT * from cypher('list', $$MATCH p=()-[]->() RETURN relationships(p)$$) as (relationships gtype);
SELECT * from cypher('list', $$MATCH p=()-[]->()-[]->() RETURN relationships(p)$$) as (relationships gtype);
-- should return nothing
SELECT * from cypher('list', $$MATCH p=()-[]->()-[]->()-[]->() RETURN relationships(p)$$) as (relationships gtype);
-- should return SQL NULL
SELECT * from cypher('list', $$RETURN relationships(NULL)$$) as (relationships gtype);
-- should return an error
SELECT * from cypher('list', $$MATCH (u) RETURN relationships([1,2,3])$$) as (relationships gtype);
SELECT * from cypher('list', $$MATCH (u) RETURN relationships("string")$$) as (relationships gtype);
SELECT * from cypher('list', $$MATCH (u) RETURN relationships(u)$$) as (relationships gtype);
SELECT * from cypher('list', $$MATCH ()-[e]->() RETURN relationships(e)$$) as (relationships gtype);
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
SELECT * FROM drop_graph('opt_forms', true);
SELECT * FROM drop_graph('type_coercion', true);
SELECT * FROM drop_graph('order_by', true);
SELECT * FROM drop_graph('group_by', true);
SELECT * FROM drop_graph('UCSC', true);
SELECT * FROM drop_graph('expr', true);
SELECT * FROM drop_graph('regex', true);
SELECT * FROM drop_graph('keys', true);
SELECT * FROM drop_graph('list', true);

--
-- End of tests
--
