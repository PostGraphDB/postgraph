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

--
-- gtype data type regression tests
--

--
-- Load extension and set path
--
LOAD 'postgraph';
SET extra_float_digits = 0;
SET search_path TO postgraph;
set timezone TO 'GMT';

--
-- Create a table using the AGTYPE type
--
CREATE TABLE gtype_table (type text, gtype gtype);

--
-- Insert values to exercise gtype_in/gtype_out
--
INSERT INTO gtype_table VALUES ('bool', 'true');
INSERT INTO gtype_table VALUES ('bool', 'false');

INSERT INTO gtype_table VALUES ('null', 'null');

INSERT INTO gtype_table VALUES ('string', '""');
INSERT INTO gtype_table VALUES ('string', '"This is a string"');

INSERT INTO gtype_table VALUES ('integer', '0');
INSERT INTO gtype_table VALUES ('integer', '9223372036854775807');
INSERT INTO gtype_table VALUES ('integer', '-9223372036854775808');

INSERT INTO gtype_table VALUES ('float', '0.0');
INSERT INTO gtype_table VALUES ('float', '1.0');
INSERT INTO gtype_table VALUES ('float', '-1.0');
INSERT INTO gtype_table VALUES ('float', '100000000.000001');
INSERT INTO gtype_table VALUES ('float', '-100000000.000001');
INSERT INTO gtype_table VALUES ('float', '0.00000000000000012345');
INSERT INTO gtype_table VALUES ('float', '-0.00000000000000012345');

INSERT INTO gtype_table VALUES ('numeric', '100000000000.0000000000001::numeric');
INSERT INTO gtype_table VALUES ('numeric', '-100000000000.0000000000001::numeric');


INSERT INTO gtype_table VALUES ('timestamp', '"2023-06-23 13:39:40.00"::timestamp');
INSERT INTO gtype_table VALUES ('timestamp', '"06/23/2023 13:39:40.00"::timestamp');
INSERT INTO gtype_table VALUES ('timestamp', '"Fri Jun 23 13:39:40.00 2023"::timestamp');

INSERT INTO gtype_table VALUES ('timestamptz', '"1997-12-17 07:37:16-06"::timestamptz');
INSERT INTO gtype_table VALUES ('timestamptz', '"12/17/1997 07:37:16.00+00"::timestamptz');
INSERT INTO gtype_table VALUES ('timestamptz', '"Wed Dec 17 07:37:16 1997+09"::timestamptz');

INSERT INTO gtype_table VALUES ('date', '"1997-12-17"::date');
INSERT INTO gtype_table VALUES ('date', '"12/17/1997"::date');
INSERT INTO gtype_table VALUES ('date', '"Wed Dec 17 1997"::date');

INSERT INTO gtype_table VALUES ('time', '"07:37:16-08"::time');
INSERT INTO gtype_table VALUES ('time', '"07:37:16.00"::time');
INSERT INTO gtype_table VALUES ('time', '"07:37:16"::time');

INSERT INTO gtype_table VALUES ('timetz', '"07:37:16-08"::timetz');
INSERT INTO gtype_table VALUES ('timetz', '"07:37:16.00"::timetz');
INSERT INTO gtype_table VALUES ('timetz', '"07:37:16"::timetz');

INSERT INTO gtype_table VALUES ('interval', '"30 Seconds"::interval');
INSERT INTO gtype_table VALUES ('interval', '"15 Minutes"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Hours"::interval');
INSERT INTO gtype_table VALUES ('interval', '"40 Days"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Weeks"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Months"::interval');
INSERT INTO gtype_table VALUES ('interval', '"3 Years"::interval');

INSERT INTO gtype_table VALUES ('interval', '"30 Seconds Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"15 Minutes Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Hours Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"40 Days Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Weeks Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"10 Months Ago"::interval');
INSERT INTO gtype_table VALUES ('interval', '"3 Years Ago"::interval');

INSERT INTO gtype_table VALUES ('inet', '"192.168.1.5"::inet');
INSERT INTO gtype_table VALUES ('inet', '"192.168.1/24"::inet');
INSERT INTO gtype_table VALUES ('inet', '"::ffff:fff0:1"::inet');

INSERT INTO gtype_table VALUES ('inet', '"192.168.1.5"::cidr');
INSERT INTO gtype_table VALUES ('inet', '"192.168.1/24"::cidr');
INSERT INTO gtype_table VALUES ('inet', '"::ffff:fff0:1"::cidr');


INSERT INTO gtype_table VALUES ('integer array',
	'[-9223372036854775808, -1, 0, 1, 9223372036854775807]');
INSERT INTO gtype_table VALUES('float array',
	'[-0.00000000000000012345, -100000000.000001, -1.0, 0.0, 1.0, 100000000.000001, 0.00000000000000012345]');
INSERT INTO gtype_table VALUES('mixed array', '[true, false, null, "string", 1, 1.0, {"bool":true}, -1::numeric, [1,3,5]]');

INSERT INTO gtype_table VALUES('object', '{"bool":true, "null":null, "string":"string", "integer":1, "float":1.2, "arrayi":[-1,0,1], "arrayf":[-1.0, 0.0, 1.0], "object":{"bool":true, "null":null, "string":"string", "int":1, "float":8.0}}');
INSERT INTO gtype_table VALUES ('numeric array',
        '[-5::numeric, -1::numeric, 0::numeric, 1::numeric, 9223372036854775807::numeric]');

--
-- Special float values: NaN, +/- Infinity
--
INSERT INTO gtype_table VALUES ('float  nan', 'nan');
INSERT INTO gtype_table VALUES ('float  Infinity', 'Infinity');
INSERT INTO gtype_table VALUES ('float -Infinity', '-Infinity');
INSERT INTO gtype_table VALUES ('float  inf', 'inf');
INSERT INTO gtype_table VALUES ('float -inf', '-inf');

SELECT * FROM gtype_table;

--
-- These should fail
--
INSERT INTO gtype_table VALUES ('bad integer', '9223372036854775808');
INSERT INTO gtype_table VALUES ('bad integer', '-9223372036854775809');
INSERT INTO gtype_table VALUES ('bad float', '-NaN');
INSERT INTO gtype_table VALUES ('bad float', 'Infi');
INSERT INTO gtype_table VALUES ('bad float', '-Infi');

--
-- Test gtype mathematical operator functions
-- +, -, unary -, *, /, %, and ^
--
SELECT gtype_add('1', '-1');
SELECT gtype_add('1', '-1.0');
SELECT gtype_add('1.0', '-1');
SELECT gtype_add('1.0', '-1.0');
SELECT gtype_add('1', '-1.0::numeric');
SELECT gtype_add('1.0', '-1.0::numeric');
SELECT gtype_add('1::numeric', '-1.0::numeric');

SELECT gtype_sub('-1', '-1');
SELECT gtype_sub('-1', '-1.0');
SELECT gtype_sub('-1.0', '-1');
SELECT gtype_sub('-1.0', '-1.0');
SELECT gtype_sub('1', '-1.0::numeric');
SELECT gtype_sub('1.0', '-1.0::numeric');
SELECT gtype_sub('1::numeric', '-1.0::numeric');


SELECT gtype_neg('-1');
SELECT gtype_neg('-1.0');
SELECT gtype_neg('0');
SELECT gtype_neg('0.0');
SELECT gtype_neg('0::numeric');
SELECT gtype_neg('-1::numeric');
SELECT gtype_neg('1::numeric');

SELECT gtype_mul('-2', '3');
SELECT gtype_mul('2', '-3.0');
SELECT gtype_mul('-2.0', '3');
SELECT gtype_mul('2.0', '-3.0');
SELECT gtype_mul('-2', '3::numeric');
SELECT gtype_mul('2.0', '-3::numeric');
SELECT gtype_mul('-2.0::numeric', '3::numeric');

SELECT gtype_div('-4', '3');
SELECT gtype_div('4', '-3.0');
SELECT gtype_div('-4.0', '3');
SELECT gtype_div('4.0', '-3.0');
SELECT gtype_div('4', '-3.0::numeric');
SELECT gtype_div('-4.0', '3::numeric');
SELECT gtype_div('4.0::numeric', '-3::numeric');

SELECT gtype_mod('-11', '3');
SELECT gtype_mod('11', '-3.0');
SELECT gtype_mod('-11.0', '3');
SELECT gtype_mod('11.0', '-3.0');
SELECT gtype_mod('11', '-3.0::numeric');
SELECT gtype_mod('-11.0', '3::numeric');
SELECT gtype_mod('11.0::numeric', '-3::numeric');

SELECT gtype_pow('-2', '3');
SELECT gtype_pow('2', '-1.0');
SELECT gtype_pow('2.0', '3');
SELECT gtype_pow('2.0', '-1.0');
SELECT gtype_pow('2::numeric', '3');
SELECT gtype_pow('2::numeric', '-1.0');
SELECT gtype_pow('-2', '3::numeric');
SELECT gtype_pow('2.0', '-1.0::numeric');
SELECT gtype_pow('2.0::numeric', '-1.0::numeric');

--
-- Should fail with divide by zero
--
SELECT gtype_div('1', '0');
SELECT gtype_div('1', '0.0');
SELECT gtype_div('1.0', '0');
SELECT gtype_div('1.0', '0.0');
SELECT gtype_div('1', '0::numeric');
SELECT gtype_div('1.0', '0::numeric');
SELECT gtype_div('1::numeric', '0');
SELECT gtype_div('1::numeric', '0.0');
SELECT gtype_div('1::numeric', '0::numeric');

--
-- Should get Infinity
--
SELECT gtype_pow('0', '-1');
SELECT gtype_pow('-0.0', '-1');

--
-- Should get - ERROR:  zero raised to a negative power is undefined
--
SELECT gtype_pow('0', '-1::numeric');
SELECT gtype_pow('-0.0', '-1::numeric');
SELECT gtype_pow('0::numeric', '-1');
SELECT gtype_pow('-0.0::numeric', '-1');
SELECT gtype_pow('-0.0::numeric', '-1');

--
-- Test operators +, -, unary -, *, /, %, and ^
--
SELECT '3.14'::gtype + '3.14'::gtype;
SELECT '3.14'::gtype - '3.14'::gtype;
SELECT -'3.14'::gtype;
SELECT '3.14'::gtype * '3.14'::gtype;
SELECT '3.14'::gtype / '3.14'::gtype;
SELECT '3.14'::gtype % '3.14'::gtype;
SELECT '3.14'::gtype ^ '2'::gtype;
SELECT '3'::gtype + '3'::gtype;
SELECT '3'::gtype + '3.14'::gtype;
SELECT '3'::gtype + '3.14::numeric'::gtype;
SELECT '3.14'::gtype + '3.14::numeric'::gtype;
SELECT '3.14::numeric'::gtype + '3.14::numeric'::gtype;

--
-- Test orderability of comparison operators =, <>, <, >, <=, >=
-- These should all return true
-- Integer
SELECT gtype_in('1') = gtype_in('1');
SELECT gtype_in('1') <> gtype_in('2');
SELECT gtype_in('1') <> gtype_in('-2');
SELECT gtype_in('1') < gtype_in('2');
SELECT gtype_in('1') > gtype_in('-2');
SELECT gtype_in('1') <= gtype_in('2');
SELECT gtype_in('1') >= gtype_in('-2');

-- Float
SELECT gtype_in('1.01') = gtype_in('1.01');
SELECT gtype_in('1.01') <> gtype_in('1.001');
SELECT gtype_in('1.01') <> gtype_in('1.011');
SELECT gtype_in('1.01') < gtype_in('1.011');
SELECT gtype_in('1.01') > gtype_in('1.001');
SELECT gtype_in('1.01') <= gtype_in('1.011');
SELECT gtype_in('1.01') >= gtype_in('1.001');
SELECT gtype_in('1.01') < gtype_in('Infinity');
SELECT gtype_in('1.01') > gtype_in('-Infinity');
-- NaN, under ordering, is considered to be the biggest numeric value
-- greater than positive infinity. So, greater than any other number.
SELECT gtype_in('1.01') < gtype_in('NaN');
SELECT gtype_in('NaN') > gtype_in('Infinity');
SELECT gtype_in('NaN') > gtype_in('-Infinity');
SELECT gtype_in('NaN') = gtype_in('NaN');

-- Mixed Integer and Float
SELECT gtype_in('1') = gtype_in('1.0');
SELECT gtype_in('1') <> gtype_in('1.001');
SELECT gtype_in('1') <> gtype_in('0.999999');
SELECT gtype_in('1') < gtype_in('1.001');
SELECT gtype_in('1') > gtype_in('0.999999');
SELECT gtype_in('1') <= gtype_in('1.001');
SELECT gtype_in('1') >= gtype_in('0.999999');
SELECT gtype_in('1') < gtype_in('Infinity');
SELECT gtype_in('1') > gtype_in('-Infinity');
SELECT gtype_in('1') < gtype_in('NaN');

-- Mixed Float and Integer
SELECT gtype_in('1.0') = gtype_in('1');
SELECT gtype_in('1.001') <> gtype_in('1');
SELECT gtype_in('0.999999') <> gtype_in('1');
SELECT gtype_in('1.001') > gtype_in('1');
SELECT gtype_in('0.999999') < gtype_in('1');

-- Mixed Integer and Numeric
SELECT gtype_in('1') = gtype_in('1::numeric');
SELECT gtype_in('1') <> gtype_in('2::numeric');
SELECT gtype_in('1') <> gtype_in('-2::numeric');
SELECT gtype_in('1') < gtype_in('2::numeric');
SELECT gtype_in('1') > gtype_in('-2::numeric');
SELECT gtype_in('1') <= gtype_in('2::numeric');
SELECT gtype_in('1') >= gtype_in('-2::numeric');

-- Mixed Float and Numeric
SELECT gtype_in('1.01') = gtype_in('1.01::numeric');
SELECT gtype_in('1.01') <> gtype_in('1.001::numeric');
SELECT gtype_in('1.01') <> gtype_in('1.011::numeric');
SELECT gtype_in('1.01') < gtype_in('1.011::numeric');
SELECT gtype_in('1.01') > gtype_in('1.001::numeric');
SELECT gtype_in('1.01') <= gtype_in('1.011::numeric');
SELECT gtype_in('1.01') >= gtype_in('1.001::numeric');

-- Strings
SELECT gtype_in('"a"') = gtype_in('"a"');
SELECT gtype_in('"a"') <> gtype_in('"b"');
SELECT gtype_in('"a"') < gtype_in('"aa"');
SELECT gtype_in('"b"') > gtype_in('"aa"');
SELECT gtype_in('"a"') <= gtype_in('"aa"');
SELECT gtype_in('"b"') >= gtype_in('"aa"');

-- Lists
SELECT gtype_in('[0, 1, null, 2]') = gtype_in('[0, 1, null, 2]');
SELECT gtype_in('[0, 1, null, 2]') <> gtype_in('[2, null, 1, 0]');
SELECT gtype_in('[0, 1, null]') < gtype_in('[0, 1, null, 2]');
SELECT gtype_in('[1, 1, null, 2]') > gtype_in('[0, 1, null, 2]');

-- Objects (Maps)
SELECT gtype_in('{"bool":true, "null": null}') = gtype_in('{"null":null, "bool":true}');
SELECT gtype_in('{"bool":true}') < gtype_in('{"bool":true, "null": null}');

-- Comparisons between types
-- Object < List < String < Boolean < Integer = Float = Numeric < Null
SELECT gtype_in('1') < gtype_in('null');
SELECT gtype_in('NaN') < gtype_in('null');
SELECT gtype_in('Infinity') < gtype_in('null');
SELECT gtype_in('true') < gtype_in('1');
SELECT gtype_in('true') < gtype_in('NaN');
SELECT gtype_in('true') < gtype_in('Infinity');
SELECT gtype_in('"string"') < gtype_in('true');
SELECT gtype_in('[1,3,5,7,9,11]') < gtype_in('"string"');
SELECT gtype_in('{"bool":true, "integer":1}') < gtype_in('[1,3,5,7,9,11]');
SELECT gtype_in('[1, "string"]') < gtype_in('[1, 1]');
SELECT gtype_in('{"bool":true, "integer":1}') < gtype_in('{"bool":true, "integer":null}');
SELECT gtype_in('1::numeric') < gtype_in('null');
SELECT gtype_in('true') < gtype_in('1::numeric');

-- Lists
SELECT gtype_in('[0, 1, null, 2]') = '[0, 1, null, 2]';
SELECT gtype_in('[0, 1, null, 2]') <> '[2, null, 1, 0]';
SELECT gtype_in('[0, 1, null]') < '[0, 1, null, 2]';
SELECT gtype_in('[1, 1, null, 2]') > '[0, 1, null, 2]';

-- Objects (Maps)
SELECT gtype_in('{"bool":true, "null": null}') = '{"null":null, "bool":true}';
SELECT gtype_in('{"bool":true}') < '{"bool":true, "null": null}';

-- Comparisons between types
-- Object < List < String < Boolean < Integer = Float = Numeric < Null
SELECT gtype_in('1') < 'null';
SELECT gtype_in('NaN') < 'null';
SELECT gtype_in('Infinity') < 'null';
SELECT gtype_in('true') < '1';
SELECT gtype_in('true') < 'NaN';
SELECT gtype_in('true') < 'Infinity';
SELECT gtype_in('"string"') < 'true';
SELECT gtype_in('[1,3,5,7,9,11]') < '"string"';
SELECT gtype_in('{"bool":true, "integer":1}') < '[1,3,5,7,9,11]';
SELECT gtype_in('[1, "string"]') < '[1, 1]';
SELECT gtype_in('{"bool":true, "integer":1}') < '{"bool":true, "integer":null}';
SELECT gtype_in('1::numeric') < 'null';
SELECT gtype_in('true') < '1::numeric';

--
-- Test gtype to boolean cast
--
SELECT gtype_to_bool(gtype_in('true'));
SELECT gtype_to_bool(gtype_in('false'));
-- These should all fail
SELECT gtype_to_bool(gtype_in('null'));
SELECT gtype_to_bool(gtype_in('1'));
SELECT gtype_to_bool(gtype_in('1.0'));
SELECT gtype_to_bool(gtype_in('"string"'));
SELECT gtype_to_bool(gtype_in('[1,2,3]'));
SELECT gtype_to_bool(gtype_in('{"bool":true}'));

--
-- Test boolean to gtype cast
--
SELECT bool_to_gtype(true);
SELECT bool_to_gtype(false);
SELECT bool_to_gtype(null);
SELECT bool_to_gtype(true) = bool_to_gtype(true);
SELECT bool_to_gtype(true) <> bool_to_gtype(false);

--
-- Test gtype to text[]
--
SELECT gtype_to_text_array(gtype_in('[1,2,3]'));
SELECT gtype_to_text_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_text_array(gtype_in('["6","7",3.66]'));
SELECT gtype_to_text_array(gtype_in('["Hello","Text","Array"]'));

--
-- gtype -> smallint[]
--
SELECT gtype_to_int2_array(gtype_in('[1,2,3]'));
SELECT gtype_to_int2_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_int2_array(gtype_in('["6","7",3.66]'));


--
-- Test gtype to int[]
--
SELECT gtype_to_int4_array(gtype_in('[1,2,3]'));
SELECT gtype_to_int4_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_int4_array(gtype_in('["6","7",3.66]'));

--
-- Test gtype to bigint[]
--
SELECT gtype_to_int8_array(gtype_in('[1,2,3]'));
SELECT gtype_to_int8_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_int8_array(gtype_in('["6","7",3.66]'));

--
-- Test gtype to numeric[]
--
SELECT gtype_to_numeric_array(gtype_in('[1,2,3]'));
SELECT gtype_to_numeric_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_numeric_array(gtype_in('["6","7",3.66]'));

--
-- Test gtype to float8[]
--
SELECT gtype_to_float8_array(gtype_in('[1,2,3]'));
SELECT gtype_to_float8_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_float8_array(gtype_in('["6","7",3.66]'));

--
-- Test gtype to float4[]
--
SELECT gtype_to_float4_array(gtype_in('[1,2,3]'));
SELECT gtype_to_float4_array(gtype_in('[1.6,2.3,3.66]'));
SELECT gtype_to_float4_array(gtype_in('["6","7",3.66]'));


--
-- gtype to numeric
--
SELECT '1'::gtype::numeric;
SELECT '1.6'::gtype::numeric;
SELECT '1.6::numeric'::gtype::numeric;
SELECT '"6"'::gtype::numeric;


--
-- Map Literal
--

--Invalid Map Key (should fail)
SELECT gtype_build_map('[0]'::gtype, null);

--
-- Test gtype object/array access operators object.property, object["property"], and array[element]
--
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":true, "array":[1,3,{"bool":false, "int":3, "float":3.14},7], "float":3.14}'::gtype->'array'::text->2->'float'::text as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":true, "array":[1,3,{"bool":false, "int":3, "float":3.14},7], "float":3.14}'::gtype->'array'::text->2->>'float' as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{}'::gtype->'array'::text as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[]'::gtype->0 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[0, 1]'::gtype->2 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[0, 1]'::gtype->3 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->'true'::text as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->2 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->'true'::text->2 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->'true'::text->>2 as i) a;

SELECT i, pg_typeof(i) FROM (SELECT '{"bool":true, "array":[1,3,{"bool":false, "int":3, "float":3.14},7], "float":3.14}'::gtype->'"array"'::gtype->2->'"float"'::gtype as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{}'::gtype->'"array"'::gtype as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[]'::gtype->0 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[0, 1]'::gtype->2 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '[0, 1]'::gtype->3 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->'"true"'::gtype as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->2 as i) a;
SELECT i, pg_typeof(i) FROM (SELECT '{"bool":false, "int":3, "float":3.14}'::gtype->'"true"'::gtype->2 as i) a;


SELECT gtype_contains('{"id": 1}','{"id": 1}');
SELECT gtype_contains('{"id": 1}','{"id": 2}');

SELECT '{"id": 1}'::gtype @> '{"id": 1}';
SELECT '{"id": 1}'::gtype @> '{"id": 2}';

SELECT gtype_exists('{"id": 1}','"id"');
SELECT gtype_exists('{"id": 1}','"not_id"');

SELECT '{"id": 1}'::gtype ? '"id"';
SELECT '{"id": 1}'::gtype ? '"not_id"';

SELECT gtype_exists_any('{"id": 1}'::gtype, '["id"]'::gtype);
SELECT gtype_exists_any('{"id": 1}'::gtype, '["not_id"]'::gtype);

SELECT '{"id": 1}'::gtype ?| '["id"]'::gtype;
SELECT '{"id": 1}'::gtype ?| '["not_id"]'::gtype;


SELECT gtype_exists_all('{"id": 1}', '["id"]');
SELECT gtype_exists_all('{"id": 1}', '["not_id"]');

SELECT '{"id": 1}'::gtype ?& '["id"]';
SELECT '{"id": 1}'::gtype ?& '["not_id"]';

--
-- Test STARTS WITH, ENDS WITH, and CONTAINS
--
SELECT gtype_string_match_starts_with('"abcdefghijklmnopqrstuvwxyz"', '"abcd"');
SELECT gtype_string_match_ends_with('"abcdefghijklmnopqrstuvwxyz"', '"wxyz"');
SELECT gtype_string_match_contains('"abcdefghijklmnopqrstuvwxyz"', '"abcd"');
SELECT gtype_string_match_contains('"abcdefghijklmnopqrstuvwxyz"', '"hijk"');
SELECT gtype_string_match_contains('"abcdefghijklmnopqrstuvwxyz"', '"wxyz"');
-- should all fail
SELECT gtype_string_match_starts_with('"abcdefghijklmnopqrstuvwxyz"', '"bcde"');
SELECT gtype_string_match_ends_with('"abcdefghijklmnopqrstuvwxyz"', '"vwxy"');
SELECT gtype_string_match_contains('"abcdefghijklmnopqrstuvwxyz"', '"hijl"');

--Agtype Hash Comparison Function
SELECT gtype_hash_cmp(NULL);
SELECT gtype_hash_cmp('1'::gtype);
SELECT gtype_hash_cmp('1.0'::gtype);
SELECT gtype_hash_cmp('"1"'::gtype);
SELECT gtype_hash_cmp('[1]'::gtype);
SELECT gtype_hash_cmp('[1, 1]'::gtype);
SELECT gtype_hash_cmp('[1, 1, 1]'::gtype);
SELECT gtype_hash_cmp('[1, 1, 1, 1]'::gtype);
SELECT gtype_hash_cmp('[1, 1, 1, 1, 1]'::gtype);
SELECT gtype_hash_cmp('[[1]]'::gtype);
SELECT gtype_hash_cmp('[[1, 1]]'::gtype);
SELECT gtype_hash_cmp('[[1], 1]'::gtype);
SELECT gtype_hash_cmp('[1543872]'::gtype);
SELECT gtype_hash_cmp('[1, "abcde", 2.0]'::gtype);
SELECT gtype_hash_cmp(gtype_in('null'));
SELECT gtype_hash_cmp(gtype_in('[null]'));
SELECT gtype_hash_cmp(gtype_in('[null, null]'));
SELECT gtype_hash_cmp(gtype_in('[null, null, null]'));
SELECT gtype_hash_cmp(gtype_in('[null, null, null, null]'));
SELECT gtype_hash_cmp(gtype_in('[null, null, null, null, null]'));
SELECT gtype_hash_cmp('{"id":1, "label":"test", "properties":{"id":100}}'::gtype);
SELECT gtype_hash_cmp('{"id":2, "start_id":1, "end_id": 3, "label":"elabel", "properties":{}}'::gtype);

--Agtype BTree Comparison Function
SELECT gtype_btree_cmp('1'::gtype, '1'::gtype);
SELECT gtype_btree_cmp('1'::gtype, '1.0'::gtype);
SELECT gtype_btree_cmp('1'::gtype, '"1"'::gtype);

SELECT gtype_btree_cmp('"string"'::gtype, '"string"'::gtype);
SELECT gtype_btree_cmp('"string"'::gtype, '"string "'::gtype);

SELECT gtype_btree_cmp(NULL, NULL);
SELECT gtype_btree_cmp(NULL, '1'::gtype);
SELECT gtype_btree_cmp('1'::gtype, NULL);
SELECT gtype_btree_cmp(gtype_in('null'), NULL);

SELECT gtype_btree_cmp(
	'{"id":1, "label":"test", "properties":{"id":100}}'::gtype,
	'{"id":1, "label":"test", "properties":{"id":100}}'::gtype);
SELECT gtype_btree_cmp(
	'{"id":1, "label":"test", "properties":{"id":100}}'::gtype,
	'{"id":1, "label":"test", "properties":{"id":200}}'::gtype);

--
-- Cleanup
--
DROP TABLE gtype_table;

--
-- End of AGTYPE data type regression tests
--
