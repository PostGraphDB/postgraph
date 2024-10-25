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

LOAD 'postgraph';
SET search_path TO postgraph;

CREATE GRAPH cypher_set;
USE GRAPH cypher_set;

CREATE (:v);
CREATE (:v {i: 0, j: 5, a: 0});
CREATE (:v {i: 1});

--Simple SET test case
MATCH (n) SET n.i = 3;

MATCH (n) WHERE n.j = 5 SET n.i = NULL RETURN n;
MATCH (n) RETURN n;

MATCH (n) SET n.i = NULL RETURN n;
MATCH (n) RETURN n;

MATCH (n) SET n.i = 3 RETURN n;
MATCH (n) RETURN n;

--Handle Inheritance
CREATE ();
MATCH (n) SET n.i = 3 RETURN n;
MATCH (n) RETURN n;

--Validate Paths are updated
MATCH (n) CREATE (n)-[:e {j:20}]->(:other_v {k:10}) RETURN n;
MATCH p=(n)-[]->() SET n.i = 50 RETURN p;

--Edges
MATCH ()-[n]-(:other_v) SET n.i = 3 RETURN n;
MATCH ()-[n]->(:other_v) RETURN n;


MATCH (n {j: 5}) SET n.y = 50 SET n.z = 99 RETURN n;

MATCH (n {j: 5}) RETURN n;

--Create a loop and see that set can work after create
MATCH (n {j: 5}) CREATE p=(n)-[e:e {j:34}]->(n) SET n.y = 99 RETURN n, p;

--Create a loop and see that set can work after create
CREATE ()-[e:e {j:34}]->() SET e.y = 99 RETURN e;

MATCH (n) MATCH (n)-[e:e {j:34}]->() SET n.y = 1 RETURN n;

MATCH (n) MATCH ()-[e:e {j:34}]->(n) SET n.y = 2 RETURN n;

MATCH (n)-[]->(n) SET n.y = 99 RETURN n;

MATCH (n) MATCH (n)-[]->(m) SET n.t = 150 RETURN n;

-- prepared statements
/*
PREPARE p_1 AS MATCH (n) SET n.i = 3 RETURN n ;
EXECUTE p_1;

EXECUTE p_1;

PREPARE p_2 AS MATCH (n) SET n.i = $var_name RETURN n ;
EXECUTE p_2('{"var_name": 4}');

EXECUTE p_2('{"var_name": 6}');

CREATE FUNCTION set_test()
RETURNS TABLE(vertex vertex)
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
	RETURN QUERY MATCH (n) SET n.i = 7 RETURN n ;
END
$BODY$;

SELECT set_test();

SELECT set_test();
*/
--
-- Updating multiple fieds
--
MATCH (n) SET n.i = 3, n.j = 5 RETURN n ;

MATCH (n)-[m]->(n) SET m.y = n.y RETURN n, m;

--Errors
SET n.i = NULL;

MATCH (n) SET wrong_var.i = 3;

MATCH (n) SET i = 3;

--
-- SET refactor regression tests
--

-- INSERT INTO
/*
CREATE TABLE tbl (result vertex);


MATCH (u:vertices) return u ;
MATCH (u:begin)-[:edge]->(v:end) return u, v ;

INSERT INTO tbl (MATCH (u:vertices) SET u.i = 7 return u ;
INSERT INTO tbl (MATCH (u:vertices) SET u.i = 13 return u ;

SELECT * FROM tbl;
*/
CREATE (u:vertices) ;
CREATE (u:begin)-[:edge]->(v:end) ;
MATCH (u:vertices) return u ;

BEGIN;
MATCH (u:vertices) SET u.i = 1, u.j = 3, u.k = 5 return u ;
MATCH (u:vertices) return u ;

MATCH (u:vertices) SET u.i = 2, u.j = 4, u.k = 6 return u ;
MATCH (u:vertices) return u ;

MATCH (u:vertices) SET u.i = 3, u.j = 6, u.k = 9 return u ;
MATCH (u:vertices) return u ;

MATCH (u:begin)-[:edge]->(v:end) SET u.i = 1, v.i = 2, u.j = 3, v.j = 4 return u, v ;
MATCH (u:begin)-[:edge]->(v:end) return u, v ;

MATCH (u:begin)-[:edge]->(v:end) SET u.i = 2, v.i = 1, u.j = 4, v.j = 3 return u, v ;
MATCH (u:begin)-[:edge]->(v:end) return u, v ;
END;

MATCH (u:vertices) return u ;
MATCH (u:begin)-[:edge]->(v:end) return u, v ;

-- test lists
MATCH (n) SET n.i = [3, 'test', [1, 2, 3], {id: 1}, 1.0, 1.0::numeric] RETURN n;

MATCH (n) RETURN n;

-- test that lists get updated in paths
MATCH p=(u:begin)-[:edge]->(v:end) SET u.i = [1, 2, 3] return u, p ;

MATCH p=(u:begin)-[:edge]->(v:end) return u, p ;

-- test empty lists
MATCH (n) SET n.i = [] RETURN n;

MATCH (n) RETURN n;

-- test maps
MATCH (n) SET n.i = {prop1: 3, prop2:'test', prop3: [1, 2, 3], prop4: {id: 1}, prop5: 1.0, prop6:1.0::numeric} RETURN n;

MATCH (n) RETURN n;

-- test maps in paths
MATCH p=(u:begin)-[:edge]->(v:end) SET u.i = {prop1: 1, prop2: 2, prop3: 3} return u, p ;

MATCH p=(u:begin)-[:edge]->(v:end) return u, p ;

-- test empty maps
MATCH (n) SET n.i = {} RETURN n;

MATCH (n) RETURN n;

--
-- Clean up
--
DROP TABLE tbl;
DROP FUNCTION set_test;
DROP GRAPH cypher_set CASCADE;
