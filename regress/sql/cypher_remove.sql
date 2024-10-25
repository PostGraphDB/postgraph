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

CREATE GRAPH cypher_remove;
USE GRAPH cypher_remove;

--test 1
CREATE (:test_1);
CREATE (:test_1 {i: 0, j: 5, a: 0});
CREATE (:test_1 {i: 1});

MATCH (n:test_1) REMOVE n.i ;
MATCH (n:test_1) RETURN n;

--test 2
CREATE (:test_2);
CREATE (:test_2 {i: 0, j: 5, a: 0});
CREATE (:test_2 {i: 1});

MATCH (n:test_2) REMOVE n.j RETURN n;
MATCH (n:test_2) RETURN n;

--test 3 Validate Paths are updated
CREATE (:test_3 { i : 20 } )-[:test_3_edge {j:20}]->(:test_3 {i:10});

MATCH p=(n)-[:test_3_edge]->() REMOVE n.i RETURN p;

--test 4 Edges
MATCH (n) REMOVE n.i RETURN n;
CREATE (:test_4 { i : 20 } )-[:test_4_edge {j:20}]->(:test_4 {i:10});

MATCH ()-[n]->(:test_4) REMOVE n.i RETURN n;
MATCH ()-[n]->(:test_4) RETURN n;

--test 5 two REMOVE clauses
CREATE (:test_5 {i: 1, j : 2, k : 3}) ;

MATCH (n:test_5)
REMOVE n.i
REMOVE n.j
RETURN n
;


MATCH (n:test_5)
RETURN n
;

--test 6 Create a loop and see that set can work after create
CREATE (:test_6 {j: 5, y: 99});

MATCH (n {j: 5})
CREATE p=(n)-[e:e {j:34}]->(n)
REMOVE n.y
RETURN n, p
;
MATCH (n:test_6) RETURN n;


--test 7 Create a loop and see that set can work after create
CREATE (:test_7)-[e:e {j:34}]->()
REMOVE e.y
RETURN e
;

MATCH (n:test_7) RETURN n;



--test 8
MATCH (n:test_7)
MATCH (n)-[e:e {j:34}]->()
REMOVE n.y
RETURN n
;

--Handle Inheritance
CREATE ( {i : 1 });
MATCH (n) REMOVE n.i RETURN n;
MATCH (n) RETURN n;

-- prepared statements
/*
CREATE ( {i : 1 });
PREPARE p_1 AS MATCH (n) REMOVE n.i RETURN n ;
EXECUTE p_1;

CREATE ( {i : 1 });
EXECUTE p_1;
-- pl/pgsql
CREATE ( {i : 1 });

CREATE FUNCTION remove_test()
RETURNS TABLE(vertex vertex)
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
	RETURN QUERY MATCH (n) REMOVE n.i RETURN n;
END
$BODY$;

SELECT remove_test();

CREATE ( {i : 1 });
SELECT remove_test();

*/
--
-- Updating Multiple Fields
--
MATCH (n) RETURN n;
MATCH (n) REMOVE n.i, n.j, n.k RETURN n;
MATCH (n) RETURN n;

CREATE ()-[:edge_multi_property { i: 5, j: 20}]->();
MATCH ()-[e:edge_multi_property]-() RETURN e;
MATCH ()-[e:edge_multi_property]-() REMOVE e.i, e.j RETURN e;

--Errors
REMOVE n.i;

MATCH (n) REMOVE n.i = NULL;

MATCH (n) REMOVE wrong_var.i;

--
-- Clean up
--
DROP FUNCTION remove_test;
DROP GRAPH cypher_remove CASCADE;

--
-- End
--
