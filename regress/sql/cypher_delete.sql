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

SELECT create_graph('cypher_delete');
CREATE GRAPH cypher_delete;
USE GRAPH cypher_delete;

--Test 1: Delete Vertices
CREATE (:v);
CREATE (:v {i: 0, j: 5, a: 0});
CREATE (:v {i: 1});

MATCH(n) DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 2: Delete Edges
CREATE (:v)-[:e]->(:v);

--Should Fail
MATCH(n1)-[e]->(n2) DELETE n1 RETURN n1;
MATCH(n1)-[e]->(n2) DELETE n2 RETURN n2;

MATCH(n1)-[e]->(n2) DELETE e RETURN e;
MATCH()-[e]->() DELETE e RETURN e;

--Cleanup
MATCH(n) DELETE n RETURN n;

--Test 4: DETACH DELECT a vertex
CREATE (:v)-[:e]->(:v);
MATCH(n1)-[e]->(n2) DETACH DELETE n1 RETURN e;

--Cleanup
MATCH(n) RETURN n;

--Test 4: DETACH DELECT two vertices tied to the same edge
CREATE (:v)-[:e]->(:v);
MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e;

--Test 4: DETACH DELECT a vertex
CREATE (:v)-[:e]->(:v);
MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e;

--Test 5: DETACH DELETE a vertex that points to itself
CREATE (n:v)-[:e]->(n);
MATCH(n1)-[e]->() DETACH DELETE n1 RETURN e;

--Test 6: DETACH Delete a vertex twice
CREATE (n:v)-[:e]->(n);
MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e;

--Test 7: Test the SET Clause on DELETED node
CREATE (:v);
CREATE (:v {i: 0, j: 5, a: 0});
CREATE (:v {i: 1});

MATCH(n) DELETE n SET n.lol = 'ftw' RETURN n;
MATCH(n) RETURN n;

--Test 8:
CREATE (n:v)-[:e]->(:v) CREATE (n)-[:e]->(:v);
MATCH(n1)-[]->() DELETE n1 RETURN n1;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 9:
CREATE (n:v)-[:e]->(:v);
MATCH(n1)-[e]->() DELETE e, n1 RETURN n1;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 10:
CREATE (n:v)-[:e]->(:v);
MATCH(n1)-[e]->() DELETE n1, e RETURN n1;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 11: Delete a vertex twice
CREATE (n:v)-[:e]->(:v), (n)-[:e]->(:v) RETURN n;
MATCH(n1)-[e]->() DETACH DELETE n1 RETURN n1, e;

--Cleanup
MATCH(n) DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 12:
CREATE (:v)-[:e]->(:v);
MATCH(n)-[e]->() DETACH DELETE n CREATE (n)-[:e2]->(:v) RETURN e;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 13:
CREATE (n:v)-[:e]->(n);
MATCH(n)-[e]->(m) DETACH DELETE n CREATE (m)-[:e2]->(:v) RETURN e;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 14:
CREATE (n:v)-[:e]->(n);
CREATE (:v)-[:e]->(:v);
MATCH(n)-[e]->(m) DETACH DELETE n CREATE (m)-[:e2]->(:v) RETURN e;

--Cleanup
MATCH(n) DETACH DELETE n RETURN n;
MATCH(n) RETURN n;

--Test 15:
CREATE (:v);
MATCH(n) SET n.i = 0 DELETE n RETURN n;

--Cleanup
MATCH(n) RETURN n;

--Test 16:
CREATE (:v);
MATCH(n) DELETE n SET n.i = 0 RETURN n;

--Cleanup
MATCH(n) RETURN n;

--Test 17:
CREATE (n:v)-[:e]->(:v);
MATCH(n)-[e]->(m) DETACH DELETE n SET e.i = 1 RETURN e;

--Cleanup
--Note: Expect 1 vertex
MATCH(n) DELETE n RETURN n;

--Test 18:
CREATE (n:v)-[:e]->(:v);
MATCH(n)-[e]->(m) SET e.i = 1 DETACH DELETE n RETURN e;

--Cleanup
MATCH(n) DELETE n RETURN n;

--Test 19:
CREATE (n:v);
MATCH (n) DELETE n CREATE (n)-[:e]->(:v) RETURN n;

--Cleanup
MATCH(n) DELETE n RETURN n;

--Test 20 Undefined Reference:
MATCH (n) DELETE m RETURN n;

--Test 21 Prepared Statements
/*
CREATE (v:v);

PREPARE d AS MATCH (v) DELETE (v) RETURN v;
EXECUTE d;

CREATE (v:v);
EXECUTE d;

--Test 22 pl/pgsql Functions
CREATE (v:v);

CREATE FUNCTION delete_test()
RETURNS TABLE(vertex vertex)
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
	RETURN QUERY MATCH (v) DELETE (v) RETURN v;
END
$BODY$;

SELECT delete_test();

CREATE (v:v);
SELECT delete_test();

-- Clean Up
MATCH(n) DETACH DELETE n RETURN n;
*/
--Test 23
CREATE (n:v)-[:e]->();
CREATE (n:v)-[:e2]->();

MATCH p=()-[]->() RETURN p;
MATCH(n)-[e]->(m)  DELETE e;
MATCH p=()-[]->() RETURN p;

-- Clean Up
MATCH(n)  DELETE n RETURN n;

--Test 24
CREATE (n:v)-[:e]->();
CREATE (n:v)-[:e2]->();

MATCH p=()-[]->() RETURN p;
MATCH(n)-[]->() DETACH DELETE n;
MATCH p=()-[]->() RETURN p;

-- Clean Up
MATCH(n) DELETE n RETURN n;

--
-- Clean up
--
DROP FUNCTION delete_test;
SELECT drop_graph('cypher_delete', true);

--
-- End
--
