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

--Test 1: Delete Vertices
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (:v {i: 0, j: 5, a: 0})$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (:v {i: 1})$$) AS (a gtype);

SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 2: Delete Edges
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);

--Should Fail
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DELETE n1 RETURN n1$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DELETE n2 RETURN n2$$) AS (a vertex);

SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DELETE e RETURN e$$) AS (a edge);
SELECT * FROM cypher('cypher_delete', $$MATCH()-[e]->() DELETE e RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);

--Test 4: DETACH DELECT a vertex
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DETACH DELETE n1 RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 4: DETACH DELECT two vertices tied to the same edge
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e$$) AS (a edge);

--Test 4: DETACH DELECT a vertex
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e$$) AS (a edge);

--Test 5: DETACH DELETE a vertex that points to itself
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(n)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->() DETACH DELETE n1 RETURN e$$) AS (a edge);

--Test 6: DETACH Delete a vertex twice
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(n)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->(n2) DETACH DELETE n1, n2 RETURN e$$) AS (a edge);

--Test 7: Test the SET Clause on DELETED node
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (:v {i: 0, j: 5, a: 0})$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (:v {i: 1})$$) AS (a gtype);

SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n SET n.lol = 'ftw' RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 8:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v) CREATE (n)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[]->() DELETE n1 RETURN n1$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 9:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->() DELETE e, n1 RETURN n1$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 10:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->() DELETE n1, e RETURN n1$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 11: Delete a vertex twice
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v), (n)-[:e]->(:v) RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n1)-[e]->() DETACH DELETE n1 RETURN n1, e$$) AS (a vertex, b edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 12:
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->() DETACH DELETE n CREATE (n)-[:e2]->(:v) RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 13:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(n)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->(m) DETACH DELETE n CREATE (m)-[:e2]->(:v) RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 14:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(n)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->(m) DETACH DELETE n CREATE (m)-[:e2]->(:v) RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 15:
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) SET n.i = 0 DELETE n RETURN n$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 16:
SELECT * FROM cypher('cypher_delete', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n SET n.i = 0 RETURN n$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) RETURN n$$) AS (a vertex);

--Test 17:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->(m) DETACH DELETE n SET e.i = 1 RETURN e$$) AS (a edge);

--Cleanup
--Note: Expect 1 vertex
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);

--Test 18:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->(:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->(m) SET e.i = 1 DETACH DELETE n RETURN e$$) AS (a edge);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);

--Test 19:
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH (n) DELETE n CREATE (n)-[:e]->(:v) RETURN n$$) AS (a vertex);

--Cleanup
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);

--Test 20 Undefined Reference:
SELECT * FROM cypher('cypher_delete', $$MATCH (n) DELETE m RETURN n$$) AS (a vertex);

--Test 21 Prepared Statements
SELECT * FROM cypher('cypher_delete', $$CREATE (v:v)$$) AS (a gtype);

PREPARE d AS SELECT * FROM cypher('cypher_delete', $$MATCH (v) DELETE (v) RETURN v$$) AS (a vertex);
EXECUTE d;

SELECT * FROM cypher('cypher_delete', $$CREATE (v:v)$$) AS (a gtype);
EXECUTE d;

--Test 22 pl/pgsql Functions
SELECT * FROM cypher('cypher_delete', $$CREATE (v:v)$$) AS (a gtype);

CREATE FUNCTION delete_test()
RETURNS TABLE(vertex vertex)
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
	RETURN QUERY SELECT * FROM cypher('cypher_delete', $$MATCH (v) DELETE (v) RETURN v$$) AS (a vertex);
END
$BODY$;

SELECT delete_test();

SELECT * FROM cypher('cypher_delete', $$CREATE (v:v)$$) AS (a gtype);
SELECT delete_test();

-- Clean Up
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DETACH DELETE n RETURN n$$) AS (a vertex);

--Test 23
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->()$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e2]->()$$) AS (a gtype);

SELECT * FROM cypher('cypher_delete', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[e]->(m)  DELETE e$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);

-- Clean Up
SELECT * FROM cypher('cypher_delete', $$MATCH(n)  DELETE n RETURN n$$) AS (a vertex);

--Test 24
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e]->()$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$CREATE (n:v)-[:e2]->()$$) AS (a gtype);

SELECT * FROM cypher('cypher_delete', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);
SELECT * FROM cypher('cypher_delete', $$MATCH(n)-[]->() DETACH DELETE n$$) AS (a gtype);
SELECT * FROM cypher('cypher_delete', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);

-- Clean Up
SELECT * FROM cypher('cypher_delete', $$MATCH(n) DELETE n RETURN n$$) AS (a vertex);

-- test DELETE in transaction block
SELECT * FROM cypher('cypher_delete', $$CREATE (u:vertices) RETURN u $$) AS (result vertex);

BEGIN;

SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);

SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) DELETE u RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);

SELECT * FROM cypher('cypher_delete', $$CREATE (u:vertices) RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) DELETE u RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);

SELECT * FROM cypher('cypher_delete', $$CREATE (u:vertices) RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) DELETE u SET u.i = 1 RETURN u $$) AS (result vertex);
SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);

END;

SELECT * FROM cypher('cypher_delete', $$MATCH (u:vertices) RETURN u $$) AS (result vertex);

--
-- Clean up
--
DROP FUNCTION delete_test;
SELECT drop_graph('cypher_delete', true);

--
-- End
--
