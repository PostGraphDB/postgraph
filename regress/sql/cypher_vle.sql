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

SELECT create_graph('cypher_vle');

--
-- Create table to hold the start and end vertices to test the SRF
--

CREATE TABLE start_and_end_points (start_vertex vertex, end_vertex vertex);

-- Create a graph to test
SELECT * FROM cypher('cypher_vle', $$
	CREATE (b:begin)-[:edge {name: 'main edge'}]->(u1:middle)-[:edge {name: 'main edge'}]->(u2:middle)-[:edge {name: 'main edge'}]->(u3:middle)-[:edge {name: 'main edge'}]->(e:end),
	(u1)-[:self_loop {name: 'self loop'}]->(u1),
	(e)-[:self_loop {name: 'self loop'}]->(e),
	(b)-[:alternate_edge {name: 'alternate edge'}]->(u1),
	(u2)-[:alternate_edge {name: 'alternate edge'}]->(u3),
	(u3)-[:alternate_edge {name: 'alternate edge'}]->(e),
	(u2)-[:bypass_edge {name: 'bypass edge'}]->(e),
	(e)-[:alternate_edge {name: 'backup edge'}]->(u3), 
	(u3)-[:alternate_edge {name: 'backup edge'}]->(u2),
	(u2)-[:bypass_edge {name: 'bypass edge'}]->(b)
	RETURN b, e
$$) AS (b vertex, e vertex);

-- Insert start and end points for graph
INSERT INTO start_and_end_points 
SELECT * FROM cypher('cypher_vle', $$
	MATCH (b:begin)-[:edge]->()-[:edge]->()-[:edge]->()-[:edge]->(e:end)
	RETURN b, e
$$) AS (b vertex, e vertex);


-- Count the total paths from left (start) to right (end) -[]-> should be 400
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '1'::gtype, 'null'::gtype, '1'::gtype, NULL, NULL);

-- Count the total paths from right (end) to left (start) <-[]- should be 2
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '1'::gtype, 'null'::gtype, '-1'::gtype, NULL, NULL);

-- Count the total paths undirectional -[]- should be 7092
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '1'::gtype, 'null'::gtype, '0'::gtype, NULL, NULL);

-- All paths of length 3 -[]-> should be 2
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '3'::gtype, '3'::gtype, '1'::gtype, NULL, NULL);

-- All paths of length 3 <-[]- should be 1
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '3'::gtype, '3'::gtype, '-1'::gtype, NULL, NULL);

-- All paths of length 3 -[]- should be 12
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '3'::gtype, '3'::gtype, '0'::gtype, NULL, NULL);

-- Test edge label matching - should match 1
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '1'::gtype, 'null'::gtype, '1'::gtype, '"edge"'::gtype, NULL);

-- Test scalar property matching - should match 1
SELECT count(edges) FROM start_and_end_points, vle( '"cypher_vle"'::gtype, start_vertex, end_vertex, '1'::gtype, 'null'::gtype, '1'::gtype, NULL::gtype, '{"name": "main edge"}'::gtype);

-- Test the VLE match integration
-- Each should find 400
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*]->(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*..]->(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*0..]->(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*1..]->(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*1..200]->(v:end) RETURN count(*) $$) AS (e gtype);
-- Each should find 2
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[*]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[*..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[*0..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[*1..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[*1..200]-(v:end) RETURN count(*) $$) AS (e gtype);
-- Each should find 7092
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*0..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*1..]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[*1..200]-(v:end) RETURN count(*) $$) AS (e gtype);
-- Each should find 1
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[:edge*]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[:edge* {name: "main edge"}]-(v:end) RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[* {name: "main edge"}]-(v:end) RETURN count(*) $$) AS (e gtype);
-- Each should find 1
SELECT * FROM cypher('cypher_vle', $$MATCH ()<-[*4..4 {name: "main edge"}]-() RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u)<-[*4..4 {name: "main edge"}]-() RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH ()<-[*4..4 {name: "main edge"}]-(v) RETURN count(*) $$) AS (e gtype);
-- Each should find 2922
SELECT * FROM cypher('cypher_vle', $$MATCH ()-[*]->() RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH (u)-[*]->() RETURN count(*) $$) AS (e gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH ()-[*]->(v) RETURN count(*) $$) AS (e gtype);
-- Should find 2
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)<-[e*]-(v:end) RETURN e $$) AS (e variable_edge);
-- Should find 5
SELECT * FROM cypher('cypher_vle', $$MATCH p=(:begin)<-[*1..1]-()-[]-() RETURN p ORDER BY p $$) AS (e traversal);
-- Should find 2922
SELECT * FROM cypher('cypher_vle', $$MATCH p=()-[*]->(v) RETURN count(*) $$) AS (e gtype);
-- Should find 2
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)-[*3..3]->(v:end) RETURN p $$) AS (e traversal);
-- Should find 12
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)-[*3..3]-(v:end) RETURN p $$) AS (e traversal);
-- Each should find 2
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)<-[*]-(v:end) RETURN p $$) AS (e traversal);
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)<-[e*]-(v:end) RETURN p $$) AS (e traversal);
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)<-[e*]-(v:end) RETURN e $$) AS (e variable_edge);
SELECT * FROM cypher('cypher_vle', $$MATCH p=(:begin)<-[*]-()<-[]-(:end) RETURN p $$) AS (e traversal);
-- Each should return 31
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1]->(v)-[e2]->() RETURN e1,e2 $$) AS (e1 edge, e2 edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1*1..1]->(v)-[e2*1..1]->() RETURN e1, e2 $$) AS (e1 variable_edge, e2 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH (v)-[e1*1..1]->()-[e2*1..1]->() RETURN e1, e2 $$) AS (e1 variable_edge, e2 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1]->(v)-[e2*1..1]->() RETURN e1, e2 $$) AS (e1 edge, e2 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1]->()-[e2*1..1]->() RETURN e1, e2 $$) AS (e1 edge, e2 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1*1..1]->(v)-[e2]->() RETURN e1, e2
$$) AS (e1 variable_edge, e2 edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH ()-[e1*1..1]->()-[e2]->() RETURN e1, e2 $$) AS (e1 variable_edge, e2 edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH (a)-[e1]->(a)-[e2*1..1]->() RETURN e1, e2 $$) AS (e1 edge, e2 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH (a) MATCH (a)-[e1*1..1]->(v) RETURN e1 $$) AS (e1 variable_edge);
SELECT count(*) FROM cypher('cypher_vle', $$ MATCH (a) MATCH ()-[e1*1..1]->(a) RETURN e1 $$) AS (e1 variable_edge);
SELECT count(*)
FROM cypher('cypher_vle', $$
    MATCH (a)-[e*1..1]->()
    RETURN a, e
$$) AS (e1 vertex, e2 variable_edge);
-- Should return 1 path
SELECT * FROM cypher('cypher_vle', $$ MATCH p=()<-[e1*]-(:end)-[e2*]->(:begin) RETURN p $$) AS (result traversal);
-- Each should return 3
SELECT * FROM cypher('cypher_vle', $$MATCH (u:begin)-[e*0..1]->(v) RETURN id(u), e, id(v) $$) AS (u gtype, e variable_edge, v gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u:begin)-[e*0..1]->(v) RETURN p $$) AS (p traversal);
-- Each should return 5
SELECT * FROM cypher('cypher_vle', $$MATCH (u)-[e*0..0]->(v) RETURN id(u), e, id(v) $$) AS (u gtype, e variable_edge, v gtype);
SELECT * FROM cypher('cypher_vle', $$MATCH p=(u)-[e*0..0]->(v) RETURN id(u), p, id(v) $$) AS (u gtype, p traversal, v gtype);
-- Each should return 13 and will be the same
SELECT * FROM cypher('cypher_vle', $$MATCH p=()-[*0..0]->()-[]->() RETURN p $$) AS (p traversal);
SELECT * FROM cypher('cypher_vle', $$MATCH p=()-[]->()-[*0..0]->() RETURN p $$) AS (p traversal);

SELECT count(*) FROM cypher('cypher_vle', $$MATCH (u)-[*]-(v) RETURN u, v$$) AS (u vertex, v vertex);
SELECT count(*) FROM cypher('cypher_vle', $$MATCH (u)-[*0..1]-(v) RETURN u, v$$) AS (u vertex, v vertex);
SELECT count(*) FROM cypher('cypher_vle', $$MATCH (u)-[*..1]-(v) RETURN u, v$$) AS (u vertex, v vertex);
SELECT count(*) FROM cypher('cypher_vle', $$MATCH (u)-[*..5]-(v) RETURN u, v$$) AS (u vertex, v vertex);

SELECT * FROM cypher('cypher_vle', $$MATCH p=(:begin)<-[ve1*]-(:end), (:end)-[ve2*]->(:begin) RETURN ve1 && ve2 $$) AS (overlap boolean);

SELECT * FROM cypher('cypher_vle', $$MATCH p=()<-[ve1:edge*]-(), ()-[ve2:alternate_edge*]->() RETURN ve1 && ve2 $$) AS (overlap boolean);

SELECT * FROM ag_label;

--
-- Clean up
--
DROP TABLE start_and_end_points;
SELECT drop_graph('cypher_vle', true);

--
-- End
--
