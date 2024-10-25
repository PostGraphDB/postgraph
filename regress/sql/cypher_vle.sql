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

CREATE GRAPH cypher_vle;
USE GRAPH cypher_vle;

-- Create a graph to test
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
RETURN b, e;

-- Test the VLE match integration
-- Each should find 400
MATCH (u:begin)-[*]->(v:end) RETURN count(*);
MATCH (u:begin)-[*..]->(v:end) RETURN count(*);
MATCH (u:begin)-[*0..]->(v:end) RETURN count(*);
MATCH (u:begin)-[*1..]->(v:end) RETURN count(*);
MATCH (u:begin)-[*1..200]->(v:end) RETURN count(*);
-- Each should find 2
MATCH (u:begin)<-[*]-(v:end) RETURN count(*);
MATCH (u:begin)<-[*..]-(v:end) RETURN count(*);
MATCH (u:begin)<-[*0..]-(v:end) RETURN count(*);
MATCH (u:begin)<-[*1..]-(v:end) RETURN count(*);
MATCH (u:begin)<-[*1..200]-(v:end) RETURN count(*);
-- Each should find 7092
MATCH (u:begin)-[*]-(v:end) RETURN count(*);
MATCH (u:begin)-[*..]-(v:end) RETURN count(*);
MATCH (u:begin)-[*0..]-(v:end) RETURN count(*);
MATCH (u:begin)-[*1..]-(v:end) RETURN count(*);
MATCH (u:begin)-[*1..200]-(v:end) RETURN count(*);
-- Each should find 1
MATCH (u:begin)-[:edge*]-(v:end) RETURN count(*);
MATCH (u:begin)-[:edge* {name: 'main edge'}]-(v:end) RETURN count(*);
MATCH (u:begin)-[* {name: 'main edge'}]-(v:end) RETURN count(*);
-- Each should find 1
MATCH ()<-[*4..4 {name: 'main edge'}]-() RETURN count(*);
MATCH (u)<-[*4..4 {name: 'main edge'}]-() RETURN count(*);
MATCH ()<-[*4..4 {name: 'main edge'}]-(v) RETURN count(*);
-- Each should find 2922
MATCH ()-[*]->() RETURN count(*);
MATCH (u)-[*]->() RETURN count(*);
MATCH ()-[*]->(v) RETURN count(*);
-- Should find 2
MATCH (u:begin)<-[e*]-(v:end) RETURN e;
-- Should find 5
MATCH p=(:begin)<-[*1..1]-()-[]-() RETURN p ORDER BY p;
-- Should find 2922
MATCH p=()-[*]->(v) RETURN count(*);
-- Should find 2
MATCH p=(u:begin)-[*3..3]->(v:end) RETURN p;
-- Should find 12
MATCH p=(u:begin)-[*3..3]-(v:end) RETURN p;
-- Each should find 2
MATCH p=(u:begin)<-[*]-(v:end) RETURN p;
MATCH p=(u:begin)<-[e*]-(v:end) RETURN p;
MATCH p=(u:begin)<-[e*]-(v:end) RETURN e;
MATCH p=(:begin)<-[*]-()<-[]-(:end) RETURN p;
-- Each should return 31
MATCH ()-[e1]->(v)-[e2]->() RETURN count(*);
MATCH ()-[e1*1..1]->(v)-[e2*1..1]->() RETURN count(*);
MATCH (v)-[e1*1..1]->()-[e2*1..1]->() RETURN count(*);
MATCH ()-[e1]->(v)-[e2*1..1]->() RETURN count(*);
MATCH ()-[e1]->()-[e2*1..1]->() RETURN count(*);
MATCH ()-[e1*1..1]->(v)-[e2]->() RETURN count(*);
MATCH ()-[e1*1..1]->()-[e2]->() RETURN count(*);
MATCH (a)-[e1]->(a)-[e2*1..1]->() RETURN count(*);
MATCH (a) MATCH (a)-[e1*1..1]->(v) RETURN count(*);
MATCH (a) MATCH ()-[e1*1..1]->(a) RETURN count(*);
MATCH (a)-[e*1..1]->() RETURN count(*);

-- Should return 1 path
 MATCH p=()<-[e1*]-(:end)-[e2*]->(:begin) RETURN p $$) AS (result traversal);
-- Each should return 3
MATCH (u:begin)-[e*0..1]->(v) RETURN id(u), e, id(v) $$) AS (u gtype, e variable_edge, v gtype);
MATCH p=(u:begin)-[e*0..1]->(v) RETURN p $$) AS (p traversal);
-- Each should return 5
MATCH (u)-[e*0..0]->(v) RETURN id(u), e, id(v) $$) AS (u gtype, e variable_edge, v gtype);
MATCH p=(u)-[e*0..0]->(v) RETURN id(u), p, id(v) $$) AS (u gtype, p traversal, v gtype);
-- Each should return 13 and will be the same
MATCH p=()-[*0..0]->()-[]->() RETURN p $$) AS (p traversal);
MATCH p=()-[]->()-[*0..0]->() RETURN p $$) AS (p traversal);

MATCH (u)-[*]-(v) RETURN count(*);
MATCH (u)-[*0..1]-(v) RETURN count(*);
MATCH (u)-[*..1]-(v) RETURN count(*);
MATCH (u)-[*..5]-(v) RETURN count(*);

MATCH p=(:begin)<-[ve1*]-(:end), (:end)-[ve2*]->(:begin) RETURN ve1 && ve2;
MATCH p=()<-[ve1:edge*]-(), ()-[ve2:alternate_edge*]->() RETURN ve1 && ve2;
SELECT * FROM ag_label;

--
-- Clean up
--
DROP GRAPH cypher_vle CASCADE;

--
-- End
--
