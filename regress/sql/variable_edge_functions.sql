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

SELECT create_graph('variable_edge_functions');

--
-- Create table to hold the start and end vertices to test the SRF
--

-- Create a graph to test
SELECT *
FROM cypher('variable_edge_functions', $$
	CREATE (b:begin)-[:edge]->(u1:middle)-[:edge]->(u2:middle)-[:edge]->(u3:middle)-[:edge]->(e:end),
	(u1)-[:self_loop]->(u1),
	(e)-[:self_loop]->(e),
	(b)-[:alternate_edge]->(u1),
	(u2)-[:alternate_edge]->(u3),
	(u3)-[:alternate_edge]->(e),
	(u2)-[:bypass_edge]->(e),
	(e)-[:alternate_edge]->(u3), 
	(u3)-[:alternate_edge]->(u2),
	(u2)-[:bypass_edge]->(b)
$$) AS (a gtype);

SELECT * FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN nodes(e)
$$) AS (e vertex[]);

SELECT e[1] FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN nodes(e)
$$) AS (e vertex[]);

SELECT e[array_length(e, 1)] FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN nodes(e)
$$) AS (e vertex[]);


SELECT * FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN relationships(e)
$$) AS (e edge[]);

SELECT e[1] FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN relationships(e)
$$) AS (e edge[]);

SELECT e[array_length(e, 1)] FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[e*3..3]->(:end) RETURN relationships(e)
$$) AS (e edge[]);


SELECT * FROM cypher('variable_edge_functions', $$
    MATCH (:begin)-[ve*3..3]->(:end) 
    MATCH ()-[e]->()
    RETURN e @> ve, id(e), edges(ve)
$$) AS (contained boolean, e graphid, edges edge[]);


--
-- Clean up
--
SELECT drop_graph('variable_edge_functions', true);

--
-- End
--
