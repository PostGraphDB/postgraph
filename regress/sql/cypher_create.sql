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

SELECT create_graph('cypher_create');

SELECT * FROM cypher('cypher_create', $$CREATE ()$$) AS (a gtype);

-- vertex graphid
SELECT * FROM cypher('cypher_create', $$CREATE (:v)$$) AS (a gtype);

SELECT * FROM cypher('cypher_create', $$CREATE (:v {})$$) AS (a gtype);

SELECT * FROM cypher('cypher_create', $$CREATE (:v {key: 'value'})$$) AS (a gtype);

SELECT * FROM cypher('cypher_create', $$MATCH (n:v) RETURN n$$) AS (n vertex);

-- Left relationship
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id:"right rel, initial node"})-[:e {id:"right rel"}]->(:v {id:"right rel, end node"})
$$) AS (a gtype);

-- Right relationship
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id:"left rel, initial node"})<-[:e {id:"left rel"}]-(:v {id:"left rel, end node"})
$$) AS (a gtype);

-- Pattern creates a path from the initial node to the last node
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id: "path, initial node"})-[:e {id: "path, edge one"}]->(:v {id:"path, middle node"})-[:e {id:"path, edge two"}]->(:v {id:"path, last node"})
$$) AS (a gtype);

-- middle vertex points to the initial and last vertex
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id: "divergent, initial node"})<-[:e {id: "divergent, edge one"}]-(:v {id: "divergent middle node"})-[:e {id: "divergent, edge two"}]->(:v {id: "divergent, end node"})
$$) AS (a gtype);

-- initial and last vertex point to the middle vertex
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id: "convergent, initial node"})-[:e {id: "convergent, edge one"}]->(:v {id: "convergent middle node"})<-[:e {id: "convergent, edge two"}]-(:v {id: "convergent, end node"})
$$) AS (a gtype);

-- Validate Paths work correctly
SELECT * FROM cypher('cypher_create', $$
    CREATE (:v {id: "paths, vertex one"})-[:e {id: "paths, edge one"}]->(:v {id: "paths, vertex two"}),
           (:v {id: "paths, vertex three"})-[:e {id: "paths, edge two"}]->(:v {id: "paths, vertex four"})
$$) AS (a gtype);

--edge with double relationship will throw an error
SELECT * FROM cypher('cypher_create', $$CREATE (:v)<-[:e]->()$$) AS (a gtype);

--edge with no relationship will throw an error
SELECT * FROM cypher('cypher_create', $$CREATE (:v)-[:e]-()$$) AS (a gtype);

--edges without labels are not supported
SELECT * FROM cypher('cypher_create', $$CREATE (:v)-[]->(:v)$$) AS (a gtype);

SELECT * FROM cypher_create.e;

SELECT * FROM cypher_create.v;

SELECT * FROM cypher('cypher_create', $$
	CREATE (:n_var {name: 'Node A'})
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE (:n_var {name: 'Node B'})
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE (:n_var {name: 'Node C'})
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var), (b:n_var)
	WHERE a.name <> b.name
	CREATE (a)-[:e_var {name: a.name + ' -> ' + b.name}]->(b)
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	CREATE (a)-[:e_var {name: a.name + ' -> ' + a.name}]->(a)
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	CREATE (a)-[:e_var {name: a.name + ' -> new node'}]->(:n_other_node)
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	WHERE a.name = 'Node A'
	CREATE (a)-[b:e_var]->()
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE (a)-[:b_var]->()
	RETURN a, id(a)
$$) as (a vertex, b gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE ()-[b:e_var]->()
	RETURN b, id(b)
$$) as (a edge, b gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE (a)-[b:e_var {id: 0}]->()
	RETURN a, b, b.id, b.id + 1
$$) as (a vertex, b edge, c gtype, d gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	CREATE (a)-[b:e_var]->(a)
	RETURN a, b
$$) as (a vertex, b edge);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	CREATE (a)-[b:e_var]->(c)
	RETURN a, b, c
$$) as (a vertex, b edge, c vertex);

SELECT * FROM cypher('cypher_create', $$
	CREATE (a)-[:e_var]->()
	RETURN a
$$) as (b vertex);

SELECT * FROM cypher('cypher_create', $$
	CREATE ()-[b:e_var]->()
	RETURN b
$$) as (b edge);

SELECT * FROM cypher('cypher_create', $$
	CREATE p=()-[:e_var]->()
	RETURN p
$$) as (b traversal);

SELECT * FROM cypher('cypher_create', $$
	CREATE p=(a {id:0})-[:e_var]->(a)
	RETURN p
$$) as (b traversal);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	CREATE p=(a)-[:e_var]->(a)
	RETURN p
$$) as (b traversal);

SELECT * FROM cypher('cypher_create', $$
	CREATE p=(a)-[:e_var]->(), (a)-[b:e_var]->(a)
	RETURN p, b
$$) as (a traversal, b edge);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	WHERE a.name = 'Node Z'
	CREATE (a)-[:e_var {name: a.name + ' -> doesnt exist'}]->(:n_other_node)
	RETURN a
$$) as (a vertex);

SELECT * FROM cypher_create.n_var;
SELECT * FROM cypher_create.e_var;

--Check every label has been created
SELECT name, kind FROM ag_label ORDER BY name;

--Validate every vertex has the correct label
SELECT * FROM cypher('cypher_create', $$MATCH (n) RETURN n$$) AS (n vertex);

-- prepared statements
PREPARE p_1 AS SELECT * FROM cypher('cypher_create', $$CREATE (v:new_vertex {key: 'value'}) RETURN v$$) AS (a vertex);
EXECUTE p_1;
EXECUTE p_1;

PREPARE p_2 AS SELECT * FROM cypher('cypher_create', $$CREATE (v:new_vertex {key: $var_name}) RETURN v$$, $1) AS (a vertex);
EXECUTE p_2('{"var_name": "Hello Prepared Statements"}');
EXECUTE p_2('{"var_name": "Hello Prepared Statements 2"}');

-- pl/pgsql
CREATE FUNCTION create_test()
RETURNS TABLE(v vertex)
LANGUAGE plpgsql
VOLATILE
AS $BODY$
BEGIN
	RETURN QUERY SELECT * FROM cypher('cypher_create', $$CREATE (v:new_vertex {key: 'value'}) RETURN v$$) AS (a vertex);
END
$BODY$;

SELECT create_test();
SELECT create_test();

--
-- Errors
--
-- Var 'a' cannot have properties in the create clause
SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	WHERE a.name = 'Node A'
	CREATE (a {test:1})-[:e_var]->()
$$) as (a gtype);

-- Var 'a' cannot change labels
SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)
	WHERE a.name = 'Node A'
	CREATE (a:new_label)-[:e_var]->()
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH (a:n_var)-[b]-()
	WHERE a.name = 'Node A'
	CREATE (a)-[b:e_var]->()
$$) as (a gtype);

-- A valid single vertex path
SELECT * FROM cypher('cypher_create', $$
	CREATE p=(a)
	RETURN p
$$) as (a traversal);

--CREATE with joins
SELECT *
FROM cypher('cypher_create', $$
	CREATE (a)
	RETURN a
$$) as q(a vertex),
cypher('cypher_create', $$
	CREATE (b)
	RETURN b
$$) as t(b vertex);

-- column definition list for CREATE clause must contain a single gtype
-- attribute
SELECT * FROM cypher('cypher_create', $$CREATE ()$$) AS (a int);
SELECT * FROM cypher('cypher_create', $$CREATE ()$$) AS (a gtype, b int);

-- nodes cannot use edge labels and edge labels cannot use node labels
SELECT * FROM cypher('cypher_create', $$
	CREATE
		(:existing_vlabel {id: 1})
		-[c:existing_elabel {id: 3}]->
		(:existing_vlabel {id: 2})
$$) as (a gtype);

SELECT * FROM cypher('cypher_create', $$
	MATCH(a), (b)
		WHERE a.id = 1 AND b.id = 2
	CREATE (a)-[c:existing_vlabel { id: 4}]->(b)
	RETURN c.id
$$) as (c gtype);

SELECT * FROM cypher('cypher_create', $$
	CREATE (a:existing_elabel { id: 5})
	RETURN a.id
$$) as (a gtype);

--
-- check the cypher CREATE clause inside an INSERT INTO
--
CREATE TABLE simple_path (u vertex, e edge, v vertex);

INSERT INTO simple_path(SELECT * FROM cypher('cypher_create',
    $$CREATE (u)-[e:knows]->(v) return u, e, v
    $$) AS (u vertex, e edge, v vertex));

SELECT count(*) FROM simple_path;

--
-- check the cypher CREATE clause inside of a BEGIN/END/COMMIT block
--
BEGIN;
SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '670'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);

SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '671'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '672'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);

SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '673'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);
END;

--
-- Clean up
--
DROP TABLE simple_path;
DROP FUNCTION create_test;
SELECT drop_graph('cypher_create', true);

--
-- End
--
