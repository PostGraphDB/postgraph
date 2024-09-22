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

CREATE GRAPH cypher_create;
USE GRAPH cypher_create;

CREATE ();

CREATE (:v);

CREATE (:v {});

CREATE (:v {key: 'value'});

MATCH (n:v) RETURN n;

-- Left relationship
CREATE (:v {id:"right rel, initial node"})-[:e {id:"right rel"}]->(:v {id:"right rel, end node"});

-- Right relationship
CREATE (:v {id:"left rel, initial node"})<-[:e {id:"left rel"}]-(:v {id:"left rel, end node"});

-- Pattern creates a path from the initial node to the last node
CREATE (:v {id: "path, initial node"})-[:e {id: "path, edge one"}]->(:v {id:"path, middle node"})-[:e {id:"path, edge two"}]->(:v {id:"path, last node"});

-- middle vertex points to the initial and last vertex
CREATE (:v {id: "divergent, initial node"})<-[:e {id: "divergent, edge one"}]-(:v {id: "divergent middle node"})-[:e {id: "divergent, edge two"}]->(:v {id: "divergent, end node"});

-- initial and last vertex point to the middle vertex
CREATE (:v {id: "convergent, initial node"})-[:e {id: "convergent, edge one"}]->(:v {id: "convergent middle node"})<-[:e {id: "convergent, edge two"}]-(:v {id: "convergent, end node"});

-- Validate Paths work correctly
CREATE (:v {id: "paths, vertex one"})-[:e {id: "paths, edge one"}]->(:v {id: "paths, vertex two"}),
       (:v {id: "paths, vertex three"})-[:e {id: "paths, edge two"}]->(:v {id: "paths, vertex four"});

--edge with double relationship will throw an error
CREATE (:v)<-[:e]->();

--edge with no relationship will throw an error
CREATE (:v)-[:e]-();

--edges without labels are not supported
CREATE (:v)-[]->(:v);

MATCH (n) RETURN n;
MATCH ()-[e]-() RETURN e;

CREATE (:n_var {name: 'Node A'});
CREATE (:n_var {name: 'Node B'});
CREATE (:n_var {name: 'Node C'});

MATCH (a:n_var), (b:n_var) WHERE a.name <> b.name CREATE (a)-[:e_var {name: a.name + ' -> ' + b.name}]->(b);

MATCH (a:n_var) CREATE (a)-[:e_var {name: a.name + ' -> ' + a.name}]->(a);

MATCH (a:n_var) CREATE (a)-[:e_var {name: a.name + ' -> new node'}]->(:n_other_node);

MATCH (a:n_var) WHERE a.name = 'Node A' CREATE (a)-[b:e_var]->();

CREATE (a)-[:b_var]->() RETURN a, id(a);

CREATE ()-[b:e_var]->() RETURN b, id(b);

CREATE (a)-[b:e_var {id: 0}]->() RETURN a, b, b.id, b.id + 1;

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

SELECT * FROM cypher('cypher_create', $$MATCH (n:n_var) RETURN n$$) AS (n vertex);
SELECT * FROM cypher('cypher_create', $$MATCH ()-[e:e_var]->() RETURN e$$) AS (e edge);

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


--
-- check the cypher CREATE clause inside an INSERT INTO
--
/*
CREATE TABLE simple_path (u vertex, e edge, v vertex);

INSERT INTO simple_path(SELECT * FROM cypher('cypher_create',
    $$CREATE (u)-[e:knows]->(v) return u, e, v
    $$) AS (u vertex, e edge, v vertex));

SELECT count(*) FROM simple_path;
*/
--
-- check the cypher CREATE clause inside of a BEGIN/END/COMMIT block
--
/*
BEGIN;
SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '670'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);

SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '671'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '672'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);

SELECT * FROM cypher('cypher_create', $$ CREATE (a:Part {part_num: '673'}) $$) as (a gtype);
SELECT * FROM cypher('cypher_create', $$ MATCH (a:Part) RETURN a $$) as (a vertex);
END;
*/
--
-- Clean up
--
SELECT drop_graph('cypher_create', true);

--
-- End
--
