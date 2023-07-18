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

SELECT create_graph('cypher_match');

SELECT * FROM cypher('cypher_match', $$CREATE (:v)$$) AS (a gtype);
SELECT * FROM cypher('cypher_match', $$CREATE (:v {i: 0})$$) AS (a gtype);
SELECT * FROM cypher('cypher_match', $$CREATE (:v {i: 1})$$) AS (a gtype);

SELECT * FROM cypher('cypher_match', $$MATCH (n:v) RETURN n$$) AS (n vertex);
SELECT * FROM cypher('cypher_match', $$MATCH (n:v) RETURN n.i$$) AS (i gtype);

SELECT * FROM cypher('cypher_match', $$
MATCH (n:v) WHERE n.i > 0
RETURN n.i
$$) AS (i gtype);

--Directed Paths
SELECT * FROM cypher('cypher_match', $$
	CREATE (:v1 {id:'initial'})-[:e1]->(:v1 {id:'middle'})-[:e1]->(:v1 {id:'end'})
$$) AS (a gtype);

--Undirected Path Tests
SELECT * FROM cypher('cypher_match', $$
	MATCH p=(:v1)-[:e1]-(:v1)-[:e1]-(:v1) RETURN p
$$) AS (a traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(a:v1)-[]-()-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]-()-[]-(a:v1) RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]-(a:v1)-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[b:e1]-()-[]-() RETURN b
$$) AS (a edge);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)-[]->(), ()-[]->(a) RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
    MATCH p=()-[e]-() RETURN e
$$) AS (a edge);

-- Right Path Test
SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)-[:e1]->(b:v1)-[:e1]->(c:v1) RETURN a, b, c
$$) AS (a vertex, b vertex, c vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(a:v1)-[]-()-[]->() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(a:v1)-[]->()-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]-()-[]->(a:v1) RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]-(a:v1)-[]->() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[b:e1]-()-[]->() RETURN b
$$) AS (a edge);

--Left Path Test
SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)<-[:e1]-(b:v1)<-[:e1]-(c:v1) RETURN a, b, c
$$) AS (a vertex, b vertex, c vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(a:v1)<-[]-()-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(a:v1)-[]-()<-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()<-[]-()-[]-(a:v1) RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()<-[]-(a:v1)-[]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()<-[b:e1]-()-[]-() RETURN b
$$) AS (a edge);

--Divergent Path Tests
SELECT * FROM cypher('cypher_match', $$
	CREATE (:v2 {id:'initial'})<-[:e2]-(:v2 {id:'middle'})-[:e2]->(:v2 {id:'end'})
$$) AS (a gtype);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()<-[]-(n:v2)-[]->()
	MATCH p=()-[]->(n)
	RETURN p
$$) AS (i traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()<-[]-(n:v2)-[]->()
	MATCH p=(n)-[]->()
	RETURN p
$$) AS (i traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]-(n:v2)
	RETURN n
$$) AS (i vertex);

--Convergent Path Tests
SELECT * FROM cypher('cypher_match', $$
	CREATE (:v3 {id:'initial'})-[:e3]->(:v3 {id:'middle'})<-[:e3]-(:v3 {id:'end'})
$$) AS (a gtype);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[b:e1]->()
	RETURN b
$$) AS (i edge);


SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]->(n:v1)<-[]-()
	MATCH p=(n)<-[]-()
	RETURN p
$$) AS (i traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]->(n:v1)<-[]-()
	MATCH p=()-[]->(n)
	RETURN p
$$) AS (i traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH ()-[]->(n:v1)<-[]-()
	MATCH p=(n)-[]->()
	RETURN p
$$) AS (i traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH con_path=(a)-[]->()<-[]-()
	where a.id = 'initial'
	RETURN con_path
$$) AS (con_path traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH div_path=(b)<-[]-()-[]->()
	where b.id = 'initial'
	RETURN div_path
$$) AS (div_path traversal);

--Patterns
SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1), p=(a)-[]-()-[]-()
	where a.id = 'initial'
	RETURN p
$$) AS (p traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH con_path=(a)-[]->()<-[]-(), div_path=(b)<-[]-()-[]->()
	where a.id = 'initial'
	and b.id = 'initial'
	RETURN con_path, div_path
$$) AS (con_path traversal, div_path traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v), p=()-[]->()-[]->()
	RETURN a.i, p
$$) AS (i gtype, p traversal);

--Multiple Match Clauses
SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)
	where a.id = 'initial'
	MATCH p=(a)-[]-()-[]-()
	RETURN p
$$) AS (p traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v)
	MATCH p=()-[]->()-[]->()
	RETURN a.i, p
$$) AS (i gtype, p traversal);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v)
	MATCH (b:v1)-[]-(c)
	RETURN a.i, b.id, c.id
$$) AS (i gtype, b gtype, c gtype);

--
-- Property constraints
--
SELECT * FROM cypher('cypher_match',
 $$CREATE ({string_key: "test", int_key: 1, float_key: 3.14, map_key: {key: "value"}, list_key: [1, 2, 3]}) $$)
AS (p gtype);

SELECT * FROM cypher('cypher_match',
 $$CREATE ({lst: [1, NULL, 3.14, "string", {key: "value"}, []]}) $$)
AS (p gtype);

SELECT * FROM cypher('cypher_match',
 $$MATCH (n  {string_key: NULL}) RETURN n $$)
AS (n vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (n  {string_key: "wrong value"}) RETURN n $$)
AS (n vertex);


SELECT * FROM cypher('cypher_match', $$
    MATCH (n {string_key: "test", int_key: 1, float_key: 3.14, map_key: {key: "value"}, list_key: [1, 2, 3]})
    RETURN n $$)
AS (p vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (n {string_key: "test"}) RETURN n $$)
AS (p vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (n {lst: [1, NULL, 3.14, "string", {key: "value"}, []]})  RETURN n $$)
AS (p vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (n {lst: [1, NULL, 3.14, "string", {key: "value"}, [], "extra value"]})  RETURN n $$)
AS (p vertex);


--
-- Prepared Statement Property Constraint
--
PREPARE property_ps(gtype) AS SELECT * FROM cypher('cypher_match',
 $$MATCH (n $props) RETURN n $$, $1)
AS (p vertex);

EXECUTE property_ps(gtype_build_map('props',
                                     gtype_build_map('string_key', 'test')));

-- need a following RETURN clause (should fail)
SELECT * FROM cypher('cypher_match', $$MATCH (n:v)$$) AS (a gtype);

--Invalid Variables
SELECT * FROM cypher('cypher_match', $$
	MATCH (a)-[]-()-[]-(a:v1) RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)-[]-()-[a]-() RETURN a
$$) AS (a vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH (a:v1)-[]-()-[]-(a {id:'will_fail'}) RETURN a
$$) AS (a vertex);

--Incorrect Labels
SELECT * FROM cypher('cypher_match', $$MATCH (n)-[:v]-() RETURN n$$) AS (n vertex);

SELECT * FROM cypher('cypher_match', $$MATCH (n)-[:emissing]-() RETURN n$$) AS (n vertex);

SELECT * FROM cypher('cypher_match', $$MATCH (n:e1)-[]-() RETURN n$$) AS (n vertex);

SELECT * FROM cypher('cypher_match', $$MATCH (n:vmissing)-[]-() RETURN n$$) AS (n vertex);

--
-- Path of one vertex. This should select 14
--
SELECT * FROM cypher('cypher_match', $$
       MATCH p=() RETURN p
$$) AS (p traversal);

--
-- MATCH with WHERE EXISTS(pattern)
--
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) RETURN u, e, v $$) AS (u vertex, e edge, v vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS ((u)-[e]->(v)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);


-- Property Constraint in EXISTS
SELECT * FROM cypher('cypher_match',
 $$MATCH (u) WHERE EXISTS((u)-[]->({id: "middle"})) RETURN u $$)
AS (u vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (u) WHERE EXISTS((u)-[]->({id: "not a valid id"})) RETURN u $$)
AS (u vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (u) WHERE EXISTS((u)-[]->({id: NULL})) RETURN u $$)
AS (u vertex);

-- Exists checks for a loop. There shouldn't be any.
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

-- Create a loop
SELECT * FROM cypher('cypher_match', $$
        CREATE (u:loop {id:'initial'})-[:self]->(u)
$$) AS (a gtype);

-- dump paths
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(v)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

-- Exists checks for a loop. There should be one.
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

-- Exists checks for a loop. There should be one.
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((v)-[e]->(v)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

-- Exists checks for a loop. There should be none because of edge uniqueness
-- requirement.
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)-[e]->(u)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) AND EXISTS((v)-[e]->(v)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

-- variable creation error
SELECT * FROM cypher('cypher_match',
 $$MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(x)) RETURN u, e, v $$)
AS (u vertex, e edge, v vertex);

--
--Distinct
--
SELECT * FROM cypher('cypher_match', $$
	MATCH (u)
	RETURN DISTINCT u.id
$$) AS (i gtype);

SELECT * FROM cypher('cypher_match', $$
	CREATE (u:duplicate)-[:dup_edge {id:1 }]->(:other_v)
$$) AS (a gtype);

SELECT * FROM cypher('cypher_match', $$
	MATCH (u:duplicate)
	CREATE (u)-[:dup_edge {id:2 }]->(:other_v)
$$) AS (a gtype);

SELECT * FROM cypher('cypher_match', $$
	MATCH (u:duplicate)-[]-(:other_v)
	RETURN DISTINCT u
$$) AS (i vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH p=(:duplicate)-[]-(:other_v)
	RETURN DISTINCT p
$$) AS (i traversal);

--
-- Limit
--
SELECT * FROM cypher('cypher_match', $$
	MATCH (u)
	RETURN u
$$) AS (i vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH (u)
	RETURN u LIMIT 3
$$) AS (i vertex);

--
-- Skip
--
SELECT * FROM cypher('cypher_match', $$
	MATCH (u)
	RETURN u SKIP 7
$$) AS (i vertex);

SELECT * FROM cypher('cypher_match', $$
	MATCH (u)
	RETURN u SKIP 7 LIMIT 3
$$) AS (i vertex);


--
-- Optional Match
--
SELECT * FROM cypher('cypher_match', $$
    CREATE (:opt_match_v {name: 'someone'})-[:opt_match_e]->(:opt_match_v {name: 'somebody'}),
           (:opt_match_v {name: 'anybody'})-[:opt_match_e]->(:opt_match_v {name: 'nobody'})
$$) AS (u gtype);

SELECT * FROM cypher('cypher_match', $$
    MATCH (u:opt_match_v)
    OPTIONAL MATCH (u)-[m]-(l)
    RETURN u.name as u, type(m), l.name as l
    ORDER BY u, m, l
$$) AS (u gtype, m gtype, l gtype);

SELECT * FROM cypher('cypher_match', $$
    OPTIONAL MATCH (n:opt_match_v)-[r]->(p), (m:opt_match_v)-[s]->(q)
    WHERE id(n) <> id(m)
    RETURN n.name as n, type(r) AS r, p.name as p,
           m.name AS m, type(s) AS s, q.name AS q
    ORDER BY n, p, m, q
$$) AS (n gtype, r gtype, p gtype, m gtype, s gtype, q gtype);

SELECT * FROM cypher('cypher_match', $$
    MATCH (n:opt_match_v), (m:opt_match_v)
    WHERE id(n) <> id(m)
    OPTIONAL MATCH (n)-[r]->(p), (m)-[s]->(q)
    RETURN n.name AS n, type(r) AS r, p.name AS p,
           m.name AS m, type(s) AS s, q.name AS q
    ORDER BY n, p, m, q
 $$) AS (n gtype, r gtype, p gtype, m gtype, s gtype, q gtype);

-- Clean up
SELECT DISTINCT * FROM cypher('cypher_match', $$
    MATCH (u) DETACH DELETE (u)
$$) AS (i gtype);

-- Prepare
SELECT * FROM cypher('cypher_match', $$
    CREATE (u {name: "orphan"})
    CREATE (u1 {name: "F"})-[u2:e1]->(u3 {name: "T"})
    RETURN u1, u2, u3
$$) as (u1 vertex, u2 edge, u3 vertex);

-- Querying NOT EXISTS syntax
SELECT * FROM cypher('cypher_match', $$
     MATCH (f),(t)
     WHERE NOT EXISTS((f)-[]->(t))
     RETURN f.name, t.name
 $$) as (f gtype, t gtype);

-- Querying EXISTS syntax
SELECT * FROM cypher('cypher_match', $$
    MATCH (f),(t)
    WHERE EXISTS((f)-[]->(t))
    RETURN f.name, t.name
 $$) as (f gtype, t gtype);

-- Querying ALL
SELECT * FROM cypher('cypher_match', $$
    MATCH (f),(t)
    WHERE NOT EXISTS((f)-[]->(t)) or true
    RETURN f.name, t.name
$$) as (f gtype, t gtype);

-- Querying ALL
SELECT * FROM cypher('cypher_match', $$
    MATCH (f),(t)
    RETURN f.name, t.name
$$) as (f gtype, t gtype);

--
-- Constraints and WHERE clause together
--
SELECT * FROM cypher('cypher_match', $$
    CREATE ({i: 1, j: 2, k: 3}), ({i: 1, j: 3}), ({i:2, k: 3})
$$) as (a gtype);

SELECT * FROM cypher('cypher_match', $$
    MATCH (n {j: 2})
    WHERE n.i = 1
    RETURN n
$$) as (n vertex);

--
-- Clean up
--
SELECT drop_graph('cypher_match', true);

--
-- End
--
