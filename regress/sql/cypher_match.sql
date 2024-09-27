
/*
 * Copyright (C) 2023-2024 PostGraphDB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */ 

LOAD 'postgraph';

CREATE GRAPH cypher_match;

USE GRAPH cypher_match;

CREATE (:v);
CREATE (:v {i: 0});
CREATE (:v {i: 1});

MATCH (n:v) RETURN n;
MATCH (n:v) RETURN n.i;


MATCH (n:v) WHERE n.i > 0
RETURN n.i;
--Directed Paths
CREATE (:v1 {id:'initial'})-[:e1]->(:v1 {id:'middle'})-[:e1]->(:v1 {id:'end'});

--Undirected Path Tests
MATCH p=(:v1)-[:e1]-(:v1)-[:e1]-(:v1) RETURN p;

MATCH p=(a:v1)-[]-()-[]-() RETURN a;

MATCH ()-[]-()-[]-(a:v1) RETURN a;

MATCH ()-[]-(a:v1)-[]-() RETURN a;

MATCH ()-[b:e1]-()-[]-() RETURN b;

MATCH (a:v1)-[]->(), ()-[]->(a) RETURN a;

MATCH p=()-[e]-() RETURN e;

-- Right Path Test
MATCH (a:v1)-[:e1]->(b:v1)-[:e1]->(c:v1) RETURN a, b, c;

MATCH p=(a:v1)-[]-()-[]->() RETURN a;

MATCH p=(a:v1)-[]->()-[]-() RETURN a;

MATCH ()-[]-()-[]->(a:v1) RETURN a;

MATCH ()-[]-(a:v1)-[]->() RETURN a;

MATCH ()-[b:e1]-()-[]->() RETURN b;

--Left Path Test
MATCH (a:v1)<-[:e1]-(b:v1)<-[:e1]-(c:v1) RETURN a, b, c;

MATCH p=(a:v1)<-[]-()-[]-() RETURN a;

MATCH p=(a:v1)-[]-()<-[]-() RETURN a;

MATCH ()<-[]-()-[]-(a:v1) RETURN a;

MATCH ()<-[]-(a:v1)-[]-() RETURN a;

MATCH ()<-[b:e1]-()-[]-() RETURN b;

--Divergent Path Tests
CREATE (:v2 {id:'initial'})<-[:e2]-(:v2 {id:'middle'})-[:e2]->(:v2 {id:'end'});

MATCH ()<-[]-(n:v2)-[]->() MATCH p=()-[]->(n) RETURN p;

MATCH ()<-[]-(n:v2)-[]->() MATCH p=(n)-[]->() RETURN p;

MATCH ()-[]-(n:v2) RETURN n;

--Convergent Path Tests
CREATE (:v3 {id:'initial'})-[:e3]->(:v3 {id:'middle'})<-[:e3]-(:v3 {id:'end'});

MATCH ()-[b:e1]->() RETURN b;

MATCH ()-[]->(n:v1)<-[]-() MATCH p=(n)<-[]-() RETURN p;

MATCH ()-[]->(n:v1)<-[]-() MATCH p=()-[]->(n) RETURN p;

MATCH ()-[]->(n:v1)<-[]-() MATCH p=(n)-[]->() RETURN p;

MATCH con_path=(a)-[]->()<-[]-() WHERE a.id = 'initial' RETURN con_path;

MATCH div_path=(b)<-[]-()-[]->() WHERE b.id = 'initial' RETURN div_path;

--Patterns
MATCH (a:v1), p=(a)-[]-()-[]-() WHERE a.id = 'initial' RETURN p;

MATCH con_path=(a)-[]->()<-[]-(), div_path=(b)<-[]-()-[]->() WHERE a.id = 'initial' AND b.id = 'initial' RETURN con_path, div_path;

MATCH (a:v), p=()-[]->()-[]->() RETURN a.i, p;

--Multiple Match Clauses
MATCH (a:v1) WHERE a.id = 'initial' MATCH p=(a)-[]-()-[]-() RETURN p;

MATCH (a:v) MATCH p=()-[]->()-[]->() RETURN a.i, p;

MATCH (a:v) MATCH (b:v1)-[]-(c) RETURN a.i, b.id, c.id;

--
-- Property constraints
--
CREATE ({string_key: "test", int_key: 1, float_key: 3.14, map_key: {key: "value"}, list_key: [1, 2, 3]});

CREATE ({lst: [1, NULL, 3.14, "string", {key: "value"}, []]});

MATCH (n  {string_key: NULL}) RETURN n;

MATCH (n  {string_key: "wrong value"}) RETURN n ;

MATCH (n {string_key: "test", int_key: 1, float_key: 3.14, map_key: {key: "value"}, list_key: [1, 2, 3]}) RETURN n;

MATCH (n {string_key: "test"}) RETURN n;

MATCH (n {lst: [1, NULL, 3.14, "string", {key: "value"}, []]}) RETURN n;

MATCH (n {lst: [1, NULL, 3.14, "string", {key: "value"}, [], "extra value"]})  RETURN n;

-- Path of one vertex.
MATCH p=() RETURN p;

--
-- MATCH with WHERE EXISTS(pattern)
--
MATCH (u)-[e]->(v) RETURN u, e, v ;

MATCH (u)-[e]->(v) WHERE EXISTS ((u)-[e]->(v)) RETURN u, e, v;

-- Property Constraint in EXISTS
MATCH (u) WHERE EXISTS((u)-[]->({id: "middle"})) RETURN u;

MATCH (u) WHERE EXISTS((u)-[]->({id: "not a valid id"})) RETURN u;

MATCH (u) WHERE EXISTS((u)-[]->({id: NULL})) RETURN u;

-- Exists checks for a loop. There shouldn't be any.
MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) RETURN u, e, v;

-- Querying NOT EXISTS syntax
-- Create a loop

CREATE (u:loop {id:'initial'})-[:self]->(u);
-- dump paths

MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(v)) RETURN u, e, v;

-- Exists checks for a loop. There should be one.
MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) RETURN u, e, v;

-- Exists checks for a loop. There should be one.
MATCH (u)-[e]->(v) WHERE EXISTS((v)-[e]->(v)) RETURN u, e, v;

-- Exists checks for a loop. There should be none because of edge uniqueness
-- requirement.

MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)-[e]->(u)) RETURN u, e, v;

MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(u)) AND EXISTS((v)-[e]->(v)) RETURN u, e, v;

MATCH (u)-[e]->(v) WHERE EXISTS((u)-[e]->(x)) RETURN u, e, v;

MATCH (u) WHERE EXISTS(MATCH (u)-[]->({id: "middle"}) RETURN 1) RETURN u;

MATCH (u) WHERE EXISTS(MATCH (u)-[]->({id: "middle"}) RETURN u) RETURN u;

MATCH (u) WHERE u.id = ANY (MATCH (v) RETURN v.id) RETURN u;

MATCH (u) WHERE u.id = ANY (MATCH (v) RETURN NULL) RETURN u;

MATCH (u) WHERE u.id = SOME (MATCH (v) RETURN v.id) RETURN u;

MATCH (u) WHERE u.id = ALL (MATCH (v) RETURN v.id) RETURN u;

MATCH (u)
WHERE EXISTS( MATCH (u)-[]->(v) WHERE v.id = "middle" RETURN 1)
RETURN u;

MATCH (u) WHERE EXISTS(MATCH (u)-[]->({id: "gsjka"}) RETURN 1) RETURN u;

MATCH (u) WHERE EXISTS(MATCH (v {id: 'middle'}) MATCH (u)-[]->(v) RETURN 1) RETURN u;

MATCH (u) WHERE EXISTS(MATCH (v {id: 'middle'}) MATCH (u)-[]->(v) RETURN 1) RETURN u;

MATCH (u) WHERE EXISTS(MATCH (u)-[]->(v {id: 'middle'}) RETURN 1) RETURN u;

MATCH (u) RETURN case WHEN EXISTS(MATCH (u)-[]->({id: "gsjka"}) RETURN 1) THEN 1 ELSE 2 END;


MATCH (u) RETURN case WHEN EXISTS(MATCH (u)-[]->() RETURN 1) THEN 1 ELSE 2 END;

 
MATCH (u) RETURN case WHEN EXISTS(MATCH (u)-[]->() RETURN 1) THEN 1 ELSE 2 END;


MATCH (u) RETURN case WHEN EXISTS((u)-[]->()) THEN 1 ELSE 2 END;


/*
EXPLAIN
SELECT *
FROM cypher_match._ag_label_vertex as u
WHERE EXISTS (
SELECT * FROM cypher_match._ag_label_edge as e 
JOIN cypher_match._ag_label_vertex as v ON v.id = e.end_id
WHERE u.id = e.start_id
);
*/

--
--Distinct
--
MATCH (u) RETURN DISTINCT props(u);

CREATE (u:duplicate)-[:dup_edge {id:1 }]->(:other_v);

MATCH (u:duplicate) CREATE (u)-[:dup_edge {id:2 }]->(:other_v);

MATCH (u:duplicate)-[]-(:other_v) RETURN DISTINCT u;

MATCH p=(:duplicate)-[]-(:other_v) RETURN DISTINCT p;

--
-- Limit & Skip
--
MATCH (u) RETURN u;

MATCH (u) RETURN u LIMIT 3;

MATCH (u) RETURN u SKIP 7;

MATCH (u) RETURN u SKIP 7 LIMIT 3;

--
-- Optional Match
--
CREATE (:opt_match_v {name: 'someone'})-[:opt_match_e]->(:opt_match_v {name: 'somebody'}),
        (:opt_match_v {name: 'anybody'})-[:opt_match_e]->(:opt_match_v {name: 'nobody'});


MATCH (u:opt_match_v) OPTIONAL MATCH (u)-[m]-(l) RETURN u.name as u, type(m), l.name as l ORDER BY u, m, l;

OPTIONAL MATCH (n:opt_match_v)-[r]->(p), (m:opt_match_v)-[s]->(q) WHERE id(n) <> id(m) RETURN n.name as n, type(r); m.name AS m, type(s); ORDER BY n, p, m, q;

MATCH (n:opt_match_v), (m:opt_match_v)
WHERE id(n) <> id(m)
OPTIONAL MATCH (n)-[r]->(p), (m)-[s]->(q)
RETURN n.name AS n, type(r),
           m.name AS m, type(s)
ORDER BY n, p, m, q;

CREATE (u {name: "orphan"}) CREATE (u1 {name: "F"})-[u2:e1]->(u3 {name: "T"}) RETURN u1, u2, u3;

-- Querying NOT EXISTS syntax
MATCH (f),(t) WHERE NOT EXISTS((f)-[]->(t)) RETURN f.name, t.name;

-- Querying EXISTS syntax
MATCH (f),(t) WHERE EXISTS((f)-[]->(t)) RETURN f.name, t.name;


--
-- Constraints and WHERE clause together
--
CREATE ({i: 1, j: 2, k: 3}), ({i: 1, j: 3}), ({i:2, k: 3});

MATCH (n {j: 2}) WHERE n.i = 1 RETURN n;

--
-- Prepared Statement Property Constraint
--
-- TODO

--
-- Errors
--
-- need a following RETURN clause
MATCH (n:v);

--Invalid Variables
MATCH (a)-[]-()-[]-(a:v1) RETURN a;

MATCH (a:v1)-[]-()-[a]-() RETURN a;

MATCH (a:v1)-[]-()-[]-(a {id:'will_fail'}) RETURN a;

--Incorrect Labels
MATCH (n)-[:v]-() RETURN n;

MATCH (n)-[:emissing]-() RETURN n;

MATCH (n:e1)-[]-() RETURN n;

MATCH (n:vmissing)-[]-() RETURN n;

--
-- Clean up
--
DROP GRAPH cypher_match CASCADE;

--
-- End
--
