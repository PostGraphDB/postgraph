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

SELECT create_graph('cypher_merge');


/*
 * Section 1: MERGE with single vertex
 */
/*
 * test 1: Single MERGE Clause, path doesn't exist
 */
--test query
SELECT * FROM cypher('cypher_merge', $$MERGE (n {i: "Hello Merge"})$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 2: Single MERGE Clause, path exists
 */
--data setup
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: "Hello Merge"}) $$) AS (a gtype);

--test_query
SELECT * FROM cypher('cypher_merge', $$MERGE ({i: "Hello Merge"})$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 3: Prev clause returns no results, no data created
 */
--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE ({i: n.i})$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 4: Prev clause has results, path exists
 */
--test query
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: "Hello Merge"}) $$) AS (a gtype);
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE ({i: n.i})$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 5: Prev clause has results, path does not exist (differnt property name)
 */
--data setup
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: "Hello Merge"}) $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE ({j: n.i})$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 6: MERGE with no prev clause, filters correctly, data created
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: 2}) $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MERGE (n {i: 1}) RETURN n$$) AS (a vertex);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 7: MERGE with no prev clause, filters correctly, no data created
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: 1}) $$) AS (a gtype);
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: 1}) $$) AS (a gtype);
SELECT * FROM cypher('cypher_merge', $$CREATE ({i: 2}) $$) AS (a gtype);
SELECT * FROM cypher('cypher_merge', $$CREATE () $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MERGE (n {i: 1}) RETURN n$$) AS (a vertex);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);


/*
 * Section 2: MERGE with edges
 */

/*
 * test 8: MERGE creates edge
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE () $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE (n)-[:e]->(:v)$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e:e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);


/*
 * test 9: edge already exists
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ()-[:e]->() $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MERGE (n)-[:e]->(:v)$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e:e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 10: edge doesn't exist, using MATCH
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE () $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE (n)-[:e]->(:v)$$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e:e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 11: edge already exists, using MATCH
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ()-[:e]->() $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE (n)-[:e]->(:v)$$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e:e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 12: Partial Path Exists, creates whole path
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ()-[:e]->() $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MERGE ()-[:e]->()-[:e]->()$$) AS (a gtype);

--validate created correctly
--Returns 3. One for the data setup and 2 for the longer path in MERGE
SELECT count(*) FROM cypher('cypher_merge', $$MATCH p=()-[e:e]->() RETURN p$$) AS (p traversal)

-- Returns 1, the path created in MERGE
SELECT count(*) FROM cypher('cypher_merge', $$MATCH p=()-[:e]->()-[]->() RETURN p$$) AS (p traversal);

-- 5 vertices total should have been created
SELECT count(*) FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 13: edge doesn't exists (differnt label), using MATCH
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ()-[:e]->() $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) MERGE (n)-[:e_new]->(:v)$$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 14: edge doesn't exists (different label), without MATCH
 */
-- setup
SELECT * FROM cypher('cypher_merge', $$CREATE ()-[:e]->() $$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MERGE (n)-[:e_new]->(:v)$$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (n)-[e]->(m:v) RETURN n, e, m$$) AS (n vertex, e edge, m vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * Section 3: MERGE with writing clauses
 */

/*
 * test 15:
 */

--test query
SELECT * FROM cypher('cypher_merge', $$CREATE () MERGE (n)$$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 16:
 */

--test query
SELECT * FROM cypher('cypher_merge', $$CREATE (n) WITH n as a MERGE (a)-[:e]->() $$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH p=()-[:e]->() RETURN p$$) AS (p traversal);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);


/*
 * test 17:
 * XXX: Incorrect Output. To FIX
 */

--test query
SELECT * FROM cypher('cypher_merge', $$CREATE (n) MERGE (n)-[:e]->() $$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH p=()-[:e]->() RETURN p$$) AS (p traversal);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);


/*
 * test 18:
 */

--test query
SELECT * FROM cypher('cypher_merge', $$CREATE (n {i : 1}) SET n.i = 2 MERGE ({i: 2}) $$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 19:
 */
--test query
SELECT * FROM cypher('cypher_merge', $$CREATE (n {i : 1}) SET n.i = 2 WITH n as a MERGE ({i: 2}) $$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 20:
 */
--data setup
SELECT * FROM cypher('cypher_merge', $$CREATE (n {i : 1})$$) AS (a gtype);


--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n {i : 1}) SET n.i = 2 WITH n as a MERGE ({i: 2}) $$) AS (a gtype);

--validate created correctly
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 21:
 */
--data setup
SELECT * FROM cypher('cypher_merge', $$CREATE (n {i : 1})$$) AS (a gtype);


--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n {i : 1}) DELETE n  MERGE (n)-[:e]->() $$) AS (a gtype);

--validate, transaction was rolled back because of the error message
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 22:
 * MERGE after MERGE
 */
SELECT * FROM cypher('cypher_merge', $$
    CREATE (n:Person {name : "Rob Reiner", bornIn: "New York"})
$$) AS (a gtype);

SELECT * FROM cypher('cypher_merge', $$
    CREATE (n:Person {name : "Michael Douglas", bornIn: "New Jersey"})
$$) AS (a gtype);

SELECT * FROM cypher('cypher_merge', $$
    CREATE (n:Person {name : "Martin Sheen", bornIn: "Ohio"})
$$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$
    MATCH (person:Person)
    MERGE (city:City {name: person.bornIn})
    MERGE (person)-[r:BORN_IN]->(city)
    RETURN person.name, person.bornIn, city
$$) AS (name gtype, bornIn gtype, city vertex);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 23:
 */
SELECT * FROM cypher('cypher_merge', $$MERGE ()-[:e]-()$$) AS (a gtype);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 24:
 */
SELECT * FROM cypher('cypher_merge', $$MERGE (a) RETURN a$$) AS (a vertex);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH (a) RETURN a$$) AS (a vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 25:
 */
SELECT * FROM cypher('cypher_merge', $$MERGE p=()-[:e]-() RETURN p$$) AS (a traversal);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 26:
 */
SELECT * FROM cypher('cypher_merge', $$MERGE (a)-[:e]-(b) RETURN a$$) AS (a vertex);

--validate
SELECT * FROM cypher('cypher_merge', $$MATCH p=()-[]->() RETURN p$$) AS (a traversal);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * test 27:
 */
SELECT  * FROM cypher('cypher_merge', $$CREATE p=()-[:e]->() RETURN p$$) AS (a traversal);

SELECT * FROM cypher('cypher_merge', $$MERGE p=()-[:e]-() RETURN p$$) AS (a traversal);


--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * Section 4: Error Messages
 */
/*
 * test 28:
 * Only single paths allowed
 */
SELECT * FROM cypher('cypher_merge', $$MERGE (n), (m) RETURN n, m$$) AS (a vertex, b vertex);

/*
 * test 29:
 * Edges cannot reference existing variables
 */
SELECT * FROM cypher('cypher_merge', $$MATCH ()-[e]-() MERGE ()-[e]->()$$) AS (a gtype);

/*
 * test 30:
 * NULL vertex given to MERGE
 */
--data setup
SELECT * FROM cypher('cypher_merge', $$CREATE (n)$$) AS (a gtype);

--test query
SELECT * FROM cypher('cypher_merge', $$MATCH (n) OPTIONAL MATCH (n)-[:e]->(m) MERGE (m)$$) AS (a gtype);

-- validate only 1 vertex exits
SELECT * FROM cypher('cypher_merge', $$MATCH (n) RETURN n$$) AS (a vertex);

--
-- MERGE/SET test
SELECT * FROM cypher('cypher_merge', $$ CREATE (n:node {name: 'Jason'}) RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa' RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);

-- Node doesn't exist, is created, then set
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) DELETE n $$) AS (n gtype);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa' RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);
-- Multiple SETs
SELECT * FROM cypher('cypher_merge', $$ MERGE (n:node {name: 'Lisa'}) SET n.age = 23, n.gender = "Female" RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa', n.age = 23, n.gender = 'Female' RETURN n $$) AS (n vertex);
SELECT * FROM cypher('cypher_merge', $$ MATCH (n:node) RETURN n $$) AS (n vertex);

--clean up
SELECT * FROM cypher('cypher_merge', $$MATCH (n) DETACH DELETE n $$) AS (a gtype);

/*
 * Clean up graph
 */
SELECT drop_graph('cypher_merge', true);

