/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

LOAD 'postgraph';
SET search_path TO postgraph;

CREATE GRAPH cypher_merge;
USE GRAPH cypher_merge;

/*
 * Section 1: MERGE with single vertex
 */
/*
 * test 1: Single MERGE Clause, path doesn't exist
 */
--test query
MERGE (n {i: 'Hello Merge'});

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 2: Single MERGE Clause, path exists
 */
--data setup
CREATE ({i: 'Hello Merge'});

--test_query
MERGE ({i: 'Hello Merge'});

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 3: Prev clause returns no results, no data created
 */
--test query
MATCH (n) MERGE ({i: n.i});

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 4: Prev clause has results, path exists
 */
--test query
CREATE ({i: 'Hello Merge'});
MATCH (n) MERGE ({i: n.i});

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 5: Prev clause has results, path does not exist (differnt property name)
 */
--data setup
CREATE ({i: 'Hello Merge'});

--test query
MATCH (n) MERGE ({j: n.i});

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 6: MERGE with no prev clause, filters correctly, data created
 */
-- setup
CREATE ({i: 2});

--test query
MERGE (n {i: 1}) RETURN n;

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 7: MERGE with no prev clause, filters correctly, no data created
 */
-- setup
CREATE ({i: 1});
CREATE ({i: 1});
CREATE ({i: 2});
CREATE ();

--test query
MERGE (n {i: 1}) RETURN n;

--validate
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;


/*
 * Section 2: MERGE with edges
 */

/*
 * test 8: MERGE creates edge
 */
-- setup
CREATE ();

--test query
MATCH (n) MERGE (n)-[:e]->(:v);

--validate
MATCH (n)-[e:e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;


/*
 * test 9: edge already exists
 */
-- setup
CREATE ()-[:e]->();

--test query
MERGE (n)-[:e]->(:v);

--validate
MATCH (n)-[e:e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 10: edge doesn't exist, using MATCH
 */
-- setup
CREATE ();

--test query
MATCH (n) MERGE (n)-[:e]->(:v);

--validate created correctly
MATCH (n)-[e:e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 11: edge already exists, using MATCH
 */
-- setup
CREATE ()-[:e]->();

--test query
MATCH (n) MERGE (n)-[:e]->(:v);

--validate created correctly
MATCH (n)-[e:e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 12: Partial Path Exists, creates whole path
 */
-- setup
CREATE ()-[:e]->();

--test query
MERGE ()-[:e]->()-[:e]->();

--validate created correctly
--Returns 3. One for the data setup and 2 for the longer path in MERGE
SELECT count(*) FROM cypher('cypher_merge',;

-- Returns 1, the path created in MERGE
SELECT count(*) FROM cypher('cypher_merge',;

-- 5 vertices total should have been created
SELECT count(*) FROM cypher('cypher_merge',;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 13: edge doesn't exists (differnt label), using MATCH
 */
-- setup
CREATE ()-[:e]->();

--test query
MATCH (n) MERGE (n)-[:e_new]->(:v);

--validate created correctly
MATCH (n)-[e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 14: edge doesn't exists (different label), without MATCH
 */
-- setup
CREATE ()-[:e]->();

--test query
MERGE (n)-[:e_new]->(:v);

--validate created correctly
MATCH (n)-[e]->(m:v) RETURN n, e, m;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * Section 3: MERGE with writing clauses
 */

/*
 * test 15:
 */

--test query
CREATE () MERGE (n);

--validate created correctly
MATCH (n) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 16:
 */

--test query
CREATE (n) WITH n as a MERGE (a)-[:e]->();

--validate created correctly
MATCH p=()-[:e]->() RETURN p;

--clean up
MATCH (n) DETACH DELETE n;


/*
 * test 17:
 * TODO
 */

--test query
CREATE (n) MERGE (n)-[:e]->();

--validate created correctly
MATCH p=()-[:e]->() RETURN p;

--clean up
MATCH (n) DETACH DELETE n;


/*
 * test 18:
 */

--test query
CREATE (n {i : 1}) SET n.i = 2 MERGE ({i: 2});

--validate created correctly
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 19:
 */
--test query
CREATE (n {i : 1}) SET n.i = 2 WITH n as a MERGE ({i: 2});

--validate created correctly
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 20:
 */
--data setup
CREATE (n {i : 1});


--test query
MATCH (n {i : 1}) SET n.i = 2 WITH n as a MERGE ({i: 2});

--validate created correctly
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 21:
 */
--data setup
CREATE (n {i : 1});


--test query
MATCH (n {i : 1}) DELETE n  MERGE (n)-[:e]->();

--validate, transaction was rolled back because of the error message
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 22:
 * MERGE after MERGE
 */

CREATE (n:Person {name : 'Rob Reiner', bornIn: 'New York'});
CREATE (n:Person {name : 'Michael Douglas', bornIn: 'New Jersey'});
CREATE (n:Person {name : 'Martin Sheen', bornIn: 'Ohio'});

--test query
MATCH (person:Person)
MERGE (city:City {name: person.bornIn})
MERGE (person)-[r:BORN_IN]->(city)
RETURN person.name, person.bornIn, city;

--validate
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 23:
 */
MERGE ()-[:e]-();

--validate
MATCH p=()-[]->() RETURN p;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 24:
 */
MERGE (a) RETURN a;

--validate
MATCH (a) RETURN a;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 25:
 */
MERGE p=()-[:e]-() RETURN p;

--validate
MATCH p=()-[]->() RETURN p;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 26:
 */
MERGE (a)-[:e]-(b) RETURN a;

--validate
MATCH p=()-[]->() RETURN p;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * test 27:
 */
SELECT  * FROM cypher('cypher_merge',;

MERGE p=()-[:e]-() RETURN p;


--clean up
MATCH (n) DETACH DELETE n;

/*
 * Section 4: Error Messages
 */
/*
 * test 28:
 * Only single paths allowed
 */
MERGE (n), (m) RETURN n, m;

/*
 * test 29:
 * Edges cannot reference existing variables
 */
MATCH ()-[e]-() MERGE ()-[e]->();

/*
 * test 30:
 * NULL vertex given to MERGE
 */
--data setup
CREATE (n);

--test query
MATCH (n) OPTIONAL MATCH (n)-[:e]->(m) MERGE (m);

-- validate only 1 vertex exits
MATCH (n) RETURN n;

--
-- MERGE/SET test
 CREATE (n:node {name: 'Jason'}) RETURN n;
 MATCH (n:node) RETURN n;
 MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa' RETURN n;
 MATCH (n:node) RETURN n;

-- Node doesn't exist, is created, then set
 MATCH (n:node) DELETE n;
 MATCH (n:node) RETURN n;
 MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa' RETURN n;
 MATCH (n:node) RETURN n;
-- Multiple SETs
 MERGE (n:node {name: 'Lisa'}) SET n.age = 23, n.gender = 'Female' RETURN n;
 MATCH (n:node) RETURN n;
 MERGE (n:node {name: 'Jason'}) SET n.name = 'Lisa', n.age = 23, n.gender = 'Female' RETURN n;
 MATCH (n:node) RETURN n;

--clean up
MATCH (n) DETACH DELETE n;

/*
 * Clean up graph
 */
DROP GRAPH cypher_merge CASCADE;

