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

SELECT create_graph('regex');

SELECT * FROM cypher('regex', $$ CREATE (n:Person {name: 'John'}) $$) AS r(result gtype);
SELECT * FROM cypher('regex', $$ CREATE (n:Person {name: 'Jeff'}) $$) AS r(result gtype);
SELECT * FROM cypher('regex', $$ CREATE (n:Person {name: 'Joan'}) $$) AS r(result gtype);

--
-- =~ case sensitive regular expression 
--
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name =~ 'JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name =~ '(?i)JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name =~ 'Jo.n' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name =~ 'J.*' RETURN n
$$) AS r(result vertex);


SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name ~ 'JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name ~ '(?i)JoHn' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name ~ 'Jo.n' RETURN n
$$) AS r(result vertex);
SELECT * FROM cypher('regex', $$
    MATCH (n:Person) WHERE n.name ~ 'J.*' RETURN n
$$) AS r(result vertex);

