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

SELECT create_graph('cypher_union');

SELECT * FROM cypher('cypher_union', $$CREATE ()$$) as (a gtype);


SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION MATCH (n) RETURN n$$) as (a vertex);

SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n$$) as (a vertex);

SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION RETURN 1$$) as (a vertex);

SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION RETURN NULL$$) as (a vertex);

SELECT * FROM cypher('cypher_union', $$RETURN [1,2,3] UNION RETURN 1$$) as (a gtype);

/*should return 1 row*/
SELECT * FROM cypher('cypher_union', $$RETURN NULL UNION RETURN NULL$$) AS (result gtype);

/*should return 2 rows*/
SELECT * FROM cypher('cypher_union', $$RETURN NULL UNION ALL RETURN NULL$$) AS (result gtype);

/*
 *multiple unions, precedence
 */
SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION MATCH (n) RETURN n UNION MATCH (n) RETURN n$$) AS (result vertex);

/*should return triple*/
SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n UNION ALL MATCH(n) RETURN n$$) AS (result vertex);

/*should return single*/
SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n UNION MATCH(n) RETURN n$$) AS (result vertex);

/*should return just a pair*/
SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION MATCH (n) RETURN n UNION ALL MATCH(n) RETURN n$$) AS (result vertex);

/*should return 3 null rows*/
SELECT * FROM cypher('cypher_union', $$RETURN NULL UNION ALL RETURN NULL UNION ALL RETURN NULL$$) AS (result gtype);

/*should return 1 null row*/
SELECT * FROM cypher('cypher_union', $$RETURN NULL UNION ALL RETURN NULL UNION RETURN NULL$$) AS (result gtype);

/*should return 2 null rows*/
SELECT * FROM cypher('cypher_union', $$RETURN NULL UNION RETURN NULL UNION ALL RETURN NULL$$) AS (result gtype);

/* scoping */
SELECT * FROM cypher('cypher_union', $$MATCH (n) RETURN n UNION ALL MATCH (m) RETURN n$$) AS (result vertex);

/*
 *UNION and UNION ALL, type casting
 */
SELECT * FROM cypher('cypher_union', $$RETURN 1.0::int UNION RETURN 1::float UNION ALL RETURN 2.0::float$$) AS (result gtype);

SELECT * FROM cypher('cypher_union', $$RETURN 1.0::int UNION RETURN 1.0::float UNION ALL RETURN 1::int$$) AS (result gtype);

SELECT * FROM cypher('cypher_union', $$RETURN 1.0::float UNION RETURN 1::int UNION RETURN 1::float$$) AS (result gtype);


SELECT drop_graph('cypher_union', true);
