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

SELECT create_graph('cypher_with');

SELECT * FROM cypher('cypher_with', $$
WITH true AS b
RETURN b
$$) AS (b bool);

-- Expression item must be aliased.
SELECT * FROM cypher('cypher_with', $$
WITH 1 + 1
RETURN i
$$) AS (i int);

SELECT * FROM cypher('cypher_with', $$
    CREATE ({i: 1}), ({i: 1, j: 2}), ({i: 2})
$$) as (a gtype);

SELECT * FROM cypher('cypher_with', $$
    MATCH (n)
    WITH n as a WHERE n.i = 1
    RETURN a
$$) as (n vertex);

SELECT * FROM cypher('cypher_with', $$
    MATCH (n)
    WITH n as a WHERE n.i = 1 and n.j = 2
    RETURN a
$$) as (n vertex);


SELECT drop_graph('cypher_with', true);
