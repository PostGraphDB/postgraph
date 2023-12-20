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

SELECT create_graph('order_by');

SELECT * FROM cypher('order_by', $$CREATE ()$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: '1', j: 1})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1.0})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: 1::numeric})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: true})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: false})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: {key: 'value'}})$$) AS (result gtype);
SELECT * FROM cypher('order_by', $$CREATE ({i: [1]})$$) AS (result gtype);

SELECT * FROM cypher('order_by', $$
        MATCH (u)
        RETURN u.i
        ORDER BY u.i
$$) AS (i gtype);

SELECT * FROM cypher('order_by', $$
        MATCH (u)
        RETURN u.i
        ORDER BY u.i DESC
$$) AS (i gtype);

SELECT * FROM cypher('order_by', $$
    MATCH (x)
    RETURN x.j ORDER BY x.j NULLS FIRST  
$$) AS (j gtype);


SELECT * FROM cypher('order_by', $$
    MATCH (x)
    RETURN x.j ORDER BY x.j NULLS LAST
$$) AS (j gtype);

SELECT drop_graph('order_by', true);
