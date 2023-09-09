/*
 * PostGraph
 * Copyright (C) 2023 by PostGraph
 *
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

SELECT create_graph('edge');
SELECT create_elabel('edge', 'elabel');
--
-- Vertex
--
--Basic Vertex Creation
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) FROM ag_graph;
SELECT build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2)) FROM ag_graph;

--Null properties
SELECT build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid,  graphid, NULL) FROM ag_graph;
SELECT build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_list()) FROM ag_graph;


SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()) =
       build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map()) FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()) =
       build_edge(_graphid(3, 4),  '2'::graphid, '3'::graphid, graphid, gtype_build_map()) FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()) <>
       build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map()) FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()) <>
       build_edge(_graphid(3, 4),  '2'::graphid, '3'::graphid, graphid, gtype_build_map()) FROM ag_graph;

--
-- Equality Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) =
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) =
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) =
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

--
-- Not Equals Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <>
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <>
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <>
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

--
-- Greater than Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

--
-- Greater Than Or Equal To Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >=
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >=
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) >=
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

--
-- Less than Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

--
-- Less Than Or Equal To Operator
--
SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <=
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <=
       build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

SELECT build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map()) <=
       build_edge(_graphid(3, 2), '2'::graphid, '3'::graphid, graphid, gtype_build_map())
FROM ag_graph;

-- id
SELECT id(build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())) FROM ag_graph;
SELECT id(build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2))) FROM ag_graph;

-- start_id
SELECT start_id(build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())) FROM ag_graph;
SELECT start_id(build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2))) FROM ag_graph;

-- end_id
SELECT end_id(build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())) FROM ag_graph;
SELECT end_id(build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2))) FROM ag_graph;

-- label
SELECT label(build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())) FROM ag_graph;
SELECT label(build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2))) FROM ag_graph;

--properties
SELECT properties(build_edge(_graphid(3, 1), '2'::graphid, '3'::graphid, graphid, gtype_build_map())) FROM ag_graph;
SELECT properties(build_edge(_graphid(3, 1),  '2'::graphid, '3'::graphid, graphid, gtype_build_map('id', 2))) FROM ag_graph;


SELECT drop_graph('edge', true);
