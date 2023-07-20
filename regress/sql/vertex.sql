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

--
-- Vertex
--
--Basic Vertex Creation
SELECT create_graph('vertex');

SELECT create_vlabel('vertex', 'vlabel');
SELECT build_vertex(_graphid(3, 1), graphid, gtype_build_map()) FROM ag_graph;


SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) FROM ag_graph;

--Null properties
SELECT build_vertex(_graphid(1, 1), $$label_name$$, NULL);
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_list()) FROM ag_graph;

SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) =
       build_vertex(_graphid(1, 1), graphid, gtype_build_map()) FROM ag_graph;

SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) =
       build_vertex(_graphid(1, 2), graphid, gtype_build_map()) FROM ag_graph;

SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) <>
       build_vertex(_graphid(1, 1), graphid, gtype_build_map()) FROM ag_graph;

SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) <>
       build_vertex(_graphid(1, 2), graphid, gtype_build_map()) FROM ag_graph;
--
-- id function
--
SELECT id(build_vertex(_graphid(1, 1), graphid, gtype_build_map())) FROM ag_graph;
SELECT id(NULL) FROM ag_graph;

--
-- label function
--
SELECT label(build_vertex(_graphid(1, 1), graphid, gtype_build_map())) FROM ag_graph;
SELECT label(NULL) FROM ag_graph;

--
-- properties function
--
SELECT properties(build_vertex(_graphid(1, 1), graphid, gtype_build_map())) FROM ag_graph;
SELECT properties(build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2))) FROM ag_graph;

--
-- -> operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map())->'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2))->'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2))->'idd' FROM ag_graph;

--
-- ->> operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map())->>'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2))->>'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2))->>'idd' FROM ag_graph;

--
-- @> operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) @> gtype_build_map() FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) @> gtype_build_map() FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) @> gtype_build_map('id', 2) FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) @> gtype_build_map('id', 1) FROM ag_graph;

--
-- <@ operator
--
SELECT gtype_build_map() <@ build_vertex(_graphid(1, 1), graphid, gtype_build_map()) FROM ag_graph;
SELECT gtype_build_map() <@ build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) FROM ag_graph;
SELECT gtype_build_map('id', 2) <@ build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) FROM ag_graph;
SELECT gtype_build_map('id', 1) <@ build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) FROM ag_graph;

--
-- ? operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) ? 'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ? 'id' FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ? 'idd' FROM ag_graph;

--
-- ?| operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) ?| array['id'] FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ?| array['id'] FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ?| array['idd'] FROM ag_graph;

--
-- ?& operator
--
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map()) ?& array['id'] FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ?& array['id'] FROM ag_graph;
SELECT build_vertex(_graphid(1, 1), graphid, gtype_build_map('id', 2)) ?& array['idd'] FROM ag_graph;

SELECT drop_graph('vertex', true);

