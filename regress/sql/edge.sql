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
SELECT build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map());
SELECT build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2));

--Null properties
SELECT build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label_name$$, NULL);
SELECT build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_list());


SELECT build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()) =
       build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map());

SELECT build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()) =
       build_edge('4'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map());

SELECT build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()) <>
       build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map());

SELECT build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()) <>
       build_edge('4'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map());


-- id
SELECT id(build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()));
SELECT id(build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2)));

-- start_id
SELECT start_id(build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()));
SELECT start_id(build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2)));

-- end_id
SELECT end_id(build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()));
SELECT end_id(build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2)));

-- label
SELECT label(build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()));
SELECT label(build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2)));

--properties
SELECT properties(build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$label$$, gtype_build_map()));
SELECT properties(build_edge('1'::graphid,  '2'::graphid, '3'::graphid, $$label$$, gtype_build_map('id', 2)));
