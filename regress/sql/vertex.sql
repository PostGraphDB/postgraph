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
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map());
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2));

--Null properties
SELECT build_vertex('1'::graphid, $$label_name$$, NULL);
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_list());

SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) =
       build_vertex('1'::graphid, $$label$$, gtype_build_map());

SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) =
       build_vertex('2'::graphid, $$label$$, gtype_build_map());

SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) <>
       build_vertex('1'::graphid, $$label$$, gtype_build_map());

SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) <>
       build_vertex('2'::graphid, $$label$$, gtype_build_map());
--
-- id function
--
SELECT id(build_vertex('1'::graphid, $$label$$, gtype_build_map()));
SELECT id(NULL);

--
-- label function
--
SELECT label(build_vertex('1'::graphid, $$label$$, gtype_build_map()));
SELECT label(NULL);

--
-- properties function
--
SELECT properties(build_vertex('1'::graphid, $$label$$, gtype_build_map()));
SELECT properties(build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)));

--
-- -> operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map())->'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2))->'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2))->'idd';

--
-- ->> operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map())->>'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2))->>'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2))->>'idd';

--
-- @> operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) @> gtype_build_map();
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) @> gtype_build_map();
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) @> gtype_build_map('id', 2);
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) @> gtype_build_map('id', 1);

--
-- <@ operator
--
SELECT gtype_build_map() <@ build_vertex('1'::graphid, $$label$$, gtype_build_map());
SELECT gtype_build_map() <@ build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2));
SELECT gtype_build_map('id', 2) <@ build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2));
SELECT gtype_build_map('id', 1) <@ build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2));

--
-- ? operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) ? 'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ? 'id';
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ? 'idd';

--
-- ?| operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) ?| array['id'];
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ?| array['id'];
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ?| array['idd'];

--
-- ?& operator
--
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map()) ?& array['id'];
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ?& array['id'];
SELECT build_vertex('1'::graphid, $$label$$, gtype_build_map('id', 2)) ?& array['idd'];


