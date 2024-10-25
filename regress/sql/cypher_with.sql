/*
 * Copyright (C) 2023-2024 PostGraphDB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */ 
 
LOAD 'postgraph';
SET search_path TO postgraph;

CREATE GRAPH cypher_with;
USE GRAPH cypher_with;

WITH true AS b RETURN b;

CREATE ({i: 1}), ({i: 1, j: 2}), ({i: 2});

MATCH (n) WITH n as a WHERE n.i = 1 RETURN a;

MATCH (n) WITH n as a WHERE n.i = 1 and n.j = 2 RETURN a;

--Error
WITH 1 + 1 RETURN i;


DROP GRAPH cypher_with CASCADE;
