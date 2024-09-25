/*
 * Copyright (C) 2024 PostGraphDB
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
 */ 
-- Regression tests don't preload extensions, gotta load it first
LOAD 'postgraph';

-- Basic Graph creation
CREATE GRAPH new_cypher;

-- Assign Graph to use
USE GRAPH new_cypher;

-- Reuse Name, should throw error
CREATE GRAPH new_cypher;

-- Graph Does not exist, should throw error
USE GRAPH new_cypher;

CREATE GRAPH new_cypher_2;
USE GRAPH new_cypher_2;
USE GRAPH new_cypher;

-- Old Grammar is at least partially plugged in.
RETURN 1 as a;

CREATE (n);

CREATE (n) RETURN n;

--CREATE (n) RETURN *;

MATCH (n) RETURN n;

MATCH (n) RETURN *;

DROP GRAPH new_cypher CASCADE;
DROP GRAPH new_cypher_2 CASCADE;