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

LOAD 'postgraph';
SET search_path TO postgraph;

CREATE GRAPH set_op;

USE GRAPH set_op;

CREATE ();


MATCH (n) RETURN n UNION MATCH (n) RETURN n;

MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n;

MATCH (n) RETURN n UNION RETURN 1;

MATCH (n) RETURN n UNION RETURN NULL;

RETURN [1,2,3] UNION RETURN 1;

RETURN NULL UNION RETURN NULL;

RETURN NULL UNION ALL RETURN NULL;

MATCH (n) RETURN n UNION MATCH (n) RETURN n UNION MATCH (n) RETURN n;

MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n UNION ALL MATCH(n) RETURN n;

MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n UNION MATCH(n) RETURN n;

MATCH (n) RETURN n UNION MATCH (n) RETURN n UNION ALL MATCH(n) RETURN n;

RETURN NULL UNION ALL RETURN NULL UNION ALL RETURN NULL;

RETURN NULL UNION ALL RETURN NULL UNION RETURN NULL;

RETURN NULL UNION RETURN NULL UNION ALL RETURN NULL;

RETURN 1.0::int UNION RETURN 1::float UNION ALL RETURN 2.0::float;

RETURN 1.0::int UNION RETURN 1.0::float UNION ALL RETURN 1::int;

RETURN 1.0::float UNION RETURN 1::int UNION RETURN 1::float;


-- Intersect

MATCH (n) RETURN n INTERSECT MATCH (m) RETURN m;

MATCH (n) RETURN n INTERSECT ALL MATCH (m) RETURN m;


-- Except
MATCH (n) RETURN n EXCEPT MATCH (m) RETURN m;

MATCH (n) RETURN n EXCEPT ALL MATCH (m) RETURN m;

-- Operator Precedence

RETURN 2.0::int UNION (RETURN 1::float UNION ALL RETURN 1.0::float);

(RETURN 2.0::int UNION RETURN 1::float ) UNION ALL RETURN 1.0::float;


-- Errors
MATCH (n) RETURN n UNION ALL MATCH (m) RETURN n;

DROP GRAPH set_op;
