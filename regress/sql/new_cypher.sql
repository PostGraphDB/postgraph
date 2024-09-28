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

CREATE EXTENSION postgraph;
CREATE EXTENSION hstore CASCADE SCHEMA public;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS pg_trgm VERSION '1.3';

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

CREATE TABLE tst (i text);
CREATE TABLE tst3 () INHERITS (tst);
CREATE TEMPORARY TABLE tst2 (i text);

CREATE TABLE tst4 (i text) PARTITION BY LIST (i);
CREATE TABLE tst5 (i text) PARTITION BY LIST (i COLLATE de_DE);
CREATE TABLE tst6 (i text) PARTITION BY LIST (i COLLATE de_DE text_ops);
CREATE TABLE tst7 (i text) USING heap;
CREATE TABLE tst8 (i text) WITH (fillfactor=70);

SELECT;
SELECT ALL;

SELECT *;
SELECT * FROM tst;
SELECT tst.i FROM tst;

SELECT a.i FROM tst AS a;
SELECT a.i FROM tst a;

SELECT a.i as j FROM tst a;


SELECT a.j FROM tst AS a(j);

SELECT i FROM tst WHERE i = i;
SELECT i FROM tst WHERE i > i;
--SELECT a.j FROM tst a(j);
SELECT a.* FROM tst as a;

SELECT a.* FROM tst as a ORDER BY a.i DESC;

SELECT a.i as j FROM tst a GROUP BY a.i;

SELECT a.i as j FROM tst a GROUP BY a.i HAVING a.i = a.i;

SELECT sum(salary) OVER w, avg(salary) OVER w
  FROM empsalary
  WINDOW w AS (PARTITION BY i ORDER BY i DESC);

SELECT a.i FROM tst a
UNION
SELECT a.i FROM tst a;

SELECT a.i FROM tst a
EXCEPT
SELECT a.i FROM tst a;

SELECT a.i FROM tst a
INTERSECT
SELECT a.i FROM tst a;

DROP GRAPH new_cypher CASCADE;
DROP GRAPH new_cypher_2 CASCADE;