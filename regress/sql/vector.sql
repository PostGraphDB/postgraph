/*
 * Copyright (C) 2023 PostGraphDB
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
 * For PostgreSQL Database Management System:
 * (formerly known as Postgres, then as Postgres95) 
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */ 

SET extra_float_digits = 0;
LOAD 'postgraph';
SET search_path TO postgraph;
set timezone TO 'GMT';

SELECT create_graph('vector'); 

SELECT * from cypher('list', $$RETURN tovector("[]")$$) as (Labels gtype);
SELECT * from cypher('list', $$RETURN tovector("[1.0, 9, 2, .9]")$$) as (Labels gtype);
SELECT * from cypher('list', $$RETURN tovector("[1.0]")$$) as (Labels gtype);

--
-- l2 distance
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  <-> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]")  <-> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]")  <-> tovector("[2.0]")
$$) as (Labels gtype);

--
-- inner product
--
SELECT * from cypher('vector', $$
    RETURN inner_product(tovector("[1.0, 9, 2, .9]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN inner_product(tovector("[5.0, 2, 4, .324]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN inner_product(tovector("[1.0]"), tovector("[2.0]"))
$$) as (Labels gtype);

--
-- cosine distance
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  <=> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]")  <=> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]")  <=> tovector("[2.0]")
$$) as (Labels gtype);


--
-- l1 distance
--
SELECT * from cypher('vector', $$
    RETURN l1_distance(tovector("[1.0, 9, 2, .9]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN l1_distance(tovector("[5.0, 2, 4, .324]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN l1_distance(tovector("[1.0]"), tovector("[2.0]"))
$$) as (Labels gtype);

--
-- dims
--
SELECT * from cypher('vector', $$
    RETURN dims(tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN dims(tovector("[5.0, 2, 4, .324]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN dims(tovector("[1.0]"))
$$) as (Labels gtype);

--
-- norm
--
SELECT * from cypher('vector', $$
    RETURN norm(tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN norm(tovector("[5.0, 2, 4, .324]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN norm(tovector("[1.0]"))
$$) as (Labels gtype);

--
-- + Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  + tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") + tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") + tovector("[2.0]")
$$) as (Labels gtype);

--
-- - Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  - tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") - tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") - tovector("[2.0]")
$$) as (Labels gtype);

--
-- * Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  * tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") * tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") * tovector("[2.0]")
$$) as (Labels gtype);

--
-- = Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]") = tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") = tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") = tovector("[2.0]")
$$) as (Labels gtype);


--
-- <> Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]") <> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") <> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") <> tovector("[2.0]")
$$) as (Labels gtype);

--
-- > Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]") > tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") > tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") > tovector("[2.0]")
$$) as (Labels gtype);

--
-- >= Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  >= tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") >= tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") >= tovector("[2.0]")
$$) as (Labels gtype);

--
-- < Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]") < tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") < tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") < tovector("[2.0]")
$$) as (Labels gtype);

--
-- <= Operator
--
SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]") <= tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]") <= tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]") <= tovector("[2.0]")
$$) as (Labels gtype);

--
-- l2 squared distance
--
SELECT * from cypher('vector', $$
    RETURN l2_squared_distance(tovector("[1.0, 9, 2, .9]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN l2_squared_distance(tovector("[5.0, 2, 4, .324]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN l2_squared_distance(tovector("[1.0]"), tovector("[2.0]"))
$$) as (Labels gtype);

--
-- cleanup
--
SELECT drop_graph('vector', true);
