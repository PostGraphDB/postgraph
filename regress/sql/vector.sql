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

SELECT * from cypher('vector', $$RETURN tovector("[]")$$) as (Labels gtype);
SELECT * from cypher('vector', $$RETURN tovector("[1.0, 9, 2, .9]")$$) as (Labels gtype);
SELECT * from cypher('vector', $$RETURN tovector("[1.0]")$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, NaN]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, Infinity]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, -Infinity]")
$$) as (Labels gtype);


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
-- negative inner product
--
SELECT * from cypher('vector', $$
    RETURN negative_inner_product(tovector("[1.0, 9, 2, .9]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN negative_inner_product(tovector("[5.0, 2, 4, .324]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN negative_inner_product(tovector("[1.0]"), tovector("[2.0]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[1.0, 9, 2, .9]")  <-> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN tovector("[5.0, 2, 4, .324]")  <#> tovector("[1.0, 9, 2, .9]")
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN tovector("[1.0]")  <#> tovector("[2.0]")
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
-- spherical distance
--
SELECT * from cypher('vector', $$
    RETURN spherical_distance(tovector("[1.0, 9, 2, .9]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);

SELECT * from cypher('vector', $$
    RETURN spherical_distance(tovector("[5.0, 2, 4, .324]"), tovector("[1.0, 9, 2, .9]"))
$$) as (Labels gtype);


SELECT * from cypher('vector', $$
    RETURN spherical_distance(tovector("[1.0]"), tovector("[2.0]"))
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


SELECT gtype_build_map('i'::text, tovector('"[0, 0, 0]"'::gtype)); 
SELECT gtype_build_list('i'::text, tovector('"[0, 0, 0]"'::gtype));


SELECT gtype_build_map('i'::text, tovector('"[0, 0, 0]"'::gtype))->'"i"';
--
-- ivfflat
--
SET enable_seqscan = off;

SELECT create_graph('ivfflat');

SELECT * FROM cypher('ivfflat', $$ CREATE ( {i: tovector('[0, 0, 0]')}) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ CREATE ( {i: tovector('[1, 2, 3]')}) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ CREATE ( {i: tovector('[1, 1, 1]')}) $$) as (i gtype);

CREATE INDEX ON ivfflat."_ag_label_vertex" USING ivfflat (properties vector_l2_ops) WITH (lists = 1);

CREATE INDEX ON ivfflat."_ag_label_vertex" USING ivfflat ((properties->'"i"'::gtype) gtype_l2_ops);-- WITH (lists = 1);

SELECT * FROM cypher('ivfflat', $$ MATCH (n) RETURN n ORDER BY n.i <-> toVector('[3,3,3]') $$) as (i vertex);
SELECT * FROM cypher('ivfflat', $$ MATCH (n) RETURN count(*) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ MATCH (n) RETURN n $$) as (i vertex);

SELECT * FROM cypher('ivfflat', $$ CREATE ( {j: tovector('[1, 1, 1]')}) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ CREATE ( {j: tovector('[1]')}) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ CREATE ( {j: tovector('[]')}) $$) as (i gtype);
SELECT * FROM cypher('ivfflat', $$ MATCH (n) RETURN n.j $$) as (i gtype);

--
-- Index Errors
--
SELECT * FROM cypher('ivfflat', $$ CREATE ( {i: tovector('[1, 1 ]')}) $$) as (i gtype);

SELECT * FROM cypher('ivfflat', $$ CREATE ( {i: 'Hello World'}) $$) as (i gtype);


--
-- cleanup
--
SELECT drop_graph('vector', true);
SELECT drop_graph('ivfflat', true);
