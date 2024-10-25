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

CREATE GRAPH vector;
USE GRAPH vector;

RETURN tovector('[]');
RETURN tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]');

RETURN '[]'::vector;
RETURN '[1.0, 9, 2, .9]'::vector;
RETURN '[1.0]'::vector;

RETURN tovector('[1.0, NaN]');
RETURN tovector('[1.0, Infinity]');
RETURN tovector('[1.0, -Infinity]');


--
-- l2 distance
--
RETURN tovector('[1.0, 9, 2, .9]')  <-> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]')  <-> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]')  <-> tovector('[2.0]');

--
-- inner product
--
RETURN inner_product(tovector('[1.0, 9, 2, .9]'), tovector('[1.0, 9, 2, .9]'));
RETURN inner_product(tovector('[5.0, 2, 4, .324]'), tovector('[1.0, 9, 2, .9]'));
RETURN inner_product(tovector('[1.0]'), tovector('[2.0]'));

--
-- negative inner product
--
RETURN negative_inner_product(tovector('[1.0, 9, 2, .9]'), tovector('[1.0, 9, 2, .9]'));
RETURN negative_inner_product(tovector('[5.0, 2, 4, .324]'), tovector('[1.0, 9, 2, .9]'));
RETURN negative_inner_product(tovector('[1.0]'), tovector('[2.0]'));


RETURN tovector('[1.0, 9, 2, .9]')  <-> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]')  <#> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]')  <#> tovector('[2.0]');

--
-- cosine distance
--
RETURN tovector('[1.0, 9, 2, .9]')  <=> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]')  <=> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]')  <=> tovector('[2.0]');


--
-- l1 distance
--
RETURN l1_distance(tovector('[1.0, 9, 2, .9]'), tovector('[1.0, 9, 2, .9]'));
RETURN l1_distance(tovector('[5.0, 2, 4, .324]'), tovector('[1.0, 9, 2, .9]'));
RETURN l1_distance(tovector('[1.0]'), tovector('[2.0]'));

--
-- spherical distance
--
RETURN spherical_distance(tovector('[1.0, 9, 2, .9]'), tovector('[1.0, 9, 2, .9]'));
RETURN spherical_distance(tovector('[5.0, 2, 4, .324]'), tovector('[1.0, 9, 2, .9]'));
RETURN spherical_distance(tovector('[1.0]'), tovector('[2.0]'));


--
-- dims
--
RETURN dims(tovector('[1.0, 9, 2, .9]'));
RETURN dims(tovector('[5.0, 2, 4, .324]'));
RETURN dims(tovector('[1.0]'));

--
-- norm
--

RETURN norm(tovector('[1.0, 9, 2, .9]'));
RETURN norm(tovector('[5.0, 2, 4, .324]'));
RETURN norm(tovector('[1.0]'));

--
-- + Operator
--

RETURN tovector('[1.0, 9, 2, .9]')  + tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') + tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') + tovector('[2.0]');

--
-- - Operator
--
RETURN tovector('[1.0, 9, 2, .9]')  - tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') - tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') - tovector('[2.0]');

--
-- * Operator
--

RETURN tovector('[1.0, 9, 2, .9]')  * tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') * tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') * tovector('[2.0]');

--
-- = Operator
--
RETURN tovector('[1.0, 9, 2, .9]') = tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') = tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') = tovector('[2.0]');


--
-- <> Operator
--
RETURN tovector('[1.0, 9, 2, .9]') <> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') <> tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') <> tovector('[2.0]');

--
-- > Operator
--
RETURN tovector('[1.0, 9, 2, .9]') > tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') > tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') > tovector('[2.0]');

--
-- >= Operator
--

RETURN tovector('[1.0, 9, 2, .9]')  >= tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') >= tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') >= tovector('[2.0]');

--
-- < Operator
--

RETURN tovector('[1.0, 9, 2, .9]') < tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') < tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') < tovector('[2.0]');

--
-- <= Operator
--
RETURN tovector('[1.0, 9, 2, .9]') <= tovector('[1.0, 9, 2, .9]');
RETURN tovector('[5.0, 2, 4, .324]') <= tovector('[1.0, 9, 2, .9]');
RETURN tovector('[1.0]') <= tovector('[2.0]');

--
-- l2 squared distance
--
RETURN l2_squared_distance(tovector('[1.0, 9, 2, .9]'), tovector('[1.0, 9, 2, .9]'));
RETURN l2_squared_distance(tovector('[5.0, 2, 4, .324]'), tovector('[1.0, 9, 2, .9]'));
RETURN l2_squared_distance(tovector('[1.0]'), tovector('[2.0]'));


SELECT gtype_build_map('i'::text, tovector('"[0, 0, 0]"'::gtype)); 
SELECT gtype_build_list('i'::text, tovector('"[0, 0, 0]"'::gtype));


SELECT gtype_build_map('i'::text, tovector('"[0, 0, 0]"'::gtype))->'"i"';

--
-- cleanup
--
DROP GRAPH vector CASCADE;