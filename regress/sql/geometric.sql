/*
 * PostGraph
 * Copyright (C) 2023 PostGraphDB
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

LOAD 'postgraph';
SET search_path TO postgraph, public;

SELECT * FROM create_graph('geometric');

--
-- Point
--
SELECT topoint('"(1,1)"');
SELECT '(1,1)'::point::gtype;
select * FROM cypher('geometric', $$
    RETURN 'POINT(1 1)'::geometry
$$) AS (c point);
SELECT * FROM cypher('geometric', $$RETURN '(1,1)'::point $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1)'::point $$) AS r(c point);

--
-- Lseg
--
SELECT tolseg('"(1,1), (2,2)"');
SELECT * FROM cypher('geometric', $$RETURN tolseg('(1,1), (2,2)') $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::lseg $$) AS r(c gtype);

--
-- Line
--
SELECT toline('"{1,1,2}"');
SELECT * FROM cypher('geometric', $$RETURN '{1,1,2}'::line $$) AS r(c gtype);

--
-- Path
--
SELECT topath('"[(1,1), (2,2)]"');
SELECT topath('"((1,1), (2,2))"');
SELECT '[(1,1), (2,2)]'::path::gtype;
SELECT topath('"((1,1), (2,2))"')::path;
SELECT * FROM cypher('geometric', $$RETURN '[(1,1), (2,2)]'::path $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '[(1,1), (2,2)]'::path $$) AS r(c path);

--
-- Polygon
--
SELECT topolygon('"(1,1), (2,2), (3, 3), (4, 4)"');
SELECT topolygon('"(1,1), (2,2), (3, 3), (4, 4)"')::polygon;
SELECT '(1,1), (2,2), (3, 3), (4, 4)'::polygon::gtype;
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c polygon);

--
-- Circle
--
SELECT tocircle('"(1,1), 3"');
SELECT * FROM cypher('geometric', $$RETURN tocircle('(1,1), 3') $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 3'::circle $$) AS r(c gtype);

--
-- Box
--
SELECT tobox('"(1,1), (2,2)"');
SELECT tobox('"(1,1), (2,2)"')::box;
SELECT '(1,1), (2,2)'::box::gtype;
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::box $$) AS r(c box);


--
-- Geometric + Point
--

--
-- Point + Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1)'::point + '(1,1)'::point $$) AS r(c gtype);

--
-- Box + Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::box + '(1,1)'::point $$) AS r(c gtype);

--
-- Path + Point
--
SELECT * FROM cypher('geometric', $$RETURN '[(1,1), (2,2)]'::path + '(1,1)'::point $$) AS r(c gtype);

--
-- Circle + Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 3'::circle + '(1,1)'::point $$) AS r(c gtype);


--
-- Geometric - Point
--

--
-- Point - Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1)'::point - '(1,1)'::point $$) AS r(c gtype);

--
-- Box - Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::box - '(1,1)'::point $$) AS r(c gtype);

--
-- Path - Point
--
SELECT * FROM cypher('geometric', $$RETURN '[(1,1), (2,2)]'::path - '(1,1)'::point $$) AS r(c gtype);

--
-- Circle - Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 3'::circle - '(1,1)'::point $$) AS r(c gtype);


--
-- Geometric / Point
--

--
-- Point / Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4)'::point / '(2,2)'::point $$) AS r(c gtype);

--
-- Box / Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box / '(2,2)'::point $$) AS r(c gtype);

--
-- Path / Point
--
SELECT * FROM cypher('geometric', $$RETURN '[(4,4), (2,2)]'::path / '(2,2)'::point $$) AS r(c gtype);

--
-- Circle / Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4), 3'::circle / '(2, 2)'::point $$) AS r(c gtype);

--
-- Geometric * Point
--

--
-- Point * Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4)'::point * '(2,2)'::point $$) AS r(c gtype);

--
-- Box * Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box * '(2,2)'::point $$) AS r(c gtype);

--
-- Path * Point
--
SELECT * FROM cypher('geometric', $$RETURN '[(4,4), (2,2)]'::path * '(2,2)'::point $$) AS r(c gtype);

--
-- Circle * Point
--
SELECT * FROM cypher('geometric', $$RETURN '(4,4), 3'::circle * '(2, 2)'::point $$) AS r(c gtype);

--
-- Box # Box
--
SELECT * FROM cypher('geometric', $$RETURN '(2,2), (-1,-1)'::box # '(1, 1), (-2, -2)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box # '(10,10), (5,5)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box # '(5, 5), (5,5)'::box $$) AS r(c gtype);

--
-- LSeg # LSeg
--
SELECT * FROM cypher('geometric', $$RETURN '(0,0), (1,1)'::lseg # '(1,0), (0,1)'::lseg $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::lseg # '(1,1), (2,2)'::lseg $$) AS r(c gtype);

--
-- Line # Line
--
SELECT * FROM cypher('geometric', $$RETURN '(0,0), (1,1)'::line # '(1,0), (0,1)'::line $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::line # '(1,1), (2,2)'::line $$) AS r(c gtype);

--
-- LSeg ?-| LSeg
--
SELECT * FROM cypher('geometric', $$RETURN '(0,0), (1,1)'::lseg ?-| '(1,0), (0,1)'::lseg $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::lseg ?-| '(2,2), (3,3)'::lseg $$) AS r(c gtype);

--
-- Line ?-| Line
--
SELECT * FROM cypher('geometric', $$RETURN '(0,0), (1,1)'::line ?-| '(1,0), (0,1)'::line $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2)'::line ?-| '(2,2), (3,3)'::line $$) AS r(c gtype);

--
-- Point ?| Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1)'::point ?| '(1,2)'::point $$) AS r(c boolean);

--
-- ?| Lseg
--
SELECT * FROM cypher('geometric', $$RETURN ?| '(0,1), (0,2)'::lseg $$) AS (c boolean);
SELECT * FROM cypher('geometric', $$RETURN ?| '(1,0), (0,1)'::lseg $$) AS (c boolean);

--
-- ?| Line
--
SELECT * FROM cypher('geometric', $$RETURN ?| '(0,1), (0,2)'::line $$) AS (c boolean);
SELECT * FROM cypher('geometric', $$RETURN ?| '(1,0), (0,1)'::line $$) AS (c boolean);

--
-- Box @> Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (5,5)'::box @> '(1,2)'::point $$) AS r(c boolean);
SELECT * FROM cypher('geometric', $$RETURN '(0,0), (1,1)'::box @> '(1,2)'::point $$) AS r(c boolean);

--
-- Box @> Box
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (0, 0)'::box @> '(4, 4), (3, 3)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(2,2), (-1,-1)'::box @> '(1, 1), (-2, -2)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box @> '(10,10), (5,5)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box @> '(5, 5), (5,5)'::box $$) AS r(c gtype);

--
-- Path @> Point
--
SELECT * FROM cypher('geometric', $$RETURN '[(4,4), (2,2)]'::path @> '(2,2)'::point $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '[(4,4), (2,2)]'::path @> '(3,5)'::point $$) AS r(c gtype);

--
-- Polygon @> Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon @> '(2,2)'::point $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (2,2), (3, 3), (4, 4)'::polygon @> '(3,5)'::point $$) AS r(c gtype);

--
-- Circle @> Point
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 2'::circle @> '(2,2)'::point $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 2'::circle @> '(3,5)'::point $$) AS r(c gtype);

--
-- Circle @> Circle
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 2'::circle @> '(1,1), 3'::circle $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 4'::circle @> '(1,1), 3'::circle $$) AS r(c gtype);


--
-- Point <@ Box
--
SELECT * FROM cypher('geometric', $$RETURN '(1,2)'::point <@ '(1,1), (5,5)'::box $$) AS r(c boolean);
SELECT * FROM cypher('geometric', $$RETURN '(1, 2)'::point <@ '(0,0), (1,1)'::box $$) AS r(c boolean);

--
-- Box <@ Box
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), (0, 0)'::box <@ '(4, 4), (3, 3)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(2,2), (-1,-1)'::box <@ '(1, 1), (-2, -2)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box <@ '(10,10), (5,5)'::box $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(4,4), (2,2)'::box <@ '(5, 5), (5,5)'::box $$) AS r(c gtype);

--
-- Point <@ Path
--
SELECT * FROM cypher('geometric', $$RETURN '(2,2)'::point <@ '[(4,4), (2,2)]'::path $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(3,5)'::point <@ '[(4,4), (2,2)]'::path $$) AS r(c gtype);

--
-- Point <@ Polygon
--
SELECT * FROM cypher('geometric', $$RETURN '(2,2)'::point <@ '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(3,5)'::point <@ '(1,1), (2,2), (3, 3), (4, 4)'::polygon $$) AS r(c gtype);

--
-- Point <@ Circle
--
SELECT * FROM cypher('geometric', $$RETURN '(2,2)'::point <@ '(1,1), 3'::circle $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(3,5)'::point <@ '(1,1), 3'::circle $$) AS r(c gtype);

--
-- Circle <@ Circle
--
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 2'::circle <@ '(1,1), 3'::circle $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN '(1,1), 4'::circle <@ '(1,1), 3'::circle $$) AS r(c gtype);

--
-- Geometric Functions
--
--
-- Bound_Box
--
SELECT * FROM cypher('geometric', $$RETURN bound_box('(1,1), (0, 0)'::box, '(4, 4), (3, 3)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN bound_box('(2,2), (-1,-1)'::box, '(1, 1), (-2, -2)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN bound_box('(4,4), (2,2)'::box, '(10,10), (5,5)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN bound_box('(4,4), (2,2)'::box, '(5, 5), (5,5)'::box) $$) AS r(c gtype);

--
-- Height
--
SELECT * FROM cypher('geometric', $$RETURN height('(1,1), (0, 0)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN height('(2,2), (-1,-1)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN height('(4,4), (2,2)'::box) $$) AS r(c gtype);

--
-- Width
--
SELECT * FROM cypher('geometric', $$RETURN width('(1,1), (0, 0)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN width('(2,2), (-1,-1)'::box) $$) AS r(c gtype);
SELECT * FROM cypher('geometric', $$RETURN width('(4,4), (2,2)'::box) $$) AS r(c gtype);

--
-- Clean Up
--
SELECT * FROM drop_graph('geometric', true);
