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
SET extra_float_digits = 0;
LOAD 'postgraph';
LOAD 'postgis-3';
SET search_path TO postgraph, public;
set timezone TO 'GMT';

SELECT * FROM create_graph('postgis');

SELECT * FROM cypher('postgis', $$RETURN 'box(1 2, 5 6)'::box2d$$) AS r(c gtype);

SELECT * FROM cypher('postgis', $$RETURN 'BOX3D(1 2 3, 4 5 6)'::box3d$$) AS r(c gtype);

SELECT * FROM cypher('postgis', $$RETURN 'SPHEROID["WGS 84",6378137,298.257223563]'::spheroid$$) AS r(c gtype);

--
-- Box3D
--
WITH u AS (SELECT 'BOX3D(1 2 3, 4 5 6)'::box3d AS g)
SELECT ST_XMin((g)), ST_YMin(g), ST_ZMin(g), ST_XMax(g), ST_YMax(g), ST_ZMax(g)from u;
SELECT * FROM cypher('postgis', $$
     WITH 'BOX3D(1 2 3, 4 5 6)'::box3d AS g	
     RETURN ST_XMin((g)), ST_YMin(g), ST_ZMin(g), ST_XMax(g), ST_YMax(g), ST_ZMax(g)
$$) AS (xmin gtype, ymin gtype, zmin gtype, xmax gtype, ymax gtype, zmax gtype);

--
-- Geometry
--

--
-- I/O Routines
--
-- NOTE: The first two queries for each type is the PostGIS Geometry type, used to validate I/O is exactly the same
--

-- Point (2 Dimensional) I/O
SELECT 'POINT( 1 2 )'::geometry;
SELECT ST_AsEWKT('POINT( 1 2 )'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'POINT( 1 2 )'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POINT( 1 2 )'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POINT( 1 2 )') $$) AS r(c gtype);

-- Point (3 Dimensional) I/O
SELECT 'POINT( 1 2 3 )'::geometry;
SELECT ST_AsEWKT('POINT( 1 2 3)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'POINT( 1 2 3 )'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POINT( 1 2 3 )'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POINT( 1 2 3 )') $$) AS r(c gtype);

-- LineString (2D, 4 Points) I/O
SELECT 'LINESTRING (0 0, 1 1, 2 2, 3 3 , 4 4)'::geometry;
SELECT ST_AsEWKT('LINESTRING (0 0, 1 1, 2 2, 3 3 , 4 4)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'LINESTRING (0 0, 1 1, 2 2, 3 3 , 4 4)'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (0 0, 1 1, 2 2, 3 3 , 4 4)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (0 0, 1 1, 2 2, 3 3 , 4 4)') $$) AS r(c gtype);

-- LineString (3D, 4 Points) I/O
SELECT 'LINESTRING (0 0 0, 1 1 1, 2 2 2, 3 3 3, 4 4 4)'::geometry;
SELECT ST_AsEWKT('LINESTRING (0 0 0, 1 1 1, 2 2 2, 3 3 3 , 4 4 4)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'LINESTRING (0 0 0, 1 1 1, 2 2 2, 3 3 3 , 4 4 4)'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (0 0 0, 1 1 1, 2 2 2, 3 3 3, 4 4 4)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (0 0 0, 1 1 1, 2 2 2, 3 3 3 , 4 4 4)') $$) AS r(c gtype);

-- LineString (3D, 2 Points) I/O
SELECT 'LINESTRING (1 2 3, 10 20 30)'::geometry;
SELECT ST_AsEWKT('LINESTRING (1 2 3, 10 20 30)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'LINESTRING (1 2 3, 10 20 30)'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (1 2 3, 10 20 30)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('LINESTRING (1 2 3, 10 20 30)') $$) AS r(c gtype);

-- Polygon (2D) I/O
SELECT 'POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )'::geometry;
SELECT ST_AsEWKT('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POLYGON( (0 0, 10 0, 10 10, 0 10, 0 0) )') $$) AS r(c gtype);

-- Polygon (3D) I/O
SELECT 'POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )'::geometry;
SELECT ST_AsEWKT('POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )'::geometry);
SELECT * FROM cypher('postgis', $$RETURN 'POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )'::geometry$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN ST_AsEWKT('POLYGON( (0 0 1 , 10 0 1, 10 10 1, 0 10 1, 0 0 1) )') $$) AS r(c gtype);

--
-- Constructors
--

--
-- ST_MakePoint
--
SELECT ST_MakePoint(1, 0);
SELECT ST_MakePoint(1, 0, 2);
SELECT ST_MakePoint(1, 0, 2, 3);

SELECT * FROM cypher('postgis', $$ RETURN ST_MakePoint(1, 0) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$ RETURN ST_MakePoint(1, 0, 2) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$ RETURN ST_MakePoint(1, 0, 2, 3) $$) AS r(c gtype);

--
-- ST_MakePointM
--
SELECT ST_MakePointM(1, 0, 2);
SELECT * FROM cypher('postgis', $$ RETURN ST_MakePointM(1, 0, 2) $$) AS r(c gtype);

--
-- addbbox
--
SELECT ST_AsEwkt(postgis_addbbox('SRID=4326;POINT(1e+15 1e+15)'::geometry));
SELECT * FROM cypher('postgis', $$RETURN ST_AsEwkt(postgis_addbbox('SRID=4326;POINT(1e+15 1e+15)'::geometry)) $$) AS r(c gtype);


--
-- 2D Operators
--
select 'POINT(1 1)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY;
select 'POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY;
select 'POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1 0)'::GEOMETRY;
select 'MULTIPOINT(1 1,2 2)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY;
select 'MULTIPOINT(2 2, 1 1)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY;
select 'GEOMETRYCOLLECTION(POINT( 1 2 3),POINT(4 5 6))'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY;
select 'MULTIPOINT(4 5 6, 1 2 3)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY;
select 'MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY;
select 'MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(MULTIPOINT(1 2 3, 4 5 6))'::GEOMETRY;
select 'LINESTRING(1 1,2 2)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY;
select 'LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(2 2, 1 1)'::GEOMETRY;
select 'LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(1 1, 2 2, 3 3)'::GEOMETRY;

select * FROM cypher('postgis', $$
    RETURN 'POINT(1 1)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'POINT(1 1 0)'::GEOMETRY ~= 'POINT(1 1 0)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'MULTIPOINT(1 1,2 2)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'MULTIPOINT(2 2, 1 1)'::GEOMETRY ~= 'MULTIPOINT(1 1,2 2)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'GEOMETRYCOLLECTION(POINT( 1 2 3),POINT(4 5 6))'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'MULTIPOINT(4 5 6, 1 2 3)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(POINT( 4 5 6),POINT(1 2 3))'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'MULTIPOINT(1 2 3, 4 5 6)'::GEOMETRY ~= 'GEOMETRYCOLLECTION(MULTIPOINT(1 2 3, 4 5 6))'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'LINESTRING(1 1,2 2)'::GEOMETRY ~= 'POINT(1 1)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(2 2, 1 1)'::GEOMETRY
$$) as (c gtype);
select * FROM cypher('postgis', $$
    RETURN 'LINESTRING(1 1, 2 2)'::GEOMETRY ~= 'LINESTRING(1 1, 2 2, 3 3)'::GEOMETRY
$$) as (c gtype);

--
-- Point Cordinates
--

--
-- ST_X
--
SELECT ST_X('POINT (0 0)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('POINT (0 0)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('POINT (0 0)'::geometry) $$) AS r(c gtype);

SELECT ST_X('POINTZ (1 2 3)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);

SELECT ST_X('POINTM (6 7 8)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);

SELECT ST_X('POINTZM (10 11 12 13)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);

SELECT ST_X('MULTIPOINT ((0 0), (1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);

SELECT ST_X('LINESTRING (0 0, 1 1)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);

SELECT ST_X('GEOMETRYCOLLECTION (POINT(0 0))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);

SELECT ST_X('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_X('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN X('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);

--
-- ST_Y
--
SELECT ST_Y('POINT (0 0)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('POINT (0 0)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('POINT (0 0)'::geometry) $$) AS r(c gtype);

SELECT ST_Y('POINTZ (1 2 3)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);

SELECT ST_Y('POINTM (6 7 8)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);

SELECT ST_Y('POINTZM (10 11 12 13)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);

SELECT ST_Y('MULTIPOINT ((0 0), (1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);

SELECT ST_Y('LINESTRING (0 0, 1 1)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);

SELECT ST_Y('GEOMETRYCOLLECTION (POINT(0 0))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);

SELECT ST_Y('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Y('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Y('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);

--
-- ST_Z
--
SELECT ST_Z('POINT (0 0)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('POINT (0 0)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('POINT (0 0)'::geometry) $$) AS r(c gtype);

SELECT ST_Z('POINTZ (1 2 3)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);

SELECT ST_Z('POINTM (6 7 8)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);

SELECT ST_Z('POINTZM (10 11 12 13)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);

SELECT ST_Z('MULTIPOINT ((0 0), (1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);

SELECT ST_Z('LINESTRING (0 0, 1 1)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);

SELECT ST_Z('GEOMETRYCOLLECTION (POINT(0 0))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);

SELECT ST_Z('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_Z('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN Z('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);

--
-- ST_M
--
SELECT ST_M('POINT (0 0)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('POINT (0 0)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('POINT (0 0)'::geometry) $$) AS r(c gtype);

SELECT ST_M('POINTZ (1 2 3)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('POINTZ (1 2 3)'::geometry) $$) AS r(c gtype);

SELECT ST_M('POINTM (6 7 8)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('POINTM (6 7 8)'::geometry) $$) AS r(c gtype);

SELECT ST_M('POINTZM (10 11 12 13)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('POINTZM (10 11 12 13)'::geometry) $$) AS r(c gtype);

SELECT ST_M('MULTIPOINT ((0 0), (1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('MULTIPOINT ((0 0), (1 1))'::geometry) $$) AS r(c gtype);

SELECT ST_M('LINESTRING (0 0, 1 1)'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('LINESTRING (0 0, 1 1)'::geometry) $$) AS r(c gtype);

SELECT ST_M('GEOMETRYCOLLECTION (POINT(0 0))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('GEOMETRYCOLLECTION (POINT(0 0))'::geometry) $$) AS r(c gtype);

SELECT ST_M('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry);
SELECT * FROM cypher('postgis', $$RETURN ST_M('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$RETURN M('GEOMETRYCOLLECTION (POINT(0 1), LINESTRING(0 0, 1 1))'::geometry) $$) AS r(c gtype);

--
-- Temporal
--

--
-- ST_ClosestPointOfApproach
--
-- Converging
select ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 1, 10 10 10 10)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)'::geometry, 'LINESTRINGZM(0 0 0 1, 10 10 10 10)')
$$) as (c gtype);
-- Following
select ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry)
$$) as (c gtype);
-- Crossing
select ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry)
$$) as (c gtype);
-- Meeting
select ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_ClosestPointOfApproach( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry)
$$) as (c gtype);
-- Disjoint
select ST_ClosestPointOfApproach( 'LINESTRINGM(0 0 0, 0 0 4)', 'LINESTRINGM(0 0 5, 0 0 10)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_ClosestPointOfApproach('LINESTRINGM(0 0 0, 0 0 4)', 'LINESTRINGM(0 0 5, 0 0 10)'::geometry)
$$) as (c gtype);

--
-- ST_DistanceCPA
--
-- Converging
select ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 1, 10 10 10 10)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)'::geometry, 'LINESTRINGZM(0 0 0 1, 10 10 10 10)')
$$) as (c gtype);
-- Following
select ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 10 10 10 10)', 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry)
$$) as (c gtype);
-- Crossing
select ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry)
$$) as (c gtype);
-- Meeting
select ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_DistanceCPA( 'LINESTRINGZM(0 0 0 0, 0 0 0 10)', 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry)
$$) as (c gtype);
-- Disjoint
select ST_DistanceCPA( 'LINESTRINGM(0 0 0, 0 0 4)', 'LINESTRINGM(0 0 5, 0 0 10)'::geometry);
select * FROM cypher('postgis', $$
    RETURN ST_DistanceCPA('LINESTRINGM(0 0 0, 0 0 4)', 'LINESTRINGM(0 0 5, 0 0 10)'::geometry)
$$) as (c gtype);

--
-- |=| Operator
--
-- Converging
select 'LINESTRINGZM(0 0 0 0, 10 10 10 10)' |=| 'LINESTRINGZM(0 0 0 1, 10 10 10 10)'::geometry;
select * FROM cypher('postgis', $$
    RETURN 'LINESTRINGZM(0 0 0 0, 10 10 10 10)'::geometry |=| 'LINESTRINGZM(0 0 0 1, 10 10 10 10)'
$$) as (c gtype);
-- Following
select 'LINESTRINGZM(0 0 0 0, 10 10 10 10)' |=| 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry;
select * FROM cypher('postgis', $$
    RETURN 'LINESTRINGZM(0 0 0 0, 10 10 10 10)' |=| 'LINESTRINGZM(0 0 0 5, 10 10 10 15)'::geometry
$$) as (c gtype);
-- Crossing
select 'LINESTRINGZM(0 0 0 0, 0 0 0 10)' |=| 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry;
select * FROM cypher('postgis', $$
    RETURN 'LINESTRINGZM(0 0 0 0, 0 0 0 10)' |=| 'LINESTRINGZM(-30 0 5 4, 10 0 5 6)'::geometry
$$) as (c gtype);
-- Meeting 
select 'LINESTRINGZM(0 0 0 0, 0 0 0 10)' |=| 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry;
select * FROM cypher('postgis', $$
    RETURN 'LINESTRINGZM(0 0 0 0, 0 0 0 10)' |=| 'LINESTRINGZM(0 5 0 10, 10 0 5 11)'::geometry
$$) as (c gtype);
-- Disjoint
select 'LINESTRINGM(0 0 0, 0 0 4)' |=| 'LINESTRINGM(0 0 5, 0 0 10)'::geometry;
select * FROM cypher('postgis', $$
    RETURN 'LINESTRINGM(0 0 0, 0 0 4)' |=| 'LINESTRINGM(0 0 5, 0 0 10)'::geometry
$$) as (c gtype);



--
-- ST_IsValidTrajectory
--
SELECT ST_IsValidTrajectory('POINTM(0 0 0)'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGZ(0 0 0,1 1 1)'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 0)'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGM(0 0 1,1 1 0)'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1,1 1 2,1 0 1)'::geometry);

SELECT ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1)'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGM EMPTY'::geometry);
SELECT ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1,1 1 2)'::geometry);

SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('POINTM(0 0 0)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGZ(0 0 0,1 1 1)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 0)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM(0 0 1,1 1 0)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1,1 1 2,1 0 1)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1)'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM EMPTY'::geometry)
$$) as (c gtype);
SELECT * FROM cypher('postgis', $$
    RETURN ST_IsValidTrajectory('LINESTRINGM(0 0 0,1 1 1,1 1 2)'::geometry)
$$) as (c gtype);


--
-- ST_CPAWithin
--
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 0.0);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1.0);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 0.5);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1.9);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2);
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2.0001);
-- temporary disjoint
SELECT ST_CPAWithin( 'LINESTRINGM(0 0 0, 10 0 10)'::geometry ,'LINESTRINGM(10 0 11, 10 10 20)'::geometry, 1e15);
SELECT ST_CPAWithin( 'LINESTRING(0 0 0, 1 0 0)'::geometry ,'LINESTRING(0 0 3 0, 1 0 2 1)'::geometry, 1e16);

SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 0.0)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1.0)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 0.5)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 1.9)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 1 0 1)'::geometry ,'LINESTRINGM(0 0 0, 1 0 1)'::geometry, 2.0001)
$$) as (c gtype);
-- temporary disjoint
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRINGM(0 0 0, 10 0 10)'::geometry ,'LINESTRINGM(10 0 11, 10 10 20)'::geometry, 1e15)
$$) as (c gtype);
SELECT  * FROM cypher('postgis', $$
    RETURN ST_CPAWithin( 'LINESTRING(0 0 0, 1 0 0)'::geometry ,'LINESTRING(0 0 3 0, 1 0 2 1)'::geometry, 1e16)
$$) as (c gtype); --XXX

--
-- GEOS
--

--
-- ST_Intersection
--
SELECT ST_Intersection('MULTIPOINT ((0 0), (1 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry);
SELECT ST_AsEWKT(ST_Intersection('MULTIPOINT ((0 0), (1 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry));
SELECT * FROM cypher('postgis', $$
     RETURN ST_Intersection('MULTIPOINT ((0 0), (1 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry, -1.0)
$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$
     RETURN ST_AsEWKT(ST_Intersection('MULTIPOINT ((0 0), (1 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry, -1.0))
$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$
     RETURN ST_AsEWKT(ST_Intersection('MULTIPOINT ((2 2), (5 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry, -1.0))
$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$
     RETURN ST_AsEWKT(ST_Intersection('MULTIPOINT ((2 2), (5 1))'::geometry, 'MULTIPOINT ((0 0), (1 1))'::geometry))
$$) AS r(c gtype);


--
-- Algorithms
-- 

--
-- ST_Simplify
--
SELECT ST_AsEWKT(ST_Simplify('POLYGON((0 0,1 1,1 3,0 4,-2 3,-1 1,0 0))'::geometry, 1));
SELECT ST_AsEWKT(ST_Simplify('POLYGON((0 0,1 1,1 3,2 3,2 0,0 0))'::geometry, 1));

SELECT * FROM cypher('postgis', $$
     RETURN ST_AsEWKT(ST_Simplify('POLYGON((0 0,1 1,1 3,0 4,-2 3,-1 1,0 0))', 1))
$$) AS r(c gtype);
SELECT * FROM cypher('postgis', $$
     RETURN ST_AsEWKT(ST_Simplify('POLYGON((0 0,1 1,1 3,2 3,2 0,0 0))'::geometry, 1))
$$) AS r(c gtype);

SELECT * FROM drop_graph('postgis', true);
