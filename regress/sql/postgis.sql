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



SELECT * FROM drop_graph('postgis', true);
