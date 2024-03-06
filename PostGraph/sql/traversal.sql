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
 */

LOAD 'postgraph';
SET search_path TO postgraph;

SELECT create_graph('traversal');
SELECT create_vlabel('traversal', 'vlabel');
SELECT create_vlabel('traversal', 'elabel');

--Basic Route Creation
SELECT build_traversal(
	build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
	build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
	build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3), graphid, gtype_build_map()),
	build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3), graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3), graphid, gtype_build_map()),
        NULL,
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;


SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_traversal(
		build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map())
	),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_traversal(
            build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
            build_vertex(_graphid(3, 3), graphid, gtype_build_map()),
            build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map())
        ),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

--
-- Equals Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) =
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;


--
-- Not Equals Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <>
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

--
-- Greater Than Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;
--
-- Greater Than Or Equals To Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) >=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

--
-- Less Than Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) < 
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) < 
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) <
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

--
-- Less Than Or Equals To Operator
--
SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map()),
        build_edge(_graphid(4, 4), '3'::graphid, '5'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 4), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 2), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 5),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
) <=
build_traversal(
        build_vertex(_graphid(3, 2), graphid, gtype_build_map()),
        build_edge(_graphid(4, 1), '2'::graphid, '3'::graphid,  graphid, gtype_build_map()),
        build_vertex(_graphid(3, 3),  graphid, gtype_build_map())
)
FROM ag_graph;

SELECT drop_graph('traversal', true);
