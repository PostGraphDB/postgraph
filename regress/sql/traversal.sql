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

--Basic Route Creation
SELECT build_traversal(
	build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
	build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
	build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
        build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map()),
	build_edge('4'::graphid, '3'::graphid, '5'::graphid, $$edge_label$$, gtype_build_map()),
        build_vertex('5'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
        build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map()),
        build_vertex('5'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
        build_edge('4'::graphid, '3'::graphid, '5'::graphid, $$edge_label$$, gtype_build_map()),
        build_vertex('5'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
        build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map()),
        NULL,
        build_vertex('5'::graphid, $$vertex_label$$, gtype_build_map())
);


SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_variable_edge(
		build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map())
	),
        build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT build_traversal(
        build_vertex('2'::graphid, $$vertex_label$$, gtype_build_map()),
        build_variable_edge(
            build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
            build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map()),
            build_edge('4'::graphid, '3'::graphid, '5'::graphid, $$edge_label$$, gtype_build_map())
        ),
        build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map())
);

SELECT _gtype_build_path(
       _gtype_build_vertex('2'::graphid, $$label_name$$, gtype_build_map()),
        build_variable_edge(
            build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map())
        ),
       _gtype_build_vertex('3'::graphid, $$label_name$$, gtype_build_map()),
       _gtype_build_edge('1'::graphid, '4'::graphid, '5'::graphid,
                         $$label$$, gtype_build_map('id', 2)),
                _gtype_build_vertex('5'::graphid, $$label_name$$, gtype_build_map())
);


SELECT _gtype_build_path(
       _gtype_build_vertex('2'::graphid, $$label_name$$, gtype_build_map()),
        build_variable_edge(
            build_edge('1'::graphid, '2'::graphid, '3'::graphid, $$edge_label$$, gtype_build_map()),
            build_vertex('3'::graphid, $$vertex_label$$, gtype_build_map()),
            build_edge('4'::graphid, '3'::graphid, '5'::graphid, $$edge_label$$, gtype_build_map())
        ),
       _gtype_build_vertex('3'::graphid, $$label_name$$, gtype_build_map()),
       _gtype_build_edge('1'::graphid, '4'::graphid, '5'::graphid,
                         $$label$$, gtype_build_map('id', 2)),
		_gtype_build_vertex('5'::graphid, $$label_name$$, gtype_build_map())
);

