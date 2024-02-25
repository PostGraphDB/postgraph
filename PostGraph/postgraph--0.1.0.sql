/*
 * PostGraph
 * Copyright (C) 2023 by PostGraph
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Portions Copyright (c) 2020-2023, Apache Software Foundation
 * Portions Copyright (c) 2019-2020, Bitnine Global
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION PostGraph" to load this file. \quit

CREATE SCHEMA postgraph;

SET SEARCH_PATH = postgraph, pg_catalog, public;

--
-- catalog tables
--
CREATE TABLE ag_graph (
    graphid oid NOT NULL, 
    name name NOT NULL, 
    namespace regnamespace NOT NULL,
    directed boolean NOT NULL
);

CREATE UNIQUE INDEX ag_graph_graphid_index 
ON ag_graph 
USING btree (graphid);

CREATE UNIQUE INDEX ag_graph_name_index 
ON ag_graph 
USING btree (name);

CREATE UNIQUE INDEX ag_graph_namespace_index 
ON ag_graph 
USING btree (namespace);

-- 0 is an invalid label ID
CREATE DOMAIN label_id
AS int 
NOT NULL 
CHECK (VALUE > 0 AND VALUE <= 65535);

CREATE DOMAIN label_kind 
AS "char" 
NOT NULL 
CHECK (VALUE = 'v' OR VALUE = 'e');

CREATE TABLE ag_label (
    name name NOT NULL, 
    graph oid NOT NULL, 
    id label_id, 
    kind label_kind, 
    relation regclass NOT NULL, 
    label_path public.ltree NOT NULL,
    CONSTRAINT fk_graph_oid FOREIGN KEY(graph) REFERENCES ag_graph(graphid)
);

CREATE UNIQUE INDEX ag_label_name_graph_index 
ON ag_label 
USING btree (name, graph);

CREATE UNIQUE INDEX ag_label_graph_oid_index 
ON ag_label 
USING btree (graph, id);

CREATE UNIQUE INDEX ag_label_relation_index 
ON ag_label 
USING btree (relation);

CREATE UNIQUE INDEX ag_label_path_graph_index
ON ag_label
USING btree (label_path, graph);


--
-- catalog lookup functions
--
CREATE FUNCTION _label_id(graph_name name, label_name name) 
RETURNS label_id 
LANGUAGE c 
STABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

--
-- utility functions
--
CREATE FUNCTION create_graph(graph_name name)
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_graph_if_not_exists(graph_name name) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION drop_graph(graph_name name, cascade boolean = false) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_vlabel(graph_name name, label_name name) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_elabel(graph_name name, label_name name) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION alter_graph(graph_name name, operation cstring, new_value name) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION drop_label(graph_name name, label_name name, force boolean = false) 
RETURNS void 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_ivfflat_l2_ops_index(graph_name name, label_name name, property_name name, dimensions int, lists int)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_ivfflat_ip_ops_index(graph_name name, label_name name, property_name name, dimensions int, lists int)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';

CREATE FUNCTION create_unique_properties_constraint(graph_name name, label_name name)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';


CREATE FUNCTION create_property_index(graph_name name, label_name name, property_name name, is_unique boolean = false)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME';

--
-- graphid type
--
CREATE TYPE graphid;

CREATE FUNCTION graphid_in(cstring) 
RETURNS graphid 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION graphid_out(graphid) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

-- binary I/O functions
CREATE FUNCTION graphid_send(graphid) 
RETURNS bytea 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION graphid_recv(internal) 
RETURNS graphid 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE TYPE graphid (
    INPUT = graphid_in, 
    OUTPUT = graphid_out, 
    SEND = graphid_send, 
    RECEIVE = graphid_recv, 
    INTERNALLENGTH = 8, 
    PASSEDBYVALUE, 
    ALIGNMENT = float8, 
    STORAGE = plain
);

--
-- gtype type and its I/O Routines
--
CREATE TYPE gtype;

CREATE FUNCTION gtype_in(cstring)
RETURNS gtype
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_out(gtype) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

-- binary I/O functions
CREATE FUNCTION gtype_send(gtype) 
RETURNS bytea 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_recv(internal) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE TYPE gtype (
    INPUT = gtype_in,
    OUTPUT = gtype_out,
    SEND = gtype_send,
    RECEIVE = gtype_recv,
    LIKE = jsonb,
    STORAGE = extended
);

--
-- vertex type and its I/O Routines
--
CREATE TYPE vertex;

-- throws an error
CREATE FUNCTION vertex_in(cstring)
RETURNS vertex
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION vertex_out(vertex) 
RETURNS cstring 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION build_vertex(graphid, oid, gtype) 
RETURNS vertex 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
COST 50
AS 'MODULE_PATHNAME';

CREATE TYPE vertex (INPUT = vertex_in, OUTPUT = vertex_out, LIKE = jsonb);

--
-- edge
--
CREATE TYPE edge;

CREATE FUNCTION edge_in(cstring) 
RETURNS edge 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
COST 50
AS 'MODULE_PATHNAME';

CREATE FUNCTION edge_out(edge) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION build_edge(graphid, graphid, graphid, oid, gtype) 
RETURNS edge 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE TYPE edge (INPUT = edge_in, OUTPUT = edge_out, LIKE = jsonb);


--
-- path
--
CREATE TYPE traversal;

CREATE FUNCTION traversal_in(cstring) 
RETURNS traversal 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION traversal_out(traversal) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION build_traversal(variadic "any") 
RETURNS traversal 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
COST 250
AS 'MODULE_PATHNAME';

CREATE TYPE traversal (INPUT = traversal_in, OUTPUT = traversal_out, LIKE = jsonb);

--
-- partial traversal
--
CREATE TYPE variable_edge;

CREATE FUNCTION variable_edge_in(cstring) 
RETURNS variable_edge 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION variable_edge_out(variable_edge) 
RETURNS cstring 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION build_variable_edge(variadic "any") 
RETURNS variable_edge 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
COST 250
AS 'MODULE_PATHNAME';

CREATE TYPE variable_edge (INPUT = variable_edge_in, OUTPUT = variable_edge_out, LIKE = jsonb);

--
-- Access Methods
--

CREATE FUNCTION ivfflathandler(internal) RETURNS index_am_handler AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD ivfflat TYPE INDEX HANDLER ivfflathandler;

COMMENT ON ACCESS METHOD ivfflat IS 'ivfflat index access method';


--
-- MATCH edge uniqueness
--
CREATE FUNCTION _ag_enforce_edge_uniqueness(VARIADIC "any")
RETURNS bool 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
as 'MODULE_PATHNAME';


--
-- There are times when the optimizer might eliminate
-- functions we need. Wrap the function with this to
-- prevent that from happening
--
CREATE FUNCTION gtype_volatile_wrapper(agt gtype) RETURNS gtype AS $return_value$
BEGIN RETURN agt; END;
$return_value$ LANGUAGE plpgsql
VOLATILE
CALLED ON NULL INPUT
PARALLEL SAFE;

CREATE FUNCTION gtype_volatile_wrapper(agt edge) RETURNS edge AS $return_value$
BEGIN RETURN agt; END;
$return_value$ LANGUAGE plpgsql
VOLATILE
CALLED ON NULL INPUT
PARALLEL SAFE;

CREATE FUNCTION gtype_volatile_wrapper(agt vertex) RETURNS vertex AS $return_value$
BEGIN RETURN agt; END;
$return_value$ LANGUAGE plpgsql
VOLATILE
CALLED ON NULL INPUT
PARALLEL SAFE;

CREATE FUNCTION gtype_volatile_wrapper(agt variable_edge) RETURNS variable_edge AS $return_value$
BEGIN RETURN agt; END;
$return_value$ LANGUAGE plpgsql
VOLATILE
CALLED ON NULL INPUT
PARALLEL SAFE;

CREATE FUNCTION gtype_volatile_wrapper(agt traversal) RETURNS traversal
AS $return_value$
BEGIN RETURN agt; END;
$return_value$
LANGUAGE plpgsql
VOLATILE
CALLED ON NULL INPUT
PARALLEL SAFE;

--
-- functions for updating clauses
--
-- This function is defined as a VOLATILE function to prevent the optimizer
-- from pulling up Query's for CREATE clauses.
CREATE FUNCTION _cypher_create_clause(internal) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';
CREATE FUNCTION _cypher_set_clause(internal)    RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';
CREATE FUNCTION _cypher_delete_clause(internal) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';
CREATE FUNCTION _cypher_merge_clause(internal)  RETURNS void LANGUAGE c AS 'MODULE_PATHNAME';

--
-- query functions
--
CREATE FUNCTION cypher(graph_name name, query_string cstring, params gtype = NULL) 
RETURNS SETOF record 
LANGUAGE c 
AS 'MODULE_PATHNAME';

CREATE FUNCTION get_cypher_keywords(OUT word text, OUT catcode "char", OUT catdesc text) 
RETURNS SETOF record 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
COST 10 ROWS 60 
AS 'MODULE_PATHNAME';

--
-- Unnest (UNWIND Clause) Functions
--
CREATE FUNCTION unnest (gtype, block_types boolean = false)
RETURNS SETOF gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_unnest';

CREATE FUNCTION unnest (vertex[], block_types boolean = false)
RETURNS SETOF vertex
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'vertex_unnest';

CREATE FUNCTION unnest (edge[], block_types boolean = false)
RETURNS SETOF edge
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_unnest';



CREATE FUNCTION vle (IN gtype, IN vertex, IN vertex, IN gtype, IN gtype,
                     IN gtype, IN gtype, IN gtype, OUT edges variable_edge)
RETURNS SETOF variable_edge 
LANGUAGE C 
STABLE 
CALLED ON NULL INPUT 
PARALLEL UNSAFE 
COST 5000
AS 'MODULE_PATHNAME', 'gtype_vle';

CREATE FUNCTION match_vles(variable_edge, variable_edge) 
RETURNS boolean 
LANGUAGE C 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR !!= (FUNCTION = match_vles, LEFTARG = variable_edge, RIGHTARG = variable_edge);


CREATE FUNCTION throw_error(gtype)
RETURNS gtype
LANGUAGE C
VOLATILE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

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
 */

--
-- graphid - comparison operators (=, <>, <, >, <=, >=)
--
CREATE FUNCTION graphid_eq(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
    FUNCTION = graphid_eq, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = =, 
    NEGATOR = <>, 
    RESTRICT = eqsel, 
    JOIN = eqjoinsel, 
    HASHES, 
    MERGES
);

CREATE FUNCTION graphid_ne(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
    FUNCTION = graphid_ne, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = <>, 
    NEGATOR = =, 
    RESTRICT = neqsel, 
    JOIN = neqjoinsel
);

CREATE FUNCTION graphid_lt(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION graphid_gt(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';


CREATE FUNCTION graphid_le(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION graphid_ge(graphid, graphid) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';


--
-- graphid - B-tree support functions
--
-- comparison support
CREATE FUNCTION graphid_btree_cmp(graphid, graphid) 
RETURNS int 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

-- sort support
CREATE FUNCTION graphid_btree_sort(internal) 
RETURNS void 
LANGUAGE c 
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';



--
-- graphid functions
--
CREATE FUNCTION _graphid(label_id int, entry_id bigint) 
RETURNS graphid 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION _label_name(graph_oid oid, graphid) 
RETURNS cstring 
LANGUAGE c 
STABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION _extract_label_id(graphid) 
RETURNS label_id 
LANGUAGE c 
STABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';



CREATE OPERATOR < (
    FUNCTION = graphid_lt, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = >, 
    NEGATOR = >=, 
    RESTRICT = scalarltsel, 
    JOIN = scalarltjoinsel
);

CREATE OPERATOR > (
    FUNCTION = graphid_gt, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = <, 
    NEGATOR = <=, 
    RESTRICT = scalargtsel, 
    JOIN = scalargtjoinsel
);

CREATE OPERATOR <= (
    FUNCTION = graphid_le, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = >=, 
    NEGATOR = >, 
    RESTRICT = scalarlesel, 
    JOIN = scalarlejoinsel);

CREATE OPERATOR >= (
    FUNCTION = graphid_ge, 
    LEFTARG = graphid, 
    RIGHTARG = graphid, 
    COMMUTATOR = <=, 
    NEGATOR = <, 
    RESTRICT = scalargesel, 
    JOIN = scalargejoinsel
);

--
-- graphid - hash operator class
--
CREATE FUNCTION graphid_hash_cmp(graphid) 
RETURNS INTEGER 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR CLASS graphid_ops_hash 
DEFAULT FOR TYPE graphid 
USING hash 
AS 
    OPERATOR 1 =, 
    FUNCTION 1 graphid_hash_cmp(graphid);
--
-- btree operator classes for graphid
--
CREATE OPERATOR CLASS graphid_ops 
DEFAULT FOR TYPE graphid 
USING btree 
AS 
    OPERATOR 1 <, 
    OPERATOR 2 <=, 
    OPERATOR 3 =, 
    OPERATOR 4 >=, 
    OPERATOR 5 >,
    FUNCTION 1 graphid_btree_cmp (graphid, graphid), 
    FUNCTION 2 graphid_btree_sort (internal);

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
 */

--
-- gtype - map literal (`{key: expr, ...}`)
--
CREATE FUNCTION gtype_build_map(VARIADIC "any") 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
CALLED ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_build_map() 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_build_map_noargs';

--
-- gtype - access operators
--
-- for series of `map.key` and `container[expr]`
CREATE FUNCTION gtype_field_access(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_field_access
);

--
-- gtype - mathematical operators (+, -, *, /, %, ^)
--
CREATE FUNCTION gtype_add(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR + (FUNCTION = gtype_add, LEFTARG = gtype, RIGHTARG = gtype, COMMUTATOR = +);

CREATE FUNCTION gtype_sub(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR - (FUNCTION = gtype_sub, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_neg(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR - (FUNCTION = gtype_neg, RIGHTARG = gtype);

CREATE FUNCTION gtype_mul(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR * (FUNCTION = gtype_mul, LEFTARG = gtype, RIGHTARG = gtype, COMMUTATOR = *);

CREATE FUNCTION gtype_div(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR / (FUNCTION = gtype_div, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_mod(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR % (FUNCTION = gtype_mod, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_pow(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ^ (FUNCTION = gtype_pow, LEFTARG = gtype, RIGHTARG = gtype);

--
-- Vector Operators
--
CREATE FUNCTION l2_distance(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
COST 250
AS 'MODULE_PATHNAME';

CREATE OPERATOR <-> (FUNCTION = l2_distance, LEFTARG = gtype, RIGHTARG = gtype, COMMUTATOR = <->);

CREATE FUNCTION inner_product(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_inner_product';

CREATE FUNCTION negative_inner_product(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE AS 'MODULE_PATHNAME', 'gtype_negative_inner_product';

CREATE OPERATOR <#> (FUNCTION = negative_inner_product, LEFTARG = gtype, RIGHTARG = gtype, COMMUTATOR = <#>);

CREATE FUNCTION cosine_distance(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_cosine_distance';

CREATE OPERATOR <=> (FUNCTION = cosine_distance, LEFTARG = gtype, RIGHTARG = gtype, COMMUTATOR = <=>);

CREATE FUNCTION l1_distance(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_l1_distance';

CREATE FUNCTION spherical_distance(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_spherical_distance';

CREATE FUNCTION dims(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_dims';

CREATE FUNCTION norm(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE AS 'MODULE_PATHNAME', 'gtype_norm';

CREATE FUNCTION l2_squared_distance(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_l2_squared_distance';

CREATE FUNCTION gtype_lt(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_gt(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_le(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_ge(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

--
-- gtype - comparison operators (=, <>, <, >, <=, >=)
--
CREATE FUNCTION gtype_eq(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
    FUNCTION = gtype_eq, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = =, 
    NEGATOR = <>, 
    RESTRICT = eqsel, 
    JOIN = eqjoinsel, 
    HASHES
);

CREATE FUNCTION gtype_ne(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
    FUNCTION = gtype_ne, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = <>, 
    NEGATOR = =, 
    RESTRICT = neqsel, 
    JOIN = neqjoinsel
);


CREATE FUNCTION gtype_btree_cmp(gtype, gtype) 
RETURNS INTEGER 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';


CREATE OPERATOR < (
    FUNCTION = gtype_lt, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = >, 
    NEGATOR = >=, 
    RESTRICT = scalarltsel, 
    JOIN = scalarltjoinsel
);

CREATE OPERATOR > (
    FUNCTION = gtype_gt, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = <, 
    NEGATOR = <=, 
    RESTRICT = scalargtsel, 
    JOIN = scalargtjoinsel
);

CREATE OPERATOR <= (
    FUNCTION = gtype_le, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = >=, 
    NEGATOR = >, 
    RESTRICT = scalarlesel, 
    JOIN = scalarlejoinsel
);

CREATE OPERATOR >= (
    FUNCTION = gtype_ge, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    COMMUTATOR = <=, 
    NEGATOR = <, 
    RESTRICT = scalargesel, 
    JOIN = scalargejoinsel
);


--
-- gtype - btree operator class
--
CREATE OPERATOR CLASS gtype_ops_btree
DEFAULT FOR TYPE gtype
USING btree
AS 
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =, 
    OPERATOR 4 >,
    OPERATOR 5 >=, 
    FUNCTION 1 gtype_btree_cmp(gtype, gtype);

--
-- gtype - ivfflat Operator Classes
--
CREATE OPERATOR CLASS gtype_l2_ops
DEFAULT FOR TYPE gtype
USING ivfflat AS 
OPERATOR 1 <-> (gtype, gtype) FOR ORDER BY gtype_ops_btree,
FUNCTION 1 l2_squared_distance(gtype, gtype),
FUNCTION 3 l2_distance(gtype, gtype);

CREATE OPERATOR CLASS gtype_ip_ops
FOR TYPE gtype USING ivfflat AS
OPERATOR 1 <#> (gtype, gtype) FOR ORDER BY gtype_ops_btree,
FUNCTION 1 negative_inner_product(gtype, gtype),
FUNCTION 3 spherical_distance(gtype, gtype),
FUNCTION 4 norm(gtype);

CREATE OPERATOR CLASS gtype_cosine_ops
FOR TYPE gtype USING ivfflat AS
OPERATOR 1 <=> (gtype, gtype) FOR ORDER BY gtype_ops_btree,
FUNCTION 1 negative_inner_product(gtype, gtype),
FUNCTION 2 norm(gtype),
FUNCTION 3 spherical_distance(gtype, gtype),
FUNCTION 4 norm(gtype);


--
-- gtype - hash operator class
--
CREATE FUNCTION gtype_hash_cmp(gtype) 
RETURNS INTEGER 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR CLASS gtype_ops_hash 
DEFAULT FOR TYPE gtype 
USING hash 
AS 
     OPERATOR 1 =, 
     FUNCTION 1 gtype_hash_cmp(gtype);

--
-- gtype - access operators (->, ->>)
--
CREATE FUNCTION gtype_object_field(gtype, text) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (LEFTARG = gtype, RIGHTARG = text, FUNCTION = gtype_object_field);

CREATE FUNCTION gtype_object_field_text(gtype, text) 
RETURNS text 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ->> (LEFTARG = gtype, RIGHTARG = text, FUNCTION = gtype_object_field_text);

CREATE FUNCTION gtype_array_element(gtype, int4) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (LEFTARG = gtype, RIGHTARG = int4, FUNCTION = gtype_array_element);

CREATE FUNCTION gtype_array_element_text(gtype, int4) 
RETURNS text 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ->> (LEFTARG = gtype, RIGHTARG = int4, FUNCTION = gtype_array_element_text);

--
-- gtype - contains operators (@>, <@)
--
CREATE FUNCTION gtype_contains(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR @> (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_contains, 
    COMMUTATOR = '<@', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION gtype_contained_by(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <@ (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_contained_by, 
    COMMUTATOR = '@>', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

--
-- Key Existence Operators (?, ?|, ?&)
--
CREATE FUNCTION gtype_exists(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ? (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_exists, 
    COMMUTATOR = '?', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION gtype_exists_any(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ?| (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_exists_any, 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION gtype_exists_all(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ?& (
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    FUNCTION = gtype_exists_all, 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

--
-- gtype GIN support
--
CREATE FUNCTION gin_compare_gtype(text, text) 
RETURNS int 
AS 'MODULE_PATHNAME' 
LANGUAGE C 
IMMUTABLE STRICT 
PARALLEL SAFE;

CREATE FUNCTION gin_extract_gtype(gtype, internal) RETURNS internal 
AS 'MODULE_PATHNAME' 
LANGUAGE C 
IMMUTABLE 
STRICT 
PARALLEL SAFE;

CREATE FUNCTION gin_extract_gtype_query(gtype, internal, int2, internal, internal) 
RETURNS internal 
AS 'MODULE_PATHNAME' 
LANGUAGE C 
IMMUTABLE 
STRICT 
PARALLEL SAFE;

CREATE FUNCTION gin_consistent_gtype(internal, int2, gtype, int4, internal, internal) 
RETURNS bool 
AS 'MODULE_PATHNAME' 
LANGUAGE C 
IMMUTABLE 
STRICT 
PARALLEL SAFE;

CREATE FUNCTION gin_triconsistent_gtype(internal, int2, gtype, int4, internal, internal, internal) 
RETURNS bool 
LANGUAGE C 
IMMUTABLE 
STRICT 
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR CLASS gin_gtype_ops 
DEFAULT FOR TYPE gtype 
USING gin 
AS
    OPERATOR 7 @>,
    OPERATOR 9 ?(gtype, gtype),
    /*OPERATOR 10 ?|(gtype, gtype),
    OPERATOR 11 ?&(gtype, gtype),*/
    FUNCTION 1 gin_compare_gtype,
    FUNCTION 2 gin_extract_gtype,
    FUNCTION 3 gin_extract_gtype_query,
    FUNCTION 4 gin_consistent_gtype,
    FUNCTION 6 gin_triconsistent_gtype,
STORAGE text;
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
 */


--
-- edge access opertators
--
CREATE FUNCTION edge_property_access_gtype(edge, gtype) 
RETURNS gtype
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (LEFTARG = edge, RIGHTARG = gtype, FUNCTION = edge_property_access_gtype);


--
-- edge equality operators
--
CREATE FUNCTION edge_eq(edge, edge) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
    FUNCTION = edge_eq, 
    LEFTARG = edge, 
    RIGHTARG = edge, 
    COMMUTATOR = =, 
    NEGATOR = <>, 
    RESTRICT = eqsel, 
    JOIN = eqjoinsel, 
    HASHES, 
    MERGES
);

CREATE FUNCTION edge_ne(edge, edge) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
    FUNCTION = edge_ne, 
    LEFTARG = edge, 
    RIGHTARG = edge, 
    COMMUTATOR = <>, 
    NEGATOR = =, 
    RESTRICT = neqsel, 
    JOIN = neqjoinsel
);

CREATE FUNCTION edge_gt(edge, edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR > (
  FUNCTION = edge_gt,
  LEFTARG = edge,
  RIGHTARG = edge,
  COMMUTATOR = >,
  NEGATOR = <=,
  RESTRICT = scalargtsel,
  JOIN = scalargtjoinsel
);

CREATE FUNCTION edge_ge(edge, edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR >= (
  FUNCTION = edge_ge,
  LEFTARG = edge,
  RIGHTARG = edge,
  COMMUTATOR = >=,
  NEGATOR = <,
  RESTRICT = scalargesel,
  JOIN = scalargejoinsel
);


CREATE FUNCTION edge_lt(edge, edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR < (
  FUNCTION = edge_lt,
  LEFTARG = edge,
  RIGHTARG = edge,
  COMMUTATOR = <,
  NEGATOR = >=,
  RESTRICT = scalarltsel,
  JOIN = scalarltjoinsel
);

CREATE FUNCTION edge_le(edge, edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <= (
  FUNCTION = edge_le,
  LEFTARG = edge,
  RIGHTARG = edge,
  COMMUTATOR = <=,
  NEGATOR = >,
  RESTRICT = scalarlesel,
  JOIN = scalarlejoinsel
);

--
-- edge - B-tree support functions
--
-- comparison support
CREATE FUNCTION edge_btree_cmp(edge, edge)
RETURNS int
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

-- sort support
CREATE FUNCTION edge_btree_sort(internal)
RETURNS void
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE AS 'MODULE_PATHNAME';

--
-- define operator classes for graphid
--
CREATE OPERATOR CLASS edge_ops
DEFAULT FOR TYPE edge
USING btree AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 edge_btree_cmp (edge, edge),
    FUNCTION 2 edge_btree_sort (internal);

--
-- edge functions
--
CREATE FUNCTION id(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_id';

CREATE FUNCTION start_id(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_start_id';

CREATE FUNCTION end_id(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_end_id';

CREATE FUNCTION label(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS 
NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_label';

CREATE FUNCTION properties(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_properties';

CREATE FUNCTION age_properties(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_properties';

CREATE FUNCTION startnode(edge) 
RETURNS vertex 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_startnode';

CREATE FUNCTION endnode(edge) 
RETURNS vertex 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_endnode';


CREATE FUNCTION type(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME','edge_label';

CREATE FUNCTION keys(edge) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'edge_keys';
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
 */

--
-- vertex - equality operators (=, <>)
--
CREATE FUNCTION vertex_eq(vertex, vertex)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
    FUNCTION = vertex_eq,
    LEFTARG = vertex,
    RIGHTARG = vertex, 
    COMMUTATOR = =, 
    NEGATOR = <>, 
    RESTRICT = eqsel, 
    JOIN = eqjoinsel,
    HASHES, 
    MERGES
);

CREATE FUNCTION vertex_ne(vertex, vertex)
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
    FUNCTION = vertex_ne, 
    LEFTARG = vertex, 
    RIGHTARG = vertex, 
    COMMUTATOR = <>, 
    NEGATOR = =, 
    RESTRICT = neqsel, 
    JOIN = neqjoinsel
);

CREATE FUNCTION vertex_lt(vertex, vertex)
RETURNS boolean
LANGUAGE c 
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR < (
    FUNCTION = vertex_lt, 
    LEFTARG = vertex, 
    RIGHTARG = vertex, 
    COMMUTATOR = >, 
    NEGATOR = >=, 
    RESTRICT = scalarltsel, 
    JOIN = scalarltjoinsel
);

CREATE FUNCTION vertex_gt(vertex, vertex)
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR > (
    FUNCTION = vertex_gt, 
    LEFTARG = vertex, 
    RIGHTARG = vertex, 
    COMMUTATOR = <, 
    NEGATOR = <=, 
    RESTRICT = scalargtsel, 
    JOIN = scalargtjoinsel
);

CREATE FUNCTION vertex_le(vertex, vertex)
RETURNS boolean
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <= (
    FUNCTION = vertex_le, 
    LEFTARG = vertex, 
    RIGHTARG = vertex, 
    COMMUTATOR = >=, 
    NEGATOR = >, 
    RESTRICT = scalarlesel, 
    JOIN = scalarlejoinsel
);

CREATE FUNCTION vertex_ge(vertex, vertex)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR >= (
    FUNCTION = vertex_ge, 
    LEFTARG = vertex, 
    RIGHTARG = vertex, 
    COMMUTATOR = <=, 
    NEGATOR = <, 
    RESTRICT = scalargesel, 
    JOIN = scalargejoinsel
);

CREATE FUNCTION vertex_btree_cmp(vertex, vertex)
RETURNS INTEGER 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'vertex_btree_cmp';

CREATE OPERATOR CLASS vertex_ops_btree
DEFAULT FOR TYPE vertex 
USING btree 
AS 
OPERATOR 1 <, 
OPERATOR 2 <=, 
OPERATOR 3 =, 
OPERATOR 4 >, 
OPERATOR 5 >=,
FUNCTION 1 vertex_btree_cmp(vertex, vertex);

--
-- vertex - access operators (->, ->> )
--
CREATE FUNCTION vertex_property_access(vertex, text) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (
    LEFTARG = vertex, 
    RIGHTARG = text, 
    FUNCTION = vertex_property_access
);

CREATE FUNCTION vertex_property_access_gtype(vertex, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR -> (
    LEFTARG = vertex, 
    RIGHTARG = gtype, 
    FUNCTION = vertex_property_access_gtype
);

CREATE FUNCTION vertex_property_access_text(vertex, text) 
RETURNS text 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ->> (
    LEFTARG = vertex, 
    RIGHTARG = text, 
    FUNCTION = vertex_property_access_text
);


--
-- vertex - contains operators (@>, <@)
--
CREATE FUNCTION vertex_contains(vertex, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR @> (
    LEFTARG = vertex, 
    RIGHTARG = gtype, 
    FUNCTION = vertex_contains, 
    COMMUTATOR = '<@', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION vertex_contained_by(gtype, vertex) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR <@ (
    LEFTARG = gtype, 
    RIGHTARG = vertex, 
    FUNCTION = vertex_contained_by, 
    COMMUTATOR = '@>', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

--
-- vertex - key existence operators (?, ?|, ?&)
--
CREATE FUNCTION vertex_exists(vertex, text)
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ? (
    LEFTARG = vertex, 
    RIGHTARG = text, 
    FUNCTION = vertex_exists, 
    COMMUTATOR = '?', 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION vertex_exists_any(vertex, text[]) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ?| (
    LEFTARG = vertex, 
    RIGHTARG = text[], 
    FUNCTION = vertex_exists_any, 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

CREATE FUNCTION vertex_exists_all(vertex, text[]) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ?& (
    LEFTARG = vertex, 
    RIGHTARG = text[], 
    FUNCTION = vertex_exists_all, 
    RESTRICT = contsel, 
    JOIN = contjoinsel
);

--
-- vertex functions
--
CREATE FUNCTION id(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'vertex_id';

CREATE FUNCTION label(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'vertex_label';

CREATE FUNCTION properties(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'vertex_properties';

CREATE FUNCTION age_properties(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'vertex_properties';

CREATE FUNCTION type(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME','vertex_label';

CREATE FUNCTION keys(vertex) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE AS 'MODULE_PATHNAME', 'vertex_keys';/*
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
 */

--
-- variable edge - equality operators (=, <>, >)
--
CREATE FUNCTION variable_edge_eq(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
  FUNCTION = variable_edge_eq,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = =,
  NEGATOR = <>,
  RESTRICT = eqsel,
  JOIN = eqjoinsel,
  HASHES
);

CREATE FUNCTION variable_edge_ne(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
  FUNCTION = variable_edge_ne,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = <>,
  NEGATOR = =,
  RESTRICT = neqsel,
  JOIN = neqjoinsel,
  HASHES
);

CREATE FUNCTION variable_edge_gt(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR > (
  FUNCTION = variable_edge_gt,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = >,
  NEGATOR = <=,
  RESTRICT = scalargtsel,
  JOIN = scalargtjoinsel
);

CREATE FUNCTION variable_edge_ge(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR >= (
  FUNCTION = variable_edge_ge,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = >=,
  NEGATOR = <,
  RESTRICT = scalargesel,
  JOIN = scalargejoinsel
);


CREATE FUNCTION variable_edge_lt(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR < (
  FUNCTION = variable_edge_lt,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = <,
  NEGATOR = >=,
  RESTRICT = scalarltsel,
  JOIN = scalarltjoinsel
);

CREATE FUNCTION variable_edge_le(variable_edge, variable_edge) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <= (
  FUNCTION = variable_edge_le,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge,
  COMMUTATOR = <=,
  NEGATOR = >,
  RESTRICT = scalarlesel,
  JOIN = scalarlejoinsel
);

--
-- edge - B-tree support functions
--
-- comparison support
CREATE FUNCTION variable_edge_btree_cmp(variable_edge, variable_edge)
RETURNS int
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

-- sort support
CREATE FUNCTION variable_edge_btree_sort(internal)
RETURNS void
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE AS 'MODULE_PATHNAME';

--
-- define operator classes for graphid
--
CREATE OPERATOR CLASS variable_edge_ops
DEFAULT FOR TYPE variable_edge
USING btree AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 variable_edge_btree_cmp (variable_edge, variable_edge),
    FUNCTION 2 variable_edge_btree_sort (internal);

--  
-- variable_edge containment operators (@>, @<, @@)
--  
CREATE FUNCTION edge_contained_in_variable_edge(edge, variable_edge)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR @> (
  FUNCTION = edge_contained_in_variable_edge,
  LEFTARG = edge,
  RIGHTARG = variable_edge,
  --COMMUTATOR = '<@',
  RESTRICT = contsel,  
  JOIN = contjoinsel
);  
    
CREATE FUNCTION variable_edge_contains_edge(variable_edge, edge)
RETURNS boolean 
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <@ (
  FUNCTION = variable_edge_contains_edge,
  LEFTARG = variable_edge,
  RIGHTARG = edge,
  COMMUTATOR = '@>',
  RESTRICT = contsel,
  JOIN = contjoinsel 
);  

CREATE FUNCTION variable_edge_edges_overlap(variable_edge, variable_edge)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR && (
  FUNCTION = variable_edge_edges_overlap,
  LEFTARG = variable_edge,
  RIGHTARG = variable_edge
);

--
-- variable_edge functions
--
CREATE FUNCTION nodes(variable_edge) RETURNS vertex[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'variable_edge_nodes';

CREATE FUNCTION relationships(variable_edge) RETURNS edge[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'variable_edge_edges';

CREATE FUNCTION edges(variable_edge) RETURNS edge[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'variable_edge_edges';

CREATE FUNCTION length(variable_edge) RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'variable_edge_length';

CREATE OPERATOR @-@ (
    FUNCTION = length,
    RIGHTARG = variable_edge
);

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
 */


--
-- traversal - equality operators (=, <>, >)
--
CREATE FUNCTION traversal_eq(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR = (
  FUNCTION = traversal_eq,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = =,
  NEGATOR = <>,
  RESTRICT = eqsel,
  JOIN = eqjoinsel,
  HASHES
);

CREATE FUNCTION traversal_ne(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <> (
  FUNCTION = traversal_ne,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = <>,
  NEGATOR = =,
  RESTRICT = neqsel,
  JOIN = neqjoinsel,
  HASHES
);

CREATE FUNCTION traversal_gt(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR > (
  FUNCTION = traversal_gt,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = >,
  NEGATOR = <=,
  RESTRICT = scalargtsel,
  JOIN = scalargtjoinsel
);

CREATE FUNCTION traversal_ge(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR >= (
  FUNCTION = traversal_ge,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = >=,
  NEGATOR = <,
  RESTRICT = scalargesel,
  JOIN = scalargejoinsel
);


CREATE FUNCTION traversal_lt(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR < (
  FUNCTION = traversal_lt,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = <,
  NEGATOR = >=,
  RESTRICT = scalarltsel,
  JOIN = scalarltjoinsel
);

CREATE FUNCTION traversal_le(traversal, traversal) RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <= (
  FUNCTION = traversal_le,
  LEFTARG = traversal,
  RIGHTARG = traversal,
  COMMUTATOR = <=,
  NEGATOR = >,
  RESTRICT = scalarlesel,
  JOIN = scalarlejoinsel
);

--
-- traversal - B-tree support functions
--
-- comparison support
CREATE FUNCTION traversal_btree_cmp(traversal, traversal)
RETURNS int
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

-- sort support
CREATE FUNCTION traversal_btree_sort(internal)
RETURNS void
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE AS 'MODULE_PATHNAME';

--
-- define operator classes for graphid
--
CREATE OPERATOR CLASS traversal_ops
DEFAULT FOR TYPE traversal
USING btree AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 traversal_btree_cmp (traversal, traversal),
    FUNCTION 2 traversal_btree_sort (internal);

--
-- traversal functions
--
CREATE FUNCTION relationships(traversal) RETURNS edge[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'traversal_edges';

CREATE FUNCTION nodes(traversal) RETURNS vertex[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'traversal_nodes';

CREATE FUNCTION size(traversal) RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'traversal_size';

CREATE FUNCTION length(traversal) RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'traversal_length';

CREATE OPERATOR @-@ (
    FUNCTION = length,
    RIGHTARG = traversal
);
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
 */
 
--
-- graphid typecasting
--
CREATE FUNCTION graphid_to_gtype(graphid) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (graphid AS gtype) WITH FUNCTION graphid_to_gtype(graphid);

CREATE FUNCTION gtype_to_graphid(gtype) 
RETURNS graphid 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS graphid) WITH FUNCTION gtype_to_graphid(gtype) AS IMPLICIT;


--
-- gtype typecasting
--
-- gtype -> text
CREATE FUNCTION gtype_to_text(gtype) 
RETURNS text 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS text) 
WITH FUNCTION gtype_to_text(gtype);

-- gtype -> text[]
CREATE FUNCTION gtype_to_text_array(gtype)
RETURNS text[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS text[])
WITH FUNCTION gtype_to_text_array(gtype);

-- text[] -> gtype
CREATE FUNCTION text_array_to_gtype(text[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (text[] as gtype)
WITH FUNCTION text_array_to_gtype(text[]);

-- text -> gtype
CREATE FUNCTION text_to_gtype(text) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (text AS gtype) 
WITH FUNCTION text_to_gtype(text);

-- gtype -> boolean
CREATE FUNCTION gtype_to_bool(gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS boolean) 
WITH FUNCTION gtype_to_bool(gtype) 
AS IMPLICIT;

-- boolean -> gtype
CREATE FUNCTION bool_to_gtype(boolean) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (boolean AS gtype) 
WITH FUNCTION bool_to_gtype(boolean);

-- boolean[] -> gtype
CREATE FUNCTION bool_array_to_gtype(boolean[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (boolean[] AS gtype)
WITH FUNCTION bool_array_to_gtype(boolean[]);


-- float8 -> gtype
CREATE FUNCTION float8_to_gtype(float8) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (float8 AS gtype) 
WITH FUNCTION float8_to_gtype(float8);

-- gtype -> float8
CREATE FUNCTION gtype_to_float8(gtype) 
RETURNS float8 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS float8) 
WITH FUNCTION gtype_to_float8(gtype);

-- float8[] -> gtype
CREATE FUNCTION float8_array_to_gtype(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (float8[] as gtype)
WITH FUNCTION float8_array_to_gtype(float8[]);


-- gtype -> float8[]
CREATE FUNCTION gtype_to_float8_array(gtype)
RETURNS float8[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS float8[])
WITH FUNCTION gtype_to_float8_array(gtype);

-- gtype -> float4[]
CREATE FUNCTION gtype_to_float4_array(gtype)
RETURNS float4[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS float4[])
WITH FUNCTION gtype_to_float4_array(gtype);

-- float4[] -> gtype
CREATE FUNCTION float4_array_to_gtype(float4[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (float4[] as gtype)
WITH FUNCTION float4_array_to_gtype(float4[]);

-- gtype -> numeric
CREATE FUNCTION gtype_to_numeric(gtype)
RETURNS numeric
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS numeric)
WITH FUNCTION gtype_to_numeric(gtype);

-- gtype -> numeric[]
CREATE FUNCTION gtype_to_numeric_array(gtype)
RETURNS numeric[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS numeric[])
WITH FUNCTION gtype_to_numeric_array(gtype);

-- numeric[] -> gtype
CREATE FUNCTION numeric_array_to_gtype(numeric[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (numeric[] as gtype)
WITH FUNCTION numeric_array_to_gtype(numeric[]);

-- int8 -> gtype
CREATE FUNCTION int8_to_gtype(int8) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (int8 AS gtype) 
WITH FUNCTION int8_to_gtype(int8);

-- gtype -> int8
CREATE FUNCTION gtype_to_int8(gtype) 
RETURNS bigint 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS bigint) 
WITH FUNCTION gtype_to_int8(gtype) 
AS ASSIGNMENT;

-- int8[] -> gtype
CREATE FUNCTION int8_array_to_gtype(int8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (int8[] as gtype)
WITH FUNCTION int8_array_to_gtype(int8[]);

-- int4[] -> gtype
CREATE FUNCTION int4_array_to_gtype(int4[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (int4[] as gtype)
WITH FUNCTION int4_array_to_gtype(int4[]);

-- int2[] -> gtype
CREATE FUNCTION int2_array_to_gtype(int2[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (int2[] as gtype)
WITH FUNCTION int2_array_to_gtype(int2[]);


-- timestamp -> gtype
CREATE FUNCTION timestamp_to_gtype(timestamp) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (timestamp AS gtype) 
WITH FUNCTION timestamp_to_gtype(timestamp);

-- timestamp[] -> gtype
CREATE FUNCTION timestamp_array_to_gtype(timestamp[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (timestamp[] as gtype)
WITH FUNCTION timestamp_array_to_gtype(timestamp[]);

-- timestamptz -> gtype
CREATE FUNCTION timestamptz_to_gtype(timestamptz) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (timestamptz AS gtype) 
WITH FUNCTION timestamptz_to_gtype(timestamptz);

-- timestamptz[] -> gtype
CREATE FUNCTION timestamptz_array_to_gtype(timestamptz[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (timestamptz[] as gtype)
WITH FUNCTION timestamptz_array_to_gtype(timestamptz[]);

-- date -> gtype
CREATE FUNCTION date_to_gtype(date) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (date AS gtype) 
WITH FUNCTION date_to_gtype(date);

-- date[] -> gtype
CREATE FUNCTION date_array_to_gtype(date[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (date[] as gtype)
WITH FUNCTION date_array_to_gtype(date[]);


-- time -> gtype
CREATE FUNCTION time_to_gtype(time) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (time AS gtype) 
WITH FUNCTION time_to_gtype(time);

-- time[] -> gtype
CREATE FUNCTION time_array_to_gtype(time[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (time[] as gtype)
WITH FUNCTION time_array_to_gtype(time[]);

-- timetz -> gtype
CREATE FUNCTION timetz_to_gtype(timetz) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (timetz AS gtype) 
WITH FUNCTION timetz_to_gtype(timetz);

-- timetz[] -> gtype
CREATE FUNCTION timetz_array_to_gtype(timetz[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (timetz[] as gtype)
WITH FUNCTION timetz_array_to_gtype(timetz[]);

-- gtype -> timestamp
CREATE FUNCTION gtype_to_timestamp(gtype)
RETURNS timestamp
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS timestamp)
WITH FUNCTION gtype_to_timestamp(gtype);

-- gtype -> date
CREATE FUNCTION gtype_to_date(gtype)
RETURNS date
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS date)
WITH FUNCTION gtype_to_date(gtype);

-- gtype -> timestamptz
CREATE FUNCTION gtype_to_timestamptz(gtype)
RETURNS timestamptz
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS timestamptz)
WITH FUNCTION gtype_to_timestamptz(gtype);

-- gtype -> time
CREATE FUNCTION gtype_to_time(gtype)
RETURNS time
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS time)
WITH FUNCTION gtype_to_time(gtype);

-- gtype -> timetz
CREATE FUNCTION gtype_to_timetz(gtype)
RETURNS timetz
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS timetz)
WITH FUNCTION gtype_to_timetz(gtype);

-- interval -> gtype
CREATE FUNCTION interval_to_gtype(interval)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (interval AS gtype)
WITH FUNCTION interval_to_gtype(interval);

-- gtype -> interval
CREATE FUNCTION gtype_to_interval(gtype)
RETURNS interval
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS interval)
WITH FUNCTION gtype_to_interval(gtype);

-- interval[] -> gtype
CREATE FUNCTION interval_array_to_gtype(interval[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'array_to_gtype';

CREATE CAST (interval[] as gtype)
WITH FUNCTION interval_array_to_gtype(interval[]);



-- gtype -> int8[]
CREATE FUNCTION gtype_to_int8_array(gtype) 
RETURNS bigint[] 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS bigint[]) 
WITH FUNCTION gtype_to_int8_array(gtype);

-- gtype -> int4
CREATE FUNCTION gtype_to_int4(gtype) 
RETURNS int 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS int) WITH FUNCTION gtype_to_int4(gtype) AS IMPLICIT;

-- gtype -> int2
CREATE FUNCTION gtype_to_int2(gtype) 
RETURNS smallint 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS smallint) 
WITH FUNCTION gtype_to_int2(gtype);

-- gtype -> int2[]
CREATE FUNCTION gtype_to_int2_array(gtype)
RETURNS smallint[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS smallint[]) WITH FUNCTION gtype_to_int2_array(gtype);

-- gtype -> int4[]
CREATE FUNCTION gtype_to_int4_array(gtype) 
RETURNS int[] 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE CAST (gtype AS int[]) WITH FUNCTION gtype_to_int4_array(gtype);

-- inet -> gtype
CREATE FUNCTION inet_to_gtype(inet)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (inet as gtype) WITH FUNCTION inet_to_gtype(inet);

-- gtype -> inet
CREATE FUNCTION gtype_to_inet(gtype)
RETURNS inet
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as inet) WITH FUNCTION gtype_to_inet(gtype);

-- gtype -> cidr
CREATE FUNCTION gtype_to_cidr(gtype)
RETURNS cidr
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as cidr) WITH FUNCTION gtype_to_cidr(gtype);

-- point -> gtype
CREATE FUNCTION point_to_gtype(point)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (point AS gtype)
WITH FUNCTION point_to_gtype(point);


-- gtype -> point
CREATE FUNCTION gtype_to_point(gtype)
RETURNS point
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as point) WITH FUNCTION gtype_to_point(gtype);

-- path -> gtype
CREATE FUNCTION path_to_gtype(path)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (path AS gtype)
WITH FUNCTION path_to_gtype(path);

-- gtype -> path
CREATE FUNCTION gtype_to_path(gtype)
RETURNS path
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as path) WITH FUNCTION gtype_to_path(gtype);

-- polygon -> gtype
CREATE FUNCTION polygon_to_gtype(polygon)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (polygon AS gtype)
WITH FUNCTION polygon_to_gtype(polygon);

-- gtype -> polygon
CREATE FUNCTION gtype_to_polygon(gtype)
RETURNS polygon
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as polygon) WITH FUNCTION gtype_to_polygon(gtype);


-- geometry -> gtype
/*CREATE FUNCTION geometry_to_gtype(public.geometry)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (public.geometry AS gtype)
WITH FUNCTION geometry_to_gtype(public.geometry);

-- gtype -> geometry
CREATE FUNCTION gtype_to_geometry(gtype)
RETURNS public.geometry
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as public.geometry) WITH FUNCTION gtype_to_geometry(gtype);

-- gtype -> box3d
CREATE FUNCTION gtype_to_box3d(gtype)
RETURNS public.box3d
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as public.box3d) WITH FUNCTION gtype_to_box3d(gtype);

-- gtype -> box2d
CREATE FUNCTION gtype_to_box2d(gtype)
RETURNS public.box2d
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as public.box2d) WITH FUNCTION gtype_to_box2d(gtype);
*/
-- box -> gtype
/*
CREATE FUNCTION box_to_gtype(box)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (box AS gtype)
WITH FUNCTION box_to_gtype(box);

-- gtype -> box
CREATE FUNCTION gtype_to_box(gtype)
RETURNS box
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as box) WITH FUNCTION gtype_to_box(gtype);
*/
-- tsvector -> gtype
CREATE FUNCTION tsvector_to_gtype(tsvector)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (tsvector as gtype) WITH FUNCTION tsvector_to_gtype(tsvector);

-- gtype -> tsvector
CREATE FUNCTION gtype_to_tsvector(gtype)
RETURNS tsvector
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as tsvector) WITH FUNCTION gtype_to_tsvector(gtype);

-- tsquery -> gtype
CREATE FUNCTION tsquery_to_gtype(tsquery)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (tsquery as gtype) WITH FUNCTION tsquery_to_gtype(tsquery);

-- gtype -> tsquery
CREATE FUNCTION gtype_to_tsquery(gtype)
RETURNS tsquery
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE CAST (gtype as tsquery) WITH FUNCTION gtype_to_tsquery(gtype);

--
-- gtype - typecasting to other gtype types
--
CREATE FUNCTION toboolean (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_toboolean';

CREATE FUNCTION tofloat (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_tofloat';

CREATE FUNCTION tointeger(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_tointeger';

CREATE FUNCTION tostring (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_tostring';

CREATE FUNCTION tobytea (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_tobytea';

CREATE FUNCTION tonumeric (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_tonumeric';

CREATE FUNCTION totimestamp (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_totimestamp';

CREATE FUNCTION totimestamptz (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME';

CREATE FUNCTION totime(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME';

CREATE FUNCTION totimetz(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME';

CREATE FUNCTION tointerval(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME';

CREATE FUNCTION todate(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME';

CREATE FUNCTION tovector(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME';

CREATE FUNCTION toinet(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_toinet';

CREATE FUNCTION tocidr(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tocidr';

CREATE FUNCTION tomacaddr(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tomacaddr';

CREATE FUNCTION tomacaddr8(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tomacaddr8';

CREATE FUNCTION topoint(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_topoint';

CREATE FUNCTION tolseg(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tolseg';

CREATE FUNCTION toline(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_toline';

CREATE FUNCTION topath(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_topath';

CREATE FUNCTION topolygon(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_topolygon';

CREATE FUNCTION tocircle(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tocircle';

CREATE FUNCTION tobox(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tobox';
/*
CREATE FUNCTION tobox2d(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tobox2d';

CREATE FUNCTION tobox3d(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tobox3d';

CREATE FUNCTION tospheroid(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tospheroid';

CREATE FUNCTION togeometry(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_togeometry';
*/
CREATE FUNCTION totsvector(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totsvector';

CREATE FUNCTION totsquery(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totsquery';


CREATE FUNCTION tointrange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tointrange';

CREATE FUNCTION tointmultirange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tointmultirange';

CREATE FUNCTION tonumrange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tonumrange';

CREATE FUNCTION tonummultirange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_tonummultirange';

CREATE FUNCTION totsrange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totsrange';

CREATE FUNCTION totsmultirange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totsmultirange';

CREATE FUNCTION totstzrange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totstzrange';

CREATE FUNCTION totstzmultirange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_totstzmultirange';

CREATE FUNCTION todaterange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_todaterange';

CREATE FUNCTION todatemultirange(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_todatemultirange';



--
-- gtype - list literal (`[expr, ...]`)
--

CREATE FUNCTION gtype_build_list(VARIADIC "any") 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
CALLED ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_build_list() 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_build_list_noargs';


CREATE FUNCTION gtype_access_slice(gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_in_operator(gtype, gtype) 
RETURNS bool 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR @= (
    FUNCTION = gtype_in_operator, 
    LEFTARG = gtype, 
    RIGHTARG = gtype, 
    NEGATOR = !@=, 
    RESTRICT = eqsel, 
    JOIN = eqjoinsel, 
    HASHES, 
    MERGES
);

--
-- Scalar Functions
--
CREATE FUNCTION head (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_head';

CREATE FUNCTION last (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_last';

CREATE FUNCTION size (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_size';

--
-- list functions (reverse is defined in string functions)
--

-- keys
CREATE FUNCTION keys (gtype)
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_keys';

--range
CREATE FUNCTION range (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_range';

CREATE FUNCTION range (gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_range_w_skip';/*
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
 */
 
 --
-- gtype - string matching (`STARTS WITH`, `ENDS WITH`, `CONTAINS`, & =~)
--
-- TODO: Operators ^=, $=, @=, PostGres Regex
--
CREATE FUNCTION gtype_string_match_starts_with(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_string_match_ends_with(gtype, gtype)
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_string_match_contains(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

--
-- LIKE
--
CREATE FUNCTION gtype_like(gtype, gtype)
RETURNS BOOLEAN
LANGUAGE C
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR ~~~ (
    FUNCTION = gtype_like,
    LEFTARG = gtype,
    RIGHTARG = gtype,
    RESTRICT = LIKESEL,
    JOIN = LIKEJOINSEL,
    NEGATOR = !~~
);

CREATE FUNCTION gtype_not_like(gtype, gtype)
RETURNS BOOLEAN
LANGUAGE C
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR !~~ (
    FUNCTION = gtype_not_like,
    LEFTARG = gtype,
    RIGHTARG = gtype,
    RESTRICT = NLIKESEL,
    JOIN = NLIKEJOINSEL,
    NEGATOR = ~~~
);


CREATE FUNCTION gtype_ilike(gtype, gtype)
RETURNS BOOLEAN
LANGUAGE C
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR ~~* (
    FUNCTION = gtype_ilike,
    LEFTARG = gtype,
    RIGHTARG = gtype,
    RESTRICT = LIKESEL,
    JOIN = LIKEJOINSEL,
    NEGATOR = !~~*
);

CREATE FUNCTION gtype_not_ilike(gtype, gtype)
RETURNS BOOLEAN
LANGUAGE C
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR !~~* (
    FUNCTION = gtype_not_ilike,
    LEFTARG = gtype,
    RIGHTARG = gtype,
    RESTRICT = NLIKESEL,
    JOIN = NLIKEJOINSEL,
    NEGATOR = ~~*
);

--
-- Regex
--
CREATE FUNCTION regex_match_case_sensitive(gtype, gtype) 
RETURNS boolean 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
COST 1
AS 'MODULE_PATHNAME', 'gtype_eq_tilde';

CREATE OPERATOR ~ (
    FUNCTION = regex_match_case_sensitive, 
    LEFTARG = gtype,  
    RIGHTARG = gtype
);

CREATE OPERATOR =~ (
    FUNCTION = regex_match_case_sensitive,
    LEFTARG = gtype,
    RIGHTARG = gtype
);

CREATE FUNCTION regex_match_case_insensitive(gtype, gtype)
RETURNS boolean  
LANGUAGE c  
IMMUTABLE 
PARALLEL SAFE  
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_match_case_insensitive';

CREATE OPERATOR ~* (
    FUNCTION = regex_match_case_insensitive,
    LEFTARG = gtype, 
    RIGHTARG = gtype
);

CREATE FUNCTION regex_not_cs(gtype, gtype) 
RETURNS boolean
LANGUAGE c  
IMMUTABLE 
PARALLEL SAFE  
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_regex_not_cs';

CREATE OPERATOR !~ (
    FUNCTION = regex_not_cs,
    LEFTARG = gtype,
    RIGHTARG = gtype
);

CREATE FUNCTION regex_not_ci(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_regex_not_ci';

CREATE OPERATOR !~* (
    FUNCTION = regex_not_ci,
    LEFTARG = gtype,
    RIGHTARG = gtype
);

--
-- String functions
--
CREATE FUNCTION reverse (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_reverse';

CREATE FUNCTION toupper (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_toupper';

CREATE FUNCTION tolower (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_tolower';

CREATE FUNCTION ltrim (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_ltrim';

CREATE FUNCTION rtrim (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_rtrim';

CREATE FUNCTION "trim" (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_trim';

CREATE FUNCTION right (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_right';

CREATE FUNCTION left (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_left';

CREATE FUNCTION initcap (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_initcap';

CREATE FUNCTION "substring" (gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_substring_w_len';

CREATE FUNCTION "substring" (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_substring';

CREATE FUNCTION split (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_split';

CREATE FUNCTION replace (gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_replace';

CREATE FUNCTION sha224 (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_sha224';

CREATE FUNCTION sha256 (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_sha256';

CREATE FUNCTION sha384 (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_sha384';

CREATE FUNCTION sha512 (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_sha512';

CREATE FUNCTION md5 (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_md5';

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
 */
 
--
-- Trig functions - radian input
--
CREATE FUNCTION sin (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_sin';

CREATE FUNCTION cos (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_cos';

CREATE FUNCTION tan (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_tan';

CREATE FUNCTION cot (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_cot';

CREATE FUNCTION asin (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_asin';

CREATE FUNCTION acos (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_acos';

CREATE FUNCTION atan (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_atan';

CREATE FUNCTION atan2 (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_atan2';

CREATE FUNCTION sinh (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_sinh';

CREATE FUNCTION cosh (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_cosh';

CREATE FUNCTION tanh (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tanh';

CREATE FUNCTION asinh (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_asinh';

CREATE FUNCTION acosh (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_acosh';

CREATE FUNCTION atanh (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_atanh';

CREATE FUNCTION degrees (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_degrees';

CREATE FUNCTION radians (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_radians';

CREATE FUNCTION round (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_round';

CREATE FUNCTION round (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_round_w_precision';

CREATE FUNCTION ceil (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_ceil';

CREATE FUNCTION ceiling (gtype)
RETURNS gtype
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_ceil';

CREATE FUNCTION floor (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_floor';

CREATE FUNCTION abs (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_abs';

CREATE FUNCTION sign (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_sign';

CREATE FUNCTION log (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_log';

CREATE FUNCTION log10 (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_log10';

CREATE FUNCTION e () 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_e';

CREATE FUNCTION exp (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_exp';

CREATE FUNCTION sqrt (gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
RETURNS NULL ON NULL INPUT 
AS 'MODULE_PATHNAME', 'gtype_sqrt';

CREATE FUNCTION cbrt (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS 'MODULE_PATHNAME', 'gtype_cbrt';


CREATE FUNCTION pi () 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_pi';

CREATE FUNCTION rand() 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_rand';

CREATE FUNCTION gcd (gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_gcd';

CREATE FUNCTION lcm (gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_lcm';

CREATE FUNCTION factorial (gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_factorial';

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
 */
 
--
-- Temporal Functions
--
CREATE FUNCTION age(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_age_w2args';

CREATE FUNCTION age(gtype) 
RETURNS gtype 
LANGUAGE c 
STABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_age_today';

CREATE FUNCTION "extract"(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_extract';

CREATE FUNCTION date_part(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_date_part';

CREATE FUNCTION date_bin(gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_date_bin';

CREATE FUNCTION date_trunc(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_date_trunc';

CREATE FUNCTION date_trunc(gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_date_trunc_zone';

CREATE FUNCTION overlaps(gtype, gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_overlaps';

CREATE FUNCTION isfinite(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_isfinite';

CREATE FUNCTION justify_hours(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_justify_hours';

CREATE FUNCTION justify_days(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_justify_days';

CREATE FUNCTION justify_interval(gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_justify_interval';

CREATE FUNCTION make_date(gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_make_date';

CREATE FUNCTION make_time(gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_make_time';

CREATE FUNCTION make_timestamp(gtype, gtype, gtype, gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_make_timestamp';

CREATE FUNCTION make_timestamptz(gtype, gtype, gtype, gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
STABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_make_timestamptz';

CREATE FUNCTION make_timestamptz(gtype, gtype, gtype, gtype, gtype, gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
STABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME','gtype_make_timestamptz_wtimezone';

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
 */

CREATE FUNCTION gtype_bitwise_not(gtype) 
RETURNS gtype
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE OPERATOR ~ (FUNCTION = gtype_bitwise_not, RIGHTARG = gtype);

CREATE FUNCTION gtype_bitwise_and(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR & (FUNCTION = gtype_bitwise_and, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_bitwise_or(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR | (FUNCTION = gtype_bitwise_or, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_inet_subnet_strict_contains(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
COST 1
AS 'MODULE_PATHNAME';

CREATE OPERATOR << (
    FUNCTION = gtype_inet_subnet_strict_contains,
    LEFTARG = gtype,
    RIGHTARG = gtype,
    COMMUTATOR = '>>',
    RESTRICT = positionsel,
    JOIN = positionjoinsel
);

CREATE FUNCTION gtype_inet_subnet_contains(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR <<= (FUNCTION = gtype_inet_subnet_contains, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_inet_subnet_strict_contained_by(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR >> (FUNCTION = gtype_inet_subnet_strict_contained_by, LEFTARG = gtype, RIGHTARG = gtype);


CREATE FUNCTION gtype_inet_subnet_contained_by(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR >>= (FUNCTION = gtype_inet_subnet_contained_by, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_inet_subnet_contain_both(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE OPERATOR && (FUNCTION = gtype_inet_subnet_contain_both, LEFTARG = gtype, RIGHTARG = gtype);
 
--
-- Network Functions
--
CREATE FUNCTION abbrev(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_abbrev';

CREATE FUNCTION broadcast(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_broadcast';

CREATE FUNCTION family(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_family';

CREATE FUNCTION host(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_host';

CREATE FUNCTION hostmask(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_hostmask';

CREATE FUNCTION inet_merge(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_inet_merge';

CREATE FUNCTION inet_same_family(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_inet_same_family';

CREATE FUNCTION masklen(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_masklen';

CREATE FUNCTION netmask(gtype) 
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_netmask';

CREATE FUNCTION network(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_network';


CREATE FUNCTION set_masklen(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_set_masklen';

CREATE FUNCTION trunc(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_trunc';

CREATE FUNCTION macaddr8_set7bit(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_macaddr8_set7bit';
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
 */

--
-- Text Search Operators
--

CREATE FUNCTION gtype_tsquery_or(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsquery_or';

CREATE OPERATOR || (FUNCTION = gtype_tsquery_or, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_tsquery_not(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsquery_not';

CREATE OPERATOR !! (FUNCTION = gtype_tsquery_not, RIGHTARG = gtype);


--
-- Text Search Functions
--
CREATE FUNCTION ts_delete(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_ts_delete';

CREATE FUNCTION strip(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_ts_strip';

CREATE FUNCTION tsquery_phrase(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsquery_phrase';

CREATE FUNCTION tsquery_phrase(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsquery_phrase_distance';

CREATE FUNCTION plainto_tsquery(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_plainto_tsquery';

CREATE FUNCTION phraseto_tsquery(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_phraseto_tsquery';

CREATE FUNCTION websearch_to_tsquery(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_websearch_to_tsquery';


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
 */

--
-- Range Functions
--
CREATE FUNCTION intrange(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_intrange';

CREATE FUNCTION intrange(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_intrange';

CREATE FUNCTION numrange(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_numrange';

CREATE FUNCTION numrange(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_numrange';

CREATE FUNCTION tsrange(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsrange';

CREATE FUNCTION tsrange(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tsrange';

CREATE FUNCTION tstzrange(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tstzrange';

CREATE FUNCTION tstzrange(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_tstzrange';

CREATE FUNCTION daterange(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_daterange';

CREATE FUNCTION daterange(gtype, gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_daterange';

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
 */

--
-- Geometric Operators
--
CREATE FUNCTION intersection_point(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_intersection_point';

CREATE OPERATOR # (FUNCTION = intersection_point, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_closest_point(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_closest_point';

CREATE OPERATOR ## (FUNCTION = gtype_closest_point, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_vertical(gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_vertical';

CREATE OPERATOR ?| (FUNCTION = gtype_vertical, RIGHTARG = gtype);


CREATE FUNCTION gtype_horizontal(gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_horizontal';

CREATE OPERATOR ?- (FUNCTION = gtype_horizontal, RIGHTARG = gtype);


CREATE FUNCTION gtype_center(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_center';

CREATE OPERATOR @@ (FUNCTION = gtype_center, RIGHTARG = gtype);

CREATE FUNCTION gtype_distance(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_distance';

CREATE OPERATOR @-@ (FUNCTION = gtype_distance, RIGHTARG = gtype);

CREATE FUNCTION gtype_perp(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_perp';

CREATE OPERATOR ?-| (FUNCTION = gtype_perp, LEFTARG = gtype, RIGHTARG = gtype);

CREATE FUNCTION gtype_parallel(gtype, gtype)
RETURNS boolean
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_parallel';

CREATE OPERATOR ?|| (FUNCTION = gtype_parallel, LEFTARG = gtype, RIGHTARG = gtype);

--
-- Geometric Functions
--
CREATE FUNCTION height(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_height';

CREATE FUNCTION width(gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_width';

CREATE FUNCTION bound_box(gtype, gtype)
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_bound_box';

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
 */
--
-- Agreggation
--

-- accumlates floats into an array for aggregation
CREATE FUNCTION gtype_accum(float8[], gtype)
RETURNS float8[]
LANGUAGE c 
IMMUTABLE 
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

--
-- count
--
CREATE AGGREGATE count(*) (
     stype = int8,
     sfunc = int8inc,
     finalfunc = int8_to_gtype,
     combinefunc = int8pl,
     finalfunc_modify = READ_ONLY,
     initcond = 0,
     parallel = SAFE
);

CREATE AGGREGATE count(gtype) (
     stype = int8,
     sfunc = int8inc_any,
     finalfunc = int8_to_gtype,
     combinefunc = int8pl,
     finalfunc_modify = READ_ONLY,
     initcond = 0,
     parallel = SAFE
);

CREATE AGGREGATE count(vertex) (
    stype = int8, 
    sfunc = int8inc_any, 
    finalfunc = int8_to_gtype, 
    combinefunc = int8pl, 
    finalfunc_modify = READ_ONLY, 
    initcond = 0, 
    parallel = SAFE
);

CREATE AGGREGATE count(edge) (
    stype = int8,
    sfunc = int8inc_any, 
    finalfunc = int8_to_gtype, 
    combinefunc = int8pl, 
    finalfunc_modify = READ_ONLY, 
    initcond = 0, parallel = SAFE
);

CREATE AGGREGATE count(traversal) (
    stype = int8,
    sfunc = int8inc_any,
    finalfunc = int8_to_gtype,
    combinefunc = int8pl,
    finalfunc_modify = READ_ONLY,
    initcond = 0,
    parallel = SAFE
);

CREATE AGGREGATE count(variable_edge) (
    stype = int8,
    sfunc = int8inc_any,
    finalfunc = int8_to_gtype,
    combinefunc = int8pl,
    finalfunc_modify = READ_ONLY,
    initcond = 0,
    parallel = SAFE
);

--
-- stdev
--
CREATE FUNCTION gtype_stddev_samp(float8[]) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE AGGREGATE stdev(gtype) (
    stype = float8[],
    sfunc = gtype_accum, 
    finalfunc = gtype_stddev_samp, 
    combinefunc = float8_combine, 
    finalfunc_modify = READ_ONLY, 
    initcond = '{0,0,0}',
    parallel = SAFE
);

--
-- stdevp
--
CREATE FUNCTION gtype_stddev_pop(float8[])
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
CALLED ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE AGGREGATE stdevp(gtype) (
    stype = float8[],
    sfunc = gtype_accum, 
    finalfunc = gtype_stddev_pop, 
    combinefunc = float8_combine, 
    finalfunc_modify = READ_ONLY, 
    initcond = '{0,0,0}', 
    parallel = SAFE
);

--
-- avg
--
CREATE AGGREGATE avg(gtype) (
    stype = float8[], 
    sfunc = gtype_accum, 
    finalfunc = float8_avg, 
    combinefunc = float8_combine, 
    finalfunc_modify = READ_ONLY, 
    initcond = '{0,0,0}', 
    parallel = SAFE
);

--
-- sum
--
CREATE FUNCTION gtype_sum (gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
STRICT 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_gtype_sum';

CREATE AGGREGATE sum(gtype) (
    stype = gtype, 
    sfunc = gtype_sum, 
    combinefunc = gtype_sum, 
    finalfunc_modify = READ_ONLY, 
    parallel = SAFE
);

--
-- max
--
CREATE FUNCTION gtype_max_trans(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE AGGREGATE max(gtype) (
    stype = gtype, 
    sfunc = gtype_max_trans, 
    combinefunc = gtype_max_trans, 
    finalfunc_modify = READ_ONLY, 
    parallel = SAFE
);

--
-- min
--
CREATE FUNCTION gtype_min_trans(gtype, gtype) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME';

CREATE AGGREGATE min(gtype) (
    stype = gtype, 
    sfunc = gtype_min_trans, 
    combinefunc = gtype_min_trans, 
    finalfunc_modify = READ_ONLY, 
    parallel = SAFE
);

--
-- percentileCont and percentileDisc
--
CREATE FUNCTION percentile_aggtransfn (internal, gtype, gtype) 
RETURNS internal 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_percentile_aggtransfn';

CREATE FUNCTION percentile_cont_aggfinalfn (internal) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_percentile_cont_aggfinalfn';

CREATE FUNCTION percentile_disc_aggfinalfn (internal) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_percentile_disc_aggfinalfn';

CREATE AGGREGATE percentilecont(gtype, gtype) (
    stype = internal, 
    sfunc = percentile_aggtransfn, 
    finalfunc = percentile_cont_aggfinalfn, 
    parallel = SAFE
);

CREATE AGGREGATE percentiledisc(gtype, gtype) (
    stype = internal,
    sfunc = percentile_aggtransfn, 
    finalfunc = percentile_disc_aggfinalfn, 
    parallel = SAFE
);

--
-- collect
--
CREATE FUNCTION collect_aggtransfn (internal, gtype)
RETURNS internal 
LANGUAGE c 
IMMUTABLE 
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_collect_aggtransfn';

CREATE FUNCTION collect_aggfinalfn (internal) 
RETURNS gtype 
LANGUAGE c 
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE 
AS 'MODULE_PATHNAME', 'gtype_collect_aggfinalfn';

CREATE AGGREGATE collect(gtype) (
    stype = internal, 
    sfunc = collect_aggtransfn, 
    finalfunc = collect_aggfinalfn, 
    parallel = safe
);

CREATE FUNCTION vertex_collect_transfn(internal, vertex)
RETURNS internal
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME';


CREATE FUNCTION vertex_collect_finalfn(internal)
RETURNS vertex[] 
LANGUAGE c  
IMMUTABLE
PARALLEL SAFE AS 'MODULE_PATHNAME';

CREATE AGGREGATE collect(vertex) (
    stype = internal,
    sfunc = vertex_collect_transfn,
    finalfunc = vertex_collect_finalfn, 
    parallel = safe 
);

CREATE FUNCTION vertex_collect_transfn_w_limit(internal, vertex, gtype)
RETURNS internal
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE collect(vertex, gtype) (
    stype = internal,
    sfunc = vertex_collect_transfn_w_limit,
    finalfunc = vertex_collect_finalfn,
    parallel = safe
); 


CREATE FUNCTION edge_collect_transfn(internal, edge)
RETURNS internal
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME';


CREATE FUNCTION edge_collect_finalfn(internal)
RETURNS edge[]
LANGUAGE c
IMMUTABLE
PARALLEL SAFE AS 'MODULE_PATHNAME';

CREATE AGGREGATE collect(edge) (
    stype = internal,
    sfunc = edge_collect_transfn,
    finalfunc = edge_collect_finalfn,
    parallel = safe
);

CREATE FUNCTION edge_collect_transfn_w_limit(internal, edge, gtype)
RETURNS internal
LANGUAGE c
IMMUTABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE collect(edge, gtype) (
    stype = internal,
    sfunc = edge_collect_transfn_w_limit,
    finalfunc = edge_collect_finalfn,
    parallel = safe
);

CREATE FUNCTION gtype_regr_accum(float8[], gtype, gtype)
RETURNS float8[]
LANGUAGE c
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE FUNCTION gtype_corr(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME', 'gtype_corr';

CREATE AGGREGATE corr(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_corr,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_covar_pop(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE covar_pop(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_covar_pop,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_covar_samp(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE covar_samp(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_covar_samp,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_sxx(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_sxx(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_sxx,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_syy(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_syy(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_syy,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_sxy(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_sxy(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_sxy,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_slope(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_slope(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_slope,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_intercept(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_intercept(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_intercept,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_avgx(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE            
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_avgx(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_avgx,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_avgy(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE            
CALLED ON NULL INPUT 
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_avgy(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_avgy,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

CREATE FUNCTION gtype_regr_r2(float8[])
RETURNS gtype
LANGUAGE c
IMMUTABLE
CALLED ON NULL INPUT
PARALLEL SAFE
AS 'MODULE_PATHNAME';

CREATE AGGREGATE regr_r2(gtype, gtype) (
    stype = float8[],
    sfunc = gtype_regr_accum,
    finalfunc = gtype_regr_avgy,
    combinefunc = float8_regr_combine,
    finalfunc_modify = READ_ONLY,
    initcond = '{0,0,0,0,0,0}',
    parallel = SAFE
);

