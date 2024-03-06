cmake_minimum_required(VERSION 3.25.1)
project(PostGraph)

include(CTest)
include(CheckIncludeFile)

list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../cmake)

enable_testing()

find_package(PostgreSQL REQUIRED)
find_package(PostGIS REQUIRED)

execute_process(COMMAND perl -I ${CMAKE_BINARY_DIR}/../PostGraph/tools ${CMAKE_BINARY_DIR}/../PostGraph/tools/gen_keywordlist.pl --extern --varname CypherKeyword --output ${CMAKE_BINARY_DIR}/../PostGraph/src/include/parser/ ${CMAKE_BINARY_DIR}/../PostGraph/src/include/parser/cypher_kwlist.h)

find_package(BISON)
find_package(FLEX)

BISON_TARGET(CypherParser src/backend/parser/cypher_gram.y ${CMAKE_BINARY_DIR}/../PostGraph/src/backend/parser/cypher_gram.c
        DEFINES_FILE ${CMAKE_BINARY_DIR}/../PostGraph/src/include/parser/cypher_gram_def.h)

FLEX_TARGET(CypherScanner src/backend/parser/ag_scanner.l  ${CMAKE_BINARY_DIR}/../PostGraph/src/backend/parser/ag_scanner.c)

ADD_FLEX_BISON_DEPENDENCY(CypherScanner CypherParser)

include_directories(${CMAKE_BINARY_DIR}/../PostGraph/src/include)
include_directories(${PostgreSQL_INCLUDE_DIRS}
        ${PostgreSQL_SERVER_INCLUDE_DIRS})

add_postgresql_extension(
        postgraph
        SCHEMA postgraph
        RELOCATABLE false
        SCRIPTS postgraph--0.1.0.sql
        VERSION 0.1.0
        SHARED_PRELOAD ON
        REGRESS scan
          graphid
          gtype
	      network
          temporal
          range
          vertex
          edge
          variable_edge
          traversal
          catalog
          cypher
          lists
          expr
	      vector
          cypher_create
          cypher_match
          cypher_unwind
          cypher_set
          cypher_remove
          cypher_delete
          cypher_with
          cypher_vle
          cypher_union
          cypher_merge
          cypher_call
          order_by
        SOURCES src/backend/postgraph.c
            src/backend/access/ivfbuild.c
            src/backend/access/ivfflat.c
            src/backend/access/ivfinsert.c
            src/backend/access/ivfkmeans.c
            src/backend/access/ivfscan.c
            src/backend/access/ivfutils.c
            src/backend/access/ivfvacuum.c
            src/backend/catalog/ag_catalog.c
            src/backend/catalog/ag_graph.c
            src/backend/catalog/ag_label.c
            src/backend/catalog/ag_namespace.c
            src/backend/commands/graph_commands.c
            src/backend/commands/label_commands.c
            src/backend/executor/cypher_create.c
            src/backend/executor/cypher_merge.c
            src/backend/executor/cypher_set.c
            src/backend/executor/cypher_utils.c
            src/backend/nodes/ag_nodes.c
            src/backend/nodes/cypher_copyfuncs.c
            src/backend/nodes/cypher_outfuncs.c
            src/backend/nodes/cypher_readfuncs.c
            src/backend/optimizer/cypher_createplan.c
            src/backend/optimizer/cypher_pathnode.c
            src/backend/optimizer/cypher_paths.c
            src/backend/parser/ag_scanner.c
            src/backend/parser/cypher_analyze.c
            src/backend/parser/cypher_clause.c
            src/backend/executor/cypher_delete.c
            src/backend/parser/cypher_expr.c
            src/backend/parser/cypher_gram.c
            src/backend/parser/cypher_item.c
            src/backend/parser/cypher_keywords.c
            src/backend/parser/cypher_parse_agg.c
            src/backend/parser/cypher_parse_node.c
            src/backend/parser/cypher_parser.c
            src/backend/parser/cypher_transform_entity.c
            src/backend/utils/adt/queue.c
            src/backend/utils/adt/gtype.c
            src/backend/utils/adt/gtype_ext.c
            src/backend/utils/adt/gtype_gin.c
            src/backend/utils/adt/gtype_network.c
            src/backend/utils/adt/gtype_ops.c
            src/backend/utils/adt/gtype_parser.c
            src/backend/utils/adt/gtype_range.c
            src/backend/utils/adt/gtype_string.c
            src/backend/utils/adt/gtype_numbers.c
            src/backend/utils/adt/gtype_temporal.c
            src/backend/utils/adt/gtype_tsearch.c
            src/backend/utils/adt/gtype_typecasting.c
            src/backend/utils/adt/gtype_geometric.c
            src/backend/utils/adt/gtype_util.c
            src/backend/utils/path_finding/global_graph.c
            src/backend/utils/path_finding/dfs.c
            src/backend/utils/adt/cypher_funcs.c
            src/backend/utils/adt/edge.c
            src/backend/utils/adt/graphid.c
            src/backend/utils/adt/traversal.c
            src/backend/utils/adt/variable_edge.c
            src/backend/utils/adt/vector.c
            src/backend/utils/adt/vertex.c
            src/backend/utils/ag_func.c
            src/backend/utils/cache/ag_cache.c )

