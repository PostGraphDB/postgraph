# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

MODULE_big = postgraph

OBJS = src/backend/postgraph.o \
       src/backend/access/ivfbuild.o \
       src/backend/access/ivfflat.o \
       src/backend/access/ivfinsert.o \
       src/backend/access/ivfkmeans.o \
       src/backend/access/ivfscan.o \
       src/backend/access/ivfutils.o \
       src/backend/access/ivfvacuum.o \
       src/backend/catalog/ag_catalog.o \
       src/backend/catalog/ag_graph.o \
       src/backend/catalog/ag_label.o \
       src/backend/catalog/ag_namespace.o \
       src/backend/commands/graph_commands.o \
       src/backend/commands/label_commands.o \
       src/backend/executor/cypher_create.o \
       src/backend/executor/cypher_merge.o \
       src/backend/executor/cypher_set.o \
       src/backend/executor/cypher_utils.o \
       src/backend/nodes/ag_nodes.o \
       src/backend/nodes/cypher_copyfuncs.o \
       src/backend/nodes/cypher_outfuncs.o \
       src/backend/nodes/cypher_readfuncs.o \
       src/backend/optimizer/cypher_createplan.o \
       src/backend/optimizer/cypher_pathnode.o \
       src/backend/optimizer/cypher_paths.o \
       src/backend/parser/ag_scanner.o \
       src/backend/parser/cypher_analyze.o \
       src/backend/parser/cypher_clause.o \
       src/backend/executor/cypher_delete.o \
       src/backend/parser/cypher_expr.o \
       src/backend/parser/cypher_gram.o \
       src/backend/parser/cypher_item.o \
       src/backend/parser/cypher_keywords.o \
       src/backend/parser/cypher_parse_agg.o \
       src/backend/parser/cypher_parse_node.o \
       src/backend/parser/cypher_parser.o \
       src/backend/parser/cypher_transform_entity.o \
       src/backend/utils/adt/queue.o \
       src/backend/utils/adt/gtype.o \
       src/backend/utils/adt/gtype_ext.o \
       src/backend/utils/adt/gtype_gin.o \
       src/backend/utils/adt/gtype_network.o \
       src/backend/utils/adt/gtype_ops.o \
       src/backend/utils/adt/gtype_parser.o \
       src/backend/utils/adt/gtype_postgis.o \
       src/backend/utils/adt/gtype_range.o \
       src/backend/utils/adt/gtype_string.o \
       src/backend/utils/adt/gtype_numbers.o \
       src/backend/utils/adt/gtype_temporal.o \
       src/backend/utils/adt/gtype_tsearch.o \
       src/backend/utils/adt/gtype_typecasting.o \
       src/backend/utils/adt/gtype_geometric.o \
       src/backend/utils/adt/gtype_util.o \
       src/backend/utils/path_finding/global_graph.o \
       src/backend/utils/path_finding/dfs.o \
       src/backend/utils/adt/cypher_funcs.o \
       src/backend/utils/adt/edge.o \
       src/backend/utils/adt/graphid.o \
       src/backend/utils/adt/traversal.o \
       src/backend/utils/adt/variable_edge.o \
       src/backend/utils/adt/vector.o \
       src/backend/utils/adt/vertex.o \
       src/backend/utils/ag_func.o \
       src/backend/utils/cache/ag_cache.o \

EXTENSION = postgraph

DATA = postgraph--0.1.0.sql

REGRESS = new_cypher \
          expr \
          lists \
          cypher_create \
          cypher_match \
          order_by \
          cypher_setop

srcdir=`pwd`
POSTGIS_DIR ?= postgis_dir

.PHONY:all

all: postgraph--0.1.0.sql

postgraph--0.1.0.sql: sql/postgraph.sql.in sql/postgraph-graphid.sql.in sql/postgraph-gtype.sql.in sql/postgraph-edge.sql.in sql/postgraph-vertex.sql.in sql/postgraph-variable_edge.sql.in sql/postgraph-traversal.sql.in sql/postgraph-typecasting.sql.in sql/postgraph-gtype-lists.sql.in sql/postgraph-string-functions.sql.in sql/postgraph-number-functions.sql.in sql/postgraph-temporal.sql.in sql/postgraph-network.sql.in sql/postgraph-tsearch.sql.in sql/postgraph-range.sql.in sql/postgraph-geometric.sql.in sql/postgraph-postgis.sql.in sql/postgraph-aggregation.sql.in
	cat $^ > $@
ag_regress_dir = $(srcdir)/regress
REGRESS_OPTS = --load-extension=postgis --load-extension=ltree --load-extension=postgraph --inputdir=$(ag_regress_dir) --outputdir=$(ag_regress_dir) --temp-instance=$(ag_regress_dir)/instance --port=61958 --encoding=UTF-8

ag_regress_out = instance/ log/ results/ regression.*
EXTRA_CLEAN = $(addprefix $(ag_regress_dir)/, $(ag_regress_out)) src/backend/parser/cypher_gram.c src/backend/parser/ag_scanner.c src/include/parser/cypher_gram_def.h src/include/parser/cypher_kwlist_d.h

GEN_KEYWORDLIST = $(PERL) -I ./tools/ ./tools/gen_keywordlist.pl
GEN_KEYWORDLIST_DEPS = ./tools/gen_keywordlist.pl tools/PerfectHash.pm

ag_include_dir = $(srcdir)/src/include
PG_CPPFLAGS = -w -I$(ag_include_dir) -I$(ag_include_dir)/parser -I$(POSTGIS_DIR) -I$(POSTGIS_DIR)/liblwgeom
PG_LDFLAGS = -lgeos_c
SHLIB_LINK=  $(POSTGIS_DIR)/liblwgeom/.libs/liblwgeom.a -Wl,--no-as-needed -Wl,-l:postgis-3.so -Wl,-l:ltree.so

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

src/backend/parser/cypher_keywords.o: src/include/parser/cypher_kwlist_d.h

src/include/parser/cypher_kwlist_d.h: src/include/parser/cypher_kwlist.h $(GEN_KEYWORDLIST_DEPS)
	$(GEN_KEYWORDLIST) --extern --varname CypherKeyword --output src/include/parser $<

src/include/parser/cypher_gram_def.h: src/backend/parser/cypher_gram.c

src/backend/parser/cypher_gram.c: BISONFLAGS += --defines=src/include/parser/cypher_gram_def.h -Wconflicts-sr

src/backend/parser/cypher_parser.o: src/backend/parser/cypher_gram.c
src/backend/parser/cypher_keywords.o: src/backend/parser/cypher_gram.c

src/backend/parser/ag_scanner.c: FLEX_NO_BACKUP=yes

installcheck: export LC_COLLATE=C
