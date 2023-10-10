# PostGraph

[manual](https://postgraphdb.github.io/docs/intro)

PostGraph is a fork of the Apache AGE project. The goal of PostGraph is to go beyond AGE's stated goal of implementing the openCypher query language. PostGraph's internals have been greatly overhauled with the expressed goal of improving performance and allowing for new and powerful features.

## Completed:
 - Date and Time Support
 - Vector Datatype Support and Exact K-Nearest Neighbors
 - Intersect and Union Clauses

## In Progress
 - Full support of database normalization
 - Indexing
 - Approximate K-Nearest Neighbors Support for Vectors
 - Conformity of logic between Cypher and Postgres Functions
 - JsonB Style Key existance operators
 - Property Constraints
 - Network Datatypes - Support Data types for MAC and IP Addresses
 - Regex Support - Robust Regular Expression Support
 - Spatial Datatypes via [PostGIS](http://postgis.net/)

# Todo List
 - geospatial trajectory data [MobilityDB](https://github.com/MobilityDB/MobilityDB)
 - uri datatypes - Support holding uris in Postgraph.
 - English Full Text Search - Support to perform Full Text Search in English
 - Windows Operating System Support - Allow users to run natively on Windows
 - Performance Improvements - Improvements to performance without the use of indices and constraints.
 - Indexing - Full support for creating Indices
 - Label Partitioning - Allow Labels to be partitioned
 - Shortest Path Algorithms - Weighted and Unweighted Shortest Path.
 - [pgRouting](https://github.com/pgRouting)
 - Cryptology Support via [pgsodium](https://github.com/michelp/pgsodium)
 - Chinese Full Text Search - Allow users to use a Chinese Full Text Dictionary.
 - Apache Spark Connector - Allow PostGraph to be a data source or sink for Apache Spark.
 - Native Cypher Support Command Line Interface - Allow users to enter queries without having to call the cypher function, via a CLI similar to psql.
 - PgAdmin Support - Support the PostGraph in PgAdmin.
 - PgPool-2 Support - Support PostGraph queries in PG-Pool2
 - Windows Functions - Support Windows Functions such as rank and row_number
 - Localization - Korean, Japanese, Mandarin Chinese, Cantonese Chinese, Hindi, Spanish, Arabic, Portugese, French, German, Italian, Polish, etc.
 - Table Triggers
 - SQL Support in Cypher Queries
 - Time Series Support via TimeScaleDB
 - Native Cypher support for drivers.
 - GraphQL support
 - Distributed Graphs via citus
 - Orioledb Support
 - Data Science Library
 - RUM Indices for Full Text Search
 - ElasticSearch Support via [ZomboDB](https://www.zombodb.com/).
 - Multi Graph Queries
 - Undirected Graphs
 - zson properties - The original writers of JsonB, which gtype is based on, created a new zson. Zson in certain uses case see a 10% Improvement in throughput and 40% improvement in compression. Allow users to be able to use zson as its storage mechanism for properties.

POSTGIS Dependencies:
autoconf
automake
libtool
libxml2-devel
geos-devel
proj-devel
protobuf-devel protobuf-c-compiler protobuf-compiler
gdal-devel
