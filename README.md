# PostGraph

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/854cafdbd0394189bec10e8fdd17df7f)](https://app.codacy.com/gh/PostGraphDB/postgraph?utm_source=github.com&utm_medium=referral&utm_content=PostGraphDB/postgraph&utm_campaign=Badge_Grade)   <a href="https://github.com/PostGraphDB/PostGraph/blob/master/LICENSE"><img src="https://img.shields.io/github/license/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/issues"><img src="https://img.shields.io/github/issues/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/network/members"><img src="https://img.shields.io/github/forks/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/stargazers"><img src="https://img.shields.io/github/stars/PostGraphDB/PostGraph"/></a>
<p align="center">
 <img src="/logo.png">
</p>

## Introduction
PostGraph is a multi-model, graph centric query engine build on Postgres. PostGraph is designed to work fast with your OLTP, OLAP and AI Applications.

### PostGraph Supports
-   Cypher - Use the Cypher Query Language to explore your data and run Graph queries.
-   Vectors - Combine Vectors with your graph data to unlock new and valuable insights 
-   PostGIS - Unlock the power of spatial data in your graph database.
-   Full Text Search - Sift through vast amounts of textual data and combine with Vectors to unlock your natural language processing models

## Requirements
-   Linux
-   PosgreSQL v14
-   PostGIS 3.3
-   Development Files for PostgreSQL and PostGIS

## Building & Installation From Source
```bash
git clone https://github.com/PostGraphDB/postgraph
cd postgraph
make POSTGIS_DIR=/path/to/postgis/source/files
sudo make install
```

Once PostGraph is installed, it needs to be enabled in each database you want to use it in. In the example below we use a database named `postgraph`.
```bash
createdb postgraph
psql postgraph -c "CREATE EXTENSION PostGIS"
psql postgraph -c "CREATE EXTENSION LTree"
psql postgraph -c "CREATE EXTENSION PostGraph"
```

You can find the location of the `postgresql.conf` file as given next:
```bash
$ which postgres
/usr/local/pgsql/bin/postgres
$ ls /usr/local/pgsql/data/postgresql.conf
/usr/local/pgsql/data/postgresql.conf
```
As can be seen, the PostgreSQL binaries are in the `bin` subdirectory while the `postgresql.conf` file is in the `data` subdirectory.

Set the following in `postgresql.conf` depending on the version of PostGraph and it's dependecy PostGIS:
```bash
shared_preload_libraries = 'postgraph'
```

POSTGIS Dependencies:
autoconf automake libtool libxml2-devel geos-devel proj-devel protobuf-devel protobuf-c-compiler protobuf-compiler gdal-devel

## Quick Start

Use `psql` to start a command line session with your database
```bash
psql postgraph
```

Create a graph
```sql
CREATE GRAPH graph_name;
```

Set the Graph being used
```sql
USE GRAPH graph_name;
```

Create Data with the CREATE command
```sql
CREATE (v { name: 'Hello Graph!'}) RETURN v;
```

Find data with the MATCH command
```sql
MATCH (n) RETURN n;
```

## Thank You to All the Supporters!
![Stargazers repo roster for @Postgraphdb/postgraph](http://bytecrank.com/nastyox/reporoster/php/stargazersSVG.php?user=Postgraphdb&repo=postgraph)
![Forkers repo roster for @Postgraphdb/postgraph](http://bytecrank.com/nastyox/reporoster/php/forkersSVG.php?user=Postgraphdb&repo=postgraph)
