# PostGraph
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/854cafdbd0394189bec10e8fdd17df7f)](https://app.codacy.com/gh/PostGraphDB/postgraph?utm_source=github.com&utm_medium=referral&utm_content=PostGraphDB/postgraph&utm_campaign=Badge_Grade)   <a href="https://github.com/PostGraphDB/PostGraph/blob/master/LICENSE"><img src="https://img.shields.io/github/license/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/issues"><img src="https://img.shields.io/github/issues/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/network/members"><img src="https://img.shields.io/github/forks/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/stargazers"><img src="https://img.shields.io/github/stars/PostGraphDB/PostGraph"/></a>   <a href="https://discord.gg/KDTTx2vz2m"><img src="https://img.shields.io/discord/1036610864071053413.svg?label=discord&style=flat&color=5a66f6"></a>

## Introduction
PostGraph is the first Graph based Postgres extension to support Vector Embeddings. Query your database using a tradition graph query query language, use embeddings to infer relationships or combine the two along with relational data and spatial data powered by PostGIS. 

Utilize Graph, Vector, and Relational models together to derive insights from your data in innovative powerful ways. Including the ability to use database normalization techniques from Relational Databases in your Graph Database.

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
shared_preload_libraries = 'postgis-3', 'postgraph'
```
POSTGIS Dependencies:
autoconf
automake
libtool
libxml2-devel
geos-devel
proj-devel
protobuf-devel protobuf-c-compiler protobuf-compiler
gdal-devel

## Quick Start

Use `psql` to start a command line session with your database
```bash
psql postgraph
```

Create a graph
```sql
SELECT create_graph('graph_name');
```

Create Data with the Cypher Command
```sql
SELECT *
FROM cypher('graph_name', $$
    CREATE (v { name: 'Hello Graph!'})
    RETURN (v)
as (v vertex);
```


## Roadmap

Completed:
-   Support for the OpenCypher Query Language
-   Vector Embeddings, powered by [pgVector](https://github.com/pgvector/pgvector)
-   Support for Time data

In Progress:
-   Support For IVFFLAT Indices, Approximate K-Nearest Neighbors
-   Support for Geographic Information Systems, powered by [PostGIS](http://postgis.net/)

Planned
-   Support For HNSW Indices, Faster Approximate K-Nearest Neighbors
-   Support For Full Text Search, For combing with Vector Embeddings for Natural Language Processing
-   Support of Time-Series Graph Data, powered by [TimeScaleDB](https://github.com/timescale/timescaledb)
-   [Machine Learning Techniques](https://github.com/postgresml/postgresml)
-   Label Partitioning - For advanced storage and performance techniques
-   Geospatial Trajectory Data, powered by [MobilityDB](https://github.com/MobilityDB/MobilityDB)

## Thank You to All the Supporters!
![Stargazers repo roster for @Postgraphdb/postgraph](http://bytecrank.com/nastyox/reporoster/php/stargazersSVG.php?user=Postgraphdb&repo=postgraph)
![Forkers repo roster for @Postgraphdb/postgraph](http://bytecrank.com/nastyox/reporoster/php/forkersSVG.php?user=Postgraphdb&repo=postgraph)
