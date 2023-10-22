# PostGraph
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/854cafdbd0394189bec10e8fdd17df7f)](https://app.codacy.com/gh/PostGraphDB/postgraph?utm_source=github.com&utm_medium=referral&utm_content=PostGraphDB/postgraph&utm_campaign=Badge_Grade)   <a href="https://github.com/PostGraphDB/PostGraph/blob/master/LICENSE"><img src="https://img.shields.io/github/license/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/issues"><img src="https://img.shields.io/github/issues/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/network/members"><img src="https://img.shields.io/github/forks/PostGraphDB/PostGraph"/></a>   <a href="https://github.com/PostGraphDB/PostGraph/stargazers"><img src="https://img.shields.io/github/stars/PostGraphDB/PostGraph"/></a>   <a href="https://discord.gg/KDTTx2vz2m"><img src="https://img.shields.io/discord/1036610864071053413.svg?label=discord&style=flat&color=5a66f6"></a>


[manual](https://postgraphdb.github.io/docs/intro)<br>
PostGraph is the first Graph based Postgres extension to support Vector Embeddings. Query your database using a tradition graph query query language, use embeddings to infer relationships or combine the two along with relational data and spatial data powered by PostGIS. 

Utilize Graph, Vector, and Relational models together to derive insights from your data in innovative powerful ways. Including the ability to use database normalization techniques from Relational Databases in your Graph Database.

Roadmap:

Completed:
-   Support for the OpenCypher Query Language
-   Vector Embeddings, powered by pgVector
-   Support for Time data

In Progress:
-   Support For IVFFLAT Indices, Approximate K-Nearest Neighbors
-   Support for Geographic Information Systems, powered by [PostGIS](http://postgis.net/)

Planned
-   Support For HNSW Indices, Faster Approximate K-Nearest Neighbors
-   Support For Full Text Search, For combing with Vector Embeddings for Natural Language Processing
-   Support of Time-Series Graph Data, powered by TimeScaleDB
-   Machine Learning Techniques
-   Label Partitioning - For advanced storage and performance techniques
-   Geospatial Trajectory Data, powered by [MobilityDB](https://github.com/MobilityDB/MobilityDB)

Some Usefule Addtional Features
-   Network Datatypes
-   Regex Support
-   Advanced Indexing Strategies
-   Property Constraints

## Requirements
-   Linux
-   PosgreSQL v14
-   PostGIS 3.3
-   Development Files for PostgreSQL and PostGIS

POSTGIS Dependencies:
autoconf
automake
libtool
libxml2-devel
geos-devel
proj-devel
protobuf-devel protobuf-c-compiler protobuf-compiler
gdal-devel
