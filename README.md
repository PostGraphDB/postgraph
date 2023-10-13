# [PostGraph](https://postgraphdb.github.io/docs/intro)

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/8cd7290c4a0c46bd9e2e452a4d06f1d0)](https://app.codacy.com/gh/PostGraphDB/postgraph/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)<br>
PostGraph is the first Graph based Postgres extension to support Vector Embeddings. Query your database using a tradition graph query query language, use embeddings to infer relationships or combine the two along with relational data and spatial data powered by PostGIS. 

Utilize Graph, Vector, and Relational models together to derive insights from your data in innovative powerful ways. Including the ability to use database normalization techniques from Relational Databases in your Graph Database. Configure your data to be either schemaless or schema-bound allowing for a graph equivalent of a wide-column database.

Roadmap:

Completed:
- Support for the OpenCypher Query Language
- Vector Embeddings, powered by pgVector
- Support for Time data

In Progress:
- Support For IVFFLAT Indices, Approximate K-Nearest Neighbors
- Support for Geographic Information Systems, powered by [PostGIS](http://postgis.net/)

Planned
- Support For HNSW Indices, Faster Approximate K-Nearest Neighbors
- Support For Full Text Search, For combing with Vector Embeddings for Natural Language Processing
- Support of Time-Series Graph Data, powered by TimeScaleDB
- Machine Learning Techniques
- Label Partitioning - For advanced storage and performance techniques
- Geospatial Trajectory Data, powered by [MobilityDB](https://github.com/MobilityDB/MobilityDB)

Some Usefule Addtional Features
- Network Datatypes
- Regex Support
- Advanced Indexing Strategies
- Property Constraints


POSTGIS Dependencies:
autoconf
automake
libtool
libxml2-devel
geos-devel
proj-devel
protobuf-devel protobuf-c-compiler protobuf-compiler
gdal-devel
