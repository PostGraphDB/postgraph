cmake_minimum_required(VERSION 3.25.1)
project(PostGraph)

include(CTest)
include(CheckIncludeFile)

list(PREPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

enable_testing()


include_directories(${PostgreSQL_INCLUDE_DIRS} ${PostgreSQL_SERVER_INCLUDE_DIRS})

find_package(PostgreSQL REQUIRED)

add_postgresql_extension(
        postgraph
        SCHEMA postgraph
        RELOCATABLE false
        SCRIPTS postgraph--0.0.1.sql
        VERSION 0.0.1
        SHARED_PRELOAD ON
        SOURCES postgraph.c
)
