cmake_minimum_required(VERSION 3.25.1)
project(postgraph)

list(PREPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

enable_testing()

include(Common)

add_subdirectory_once(misc/inja)
add_subdirectory_once(PostGraph)

include_directories(${PostgreSQL_INCLUDE_DIRS}
        ${PostgreSQL_SERVER_INCLUDE_DIRS})


include(CPM)
CPMAddPackage(
    NAME Format.cmake
    VERSION 1.7.3
    GITHUB_REPOSITORY TheLartians/Format.cmake
    OPTIONS
    "FORMAT_SKIP_CMAKE YES"
)