include(CPM)

if(NOT POSTGIS_VER)
        set(POSTGIS_VER 3.4.2)
endif()

set(BUILD_TYPE "${CMAKE_BUILD_TYPE}")
if(NOT BUILD_TYPE)
        set(BUILD_TYPE "Debug")
endif()

set(POSTGIS_DIR "${CMAKE_CURRENT_LIST_DIR}/../.postgis/${CMAKE_HOST_SYSTEM_NAME}-${BUILD_TYPE}" CACHE STRING "Path where to manage PostGIS builds")
get_filename_component(postgis_dir "${POSTGIS_DIR}" ABSOLUTE)

# This is where we manage selected PostgreSQL version's installations
set(POSTGIS_DIR_VERSION "${postgis_dir}/${POSTGIS_VER}")


if(NOT EXISTS "${POSTGIS_DIR_VERSION}/build/bin/postgis")
        file(MAKE_DIRECTORY ${POSTGIS_DIR})

        message(STATUS "Downloading Proj ${POSTGIS_DIR}")
        file(DOWNLOAD "https://github.com/OSGeo/PROJ/releases/download/9.3.1/proj-9.3.1.tar.gz" "${POSTGIS_DIR}/proj-9.3.1.tar.gz" SHOW_PROGRESS)
        message(STATUS "Extracting Proj ${POSTGIS_DIR}")
        file(ARCHIVE_EXTRACT INPUT "${POSTGIS_DIR}/proj-9.3.1.tar.gz" DESTINATION "${POSTGIS_DIR}/proj")

        file(MAKE_DIRECTORY "${POSTGIS_DIR}/proj/build")
        execute_process(COMMAND bash -c "cmake .." WORKING_DIRECTORY "${POSTGIS_DIR}/proj/build")
        execute_process(COMMAND bash -c "cmake --build ." WORKING_DIRECTORY "${POSTGIS_DIR}/proj/build")
        execute_process(COMMAND bash -c "cmake --build . --target install" WORKING_DIRECTORY "${POSTGIS_DIR}/proj/build")

        message(STATUS "Downloading PostGIS ${POSTGIS_DIR}")
        file(DOWNLOAD "https://github.com/postgis/postgis/archive/refs/tags/${POSTGIS_VER}.tar.gz" "${POSTGIS_DIR}/postgis-${POSTGIS_VER}.tar.gz" SHOW_PROGRESS)
        message(STATUS "Extracting PostGIS ${POSTGIS_DIR}")
        file(ARCHIVE_EXTRACT INPUT "${POSTGIS_DIR}/postgis-${POSTGIS_VER}.tar.gz" DESTINATION ${POSTGIS_DIR_VERSION})

        set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

        set(extra_configure_args "")
        if(BUILD_TYPE STREQUAL "RelWithDebInfo")
                string(APPEND extra_configure_args " --enable-debug")
        elseif(BUILD_TYPE STREQUAL "Release")
        else()
                string(APPEND extra_configure_args " --enable-debug --enable-cassert")
        endif()

        find_package(PROJ REQUIRED CONFIG)

        execute_process(
                COMMAND bash -c "./autogen.sh"
                WORKING_DIRECTORY "${POSTGIS_DIR_VERSION}/postgis-${POSTGIS_VER}"
                RESULT_VARIABLE pg_configure_result_postgis_autogen)

        if(NOT pg_configure_result_postgis_autogen EQUAL 0)
                message(FATAL_ERROR "Can't configure PostGIS autogen, aborting. ${POSTGIS_DIR_VERSION}/postgis-${POSTGIS_VER}")
        endif()

        execute_process(
                COMMAND bash -c "./configure ${extra_configure_args} --prefix=${PGDIR_VERSION}/build --with-pgconfig=${PG_CONFIG} --with-proj=${POSTGIS_DIR}/proj/build/bin --without-protobuf --without-raster"
                WORKING_DIRECTORY "${POSTGIS_DIR_VERSION}/postgis-${POSTGIS_VER}"
                RESULT_VARIABLE pg_configure_result_postgis_configure)

        if(NOT pg_configure_result_postgis_configure EQUAL 0)
                message(FATAL_ERROR "Can't configure PostGIS configure, aborting. ./configure ${extra_configure_args} --prefix=\"${PGDIR_VERSION}/build\" --with-pgconfig=${PG_CONFIG}  --with-proj=${POSTGIS_DIR}/proj/build/bin --without-protobuf --without-raster")
        endif()

        execute_process(COMMAND make install WORKING_DIRECTORY "${PGDIR_VERSION}/postgis-${PGVER_ALIAS}")

endif ()