# .rst: FindPostgreSQL
# --------------------
#
# Builds a PostgreSQL installation. As opposed to finding a system-wide installation, this module
# will download and build PostgreSQL with debug enabled.
#
# By default, it'll download the latest known version of PostgreSQL (at the time of last update)
# unless `PGVER` variable is set. `PGVER` can be either a major version like `15` which will be aliased
# to the latest known minor version, or a full version.
#
# This module defines the following variables
#
# ::
#
# PostgreSQL_LIBRARIES - the PostgreSQL libraries needed for linking
#
# PostgreSQL_INCLUDE_DIRS - include directories
#
# PostgreSQL_SERVER_INCLUDE_DIRS - include directories for server programming
#
# PostgreSQL_LIBRARY_DIRS  - link directories for PostgreSQL libraries
#
# PostgreSQL_EXTENSION_DIR  - the directory for extensions
#
# PostgreSQL_SHARED_LINK_OPTIONS  - options for shared libraries
#
# PostgreSQL_LINK_OPTIONS  - options for static libraries and executables
#
# PostgreSQL_VERSION_STRING - the version of PostgreSQL found (since CMake
# 2.8.8)
#
# ----------------------------------------------------------------------------
# History: This module is derived from the existing FindPostgreSQL.cmake and try
# to use most of the existing output variables of that module, but uses
# `pg_config` to extract the necessary information instead and add a macro for
# creating extensions. The use of `pg_config` is aligned with how the PGXS code
# distributed with PostgreSQL itself works.

# Copyright 2022 Omnigres Contributors
# Copyright 2020 Mats Kindahl
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# add_postgresql_extension(NAME ...)
#
# ENCODING Encoding for the control file. Optional.
#
# COMMENT Comment for the control file. Optional.
#
# SOURCES List of source files to compile for the extension. Optional.
#
# REQUIRES List of extensions that are required by this extension. Optional.
#
# TESTS_REQUIRE List of extensions that are required by tests. Optional.
#
# TESTS ON by default, if OFF, don't try finding tests for this extension
#
# REGRESS Regress tests.
#
# SCHEMA Extension schema.
#
# RELOCATABLE Is extension relocatable
#
# SUPERUSER Only superuser can create this extension
#
# SHARED_PRELOAD Is this a shared preload extension
#
# PRIVATE If true, this extension will not be automatically packaged
#
# DEPENDS_ON List of Omnigres components this extension depends
#
# VERSION Version of the extension. Is used when generating the control file.
# Optional, typically used only in scenarios when versions must be fixed.
#
# By default, version is picked from git revision, unless overriden by the following
# CMake variables:
#
# * OMNIGRES_VERSION: sets a version for all extensions
# * <EXTENSION_NAME_CAPITALIZED>_VERSION: sets a version for a particular extension
#
# TARGET Custom target name (by default, `NAME`)
#
# NO_DEFAULT_CONTROL Do not produce a default control file (can be useful alongside with `TARGET`)
#
# UPGRADE_SCRIPTS In certain cases, custom upgrade scripts need to be supplied.
#                 Currently, their vesioning is hardcoded and can't be based on extension's version
#                 (but this can change [TODO])
#
# Defines the following targets:
#
# psql_NAME   Start extension it in a fresh database and connects with psql
# (port assigned randomly unless specified using PGPORT environment variable)
# Note:
#
# If extension root directory contains `.psqlrc`, it will be executed in the begining
# of the `psql_NAME` session and `:CMAKE_BINARY_DIR` psql variable will be set to the current
# build directory (`CMAKE_BINARY_DIR`)
#
# prepare and prepare_NAME: prepares all the control/migration files
#
# NAME_update_results Updates pg_regress test expectations to match results
# test_verbose_NAME Runs tests verbosely
find_program(PGCLI pgcli)
find_program(NETCAT nc REQUIRED)

include(Version)
include(Common)

# add_postgresql_extension(NAME ...)
#
# VERSION Version of the extension. Is used when generating the control file.
# Required.
#
# ENCODING Encoding for the control file. Optional.
#
# COMMENT Comment for the control file. Optional.
#
# SOURCES List of source files to compile for the extension.
#
# REQUIRES List of extensions that are required by this extension.
#
# SCRIPTS Script files.
#
# SCRIPT_TEMPLATES Template script files.
#
# REGRESS Regress tests.
function(add_postgresql_extension NAME)
    set(_optional)
    set(_single VERSION ENCODING)
    set(_multi SOURCES SCRIPTS SCRIPT_TEMPLATES REQUIRES REGRESS)
    cmake_parse_arguments(_ext "${_optional}" "${_single}" "${_multi}" ${ARGN})

    if(NOT _ext_VERSION)
        message(FATAL_ERROR "Extension version not set")
    endif()

    # Here we are assuming that there is at least one source file, which is
    # strictly speaking not necessary for an extension. If we do not have source
    # files, we need to create a custom target and attach properties to that. We
    # expect the user to be able to add target properties after creating the
    # extension.
    add_library(${NAME} MODULE ${_ext_SOURCES})

    set(_link_flags "${PostgreSQL_SHARED_LINK_OPTIONS}")
    foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
        set(_link_flags "${_link_flags} -L${_dir}")
    endforeach()

    # Collect and build script files to install
    set(_script_files ${_ext_SCRIPTS})
    foreach(_template ${_ext_SCRIPT_TEMPLATES})
        string(REGEX REPLACE "\.in$" "" _script ${_template})
        configure_file(${_template} ${_script} @ONLY)
        list(APPEND _script_files ${CMAKE_CURRENT_BINARY_DIR}/${_script})
        message(
                STATUS "Building script file ${_script} from template file ${_template}")
    endforeach()

    if(APPLE)
        set(_link_flags "${_link_flags} -bundle_loader ${PG_BINARY}")
    endif()

    set_target_properties(
            ${NAME}
            PROPERTIES PREFIX ""
            LINK_FLAGS "${_link_flags}"
            POSITION_INDEPENDENT_CODE ON)

    target_include_directories(
            ${NAME}
            PRIVATE ${PostgreSQL_SERVER_INCLUDE_DIRS}
            PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

    # Generate control file at build time (which is when GENERATE evaluate the
    # contents). We do not know the target file name until then.
    set(_control_file "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.control")
    file(
            GENERATE
            OUTPUT ${_control_file}
            CONTENT
            "# This file is generated content from add_postgresql_extension.
# No point in modifying it, it will be overwritten anyway.

# Default version, always set
default_version = '${_ext_VERSION}'

# Module pathname generated from target shared library name. Use
# MODULE_PATHNAME in script file.
module_pathname = '$libdir/$<TARGET_FILE_NAME:${NAME}>'

# Comment for extension. Set using COMMENT option. Can be set in
# script file as well.
$<$<NOT:$<BOOL:${_ext_COMMENT}>>:#>comment = '${_ext_COMMENT}'

# Encoding for script file. Set using ENCODING option.
$<$<NOT:$<BOOL:${_ext_ENCODING}>>:#>encoding = '${_ext_ENCODING}'

# Required extensions. Set using REQUIRES option (multi-valued).
$<$<NOT:$<BOOL:${_ext_REQUIRES}>>:#>requires = '$<JOIN:${_ext_REQUIRES},$<COMMA>>'
")

    install(TARGETS ${NAME} LIBRARY DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
    install(FILES ${_control_file} ${_script_files}
            DESTINATION ${PostgreSQL_EXTENSION_DIR})

    if(_ext_REGRESS)
        foreach(_test ${_ext_REGRESS})
            set(_sql_file "${CMAKE_CURRENT_SOURCE_DIR}/sql/${_test}.sql")
            set(_out_file "${CMAKE_CURRENT_SOURCE_DIR}/expected/${_test}.out")
            if(NOT EXISTS "${_sql_file}")
                message(FATAL_ERROR "Test file '${_sql_file}' does not exist!")
            endif()
            if(NOT EXISTS "${_out_file}")
                file(WRITE "${_out_file}" )
                message(STATUS "Created empty file ${_out_file}")
            endif()
        endforeach()

        if(PG_REGRESS)
            add_test(
                    NAME ${NAME}
                    COMMAND
                    ${PG_REGRESS} --temp-instance=${CMAKE_BINARY_DIR}/tmp_check
                    --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
                    --outputdir=${CMAKE_CURRENT_BINARY_DIR} --load-extension=ltree --load-extension=${NAME}
                    ${_ext_REGRESS})
        endif()

        add_custom_target(
                ${NAME}_update_results
                COMMAND
                ${CMAKE_COMMAND} -E copy_if_different
                ${CMAKE_CURRENT_BINARY_DIR}/results/*.out
                ${CMAKE_CURRENT_SOURCE_DIR}/expected)
    endif()
endfunction()

if(PG_REGRESS)
    # We add a custom target to get output when there is a failure.
    add_custom_target(
            test_verbose COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
            --verbose --output-on-failure)
endif()
