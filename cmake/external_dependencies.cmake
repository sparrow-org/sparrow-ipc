include(FetchContent)

OPTION(FETCH_DEPENDENCIES_WITH_CMAKE "Fetch dependencies with CMake: Can be OFF, ON, or MISSING. If the latter, CMake will download only dependencies which are not previously found." OFF)
MESSAGE(STATUS "🔧 FETCH_DEPENDENCIES_WITH_CMAKE: ${FETCH_DEPENDENCIES_WITH_CMAKE}")

if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "OFF")
    set(FIND_PACKAGE_OPTIONS REQUIRED)
else()
    set(FIND_PACKAGE_OPTIONS QUIET)
endif()

function(find_package_or_fetch)
    set(options)
    set(oneValueArgs CONAN_PKG_NAME PACKAGE_NAME GIT_REPOSITORY TAG SOURCE_SUBDIR)
    set(multiValueArgs CMAKE_ARGS)
    cmake_parse_arguments(PARSE_ARGV 0 arg
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )

    set(actual_pkg_name ${arg_PACKAGE_NAME})
    if(arg_CONAN_PKG_NAME)
        set(actual_pkg_name ${arg_CONAN_PKG_NAME})
    endif()

    if(NOT FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON")
        find_package(${actual_pkg_name} ${FIND_PACKAGE_OPTIONS})
    endif()

    if(arg_GIT_REPOSITORY)
        if(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "ON" OR FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "MISSING")
            if(NOT ${actual_pkg_name}_FOUND)
                message(STATUS "📦 Fetching ${arg_PACKAGE_NAME}")
                # Apply CMAKE_ARGS before fetching
                foreach(cmake_arg ${arg_CMAKE_ARGS})
                    string(REGEX MATCH "^([^=]+)=(.*)$" _ ${cmake_arg})
                    if(CMAKE_MATCH_1)
                        set(${CMAKE_MATCH_1} ${CMAKE_MATCH_2} CACHE BOOL "" FORCE)
                    endif()
                endforeach()
                set(fetch_args
                    ${arg_PACKAGE_NAME}
                    GIT_SHALLOW TRUE
                    GIT_REPOSITORY ${arg_GIT_REPOSITORY}
                    GIT_TAG ${arg_TAG}
                    GIT_PROGRESS TRUE
                    SYSTEM
                    EXCLUDE_FROM_ALL)
                if(arg_SOURCE_SUBDIR)
                    list(APPEND fetch_args SOURCE_SUBDIR ${arg_SOURCE_SUBDIR})
                endif()
                FetchContent_Declare(${fetch_args})
                FetchContent_MakeAvailable(${arg_PACKAGE_NAME})
                message(STATUS "\t✅ Fetched ${arg_PACKAGE_NAME}")
            else()
                message(STATUS "📦 ${actual_pkg_name} found here: ${${actual_pkg_name}_DIR}")
            endif()
        endif()
    else()
        # No GIT_REPOSITORY provided - only find_package is attempted
        if(${actual_pkg_name}_FOUND)
            message(STATUS "📦 ${actual_pkg_name} found here: ${${actual_pkg_name}_DIR}")
        elseif(FETCH_DEPENDENCIES_WITH_CMAKE STREQUAL "OFF")
            message(FATAL_ERROR "Could not find ${actual_pkg_name} and no GIT_REPOSITORY provided for fetching")
        else()
            message(WARNING "Could not find ${actual_pkg_name} and no GIT_REPOSITORY provided for fetching")
        endif()
    endif()
endfunction()

set(SPARROW_BUILD_SHARED ${SPARROW_IPC_BUILD_SHARED})
if(${SPARROW_IPC_BUILD_TESTS} OR ${SPARROW_IPC_BUILD_INTEGRATION_TESTS})
    set(CREATE_JSON_READER_TARGET ON)
endif()

find_package_or_fetch(
    PACKAGE_NAME sparrow
    GIT_REPOSITORY https://github.com/man-group/sparrow.git
    TAG 2.1.0
)

unset(CREATE_JSON_READER_TARGET)

if(NOT TARGET sparrow::sparrow)
    add_library(sparrow::sparrow ALIAS sparrow)
endif()
if(${SPARROW_IPC_BUILD_TESTS} OR ${SPARROW_IPC_BUILD_INTEGRATION_TESTS})
    find_package_or_fetch(
        PACKAGE_NAME sparrow-json-reader
    )
    if(NOT TARGET sparrow::json_reader)
        add_library(sparrow::json_reader ALIAS json_reader)
    endif()
endif()

set(FLATBUFFERS_BUILD_TESTS OFF)
set(FLATBUFFERS_BUILD_SHAREDLIB ${SPARROW_IPC_BUILD_SHARED})
find_package_or_fetch(
    CONAN_PKG_NAME flatbuffers
    PACKAGE_NAME FlatBuffers
    GIT_REPOSITORY https://github.com/google/flatbuffers.git
    TAG v25.2.10
)

if(NOT TARGET flatbuffers::flatbuffers)
    add_library(flatbuffers::flatbuffers ALIAS flatbuffers)
endif()
unset(FLATBUFFERS_BUILD_TESTS CACHE)

# Fetching lz4
# Disable bundled mode to allow shared libraries if needed
# lz4 is built as static by default if bundled
# set(LZ4_BUNDLED_MODE OFF CACHE BOOL "" FORCE)
# set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
find_package_or_fetch(
    PACKAGE_NAME lz4
    GIT_REPOSITORY https://github.com/lz4/lz4.git
    TAG v1.10.0
    SOURCE_SUBDIR build/cmake
    CMAKE_ARGS
        "LZ4_BUILD_CLI=OFF"
        "LZ4_BUILD_LEGACY_LZ4C=OFF"
)

if(NOT TARGET lz4::lz4)
    add_library(lz4::lz4 ALIAS lz4)
endif()

find_package_or_fetch(
    PACKAGE_NAME zstd
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    TAG v1.5.7
    SOURCE_SUBDIR build/cmake
    CMAKE_ARGS
        "ZSTD_BUILD_PROGRAMS=OFF"
)

if(NOT TARGET zstd::libzstd)
    if(SPARROW_IPC_BUILD_SHARED)
        if(TARGET zstd::libzstd_shared) # Linux case
            add_library(zstd::libzstd ALIAS zstd::libzstd_shared)
        elseif(TARGET libzstd_shared) # Windows case
            add_library(zstd::libzstd ALIAS libzstd_shared)
        endif()
    else()
        if(TARGET zstd::libzstd_static) # Linux case
            add_library(zstd::libzstd ALIAS zstd::libzstd_static)
        elseif(TARGET libzstd_static) # Windows case
            add_library(zstd::libzstd ALIAS libzstd_static)
        endif()
    endif()
endif()

if(${SPARROW_IPC_BUILD_TESTS} OR ${SPARROW_IPC_BUILD_INTEGRATION_TESTS})
    find_package_or_fetch(
        PACKAGE_NAME doctest
        GIT_REPOSITORY https://github.com/doctest/doctest.git
        TAG v2.4.12
    )
    
    message(STATUS "📦 Fetching arrow-testing")
    cmake_policy(PUSH)
    cmake_policy(SET CMP0174 NEW)  # Suppress warning about FetchContent_Declare GIT_REPOSITORY
    # Fetch arrow-testing data (no CMake build needed)
    FetchContent_Declare(
        arrow-testing
        GIT_REPOSITORY https://github.com/apache/arrow-testing.git
        GIT_SHALLOW TRUE
        # CONFIGURE_COMMAND ""
        # BUILD_COMMAND ""
        # INSTALL_COMMAND ""
    )
    FetchContent_MakeAvailable(arrow-testing)
    cmake_policy(POP)
    
    # Create interface library for easy access to test data
    add_library(arrow-testing-data INTERFACE)
    message(STATUS "Arrow testing data directory: ${arrow-testing_SOURCE_DIR}")
    target_compile_definitions(arrow-testing-data INTERFACE
        ARROW_TESTING_DATA_DIR="${arrow-testing_SOURCE_DIR}"
    )
    message(STATUS "\t✅ Fetched arrow-testing")

    # Fetch all the files in the cpp-21.0.0 directory
    file(GLOB_RECURSE arrow_testing_data_targz_files_cpp_21 CONFIGURE_DEPENDS
        "${arrow-testing_SOURCE_DIR}/data/arrow-ipc-stream/integration/cpp-21.0.0/*.json.gz"
    )
    # Fetch all the files in the 2.0.0-compression directory
    file(GLOB_RECURSE arrow_testing_data_targz_files_compression CONFIGURE_DEPENDS
        "${arrow-testing_SOURCE_DIR}/data/arrow-ipc-stream/integration/2.0.0-compression/*.json.gz"
    )

    # Combine lists of files
    list(APPEND arrow_testing_data_targz_files ${arrow_testing_data_targz_files_cpp_21} ${arrow_testing_data_targz_files_compression})
    # Iterate over all the files in the arrow-testing-data source directory. When it's a gz, extract in place.
    foreach(file_path IN LISTS arrow_testing_data_targz_files)
            cmake_path(GET file_path PARENT_PATH parent_dir)
            cmake_path(GET file_path STEM filename)
            set(destination_file_path "${parent_dir}/${filename}.json")
            if(EXISTS "${destination_file_path}")
                message(VERBOSE "File already extracted: ${destination_file_path}")
            else()
                message(STATUS "Extracting ${file_path}")
                if(WIN32)
                    execute_process(COMMAND powershell -Command "$i=\"${file_path}\"; $o=\"${destination_file_path}\"; [IO.Compression.GZipStream]::new([IO.File]::OpenRead($i),[IO.Compression.CompressionMode]::Decompress).CopyTo([IO.File]::Create($o))")
                else()
                    execute_process(COMMAND gunzip -kf "${file_path}")
                endif()
            endif()
    endforeach()
endif()
