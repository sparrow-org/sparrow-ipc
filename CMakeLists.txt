cmake_minimum_required(VERSION 3.28)

project(sparrow-ipc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ Standard")
set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "C++ Standard Required")
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
include(CMakeDependentOption)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
message(DEBUG "CMake module path: ${CMAKE_MODULE_PATH}")

set(SPARROW_IPC_COMPILE_DEFINITIONS "" CACHE STRING "List of public compile definitions of the sparrow-ipc target")

set(SPARROW_IPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SPARROW_IPC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Linter options
# =============
OPTION(ACTIVATE_LINTER "Create targets to run clang-format" OFF)
MESSAGE(STATUS "🔧 Activate linter: ${ACTIVATE_LINTER}")
cmake_dependent_option(ACTIVATE_LINTER_DURING_COMPILATION "Run linter during the compilation" ON "ACTIVATE_LINTER" OFF)

if(ACTIVATE_LINTER)
    include(clang-format)
    include(clang-tidy)
endif()

# Versioning
# ==========
file(STRINGS "${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/config/sparrow_ipc_version.hpp" sparrow_ipc_version_defines
    REGEX "constexpr int SPARROW_IPC_VERSION_(MAJOR|MINOR|PATCH)")

foreach(ver ${sparrow_ipc_version_defines})
    if(ver MATCHES "constexpr int SPARROW_IPC_VERSION_(MAJOR|MINOR|PATCH) = ([0-9]+);$")
        set(PROJECT_VERSION_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()

set(CMAKE_PROJECT_VERSION
    ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

message(STATUS "Building sparrow-ipc v${CMAKE_PROJECT_VERSION}")

# Binary version
# See the following URL for more info on the binary versioning
# https://www.gnu.org/software/libtool/manual/html_node/Updating-version-info.html#Updating-version-info
file(STRINGS "${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/config/sparrow_ipc_version.hpp" sparrow_ipc_version_defines
    REGEX "constexpr int SPARROW_IPC_BINARY_(CURRENT|REVISION|AGE)")

foreach(ver ${sparrow_ipc_version_defines})
    if(ver MATCHES "constexpr int SPARROW_IPC_BINARY_(CURRENT|REVISION|AGE) = ([0-9]+);$")
        set(SPARROW_IPC_BINARY_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()

set(SPARROW_IPC_BINARY_VERSION
    ${SPARROW_IPC_BINARY_CURRENT}.${SPARROW_IPC_BINARY_REVISION}.${SPARROW_IPC_BINARY_AGE})

message(STATUS "sparrow-ipc binary version: v${SPARROW_IPC_BINARY_VERSION}")

# Build options
# =============

OPTION(SPARROW_IPC_BUILD_SHARED "Build sparrow-ipc as a shared library" ON)

if(SPARROW_IPC_BUILD_SHARED)
    message(STATUS "🔧 Build shared library")
    set(SPARROW_IPC_LIBRARY_TYPE SHARED)
else()
    message(STATUS "🔧 Build static library")
    set(SPARROW_IPC_LIBRARY_TYPE STATIC)
    list(APPEND SPARROW_IPC_COMPILE_DEFINITIONS SPARROW_IPC_STATIC_LIB)
endif()

OPTION(SPARROW_IPC_BUILD_TESTS "Build sparrow-ipc test suite" OFF)
MESSAGE(STATUS "🔧 Build tests: ${SPARROW_IPC_BUILD_TESTS}")

OPTION(SPARROW_IPC_BUILD_DOCS  "Build sparrow-ipc documentation" OFF)
MESSAGE(STATUS "🔧 Build docs: ${SPARROW_IPC_BUILD_DOCS}")

OPTION(SPARROW_IPC_BUILD_EXAMPLES "Build sparrow-ipc examples" OFF)
MESSAGE(STATUS "🔧 Build examples: ${SPARROW_IPC_BUILD_EXAMPLES}")

OPTION(SPARROW_IPC_BUILD_INTEGRATION_TESTS "Build sparrow-ipc integration tests" OFF)
MESSAGE(STATUS "🔧 Build integration tests: ${SPARROW_IPC_BUILD_INTEGRATION_TESTS}")

# Code coverage
# =============
OPTION(SPARROW_IPC_ENABLE_COVERAGE "Enable sparrow-ipc test coverage" OFF)
MESSAGE(STATUS "🔧 Enable coverage: ${SPARROW_IPC_ENABLE_COVERAGE}")

include(external_dependencies)

# Build
# =====
set(BINARY_BUILD_DIR "${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${BINARY_BUILD_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${BINARY_BUILD_DIR}")

if(SPARROW_IPC_ENABLE_COVERAGE)
    include(code_coverage)
endif()

set(SPARROW_IPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SPARROW_IPC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

set(SPARROW_IPC_HEADERS
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/any_output_stream.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/arrow_interface/arrow_array_schema_common_release.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/arrow_interface/arrow_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/arrow_interface/arrow_array/private_data.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/arrow_interface/arrow_schema.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/arrow_interface/arrow_schema/private_data.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/chunk_memory_output_stream.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/chunk_memory_serializer.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/compression.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/config/config.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/config/sparrow_ipc_version.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_array_impl.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_decimal_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_duration_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_fixedsizebinary_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_interval_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_null_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_primitive_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_time_related_arrays.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_utils.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize_variable_size_binary_array.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserialize.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/deserializer.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/encapsulated_message.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/flatbuffer_utils.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/magic_values.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/memory_output_stream.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/metadata.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/serialize_utils.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/serialize.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/serializer.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/stream_file_serializer.hpp
    ${SPARROW_IPC_INCLUDE_DIR}/sparrow_ipc/utils.hpp
)

set(SPARROW_IPC_SRC
    ${SPARROW_IPC_SOURCE_DIR}/any_output_stream.cpp
    ${SPARROW_IPC_SOURCE_DIR}/array_deserializer_impl.hpp
    ${SPARROW_IPC_SOURCE_DIR}/arrow_interface/arrow_array.cpp
    ${SPARROW_IPC_SOURCE_DIR}/arrow_interface/arrow_array/private_data.cpp
    ${SPARROW_IPC_SOURCE_DIR}/arrow_interface/arrow_schema.cpp
    ${SPARROW_IPC_SOURCE_DIR}/arrow_interface/arrow_schema/private_data.cpp
    ${SPARROW_IPC_SOURCE_DIR}/chunk_memory_serializer.cpp
    ${SPARROW_IPC_SOURCE_DIR}/compression.cpp
    ${SPARROW_IPC_SOURCE_DIR}/compression_impl.hpp
    ${SPARROW_IPC_SOURCE_DIR}/deserialize_fixedsizebinary_array.cpp
    ${SPARROW_IPC_SOURCE_DIR}/deserialize_null_array.cpp
    ${SPARROW_IPC_SOURCE_DIR}/deserialize_utils.cpp
    ${SPARROW_IPC_SOURCE_DIR}/deserialize.cpp
    ${SPARROW_IPC_SOURCE_DIR}/encapsulated_message.cpp
    ${SPARROW_IPC_SOURCE_DIR}/flatbuffer_utils.cpp
    ${SPARROW_IPC_SOURCE_DIR}/metadata.cpp
    ${SPARROW_IPC_SOURCE_DIR}/serialize_utils.cpp
    ${SPARROW_IPC_SOURCE_DIR}/serialize.cpp
    ${SPARROW_IPC_SOURCE_DIR}/serializer.cpp
    ${SPARROW_IPC_SOURCE_DIR}/stream_file_serializer.cpp
    ${SPARROW_IPC_SOURCE_DIR}/utils.cpp
)

# Fetch schemas from apache arrow
set(SCHEMA_DIR ${CMAKE_BINARY_DIR}/format)
set(SCHEMA_URLS
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/File.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Message.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Schema.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/Tensor.fbs"
    "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/format/SparseTensor.fbs"
    # TODO what about feather.fbs?
)

file(MAKE_DIRECTORY ${SCHEMA_DIR})

# Download schemas
set(FLATBUFFERS_SCHEMAS "")
foreach(url IN LISTS SCHEMA_URLS)
    get_filename_component(filename ${url} NAME)
    if(NOT EXISTS ${SCHEMA_DIR}/${filename})
        message(STATUS "Downloading schema: ${url}")
        file(DOWNLOAD ${url} ${SCHEMA_DIR}/${filename}
            STATUS status)
    endif()

    list(APPEND FLATBUFFERS_SCHEMAS ${SCHEMA_DIR}/${filename})
endforeach()

# Generate Flatbuffers C++ headers from the schemas
set(FLATBUFFERS_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${FLATBUFFERS_GENERATED_DIR})

# Generate output files list
set(FLATBUFFERS_GENERATED_HEADERS "")
foreach(fbs_file IN LISTS FLATBUFFERS_SCHEMAS)
    # Generate the corresponding header file name
    get_filename_component(header_name ${fbs_file} NAME_WE)
    list(APPEND FLATBUFFERS_GENERATED_HEADERS "${FLATBUFFERS_GENERATED_DIR}/${header_name}_generated.h")
endforeach()

if(TARGET flatbuffers::flatc)
    set(FLATC_EXECUTABLE flatbuffers::flatc)
else()
    set(FLATC_EXECUTABLE flatc)
endif()

add_custom_command(
    OUTPUT ${FLATBUFFERS_GENERATED_HEADERS}
    COMMAND ${FLATC_EXECUTABLE} --cpp -o ${FLATBUFFERS_GENERATED_DIR} --cpp-std c++17 --scoped-enums ${FLATBUFFERS_SCHEMAS}
    DEPENDS ${FLATBUFFERS_SCHEMAS}
    COMMENT "Generating FlatBuffers C++ headers from schemas"
)

add_custom_target(generate_flatbuffers_headers
    DEPENDS ${FLATBUFFERS_GENERATED_HEADERS}
    COMMENT "Ensuring FlatBuffers headers are generated"
)

# Interface target for generated headers
add_library(flatbuffers_interface INTERFACE)
target_include_directories(flatbuffers_interface INTERFACE 
    $<BUILD_INTERFACE:${FLATBUFFERS_GENERATED_DIR}>
    $<INSTALL_INTERFACE:include/flatbuffers_generated>)
add_dependencies(flatbuffers_interface generate_flatbuffers_headers)

add_library(sparrow-ipc ${SPARROW_IPC_LIBRARY_TYPE} ${SPARROW_IPC_SRC} ${SPARROW_IPC_HEADERS})

target_compile_options(sparrow-ipc
    PRIVATE
        ${compile_options}
)

target_compile_definitions(sparrow-ipc
    PUBLIC
        ${SPARROW_IPC_COMPILE_DEFINITIONS})

if(UNIX)
    # CMake does not compute the version number of so files as libtool
    # does on Linux. Strictly speaking, we should exclude FreeBSD and
    # Apple from this, but that would require having different version
    # numbers depending on the platform. We prefer to follow the
    # libtool pattern everywhere.
    math(EXPR SPARROW_IPC_BINARY_COMPATIBLE "${SPARROW_IPC_BINARY_CURRENT} - ${SPARROW_IPC_BINARY_AGE}")
    set_target_properties(
        sparrow-ipc
        PROPERTIES
        VERSION "${SPARROW_IPC_BINARY_COMPATIBLE}.${SPARROW_IPC_BINARY_REVISION}.${SPARROW_IPC_BINARY_AGE}"
        SOVERSION ${SPARROW_IPC_BINARY_COMPATIBLE}
    )
    target_compile_options(sparrow-ipc PRIVATE "-fvisibility=hidden")
else()
    set_target_properties(
        sparrow-ipc
        PROPERTIES
        VERSION ${SPARROW_IPC_BINARY_VERSION}
        SOVERSION ${SPARROW_IPC_BINARY_CURRENT}
    )
    target_compile_definitions(sparrow-ipc PRIVATE SPARROW_IPC_EXPORTS)
endif()

target_include_directories(sparrow-ipc 
    PUBLIC
        $<BUILD_INTERFACE:${SPARROW_IPC_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        $<BUILD_INTERFACE:${FLATBUFFERS_GENERATED_DIR}>)

target_link_libraries(sparrow-ipc 
    PUBLIC
        sparrow::sparrow
        flatbuffers::flatbuffers
    PRIVATE
        lz4::lz4
        zstd::libzstd
        )

# Ensure generated headers are available when building sparrow-ipc
add_dependencies(sparrow-ipc generate_flatbuffers_headers)

# Tests
# =====
if(SPARROW_IPC_BUILD_TESTS)
    message(STATUS "🧪 Create tests targets")
    enable_testing()
    add_subdirectory(tests)
endif()

# Docs
# ====
if(SPARROW_IPC_BUILD_DOCS)
    message(STATUS "📚 Create docs targets")
    add_subdirectory(docs)
endif()

# Examples
# ========
if(SPARROW_IPC_BUILD_EXAMPLES)
    message(STATUS "🔨 Create examples targets")
    add_subdirectory(examples)
endif()

# Integration tests
# =================
if(SPARROW_IPC_BUILD_INTEGRATION_TESTS)
    message(STATUS "🔨 Create integration tests targets")
    add_subdirectory(integration_tests)
endif()

# Installation
# ============
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install CMake configuration files
set(SPARROW_IPC_CMAKECONFIG_INSTALL_DIR "${CMAKE_INSTALL_DATADIR}/cmake/${PROJECT_NAME}" CACHE
    STRING "install path for sparrow-ipcConfig.cmake")

configure_package_config_file(cmake/${PROJECT_NAME}Config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION ${SPARROW_IPC_CMAKECONFIG_INSTALL_DIR})

write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    VERSION ${CMAKE_PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    DESTINATION ${SPARROW_IPC_CMAKECONFIG_INSTALL_DIR})

# Install target
set(SPARROW_IPC_EXPORTED_TARGETS sparrow-ipc)
if (TARGET flatbuffers)
    get_target_property(is_imported flatbuffers IMPORTED)
    if(NOT is_imported)
        # This means `flatbuffers` was fetched using FetchContent
        # We need to export `flatbuffers` target explicitly
        list(APPEND SPARROW_IPC_EXPORTED_TARGETS flatbuffers)
        if(TARGET ProjectConfig)
            # This is an internal target from the flatbuffers build system
            # that also needs to be exported
            list(APPEND SPARROW_IPC_EXPORTED_TARGETS ProjectConfig)
        endif()
    endif()
endif()

if (TARGET lz4)
    get_target_property(is_imported lz4 IMPORTED)
    if(NOT is_imported)
        # This means `lz4` was fetched using FetchContent
        # We need to export `lz4` target explicitly
        list(APPEND SPARROW_IPC_EXPORTED_TARGETS lz4)
    endif()
endif()

if (TARGET lz4_static)
    get_target_property(is_imported lz4_static IMPORTED)
    if(NOT is_imported)
        # `lz4_static` is needed as this is the actual library
        # and `lz4` is an interface pointing to it.
        # If `lz4_shared` is used instead for some reason, modify this accordingly
        list(APPEND SPARROW_IPC_EXPORTED_TARGETS lz4_static)
    endif()
endif()

install(TARGETS ${SPARROW_IPC_EXPORTED_TARGETS}
    EXPORT ${PROJECT_NAME}-targets)

install(EXPORT ${PROJECT_NAME}-targets
    FILE ${PROJECT_NAME}Targets.cmake
    NAMESPACE sparrow-ipc::
    DESTINATION ${SPARROW_IPC_CMAKECONFIG_INSTALL_DIR})

# Makes the project importable from the build directory
export(EXPORT ${PROJECT_NAME}-targets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake"
    NAMESPACE sparrow-ipc::)

# Install headers
install(DIRECTORY ${SPARROW_IPC_INCLUDE_DIR}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    PATTERN ".clang-*" EXCLUDE
    PATTERN "README.md" EXCLUDE)
