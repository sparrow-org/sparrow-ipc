cmake_minimum_required(VERSION 3.28)

# Create the integration_tools library
add_library(integration_tools src/integration_tools.cpp)

target_link_libraries(integration_tools
    PUBLIC
        sparrow-ipc
        sparrow::sparrow
        sparrow::json_reader
)

target_include_directories(integration_tools
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
)

set_target_properties(integration_tools
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        FOLDER "integration_tests"
)

add_dependencies(integration_tools generate_flatbuffers_headers)

# Create executable for arrow_file_to_stream integration test
add_executable(arrow_file_to_stream arrow_file_to_stream.cpp)

target_link_libraries(arrow_file_to_stream
    PRIVATE
        integration_tools
)

target_include_directories(arrow_file_to_stream
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

set_target_properties(arrow_file_to_stream
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION ${SPARROW_IPC_BINARY_VERSION}
        SOVERSION ${SPARROW_IPC_BINARY_CURRENT}
        FOLDER "integration_tests"
        BUILD_RPATH_USE_ORIGIN ON
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)

add_dependencies(arrow_file_to_stream generate_flatbuffers_headers)

# Create executable for arrow_stream_to_file integration test
add_executable(arrow_stream_to_file arrow_stream_to_file.cpp)

target_link_libraries(arrow_stream_to_file
    PRIVATE
        integration_tools
)

target_include_directories(arrow_stream_to_file
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

set_target_properties(arrow_stream_to_file
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION ${SPARROW_IPC_BINARY_VERSION}
        SOVERSION ${SPARROW_IPC_BINARY_CURRENT}
        FOLDER integration_tests
        BUILD_RPATH_USE_ORIGIN ON
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)

add_dependencies(arrow_stream_to_file generate_flatbuffers_headers)

# Create executable for arrow_json_to_file integration test
add_executable(arrow_json_to_file arrow_json_to_file.cpp)

target_link_libraries(arrow_json_to_file
    PRIVATE
        integration_tools
)

target_include_directories(arrow_json_to_file
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

set_target_properties(arrow_json_to_file
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION ${SPARROW_IPC_BINARY_VERSION}
        SOVERSION ${SPARROW_IPC_BINARY_CURRENT}
        FOLDER integration_tests
        BUILD_RPATH_USE_ORIGIN ON
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)

add_dependencies(arrow_json_to_file generate_flatbuffers_headers)

# Create executable for arrow_validate integration test
add_executable(arrow_validate arrow_validate.cpp)

target_link_libraries(arrow_validate
    PRIVATE
        integration_tools
)

target_include_directories(arrow_validate
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

set_target_properties(arrow_validate
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION ${SPARROW_IPC_BINARY_VERSION}
        SOVERSION ${SPARROW_IPC_BINARY_CURRENT}
        FOLDER "Integration Tests"
        BUILD_RPATH_USE_ORIGIN ON
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)

add_dependencies(arrow_validate generate_flatbuffers_headers)

# Create test executable for integration tools
add_executable(test_integration_tools main.cpp test_integration_tools.cpp)

target_link_libraries(test_integration_tools
    PRIVATE
        integration_tools
        doctest::doctest
        arrow-testing-data
        flatbuffers::flatbuffers
)

target_include_directories(test_integration_tools
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
        ${CMAKE_SOURCE_DIR}/src
)

add_custom_target(run_test_integration_tools
    COMMAND test_integration_tools
    DEPENDS 
        test_integration_tools
    COMMENT "Running tests"
    USES_TERMINAL
)

set_target_properties(run_test_integration_tools PROPERTIES FOLDER "Tests utilities")

# Register with CTest
enable_testing()
add_test(NAME integration_tools_test COMMAND test_integration_tools)

# On Windows, copy required DLLs
if(WIN32)
    add_custom_command(
        TARGET arrow_file_to_stream POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::sparrow>"
        "$<TARGET_FILE_DIR:arrow_file_to_stream>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow-ipc>"
        "$<TARGET_FILE_DIR:arrow_file_to_stream>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::json_reader>"
        "$<TARGET_FILE_DIR:arrow_file_to_stream>"
        COMMENT "Copying DLLs to arrow_file_to_stream executable directory"
    )

    add_custom_command(
        TARGET arrow_stream_to_file POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::sparrow>"
        "$<TARGET_FILE_DIR:arrow_stream_to_file>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow-ipc>"
        "$<TARGET_FILE_DIR:arrow_stream_to_file>"
        COMMENT "Copying DLLs to arrow_stream_to_file executable directory"
    )

    add_custom_command(
        TARGET arrow_json_to_file POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::sparrow>"
        "$<TARGET_FILE_DIR:arrow_json_to_file>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow-ipc>"
        "$<TARGET_FILE_DIR:arrow_json_to_file>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::json_reader>"
        "$<TARGET_FILE_DIR:arrow_json_to_file>"
        COMMENT "Copying DLLs to arrow_json_to_file executable directory"
    )

    add_custom_command(
        TARGET arrow_validate POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::sparrow>"
        "$<TARGET_FILE_DIR:arrow_validate>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow-ipc>"
        "$<TARGET_FILE_DIR:arrow_validate>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::json_reader>"
        "$<TARGET_FILE_DIR:arrow_validate>"
        COMMENT "Copying DLLs to arrow_validate executable directory"
    )

    add_custom_command(
        TARGET test_integration_tools POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::sparrow>"
        "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow-ipc>"
        "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:sparrow::json_reader>"
        "$<TARGET_FILE_DIR:test_integration_tools>"
        COMMENT "Copying DLLs to test_integration_tools executable directory"
    )
endif()

set_target_properties(arrow_file_to_stream arrow_stream_to_file arrow_json_to_file arrow_validate test_integration_tools PROPERTIES FOLDER "Integration Tests")
