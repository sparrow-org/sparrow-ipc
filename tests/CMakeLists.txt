cmake_minimum_required(VERSION 3.28)

set(test_target "test_sparrow_ipc_lib")

set(SPARROW_IPC_TESTS_SRC
    include/sparrow_ipc_tests_helpers.hpp
    main.cpp
    $<$<NOT:$<BOOL:${SPARROW_IPC_BUILD_SHARED}>>:test_flatbuffer_utils.cpp>
    test_any_output_stream.cpp
    test_arrow_array.cpp
    test_arrow_schema.cpp
    test_chunk_memory_output_stream.cpp
    test_chunk_memory_serializer.cpp
    test_compression.cpp
    test_de_serialization_with_files.cpp
    test_deserializer.cpp
    test_dictionary_components.cpp
    test_dictionary.cpp
    test_helper.hpp
    test_map_keys_sorted.cpp
    test_memory_output_streams.cpp
    test_serialize_utils.cpp
    test_serializer.cpp
    test_stream_file_serializer.cpp
    test_utils.cpp
)

add_executable(${test_target} ${SPARROW_IPC_TESTS_SRC})
target_link_libraries(${test_target}
    PRIVATE
        sparrow-ipc
        doctest::doctest
        sparrow::json_reader
        arrow-testing-data
)

if(WIN32)
    set(ZSTD_DLL_TARGET "")
    if(TARGET libzstd_shared AND SPARROW_IPC_BUILD_SHARED) # Building deps from src case: shared target without namespace
        set(ZSTD_DLL_TARGET libzstd_shared)
    elseif(TARGET libzstd_static AND NOT SPARROW_IPC_BUILD_SHARED) # Building deps from src case: static target without namespace
        set(ZSTD_DLL_TARGET libzstd_static)
    endif()

    set(DLL_COPY_COMMANDS "") # Initialize a list to hold all copy commands
    # Add unconditional copy commands
    list(APPEND DLL_COPY_COMMANDS
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:sparrow::sparrow>"
            "$<TARGET_FILE_DIR:${test_target}>"
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:sparrow-ipc>"
            "$<TARGET_FILE_DIR:${test_target}>"
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:sparrow::json_reader>"
            "$<TARGET_FILE_DIR:${test_target}>"
    )

    if(${USE_DATE_POLYFILL})
        find_package(date REQUIRED) # For copying DLLs
 
        list(APPEND DLL_COPY_COMMANDS
            COMMAND ${CMAKE_COMMAND} -E copy
                "$<TARGET_FILE:date::date-tz>"
                "$<TARGET_FILE_DIR:${test_target}>"
        )
    endif()

    # Conditionally add ZSTD copy command
    if(ZSTD_DLL_TARGET)
        list(APPEND DLL_COPY_COMMANDS
            COMMAND ${CMAKE_COMMAND} -E copy
                "$<TARGET_FILE:${ZSTD_DLL_TARGET}>"
                "$<TARGET_FILE_DIR:${test_target}>"
        )
    else()
        message(WARNING "ZSTD DLL will not be copied for tests.")
    endif()

    add_custom_command(
        TARGET ${test_target} POST_BUILD
        ${DLL_COPY_COMMANDS}
        COMMENT "Copying required DLLs to executable directory"
    )
endif()

target_include_directories(${test_target}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/generated
        ${CMAKE_SOURCE_DIR}/include
)
add_dependencies(${test_target} generate_flatbuffers_headers)
add_test(NAME sparrow-ipc-tests COMMAND ${test_target})

add_custom_target(run_tests
    COMMAND ${test_target}
    DEPENDS 
        ${test_target}
    COMMENT "Running tests"
    USES_TERMINAL
)

set_target_properties(run_tests PROPERTIES FOLDER "Tests utilities")

set(JUNIT_REPORT_FILE_DOCTEST ${CMAKE_CURRENT_BINARY_DIR}/test_sparrow-ipc_lib_report_doctest.xml)

add_custom_target(run_tests_with_junit_report
    COMMAND ${test_target} --reporters=junit --out=${JUNIT_REPORT_FILE_DOCTEST} --no-path-filenames=true # TODO: use better junit as in sparrow
    DEPENDS
        ${test_target} 
    COMMENT "Running tests with JUnit reports saved to: ${JUNIT_REPORT_FILE_DOCTEST}"
    USES_TERMINAL
)

set_target_properties(run_tests_with_junit_report PROPERTIES FOLDER "Tests utilities")
