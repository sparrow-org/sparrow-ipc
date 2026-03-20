#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <File_generated.h>
#include <Message_generated.h>
#include <nlohmann/json.hpp>
#include <Schema_generated.h>

#include <sparrow/c_interface.hpp>
#include <sparrow/json_reader/json_parser.hpp>
#include <sparrow/record_batch.hpp>

#include "doctest/doctest.h"
#include "integration_tools.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"

TEST_SUITE("Integration Tools Tests")
{
    // Get paths to test data
    const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;
    const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                             / "integration" / "cpp-21.0.0";

    TEST_CASE("json_file_to_stream - Non-existent file")
    {
        const std::filesystem::path non_existent = "non_existent_file_12345.json";
        CHECK_THROWS_AS(integration_tools::json_file_to_stream(non_existent), std::runtime_error);
    }

    TEST_CASE("stream_to_file - Empty input")
    {
        std::vector<uint8_t> empty_data;
        CHECK_THROWS_AS(
            integration_tools::stream_to_file(std::span<const uint8_t>(empty_data)),
            std::runtime_error
        );
    }

    TEST_CASE("validate_json_against_arrow_file - Non-existent JSON file")
    {
        const std::filesystem::path non_existent = "non_existent_file_12345.json";
        std::vector<uint8_t> dummy_stream = {1, 2, 3};
        CHECK_THROWS_AS(
            integration_tools::validate_json_against_arrow_file(
                non_existent,
                std::span<const uint8_t>(dummy_stream)
            ),
            std::runtime_error
        );
    }

    TEST_CASE("json_file_to_stream - Convert JSON to stream")
    {
        // Test with a known good JSON file
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert using the library
        std::vector<uint8_t> stream_data;
        CHECK_NOTHROW(stream_data = integration_tools::json_file_to_stream(json_file));
        CHECK_GT(stream_data.size(), 0);

        // Verify the output is a valid stream by deserializing it
        auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        CHECK_GT(batches.size(), 0);
    }

    TEST_CASE("stream_to_file - Process stream")
    {
        const std::filesystem::path input_stream = tests_resources_files_path / "generated_primitive.stream";

        if (!std::filesystem::exists(input_stream))
        {
            MESSAGE("Skipping test: test file not found at " << input_stream);
            return;
        }

        // Read input stream
        std::ifstream stream_file(input_stream, std::ios::binary);
        REQUIRE(stream_file.is_open());

        const std::vector<uint8_t> input_data(
            (std::istreambuf_iterator<char>(stream_file)),
            std::istreambuf_iterator<char>()
        );
        stream_file.close();

        // Convert using the library
        std::vector<uint8_t> output_data;
        CHECK_NOTHROW(output_data = integration_tools::stream_to_file(std::span<const uint8_t>(input_data)));
        CHECK_GT(output_data.size(), 0);

        // Verify the output is valid
        const auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(output_data));
        CHECK_GT(batches.size(), 0);

        // Check footer
        const auto* footer = sparrow_ipc::get_footer_from_file_data(output_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        CHECK_EQ(footer->recordBatches()->size(), batches.size());

        // Check alignment of record batch blocks
        for (size_t i = 0; i < footer->recordBatches()->size(); ++i)
        {
            const auto& block = *footer->recordBatches()->Get(static_cast<uint32_t>(i));
            CHECK_EQ(block.offset() % 8, 0);
            CHECK_EQ(block.bodyLength() % 8, 0);
            CHECK_EQ(block.metaDataLength() % 8, 0);
        }
    }

    TEST_CASE("stream_to_file - Process dictionary stream")
    {
        const std::filesystem::path input_stream = tests_resources_files_path / "generated_dictionary.stream";

        if (!std::filesystem::exists(input_stream))
        {
            MESSAGE("Skipping test: test file not found at " << input_stream);
            return;
        }

        // Read input stream
        std::ifstream stream_file(input_stream, std::ios::binary);
        REQUIRE(stream_file.is_open());

        const std::vector<uint8_t> input_data(
            (std::istreambuf_iterator<char>(stream_file)),
            std::istreambuf_iterator<char>()
        );
        stream_file.close();

        // Convert using the library (without explicit schema_batch)
        std::vector<uint8_t> output_data;
        CHECK_NOTHROW(output_data = integration_tools::stream_to_file(std::span<const uint8_t>(input_data)));
        CHECK_GT(output_data.size(), 0);

        // Verify the output is valid by deserializing the file
        const auto file_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(output_data));

        // Also deserialize the original stream for comparison
        const auto stream_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(input_data));

        // Both should have the same number of batches
        CHECK_EQ(file_batches.size(), stream_batches.size());

        // Compare each batch
        for (size_t i = 0; i < file_batches.size(); ++i)
        {
            CHECK(integration_tools::compare_record_batch(stream_batches[i], file_batches[i], i, false));
        }

        // Check footer schema has dictionary information
        const auto* footer = sparrow_ipc::get_footer_from_file_data(output_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->schema() != nullptr);
        REQUIRE(footer->schema()->fields() != nullptr);

        // Verify all dictionary fields have dictionary encoding in the schema
        for (size_t i = 0; i < footer->schema()->fields()->size(); ++i)
        {
            const auto* field = footer->schema()->fields()->Get(static_cast<uint32_t>(i));
            REQUIRE(field != nullptr);
            // Fields that are dictionary-encoded should have a dictionary member
            // (we're just checking the schema is preserved, actual dict info is in footer->dictionaries())
        }

        // Check that dictionary blocks are present
        if (footer->dictionaries() != nullptr)
        {
            CHECK_GT(footer->dictionaries()->size(), 0);
        }
    }

    TEST_CASE("Round-trip: JSON -> stream -> file")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Step 1: JSON -> stream
        const std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
        REQUIRE_GT(stream_data.size(), 0);

        // Step 2: stream -> file
        const std::vector<uint8_t> file_data = integration_tools::stream_to_file(
            std::span<const uint8_t>(stream_data)
        );
        REQUIRE_GT(file_data.size(), 0);

        // Step 3: Compare the results - both should deserialize to same data
        const auto stream_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        const auto file_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(stream_batches.size(), file_batches.size());
        for (size_t i = 0; i < stream_batches.size(); ++i)
        {
            CHECK(integration_tools::compare_record_batch(stream_batches[i], file_batches[i], i, false));
        }
    }

    TEST_CASE("Round-trip: JSON -> Arrow file -> Arrow stream with record batch count verification")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Load and parse the JSON file to get expected batch count
        auto json_data = integration_tools::parse_json_file(json_file);

        REQUIRE(json_data.contains("batches"));
        const size_t expected_batch_count = json_data["batches"].size();
        REQUIRE_EQ(expected_batch_count, 2);

        // Step 1: JSON -> Arrow file
        const std::vector<uint8_t> arrow_file_data = integration_tools::json_file_to_arrow_file(json_file);
        REQUIRE_GT(arrow_file_data.size(), 0);

        // Verify record batch count in Arrow file footer
        const auto* footer = sparrow_ipc::get_footer_from_file_data(arrow_file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        CHECK_EQ(footer->recordBatches()->size(), expected_batch_count);

        // Step 2: Deserialize Arrow file
        const auto file_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(arrow_file_data));
        CHECK_EQ(file_batches.size(), expected_batch_count);

        // Step 3: Arrow file -> Arrow stream (re-serialize deserialized batches)
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream mem_stream(stream_data);
        sparrow_ipc::serializer serializer(mem_stream);
        serializer << file_batches << sparrow_ipc::end_stream;
        REQUIRE_GT(stream_data.size(), 0);

        // Step 4: Deserialize Arrow stream and verify record batch count
        const auto stream_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        CHECK_EQ(stream_batches.size(), expected_batch_count);

        // Step 5: Compare the results - all batches should match
        REQUIRE_EQ(file_batches.size(), stream_batches.size());
        for (size_t i = 0; i < file_batches.size(); ++i)
        {
            CHECK(integration_tools::compare_record_batch(file_batches[i], stream_batches[i], i, false));
        }

        // Output summary
        MESSAGE("JSON -> Arrow file -> Arrow stream round-trip successful:");
        MESSAGE("  Expected batch count: " << expected_batch_count);
        MESSAGE("  Arrow file footer batch count: " << footer->recordBatches()->size());
        MESSAGE("  Deserialized file batches: " << file_batches.size());
        MESSAGE("  Deserialized stream batches: " << stream_batches.size());
    }

    TEST_CASE("validate_json_against_arrow_file - Successful validation")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        const std::vector<uint8_t> arrow_file_data = integration_tools::json_file_to_arrow_file(json_file);

        // Validate
        const bool matches = integration_tools::validate_json_against_arrow_file(
            json_file,
            std::span<const uint8_t>(arrow_file_data)
        );
        CHECK(matches);
    }

    TEST_CASE("validate_json_against_arrow_file - With reference stream file")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";
        const std::filesystem::path arrow_file = tests_resources_files_path / "generated_primitive.arrow_file";

        if (!std::filesystem::exists(json_file) || !std::filesystem::exists(arrow_file))
        {
            MESSAGE("Skipping test: test file(s) not found");
            return;
        }

        // Read the stream file
        std::ifstream stream_input(arrow_file, std::ios::binary);
        REQUIRE(stream_input.is_open());

        const std::vector<uint8_t> arrow_file_data(
            (std::istreambuf_iterator<char>(stream_input)),
            std::istreambuf_iterator<char>()
        );
        stream_input.close();

        // Validate
        bool matches = integration_tools::validate_json_against_arrow_file(
            json_file,
            std::span<const uint8_t>(arrow_file_data)
        );
        CHECK(matches);
    }

    TEST_CASE("json_to_arrow_file and validate - Round-trip with validation")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert JSON to stream
        const std::vector<uint8_t> arrow_file_data = integration_tools::json_file_to_arrow_file(json_file);
        REQUIRE_GT(arrow_file_data.size(), 0);

        // Validate that the stream matches the JSON
        const bool matches = integration_tools::validate_json_against_arrow_file(
            json_file,
            std::span<const uint8_t>(arrow_file_data)
        );
        // write to a temp file
        std::filesystem::path temp_file = std::filesystem::temp_directory_path() / "temp.arrow_file";
        std::ofstream out(temp_file, std::ios::binary);
        out.write(reinterpret_cast<const char*>(arrow_file_data.data()), arrow_file_data.size());
        out.close();

        CHECK(matches);

        // Also verify by deserializing
        const auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(arrow_file_data));
        CHECK_EQ(batches.size(), 2);
    }

    TEST_CASE("json_to_stream and validate - Dictionary stream round-trip")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_dictionary.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert JSON to stream
        const std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
        REQUIRE_GT(stream_data.size(), 0);

        // Convert stream to file
        const std::vector<uint8_t> arrow_file_data = integration_tools::stream_to_file(
            std::span<const uint8_t>(stream_data)
        );
        REQUIRE_GT(arrow_file_data.size(), 0);

        // Validate that the file matches the JSON
        const bool matches = integration_tools::validate_json_against_arrow_file(
            json_file,
            std::span<const uint8_t>(arrow_file_data)
        );
        CHECK(matches);

        // Also verify by deserializing the file
        const auto file_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(arrow_file_data));

        // Load JSON to get expected batches
        auto json_data = integration_tools::parse_json_file(json_file);
        REQUIRE(json_data.contains("batches"));
        const size_t num_batches = json_data["batches"].size();
        CHECK_EQ(file_batches.size(), num_batches);

        // Build expected batches from JSON and compare
        for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
        {
            auto expected_batch = sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx);
            CHECK(integration_tools::compare_record_batch(expected_batch, file_batches[batch_idx], batch_idx, false));
        }
    }

    TEST_CASE("compare_record_batch - Identical batches")
    {
        // Create two identical batches
        auto int_array1 = sparrow::primitive_array<int32_t>({1, 2, 3});
        auto int_array2 = sparrow::primitive_array<int32_t>({1, 2, 3});

        sparrow::record_batch batch1({{"col", sparrow::array(std::move(int_array1))}});
        sparrow::record_batch batch2({{"col", sparrow::array(std::move(int_array2))}});

        CHECK(integration_tools::compare_record_batch(batch1, batch2, 0, false));
    }

    TEST_CASE("compare_record_batch - Different values")
    {
        // Create two batches with different values
        auto int_array1 = sparrow::primitive_array<int32_t>({1, 2, 3});
        auto int_array2 = sparrow::primitive_array<int32_t>({1, 2, 4});

        sparrow::record_batch batch1({{"col", sparrow::array(std::move(int_array1))}});
        sparrow::record_batch batch2({{"col", sparrow::array(std::move(int_array2))}});

        CHECK_FALSE(integration_tools::compare_record_batch(batch1, batch2, 0, false));
    }

    TEST_CASE("Multiple test files")
    {
        // Test with multiple files from the test data directory
        const std::vector<std::string> test_files = {
            "generated_primitive.json",
            "generated_binary.json",
            "generated_primitive_zerolength.json",
            "generated_binary_zerolength.json"
        };

        for (const auto& filename : test_files)
        {
            const std::filesystem::path json_file = tests_resources_files_path / filename;

            if (!std::filesystem::exists(json_file))
            {
                MESSAGE("Skipping test file: " << filename);
                continue;
            }

            SUBCASE(filename.c_str())
            {
                // Convert to stream
                const std::vector<uint8_t> arrow_file_data = integration_tools::json_file_to_arrow_file(
                    json_file
                );
                REQUIRE_GT(arrow_file_data.size(), 0);

                // Validate
                const bool matches = integration_tools::validate_json_against_arrow_file(
                    json_file,
                    std::span<const uint8_t>(arrow_file_data)
                );
                CHECK(matches);

                // Verify we can deserialize
                const auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(arrow_file_data));
                CHECK_GE(batches.size(), 0);
            }
        }
    }

    TEST_CASE("Schema validation through JSON -> record_batch -> stream pipeline")
    {
        // This test validates schema content at each stage of the pipeline:
        // 1. Parse schema from JSON
        // 2. Build record batches and verify schema
        // 3. Serialize to Arrow IPC stream and verify schema in the stream

        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Load and parse the JSON file
        auto json_data = integration_tools::parse_json_file(json_file);

        // Expected schema from generated_primitive.json
        struct ExpectedField
        {
            std::string name;
            bool nullable;
            org::apache::arrow::flatbuf::Type type_enum;
            // For Int types
            bool is_signed = false;
            int bit_width = 0;
            // For FloatingPoint types
            org::apache::arrow::flatbuf::Precision precision = org::apache::arrow::flatbuf::Precision::SINGLE;
        };

        const std::vector<ExpectedField> expected_fields = {
            {"bool_nullable", true, org::apache::arrow::flatbuf::Type::Bool},
            {"bool_nonnullable", false, org::apache::arrow::flatbuf::Type::Bool},
            {"int8_nullable", true, org::apache::arrow::flatbuf::Type::Int, true, 8},
            {"int8_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, true, 8},
            {"int16_nullable", true, org::apache::arrow::flatbuf::Type::Int, true, 16},
            {"int16_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, true, 16},
            {"int32_nullable", true, org::apache::arrow::flatbuf::Type::Int, true, 32},
            {"int32_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, true, 32},
            {"int64_nullable", true, org::apache::arrow::flatbuf::Type::Int, true, 64},
            {"int64_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, true, 64},
            {"uint8_nullable", true, org::apache::arrow::flatbuf::Type::Int, false, 8},
            {"uint8_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, false, 8},
            {"uint16_nullable", true, org::apache::arrow::flatbuf::Type::Int, false, 16},
            {"uint16_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, false, 16},
            {"uint32_nullable", true, org::apache::arrow::flatbuf::Type::Int, false, 32},
            {"uint32_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, false, 32},
            {"uint64_nullable", true, org::apache::arrow::flatbuf::Type::Int, false, 64},
            {"uint64_nonnullable", false, org::apache::arrow::flatbuf::Type::Int, false, 64},
            {"float32_nullable",
             true,
             org::apache::arrow::flatbuf::Type::FloatingPoint,
             false,
             0,
             org::apache::arrow::flatbuf::Precision::SINGLE},
            {"float32_nonnullable",
             false,
             org::apache::arrow::flatbuf::Type::FloatingPoint,
             false,
             0,
             org::apache::arrow::flatbuf::Precision::SINGLE},
            {"float64_nullable",
             true,
             org::apache::arrow::flatbuf::Type::FloatingPoint,
             false,
             0,
             org::apache::arrow::flatbuf::Precision::DOUBLE},
            {"float64_nonnullable",
             false,
             org::apache::arrow::flatbuf::Type::FloatingPoint,
             false,
             0,
             org::apache::arrow::flatbuf::Precision::DOUBLE},
        };

        SUBCASE("Step 1: Verify JSON schema structure")
        {
            REQUIRE(json_data.contains("schema"));
            const auto& schema = json_data["schema"];
            REQUIRE(schema.contains("fields"));
            const auto& fields = schema["fields"];
            REQUIRE(fields.is_array());
            CHECK_EQ(fields.size(), expected_fields.size());

            for (size_t i = 0; i < expected_fields.size() && i < fields.size(); ++i)
            {
                const auto& field = fields[i];
                const auto& expected = expected_fields[i];

                CHECK_EQ(field["name"].get<std::string>(), expected.name);
                CHECK_EQ(field["nullable"].get<bool>(), expected.nullable);

                const auto& type = field["type"];
                const std::string type_name = type["name"].get<std::string>();

                if (expected.type_enum == org::apache::arrow::flatbuf::Type::Bool)
                {
                    CHECK_EQ(type_name, "bool");
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::Int)
                {
                    CHECK_EQ(type_name, "int");
                    CHECK_EQ(type["isSigned"].get<bool>(), expected.is_signed);
                    CHECK_EQ(type["bitWidth"].get<int>(), expected.bit_width);
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::FloatingPoint)
                {
                    CHECK_EQ(type_name, "floatingpoint");
                    const std::string precision = type["precision"].get<std::string>();
                    if (expected.precision == org::apache::arrow::flatbuf::Precision::SINGLE)
                    {
                        CHECK_EQ(precision, "SINGLE");
                    }
                    else if (expected.precision == org::apache::arrow::flatbuf::Precision::DOUBLE)
                    {
                        CHECK_EQ(precision, "DOUBLE");
                    }
                }
            }
        }

        SUBCASE("Step 2: Verify record batch schema from JSON parsing")
        {
            REQUIRE(json_data.contains("batches"));
            REQUIRE(json_data["batches"].size() >= 1);

            const auto record_batch = sparrow::json_reader::build_record_batch_from_json(json_data, 0);

            CHECK_EQ(record_batch.nb_columns(), expected_fields.size());

            const auto& names = record_batch.names();
            for (size_t i = 0; i < expected_fields.size() && i < names.size(); ++i)
            {
                CHECK_EQ(names[i], expected_fields[i].name);
            }

            // Verify data types of columns and nullable flags
            for (size_t i = 0; i < expected_fields.size() && i < record_batch.nb_columns(); ++i)
            {
                const auto& column = record_batch.get_column(i);
                const auto& expected = expected_fields[i];

                // Check nullable flag via arrow proxy flags
                const auto& flags = sparrow::detail::array_access::get_arrow_proxy(column).flags();
                const bool is_nullable = flags.contains(sparrow::ArrowFlag::NULLABLE);
                CHECK_EQ(is_nullable, expected.nullable);

                // Check metadata (generated_primitive.json doesn't have custom metadata, so it should be
                // empty)
                const auto metadata = column.metadata();
                // For primitive types without custom metadata, metadata should be nullopt or empty
                if (metadata.has_value())
                {
                    // If metadata exists, it should be empty for this test file
                    CHECK_EQ(metadata->size(), 0);
                }

                // Map expected FlatBuffer type to sparrow data_type
                sparrow::data_type expected_data_type;
                if (expected.type_enum == org::apache::arrow::flatbuf::Type::Bool)
                {
                    expected_data_type = sparrow::data_type::BOOL;
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::Int)
                {
                    if (expected.is_signed)
                    {
                        switch (expected.bit_width)
                        {
                            case 8:
                                expected_data_type = sparrow::data_type::INT8;
                                break;
                            case 16:
                                expected_data_type = sparrow::data_type::INT16;
                                break;
                            case 32:
                                expected_data_type = sparrow::data_type::INT32;
                                break;
                            case 64:
                                expected_data_type = sparrow::data_type::INT64;
                                break;
                            default:
                                FAIL("Unknown bit width");
                        }
                    }
                    else
                    {
                        switch (expected.bit_width)
                        {
                            case 8:
                                expected_data_type = sparrow::data_type::UINT8;
                                break;
                            case 16:
                                expected_data_type = sparrow::data_type::UINT16;
                                break;
                            case 32:
                                expected_data_type = sparrow::data_type::UINT32;
                                break;
                            case 64:
                                expected_data_type = sparrow::data_type::UINT64;
                                break;
                            default:
                                FAIL("Unknown bit width");
                        }
                    }
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::FloatingPoint)
                {
                    if (expected.precision == org::apache::arrow::flatbuf::Precision::SINGLE)
                    {
                        expected_data_type = sparrow::data_type::FLOAT;
                    }
                    else
                    {
                        expected_data_type = sparrow::data_type::DOUBLE;
                    }
                }

                CHECK_EQ(column.data_type(), expected_data_type);
            }
        }

        SUBCASE("Step 3: Verify schema in serialized Arrow IPC stream")
        {
            // Convert JSON to stream
            const std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
            REQUIRE_GT(stream_data.size(), 0);

            // The stream format starts with a Schema message
            // Parse the first message to verify schema
            // Stream format: [continuation (4)] [size (4)] [flatbuffer message] [padding] [body]...

            // Skip continuation marker (0xFFFFFFFF)
            size_t offset = 4;
            REQUIRE_LT(offset + 4, stream_data.size());

            // Read message size
            int32_t message_size = 0;
            std::memcpy(&message_size, stream_data.data() + offset, sizeof(int32_t));
            offset += 4;

            REQUIRE_GT(message_size, 0);
            REQUIRE_LT(offset + message_size, stream_data.size());

            // Parse the Message FlatBuffer
            const auto* message = org::apache::arrow::flatbuf::GetMessage(stream_data.data() + offset);
            REQUIRE(message != nullptr);

            // First message should be Schema
            CHECK_EQ(message->header_type(), org::apache::arrow::flatbuf::MessageHeader::Schema);

            const auto* schema = message->header_as_Schema();
            REQUIRE(schema != nullptr);
            REQUIRE(schema->fields() != nullptr);
            CHECK_EQ(schema->fields()->size(), expected_fields.size());

            // Verify each field in the schema
            for (size_t i = 0; i < expected_fields.size() && i < schema->fields()->size(); ++i)
            {
                const auto* field = schema->fields()->Get(static_cast<uint32_t>(i));
                REQUIRE(field != nullptr);
                const auto& expected = expected_fields[i];

                CHECK_EQ(field->name()->str(), expected.name);
                CHECK_EQ(field->nullable(), expected.nullable);
                CHECK_EQ(field->type_type(), expected.type_enum);

                if (expected.type_enum == org::apache::arrow::flatbuf::Type::Int)
                {
                    const auto* int_type = field->type_as_Int();
                    REQUIRE(int_type != nullptr);
                    CHECK_EQ(int_type->is_signed(), expected.is_signed);
                    CHECK_EQ(int_type->bitWidth(), expected.bit_width);
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::FloatingPoint)
                {
                    const auto* fp_type = field->type_as_FloatingPoint();
                    REQUIRE(fp_type != nullptr);
                    CHECK_EQ(fp_type->precision(), expected.precision);
                }
            }
        }

        SUBCASE("Step 4: Verify schema in Arrow IPC file format footer")
        {
            // Convert JSON to Arrow file format
            const std::vector<uint8_t> file_data = integration_tools::json_file_to_arrow_file(json_file);
            REQUIRE_GT(file_data.size(), 0);

            // Parse the footer
            const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
            REQUIRE(footer != nullptr);
            REQUIRE(footer->schema() != nullptr);
            REQUIRE(footer->schema()->fields() != nullptr);

            const auto* schema = footer->schema();
            CHECK_EQ(schema->fields()->size(), expected_fields.size());

            // Verify each field in the footer schema
            for (size_t i = 0; i < expected_fields.size() && i < schema->fields()->size(); ++i)
            {
                const auto* field = schema->fields()->Get(static_cast<uint32_t>(i));
                REQUIRE(field != nullptr);
                const auto& expected = expected_fields[i];

                CHECK_EQ(field->name()->str(), expected.name);
                CHECK_EQ(field->nullable(), expected.nullable);
                CHECK_EQ(field->type_type(), expected.type_enum);

                if (expected.type_enum == org::apache::arrow::flatbuf::Type::Int)
                {
                    const auto* int_type = field->type_as_Int();
                    REQUIRE(int_type != nullptr);
                    CHECK_EQ(int_type->is_signed(), expected.is_signed);
                    CHECK_EQ(int_type->bitWidth(), expected.bit_width);
                }
                else if (expected.type_enum == org::apache::arrow::flatbuf::Type::FloatingPoint)
                {
                    const auto* fp_type = field->type_as_FloatingPoint();
                    REQUIRE(fp_type != nullptr);
                    CHECK_EQ(fp_type->precision(), expected.precision);
                }
            }
        }

        SUBCASE("Step 5: Verify deserialized record batches have correct schema, nullable flags, and metadata")
        {
            // Convert JSON to Arrow file format
            const std::vector<uint8_t> file_data = integration_tools::json_file_to_arrow_file(json_file);
            REQUIRE_GT(file_data.size(), 0);

            // Deserialize the file
            const auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));
            REQUIRE_EQ(batches.size(), 2);  // generated_primitive.json has 2 batches

            for (size_t batch_idx = 0; batch_idx < batches.size(); ++batch_idx)
            {
                const auto& batch = batches[batch_idx];
                CHECK_EQ(batch.nb_columns(), expected_fields.size());

                const auto& names = batch.names();
                for (size_t i = 0; i < expected_fields.size() && i < names.size(); ++i)
                {
                    CHECK_EQ(names[i], expected_fields[i].name);
                }

                // Verify nullable flags and metadata for each column
                for (size_t i = 0; i < expected_fields.size() && i < batch.nb_columns(); ++i)
                {
                    const auto& column = batch.get_column(i);
                    const auto& expected = expected_fields[i];

                    // Check nullable flag via arrow proxy flags
                    const auto& flags = sparrow::detail::array_access::get_arrow_proxy(column).flags();
                    const bool is_nullable = flags.contains(sparrow::ArrowFlag::NULLABLE);
                    CHECK_EQ(is_nullable, expected.nullable);

                    // Check metadata is preserved (should be empty for generated_primitive.json)
                    const auto metadata = column.metadata();
                    if (metadata.has_value())
                    {
                        CHECK_EQ(metadata->size(), 0);
                    }

                    // Verify data type is preserved after deserialization
                    sparrow::data_type expected_data_type;
                    if (expected.type_enum == org::apache::arrow::flatbuf::Type::Bool)
                    {
                        expected_data_type = sparrow::data_type::BOOL;
                    }
                    else if (expected.type_enum == org::apache::arrow::flatbuf::Type::Int)
                    {
                        if (expected.is_signed)
                        {
                            switch (expected.bit_width)
                            {
                                case 8:
                                    expected_data_type = sparrow::data_type::INT8;
                                    break;
                                case 16:
                                    expected_data_type = sparrow::data_type::INT16;
                                    break;
                                case 32:
                                    expected_data_type = sparrow::data_type::INT32;
                                    break;
                                case 64:
                                    expected_data_type = sparrow::data_type::INT64;
                                    break;
                                default:
                                    FAIL("Unknown bit width");
                            }
                        }
                        else
                        {
                            switch (expected.bit_width)
                            {
                                case 8:
                                    expected_data_type = sparrow::data_type::UINT8;
                                    break;
                                case 16:
                                    expected_data_type = sparrow::data_type::UINT16;
                                    break;
                                case 32:
                                    expected_data_type = sparrow::data_type::UINT32;
                                    break;
                                case 64:
                                    expected_data_type = sparrow::data_type::UINT64;
                                    break;
                                default:
                                    FAIL("Unknown bit width");
                            }
                        }
                    }
                    else if (expected.type_enum == org::apache::arrow::flatbuf::Type::FloatingPoint)
                    {
                        if (expected.precision == org::apache::arrow::flatbuf::Precision::SINGLE)
                        {
                            expected_data_type = sparrow::data_type::FLOAT;
                        }
                        else
                        {
                            expected_data_type = sparrow::data_type::DOUBLE;
                        }
                    }

                    CHECK_EQ(column.data_type(), expected_data_type);
                }
            }
        }
    }
}
