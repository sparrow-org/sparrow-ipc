#include <doctest/doctest.h>

#include <cstring>

#include <File_generated.h>

#include <sparrow/array.hpp>
#include <sparrow/dictionary_encoded_array.hpp>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"

#include "sparrow_ipc_tests_helpers.hpp"

TEST_SUITE("Stream file serializer tests")
{
    TEST_CASE("Basic file serialization with stream_file_serializer")
    {
        // Create a simple record batch
        std::vector<std::string> names = {"int_col", "float_col"};

        // Create int32 array: [1, 2, 3, 4, 5]
        std::vector<int32_t> int_data = {1, 2, 3, 4, 5};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));

        // Create float array: [1.1, 2.2, 3.3, 4.4, 5.5]
        std::vector<float> float_data = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
        sparrow::primitive_array<float> float_array(std::move(float_data));

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));

        sparrow::record_batch batch(names, std::move(arrays));

        // Serialize using stream_file_serializer
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch << sparrow_ipc::end_file;
        }

        // Check file has correct magic bytes
        REQUIRE(file_data.size() >= 18);  // Minimum file size
        CHECK_EQ(file_data[0], 'A');
        CHECK_EQ(file_data[1], 'R');
        CHECK_EQ(file_data[2], 'R');
        CHECK_EQ(file_data[3], 'O');
        CHECK_EQ(file_data[4], 'W');
        CHECK_EQ(file_data[5], '1');

        // Check trailing magic
        const size_t trailing_offset = file_data.size() - sparrow_ipc::arrow_file_magic_size;
        CHECK_EQ(file_data[trailing_offset], 'A');
        CHECK_EQ(file_data[trailing_offset + 1], 'R');
        CHECK_EQ(file_data[trailing_offset + 2], 'R');
        CHECK_EQ(file_data[trailing_offset + 3], 'O');
        CHECK_EQ(file_data[trailing_offset + 4], 'W');
        CHECK_EQ(file_data[trailing_offset + 5], '1');

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(deserialized.batches.size(), 1);
        const auto& deserialized_batch = deserialized.batches[0];
        CHECK_EQ(deserialized_batch.nb_columns(), 2);
        CHECK_EQ(deserialized_batch.nb_rows(), 5);
        CHECK_EQ(deserialized_batch.names()[0], "int_col");
        CHECK_EQ(deserialized_batch.names()[1], "float_col");
    }

    TEST_CASE("File serialization with multiple batches using write()")
    {
        std::vector<std::string> names = {"values"};
        std::vector<sparrow::record_batch> batches;

        // Create 3 batches
        for (int batch_idx = 0; batch_idx < 3; ++batch_idx)
        {
            std::vector<int32_t> data;
            for (int i = 0; i < 10; ++i)
            {
                data.push_back(batch_idx * 10 + i);
            }
            sparrow::primitive_array<int32_t> array(std::move(data));

            std::vector<sparrow::array> arrays;
            arrays.emplace_back(std::move(array));

            auto names_copy = names;
            batches.emplace_back(std::move(names_copy), std::move(arrays));
        }

        // Serialize using stream_file_serializer
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer.write(batches);
            serializer.end();
        }

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(deserialized.batches.size(), 3);

        for (size_t batch_idx = 0; batch_idx < 3; ++batch_idx)
        {
            const auto& batch = deserialized.batches[batch_idx];
            CHECK_EQ(batch.nb_columns(), 1);
            CHECK_EQ(batch.nb_rows(), 10);

            const auto& col = batch.get_column(0);
            col.visit(
                [batch_idx](const auto& impl)
                {
                    if constexpr (sparrow::is_primitive_array_v<std::decay_t<decltype(impl)>>)
                    {
                        for (size_t i = 0; i < 10; ++i)
                        {
                            CHECK_EQ(impl[i].value(), static_cast<int32_t>(batch_idx * 10 + i));
                        }
                    }
                }
            );
        }
    }

    TEST_CASE("File serialization with operator<< chaining")
    {
        std::vector<std::string> names = {"data"};

        // Create first batch
        std::vector<int32_t> data1 = {1, 2, 3};
        sparrow::primitive_array<int32_t> array1(std::move(data1));
        std::vector<sparrow::array> arrays1;
        arrays1.emplace_back(std::move(array1));
        sparrow::record_batch batch1(names, std::move(arrays1));

        // Create second batch
        std::vector<int32_t> data2 = {4, 5, 6};
        sparrow::primitive_array<int32_t> array2(std::move(data2));
        std::vector<sparrow::array> arrays2;
        arrays2.emplace_back(std::move(array2));
        sparrow::record_batch batch2(names, std::move(arrays2));

        // Serialize using stream_file_serializer with chaining
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch1 << batch2 << sparrow_ipc::end_file;
        }

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(deserialized.batches.size(), 2);
        CHECK_EQ(deserialized.batches[0].nb_rows(), 3);
        CHECK_EQ(deserialized.batches[1].nb_rows(), 3);
    }

    TEST_CASE("File serialization with compression")
    {
        for (const auto& p : sparrow_ipc::compression_only_params)
        {
            SUBCASE(p.name)
            {
                std::vector<std::string> names = {"data"};

                std::vector<int32_t> data;
                for (int i = 0; i < 100; ++i)
                {
                    data.push_back(i);
                }
                sparrow::primitive_array<int32_t> array(std::move(data));

                std::vector<sparrow::array> arrays;
                arrays.emplace_back(std::move(array));

                sparrow::record_batch batch(names, std::move(arrays));

                // Serialize with compression
                std::vector<uint8_t> compressed_data;
                sparrow_ipc::memory_output_stream mem_stream(compressed_data);

                {
                    sparrow_ipc::stream_file_serializer serializer(mem_stream, p.type.value());
                    serializer << batch << sparrow_ipc::end_file;
                }

                // Deserialize and verify
                auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(compressed_data));

                REQUIRE_EQ(deserialized.batches.size(), 1);
                const auto& deserialized_batch = deserialized.batches[0];
                CHECK_EQ(deserialized_batch.nb_rows(), 100);

                const auto& col = deserialized_batch.get_column(0);
                col.visit(
                    [](const auto& impl)
                    {
                        if constexpr (sparrow::is_primitive_array_v<std::decay_t<decltype(impl)>>)
                        {
                            for (size_t i = 0; i < 100; ++i)
                            {
                                CHECK_EQ(impl[i].value(), static_cast<int32_t>(i));
                            }
                        }
                    }
                );
            }
        }
    }

    TEST_CASE("File serialization with destructor auto-end")
    {
        std::vector<std::string> names = {"values"};
        
        std::vector<int32_t> data = {1, 2, 3, 4, 5};
        sparrow::primitive_array<int32_t> array(std::move(data));
        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        // Destructor should automatically call end()
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch;
            // No explicit end() call - destructor will handle it
        }

        // Verify file is valid
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));
        REQUIRE_EQ(deserialized.batches.size(), 1);
        CHECK_EQ(deserialized.batches[0].nb_rows(), 5);
    }

    TEST_CASE("File serialization with schema only (zero batches)")
    {
        std::vector<std::string> names = {"int_col", "float_col"};
        std::vector<int32_t> int_data = {};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));
        std::vector<float> float_data = {};
        sparrow::primitive_array<float> float_array(std::move(float_data));
        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));
        sparrow::record_batch schema_batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);

        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream, schema_batch);
            serializer.end();
        }

        // Verify file structure
        REQUIRE(file_data.size() >= 18);
        CHECK_EQ(file_data[0], 'A');

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));
        CHECK_EQ(deserialized.batches.size(), 0);
    }

    TEST_CASE("Error: explicit end without schema")
    {
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        // Explicitly calling end() without schema should throw
        sparrow_ipc::stream_file_serializer serializer(mem_stream);
        CHECK_THROWS_WITH_AS(serializer.end(), "Cannot end file serializer without a schema", std::runtime_error);
    }

    TEST_CASE("Error: write after end")
    {
        std::vector<std::string> names = {"data"};
        std::vector<int32_t> data = {1, 2, 3};
        sparrow::primitive_array<int32_t> array(std::move(data));
        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);

        sparrow_ipc::stream_file_serializer serializer(mem_stream);
        serializer << batch;
        serializer.end();

        CHECK_THROWS_AS(serializer.write(batch), std::runtime_error);
    }

    TEST_CASE("Error: file format rejects dictionary replacement for same id")
    {
        using dict_array_t = sparrow::dictionary_encoded_array<int8_t>;

        sparrow::record_batch batch0(
            {{"col", sparrow::array(dict_array_t(
                dict_array_t::keys_buffer_type{0, 1, 2, 1},
                sparrow::array(sparrow::string_array(std::vector<std::string>{"A", "B", "C"}))
            ))}}
        );

        sparrow::record_batch batch1(
            {{"col", sparrow::array(dict_array_t(
                dict_array_t::keys_buffer_type{3, 2, 4, 0},
                sparrow::array(sparrow::string_array(std::vector<std::string>{"A", "B", "C", "D", "E"}))
            ))}}
        );

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        sparrow_ipc::stream_file_serializer serializer(mem_stream);

        CHECK_THROWS_AS(serializer.write(std::vector<sparrow::record_batch>{batch0, batch1}), std::runtime_error);
    }

    TEST_CASE("Footer contains correct number of record batch blocks")
    {
        SUBCASE("Single record batch")
        {
            std::vector<std::string> names = {"col"};
            std::vector<int32_t> data = {1, 2, 3};
            sparrow::primitive_array<int32_t> array(std::move(data));
            std::vector<sparrow::array> arrays;
            arrays.emplace_back(std::move(array));
            sparrow::record_batch batch(names, std::move(arrays));

            std::vector<uint8_t> file_data;
            sparrow_ipc::memory_output_stream mem_stream(file_data);
            
            {
                sparrow_ipc::stream_file_serializer serializer(mem_stream);
                serializer << batch << sparrow_ipc::end_file;
            }

            const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
            REQUIRE(footer != nullptr);
            REQUIRE(footer->recordBatches() != nullptr);
            CHECK_EQ(footer->recordBatches()->size(), 1);
        }

        SUBCASE("Multiple record batches")
        {
            std::vector<std::string> names = {"values"};
            std::vector<sparrow::record_batch> batches;

            for (int batch_idx = 0; batch_idx < 5; ++batch_idx)
            {
                std::vector<int32_t> data;
                for (int i = 0; i < 10; ++i)
                {
                    data.push_back(batch_idx * 10 + i);
                }
                sparrow::primitive_array<int32_t> array(std::move(data));
                std::vector<sparrow::array> arrays;
                arrays.emplace_back(std::move(array));
                auto names_copy = names;
                batches.emplace_back(std::move(names_copy), std::move(arrays));
            }

            std::vector<uint8_t> file_data;
            sparrow_ipc::memory_output_stream mem_stream(file_data);
            
            {
                sparrow_ipc::stream_file_serializer serializer(mem_stream);
                serializer.write(batches);
                serializer.end();
            }

            const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
            REQUIRE(footer != nullptr);
            REQUIRE(footer->recordBatches() != nullptr);
            CHECK_EQ(footer->recordBatches()->size(), 5);
        }
    }

    TEST_CASE("Footer record batch blocks have valid offsets")
    {
        std::vector<std::string> names = {"data"};
        
        // Create two batches with different sizes
        std::vector<int32_t> data1 = {1, 2, 3, 4, 5};
        sparrow::primitive_array<int32_t> array1(std::move(data1));
        std::vector<sparrow::array> arrays1;
        arrays1.emplace_back(std::move(array1));
        sparrow::record_batch batch1(names, std::move(arrays1));

        std::vector<int32_t> data2 = {10, 20, 30};
        sparrow::primitive_array<int32_t> array2(std::move(data2));
        std::vector<sparrow::array> arrays2;
        arrays2.emplace_back(std::move(array2));
        sparrow::record_batch batch2(names, std::move(arrays2));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch1 << batch2 << sparrow_ipc::end_file;
        }

        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        REQUIRE_EQ(footer->recordBatches()->size(), 2);

        const auto* blocks = footer->recordBatches();
        
        // First block offset should be after the header magic (8 bytes) + schema message
        const auto& block0 = *blocks->Get(0);
        CHECK_GT(block0.offset(), sparrow_ipc::arrow_file_header_magic.size());
        CHECK_GT(block0.metaDataLength(), 0);
        CHECK_GT(block0.bodyLength(), 0);

        // Second block offset should be after the first block
        const auto& block1 = *blocks->Get(1);
        CHECK_GT(block1.offset(), block0.offset());
        CHECK_GT(block1.metaDataLength(), 0);
        CHECK_GT(block1.bodyLength(), 0);

        // Verify block ordering: each subsequent block should start after the previous one ends
        const int64_t block0_end = block0.offset() + block0.metaDataLength() + block0.bodyLength();
        CHECK_GE(block1.offset(), block0_end);
    }

    TEST_CASE("Footer record batch blocks have correct metadata and body lengths")
    {
        std::vector<std::string> names = {"int_col", "float_col"};

        // Create a batch with known structure
        std::vector<int32_t> int_data = {1, 2, 3, 4, 5, 6, 7, 8};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));

        std::vector<float> float_data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
        sparrow::primitive_array<float> float_array(std::move(float_data));

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch << sparrow_ipc::end_file;
        }

        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        REQUIRE_EQ(footer->recordBatches()->size(), 1);

        const auto& block = *footer->recordBatches()->Get(0);
        
        // Metadata length should be reasonable (FlatBuffer message header + RecordBatch)
        // Typically at least 50-100 bytes for a simple record batch
        CHECK_GT(block.metaDataLength(), 40);
        CHECK_LT(block.metaDataLength(), 1000);  // Sanity check

        // Body length should accommodate:
        // - Validity bitmap for int_col (at least 1 byte, padded to 64 bytes)
        // - int32 data: 8 * 4 = 32 bytes (padded to 64 bytes)
        // - Validity bitmap for float_col (at least 1 byte, padded to 64 bytes)
        // - float data: 8 * 4 = 32 bytes (padded to 64 bytes)
        CHECK_GE(block.bodyLength(), 32 + 32);  // At minimum the raw data
    }

    TEST_CASE("Footer blocks allow direct access to record batches in file")
    {
        std::vector<std::string> names = {"value"};
        std::vector<sparrow::record_batch> original_batches;

        // Create 3 batches with different row counts
        for (int batch_idx = 0; batch_idx < 3; ++batch_idx)
        {
            const size_t num_rows = 5 + batch_idx * 3;  // 5, 8, 11 rows
            std::vector<int32_t> data;
            for (size_t i = 0; i < num_rows; ++i)
            {
                data.push_back(static_cast<int32_t>(batch_idx * 100 + i));
            }
            sparrow::primitive_array<int32_t> array(std::move(data));
            std::vector<sparrow::array> arrays;
            arrays.emplace_back(std::move(array));
            auto names_copy = names;
            original_batches.emplace_back(std::move(names_copy), std::move(arrays));
        }

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer.write(original_batches);
            serializer.end();
        }

        // Parse footer and verify we can locate each record batch
        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        REQUIRE_EQ(footer->recordBatches()->size(), 3);

        // Verify each block points to valid data within the file
        for (size_t i = 0; i < footer->recordBatches()->size(); ++i)
        {
            const auto& block = *footer->recordBatches()->Get(static_cast<uint32_t>(i));
            
            // Block should be within file bounds
            CHECK_GT(block.offset(), 0);
            CHECK_LT(static_cast<size_t>(block.offset()), file_data.size());
            
            // Block end should be within file bounds
            const size_t block_end = block.offset() + block.metaDataLength() + block.bodyLength();
            CHECK_LE(block_end, file_data.size());
        }

        // Verify deserialize_file can read the file correctly
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));
        REQUIRE_EQ(deserialized.batches.size(), 3);
        
        // Check row counts match
        CHECK_EQ(deserialized.batches[0].nb_rows(), 5);
        CHECK_EQ(deserialized.batches[1].nb_rows(), 8);
        CHECK_EQ(deserialized.batches[2].nb_rows(), 11);
    }

    TEST_CASE("Footer schema matches record batch schema")
    {
        std::vector<std::string> names = {"int_col", "float_col", "str_col"};

        std::vector<int32_t> int_data = {1, 2, 3};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));

        std::vector<float> float_data = {1.5f, 2.5f, 3.5f};
        sparrow::primitive_array<float> float_array(std::move(float_data));

        sparrow::string_array str_array(std::vector<std::string>{"a", "b", "c"});

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));
        arrays.emplace_back(std::move(str_array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch << sparrow_ipc::end_file;
        }

        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->schema() != nullptr);
        REQUIRE(footer->schema()->fields() != nullptr);
        
        // Check schema has correct number of fields
        CHECK_EQ(footer->schema()->fields()->size(), 3);
        
        // Check field names
        CHECK_EQ(footer->schema()->fields()->Get(0)->name()->str(), "int_col");
        CHECK_EQ(footer->schema()->fields()->Get(1)->name()->str(), "float_col");
        CHECK_EQ(footer->schema()->fields()->Get(2)->name()->str(), "str_col");
        
        // Check field types
        CHECK_EQ(footer->schema()->fields()->Get(0)->type_type(), org::apache::arrow::flatbuf::Type::Int);
        CHECK_EQ(footer->schema()->fields()->Get(1)->type_type(), org::apache::arrow::flatbuf::Type::FloatingPoint);
        CHECK_EQ(footer->schema()->fields()->Get(2)->type_type(), org::apache::arrow::flatbuf::Type::Utf8);
    }

    TEST_CASE("Footer block alignment - all fields must be multiples of 8")
    {
        // Arrow IPC format requires that offset, metaDataLength, and bodyLength
        // in FileBlock are all multiples of 8. This test verifies the alignment.
        
        std::vector<std::string> names = {"a", "b", "c", "d"};

        // Create arrays with the same number of rows (7 elements each)
        std::vector<int32_t> int_data = {1, 2, 3, 4, 5, 6, 7};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));

        std::vector<float> float_data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f};
        sparrow::primitive_array<float> float_array(std::move(float_data));

        std::vector<bool> bool_data = {true, false, true, false, true, false, true};
        sparrow::primitive_array<bool> bool_array(std::move(bool_data));
        
        std::vector<double> double_data = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7};
        sparrow::primitive_array<double> double_array(std::move(double_data));

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));
        arrays.emplace_back(std::move(bool_array));
        arrays.emplace_back(std::move(double_array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch << sparrow_ipc::end_file;
        }

        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        REQUIRE_EQ(footer->recordBatches()->size(), 1);

        const auto& block = *footer->recordBatches()->Get(0);
        
        // All three fields must be multiples of 8 per Arrow spec
        // (see Apache Arrow reader.cc CheckAligned function)
        CHECK_EQ(block.offset() % 8, 0);
        CHECK_EQ(block.metaDataLength() % 8, 0);
        CHECK_EQ(block.bodyLength() % 8, 0);
        
        // metaDataLength should include continuation (4) + size (4) + flatbuffer + padding
        // so it should be at least 8
        CHECK_GE(block.metaDataLength(), 8);
    }

    TEST_CASE("Footer block alignment with multiple batches")
    {
        std::vector<std::string> names = {"x", "y"};
        
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            
            // Create multiple batches with different sizes to test alignment edge cases
            for (int batch_idx = 0; batch_idx < 5; ++batch_idx)
            {
                const size_t num_rows = 3 + batch_idx * 2;  // 3, 5, 7, 9, 11 rows
                
                std::vector<int32_t> int_data(num_rows);
                std::vector<float> float_data(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                {
                    int_data[i] = static_cast<int32_t>(batch_idx * 100 + i);
                    float_data[i] = static_cast<float>(i) * 0.1f;
                }
                
                sparrow::primitive_array<int32_t> int_array(std::move(int_data));
                sparrow::primitive_array<float> float_array(std::move(float_data));
                
                std::vector<sparrow::array> arrays;
                arrays.emplace_back(std::move(int_array));
                arrays.emplace_back(std::move(float_array));
                
                auto names_copy = names;
                sparrow::record_batch batch(std::move(names_copy), std::move(arrays));
                
                serializer << batch;
            }
            
            serializer << sparrow_ipc::end_file;
        }

        const auto* footer = sparrow_ipc::get_footer_from_file_data(file_data);
        REQUIRE(footer != nullptr);
        REQUIRE(footer->recordBatches() != nullptr);
        REQUIRE_EQ(footer->recordBatches()->size(), 5);

        // Check alignment for all blocks
        for (size_t i = 0; i < footer->recordBatches()->size(); ++i)
        {
            const auto& block = *footer->recordBatches()->Get(static_cast<uint32_t>(i));
            
            // All three fields must be multiples of 8
            CHECK_MESSAGE(block.offset() % 8 == 0, "Block ", i, " offset not aligned");
            CHECK_MESSAGE(block.metaDataLength() % 8 == 0, "Block ", i, " metaDataLength not aligned");
            CHECK_MESSAGE(block.bodyLength() % 8 == 0, "Block ", i, " bodyLength not aligned");
        }
    }
}
