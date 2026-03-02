#include <chrono>

#include <doctest/doctest.h>
#include <sparrow.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/utils.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("serialize_utils")
    {
        TEST_CASE("serialize_schema_message")
        {
            SUBCASE("Valid record batch")
            {
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                auto record_batch = create_test_record_batch();
                any_output_stream astream(stream);
                serialize_schema_message(record_batch, astream);

                CHECK_GT(serialized.size(), 0);

                // Check that it starts with continuation bytes
                CHECK_EQ(serialized.size() >= continuation.size(), true);
                for (size_t i = 0; i < continuation.size(); ++i)
                {
                    CHECK_EQ(serialized[i], continuation[i]);
                }

                // Check that the total size is aligned to 8 bytes
                CHECK_EQ(serialized.size() % 8, 0);
            }
        }

        TEST_CASE("fill_body")
        {
            SUBCASE("Simple primitive array (uncompressed)")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);
                std::vector<uint8_t> body;
                sparrow_ipc::memory_output_stream stream(body);
                sparrow_ipc::any_output_stream astream(stream);
                fill_body(proxy, astream, std::nullopt, std::nullopt);
                CHECK_GT(body.size(), 0);
                // Body size should be aligned
                CHECK_EQ(body.size() % 8, 0);
            }

            SUBCASE("Simple primitive array (compressible)")
            {
                std::vector<int32_t> data(1000, 12345); // Repeating values, should be very compressible
                auto array = sp::primitive_array<int32_t>(data);
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);

                // Uncompressed
                std::vector<uint8_t> body_uncompressed;
                sparrow_ipc::memory_output_stream stream_uncompressed(body_uncompressed);
                sparrow_ipc::any_output_stream astream_uncompressed(stream_uncompressed);
                fill_body(proxy, astream_uncompressed, std::nullopt, std::nullopt);
                CHECK_GT(body_uncompressed.size(), 0);
                // Body size should be aligned
                CHECK_EQ(body_uncompressed.size() % 8, 0);

                // Compressed
                for (const auto& p : compression_only_params)
                {
                    SUBCASE(p.name)
                    {
                        // Compressed
                        std::vector<uint8_t> body_compressed;
                        sparrow_ipc::memory_output_stream stream_compressed(body_compressed);
                        sparrow_ipc::any_output_stream astream_compressed(stream_compressed);
                        CompressionCache cache;
                        fill_body(proxy, astream_compressed, p.type, cache);
                        CHECK_GT(body_compressed.size(), 0);
                        // Body size should be aligned
                        CHECK_EQ(body_compressed.size() % 8, 0);
                        // Check that compressed size is smaller than uncompressed size
                        CHECK_LT(body_compressed.size(), body_uncompressed.size());
                    }
                }
            }
        }

        TEST_CASE("generate_body")
        {
            auto record_batch = create_test_record_batch();
            SUBCASE("Record batch with multiple columns")
            {
                CompressionCache cache;
                for (const auto& p : compression_params)
                {
                    SUBCASE(p.name)
                    {
                        std::vector<uint8_t> serialized;
                        memory_output_stream stream(serialized);
                        any_output_stream astream(stream);
                        generate_body(record_batch, astream, p.type, cache);
                        CHECK_GT(serialized.size(), 0);
                        CHECK_EQ(serialized.size() % 8, 0);
                    }
                }
            }
        }

#if defined(SPARROW_IPC_STATIC_LIB)
        TEST_CASE("calculate_body_size")
        {
            CompressionCache cache;
            auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);

            SUBCASE("Single array (uncompressed)")
            {
                auto size = calculate_body_size(proxy, std::nullopt, std::nullopt);
                CHECK_GT(size, 0);
                CHECK_EQ(size % 8, 0);
            }

            SUBCASE("Single array (compressed)")
            {
                for (const auto& p : compression_only_params)
                {
                    SUBCASE(p.name)
                    {
                        auto size = calculate_body_size(proxy, p.type.value(), cache);
                        CHECK_GT(size, 0);
                        CHECK_EQ(size % 8, 0);
                    }
                }
            }

            auto record_batch = create_test_record_batch();
            SUBCASE("Record batch (uncompressed)")
            {
                auto size = calculate_body_size(record_batch, std::nullopt, std::nullopt);
                CHECK_GT(size, 0);
                CHECK_EQ(size % 8, 0);
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                generate_body(record_batch, astream, std::nullopt, std::nullopt);
                CHECK_EQ(size, static_cast<int64_t>(serialized.size()));
            }

            SUBCASE("Record batch (compressed)")
            {
                for (const auto& p : compression_only_params)
                {
                    SUBCASE(p.name)
                    {
                        auto size = calculate_body_size(record_batch, p.type.value(), cache);
                        CHECK_GT(size, 0);
                        CHECK_EQ(size % 8, 0);
                        std::vector<uint8_t> serialized;
                        memory_output_stream stream(serialized);
                        any_output_stream astream(stream);
                        generate_body(record_batch, astream, p.type.value(), cache);
                        CHECK_EQ(size, static_cast<int64_t>(serialized.size()));
                    }
                }
            }
        }
#endif

        TEST_CASE("calculate_schema_message_size")
        {
            SUBCASE("Single column record batch")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto record_batch = sp::record_batch({{"column1", sp::array(std::move(array))}});

                const auto estimated_size = calculate_schema_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                // Verify by actual serialization
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                serialize_schema_message(record_batch, astream);

                CHECK_EQ(estimated_size, serialized.size());
            }

            SUBCASE("Multi-column record batch")
            {
                auto record_batch = create_test_record_batch();

                auto estimated_size = calculate_schema_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                serialize_schema_message(record_batch, astream);

                CHECK_EQ(estimated_size, serialized.size());
            }
        }

        TEST_CASE("calculate_record_batch_message_size")
        {
            auto test_calculate_record_batch_message_size = [](const sp::record_batch& record_batch, std::optional<CompressionType> compression)
            {
                CompressionCache cache_for_estimated;
                auto estimated_size = calculate_record_batch_message_size(record_batch, compression, cache_for_estimated);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                CompressionCache cache_for_serialized;
                serialize_record_batch(record_batch, astream, compression, cache_for_serialized);

                CHECK_EQ(estimated_size, serialized.size());
            };

            SUBCASE("Single column record batch")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto record_batch = sp::record_batch({{"column1", sp::array(std::move(array))}});
                test_calculate_record_batch_message_size(record_batch, std::nullopt);
                test_calculate_record_batch_message_size(record_batch, CompressionType::LZ4_FRAME);
                test_calculate_record_batch_message_size(record_batch, CompressionType::ZSTD);
            }

            SUBCASE("Multi-column record batch")
            {
                auto record_batch = create_test_record_batch();
                test_calculate_record_batch_message_size(record_batch, std::nullopt);
                test_calculate_record_batch_message_size(record_batch, CompressionType::LZ4_FRAME);
                test_calculate_record_batch_message_size(record_batch, CompressionType::ZSTD);
            }
        }

        TEST_CASE("calculate_dictionary_batch_message_size")
        {
            auto test_calculate_dictionary_batch_message_size =
                [](const sp::record_batch& record_batch,
                   int64_t dictionary_id,
                   bool is_delta,
                   std::optional<CompressionType> compression)
            {
                CompressionCache cache_for_estimated;
                const auto estimated_size = calculate_dictionary_batch_message_size(
                    dictionary_id,
                    record_batch,
                    is_delta,
                    compression,
                    cache_for_estimated
                );
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                CompressionCache cache_for_serialized;
                serialize_dictionary_batch(
                    dictionary_id,
                    record_batch,
                    is_delta,
                    astream,
                    compression,
                    cache_for_serialized
                );

                CHECK_EQ(estimated_size, serialized.size());
            };

            auto dictionary_values = sp::primitive_array<int32_t>({10, 20, 30, 40, 50});
            auto dictionary_batch = sp::record_batch({{"dictionary_values", sp::array(std::move(dictionary_values))}});

            SUBCASE("Replacement dictionary batch")
            {
                test_calculate_dictionary_batch_message_size(dictionary_batch, 42, false, std::nullopt);
                test_calculate_dictionary_batch_message_size(
                    dictionary_batch,
                    42,
                    false,
                    CompressionType::LZ4_FRAME
                );
                test_calculate_dictionary_batch_message_size(dictionary_batch, 42, false, CompressionType::ZSTD);
            }

            SUBCASE("Delta dictionary batch")
            {
                test_calculate_dictionary_batch_message_size(dictionary_batch, 42, true, std::nullopt);
                test_calculate_dictionary_batch_message_size(
                    dictionary_batch,
                    42,
                    true,
                    CompressionType::LZ4_FRAME
                );
                test_calculate_dictionary_batch_message_size(dictionary_batch, 42, true, CompressionType::ZSTD);
            }
        }

        TEST_CASE("calculate_total_serialized_size")
        {
            auto test_calculate_total_serialized_size = [](const std::vector<sp::record_batch>& batches, std::optional<CompressionType> compression)
            {
                CompressionCache cache;
                auto estimated_size = calculate_total_serialized_size(batches, compression, cache);
                CHECK_GT(estimated_size, 0);

                // Should be equal to schema size + sum of record batch sizes
                auto schema_size = calculate_schema_message_size(batches[0]);
                int64_t batches_size = 0;
                for(const auto& batch : batches)
                {
                    batches_size += calculate_record_batch_message_size(batch, compression, cache);
                }
                CHECK_EQ(estimated_size, schema_size + batches_size);
            };

            SUBCASE("Single record batch")
            {
                auto record_batch = create_test_record_batch();
                std::vector<sp::record_batch> batches = {record_batch};
                test_calculate_total_serialized_size(batches, std::nullopt);
                test_calculate_total_serialized_size(batches, CompressionType::LZ4_FRAME);
                test_calculate_total_serialized_size(batches, CompressionType::ZSTD);
            }

            SUBCASE("Multiple record batches")
            {
                auto array1 = sp::primitive_array<int32_t>({1, 2, 3});
                auto array2 = sp::primitive_array<double>({1.0, 2.0, 3.0});
                auto record_batch1 = sp::record_batch(
                    {{"col1", sp::array(std::move(array1))}, {"col2", sp::array(std::move(array2))}}
                );

                auto array3 = sp::primitive_array<int32_t>({4, 5, 6});
                auto array4 = sp::primitive_array<double>({4.0, 5.0, 6.0});
                auto record_batch2 = sp::record_batch(
                    {{"col1", sp::array(std::move(array3))}, {"col2", sp::array(std::move(array4))}}
                );

                std::vector<sp::record_batch> batches = {record_batch1, record_batch2};
                test_calculate_total_serialized_size(batches, std::nullopt);
                test_calculate_total_serialized_size(batches, CompressionType::LZ4_FRAME);
                test_calculate_total_serialized_size(batches, CompressionType::ZSTD);
            }

            SUBCASE("Empty collection")
            {
                std::vector<sp::record_batch> empty_batches;
                auto estimated_size = calculate_total_serialized_size(empty_batches);
                CHECK_EQ(estimated_size, 0);
            }

            SUBCASE("Inconsistent schemas throw exception")
            {
                auto array1 = sp::primitive_array<int32_t>({1, 2, 3});
                auto record_batch1 = sp::record_batch({{"col1", sp::array(std::move(array1))}});

                auto array2 = sp::primitive_array<double>({1.0, 2.0, 3.0});
                auto record_batch2 = sp::record_batch(
                    {{"col2", sp::array(std::move(array2))}}  // Different column name
                );

                std::vector<sp::record_batch> batches = {record_batch1, record_batch2};

                CHECK_THROWS_AS(auto size = calculate_total_serialized_size(batches), std::invalid_argument);
                CHECK_THROWS_AS(auto size = calculate_total_serialized_size(batches, CompressionType::LZ4_FRAME), std::invalid_argument);
                CHECK_THROWS_AS(auto size = calculate_total_serialized_size(batches, CompressionType::ZSTD), std::invalid_argument);
            }
        }

        TEST_CASE("serialize_record_batch")
        {
            auto test_serialize_record_batch = [](const sp::record_batch& record_batch_to_serialize, std::optional<CompressionType> compression)
            {
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                any_output_stream astream(stream);
                CompressionCache cache;
                serialize_record_batch(record_batch_to_serialize, astream, compression, cache);
                CHECK_GT(serialized.size(), 0);

                // Check that it starts with continuation bytes
                CHECK_GE(serialized.size(), continuation.size());
                for (size_t i = 0; i < continuation.size(); ++i)
                {
                    CHECK_EQ(serialized[i], continuation[i]);
                }

                // Check that the metadata part is aligned to 8 bytes
                // Find the end of metadata (before body starts)
                size_t continuation_size = continuation.size();
                size_t length_prefix_size = sizeof(uint32_t);

                CHECK_GT(serialized.size(), continuation_size + length_prefix_size);

                // Extract message length
                uint32_t message_length;
                std::memcpy(&message_length, serialized.data() + continuation_size, sizeof(uint32_t));

                size_t metadata_end = continuation_size + length_prefix_size + message_length;
                size_t aligned_metadata_end = utils::align_to_8(static_cast<int64_t>(metadata_end));

                // Verify alignment
                CHECK_EQ(aligned_metadata_end % 8, 0);
                CHECK_LE(aligned_metadata_end, serialized.size());

                return serialized.size();
            };

            SUBCASE("Valid record batch")
            {
                auto record_batch = create_compressible_test_record_batch();
                auto uncompressed_size = test_serialize_record_batch(record_batch, std::nullopt);

                for (const auto& p : compression_only_params)
                {
                    SUBCASE(p.name)
                    {
                        auto compressed_size = test_serialize_record_batch(record_batch, p.type);
                        CHECK_LT(compressed_size, uncompressed_size);
                    }
                }
            }

            SUBCASE("Empty record batch")
            {
                auto empty_batch = sp::record_batch({});
                test_serialize_record_batch(empty_batch, std::nullopt);
                test_serialize_record_batch(empty_batch, CompressionType::LZ4_FRAME);
                test_serialize_record_batch(empty_batch, CompressionType::ZSTD);
            }
        }

        TEST_CASE("compression_caching_behavior")
        {
            auto record_batch = create_compressible_test_record_batch();

            for (const auto& p : compression_only_params)
            {
                SUBCASE(p.name)
                {
                    CompressionCache cache;

                    // Prime the cache by calculating the size
                    (void)calculate_record_batch_message_size(record_batch, p.type, cache);

                    CHECK_FALSE(cache.empty()); // Ensure cache got populated

                    size_t initial_cache_size = cache.size(); // Number of unique buffers in cache

                    // Generate the body using the same cache
                    // This should use the cached compressed data and not re-compress
                    std::vector<uint8_t> serialized_body;
                    memory_output_stream stream_body(serialized_body);
                    any_output_stream astream_body(stream_body);
                    generate_body(record_batch, astream_body, p.type, cache);

                    // Ensure no new entries were added to the cache, indicating re-use
                    CHECK_EQ(cache.size(), initial_cache_size);
                    CHECK_FALSE(serialized_body.empty()); // Ensure something was written

                    // Clear cache for next iteration (if any)
                    cache.clear();
                }
            }
        }
    }
}
