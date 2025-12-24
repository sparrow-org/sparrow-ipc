#include <stdexcept>
#include <string>
#include <vector>

#include <doctest/doctest.h>

#include "../src/compression_impl.hpp"

#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace
    {
        const std::string compressible_test_string = "Hello world, this is a test of compression and decompression. But we need more words to make this compression worth it!";
        const std::string incompressible_test_string = "abc";
    }

    TEST_SUITE("De/Compression")
    {
        TEST_CASE_TEMPLATE("Decompress empty data", T, Lz4Compression, ZstdCompression)
        {
            const std::vector<uint8_t> empty_data;
            const auto compression_type = T::type;

            CHECK_THROWS_WITH_AS((void)decompress(compression_type, empty_data), "Trying to decompress empty data.", std::runtime_error);
        }

        TEST_CASE_TEMPLATE("Empty data", T, Lz4Compression, ZstdCompression)
        {
            const std::vector<uint8_t> empty_data;
            const auto compression_type = T::type;
            CompressionCache cache;

            // Test compression of empty data
            auto compressed = compress(compression_type, empty_data, cache);
            CHECK_EQ(compressed.size(), details::CompressionHeaderSize);
            const std::int64_t header = *reinterpret_cast<const std::int64_t*>(compressed.data());
            CHECK_EQ(header, -1);

            // Test decompression of empty data
            auto decompressed = decompress(compression_type, compressed);
            std::visit([](const auto& value) { CHECK(value.empty()); }, decompressed);
        }

        TEST_CASE_TEMPLATE("Data compression and decompression round-trip", T, Lz4Compression, ZstdCompression)
        {
            std::vector<uint8_t> original_data(compressible_test_string.begin(), compressible_test_string.end());
            CompressionCache cache;

            // Compress data
            auto compression_type = T::type;
            auto compressed_data = compress(compression_type, original_data, cache);

            // Decompress
            auto decompressed_result = decompress(compression_type, compressed_data);
            std::visit(
                [&original_data](const auto& decompressed_data)
                {
                    CHECK_EQ(decompressed_data.size(), original_data.size());
                    const std::vector<uint8_t> vec(decompressed_data.begin(), decompressed_data.end());
                    CHECK_EQ(vec, original_data);
                },
                decompressed_result
            );
        }

        TEST_CASE_TEMPLATE("Data compression with incompressible data", T, Lz4Compression, ZstdCompression)
        {
            std::vector<uint8_t> original_data(incompressible_test_string.begin(), incompressible_test_string.end());
            CompressionCache cache;
            // Compress data
            auto compression_type = T::type;
            auto compressed_data = compress(compression_type, original_data, cache);

            // Decompress
            auto decompressed_result = decompress(compression_type, compressed_data);
            std::visit(
                [&original_data](const auto& decompressed_data)
                {
                    CHECK_EQ(decompressed_data.size(), original_data.size());
                    const std::vector<uint8_t> vec(decompressed_data.begin(), decompressed_data.end());
                    CHECK_EQ(vec, original_data);
                },
                decompressed_result
            );

            // Check that the compressed data is just the original data with a -1 header
            const std::int64_t header = *reinterpret_cast<const std::int64_t*>(compressed_data.data());
            CHECK_EQ(header, -1);
            std::vector<uint8_t> body(compressed_data.begin() + sizeof(header), compressed_data.end());
            CHECK_EQ(body, original_data);
        }

        TEST_CASE_TEMPLATE("Get compressed size", T, Lz4Compression, ZstdCompression)
        {
            std::vector<uint8_t> original_data(compressible_test_string.begin(), compressible_test_string.end());
            auto compression_type = T::type;
            CompressionCache cache;
            // Check compressed size
            size_t compressed_size = get_compressed_size(compression_type, original_data, cache);
            auto compressed_data_for_check = compress(compression_type, original_data, cache);
            CHECK_EQ(compressed_size, compressed_data_for_check.size());
            CHECK(cache.count(original_data.data(), original_data.size()) == 1);

            // Test with incompressible data
            std::vector<uint8_t> incompressible_data(incompressible_test_string.begin(), incompressible_test_string.end());
            size_t incompressible_size = get_compressed_size(compression_type, incompressible_data, cache);
            CHECK_EQ(incompressible_size, incompressible_data.size() + details::CompressionHeaderSize);

            // Test with empty data
            std::vector<uint8_t> empty_data;
            size_t empty_size = get_compressed_size(compression_type, empty_data, cache);
            CHECK_EQ(empty_size, details::CompressionHeaderSize);
        }
    }
}
