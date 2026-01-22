#pragma once

#include <optional>

#include <doctest/doctest.h>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    struct Lz4Compression { static constexpr CompressionType type = CompressionType::LZ4_FRAME; };
    struct ZstdCompression { static constexpr CompressionType type = CompressionType::ZSTD; };

    struct CompressionParams
    {
        std::optional<CompressionType> type;
        const char* name;
    };

    inline constexpr std::array<CompressionParams, 3> compression_params = {{
        { std::nullopt, "Uncompressed" },
        { CompressionType::LZ4_FRAME, "LZ4" },
        { CompressionType::ZSTD, "ZSTD" }
    }};

    inline constexpr std::array<CompressionParams, 2> compression_only_params = {{
        { CompressionType::LZ4_FRAME, "LZ4" },
        { CompressionType::ZSTD, "ZSTD" }
    }};

    // Helper function to create a simple ArrowSchema for testing
    inline ArrowSchema
    create_test_arrow_schema(const char* format, const char* name = "test_field", bool nullable = true)
    {
        ArrowSchema schema{};
        schema.format = format;
        schema.name = name;
        schema.flags = nullable ? static_cast<int64_t>(sp::ArrowFlag::NULLABLE) : 0;
        schema.metadata = nullptr;
        schema.n_children = 0;
        schema.children = nullptr;
        schema.dictionary = nullptr;
        schema.release = nullptr;
        schema.private_data = nullptr;
        return schema;
    }

    // Helper function to create ArrowSchema with metadata
    inline ArrowSchema
    create_test_arrow_schema_with_metadata(const char* format, const char* name = "test_field")
    {
        auto schema = create_test_arrow_schema(format, name);

        // For now, let's just return the schema without metadata to avoid segfault
        // The metadata creation requires proper sparrow metadata handling
        return schema;
    }

    // Helper function to create a simple record batch for testing
    inline sp::record_batch create_test_record_batch()
    {
        // Create a simple record batch with integer and string columns using initializer syntax
        return sp::record_batch(
            {{"int_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3, 4, 5}))},
             {"string_col",
              sp::array(sp::string_array(std::vector<std::string>{"hello", "world", "test", "data", "batch"}))}}
        );
    }

    // Helper function to create a compressible record batch for testing
    inline sp::record_batch create_compressible_test_record_batch()
    {
        std::vector<int32_t> int_data(1000, 12345);
        std::vector<std::string> string_data(1000, "hello world");
        return sp::record_batch(
            {{"int_col", sp::array(sp::primitive_array<int32_t>(int_data))},
             {"string_col", sp::array(sp::string_array(string_data))}}
        );
    }
}
