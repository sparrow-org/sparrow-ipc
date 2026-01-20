#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>
#include <sparrow/utils/format.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include "doctest/doctest.h"
#include "sparrow.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"

const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                         / "integration" / "cpp-21.0.0";

const std::filesystem::path tests_resources_files_path_with_compression = arrow_testing_data_dir / "data"
                                                                          / "arrow-ipc-stream" / "integration"
                                                                          / "2.0.0-compression";

const std::vector<std::filesystem::path> files_paths_to_test = {
    tests_resources_files_path / "generated_primitive",
    tests_resources_files_path / "generated_primitive_zerolength",
    tests_resources_files_path / "generated_primitive_no_batches",
    tests_resources_files_path / "generated_binary",
    tests_resources_files_path / "generated_large_binary",
    tests_resources_files_path / "generated_binary_zerolength",
    tests_resources_files_path / "generated_binary_no_batches",
    tests_resources_files_path / "generated_binary_view",
    tests_resources_files_path / "generated_interval",
    tests_resources_files_path / "generated_duration",
    tests_resources_files_path / "generated_datetime",
    tests_resources_files_path / "generated_null",
    tests_resources_files_path / "generated_null_trivial",
    tests_resources_files_path / "generated_decimal32",
    tests_resources_files_path / "generated_decimal64",
#if !defined(SPARROW_USE_LARGE_INT_PLACEHOLDERS)
    tests_resources_files_path / "generated_decimal",
    tests_resources_files_path / "generated_decimal256",
#endif
    tests_resources_files_path / "generated_nested",
    tests_resources_files_path / "generated_recursive_nested",
    tests_resources_files_path / "generated_map",
    tests_resources_files_path / "generated_map_non_canonical",
};

const std::vector<std::filesystem::path> files_paths_to_test_with_lz4_compression = {
    tests_resources_files_path_with_compression / "generated_lz4",
    tests_resources_files_path_with_compression / "generated_uncompressible_lz4",
};

const std::vector<std::filesystem::path> files_paths_to_test_with_zstd_compression = {
    tests_resources_files_path_with_compression / "generated_zstd",
    tests_resources_files_path_with_compression / "generated_uncompressible_zstd",
};

constexpr std::string_view canonical_map_entries = "entries";
constexpr std::string_view canonical_map_key = "key";
constexpr std::string_view canonical_map_value = "value";

size_t get_number_of_batches(const std::filesystem::path& json_path)
{
    std::ifstream json_file(json_path);
    if (!json_file.is_open())
    {
        throw std::runtime_error("Could not open file: " + json_path.string());
    }
    const nlohmann::json data = nlohmann::json::parse(json_file);
    return data["batches"].size();
}

nlohmann::json load_json_file(const std::filesystem::path& json_path)
{
    std::ifstream json_file(json_path);
    if (!json_file.is_open())
    {
        throw std::runtime_error("Could not open file: " + json_path.string());
    }
    return nlohmann::json::parse(json_file);
}

void compare_names(
    std::optional<std::string_view> opt_name1,
    std::optional<std::string_view> opt_name2
)
{
    if (!opt_name1.has_value())
    {
        CHECK_FALSE(opt_name2.has_value());
    }
    else
    {
        const auto& name1 = opt_name1.value();
        const auto& name2 = opt_name2.value();

        // When comparing Map types from different sources (JSON vs IPC streams), field names
        // may differ even though the logical structure is identical. This is because:
        //
        // - Arrow spec recommends (but doesn't require) "entries"/"key"/"value" as field names
        //    See: https://github.com/apache/arrow/blob/main/format/Schema.fbs#L127-L130
        //
        // - Different constructors and parsers may use different names:
        //    - MapType(entries_type, {key_type, val_type}) uses canonical "entries"/key"/"value"
        //    - JSON schemas may specify custom names like "some_entries"/"some_key"/"some_value"
        //    See:
        //       https://github.com/apache/arrow-testing/blob/master/data/arrow-ipc-stream/integration/cpp-21.0.0/generated_map_non_canonical.json.gz
        //       https://github.com/apache/arrow/blob/main/cpp/src/arrow/type.cc#L1028-L1048
        if (name1 != name2)
        {
            // Check if at least one name is a canonical map field name
            const bool involves_canonical_name =
                (name1 == canonical_map_entries || name2 == canonical_map_entries) ||
                (name1 == canonical_map_key || name2 == canonical_map_key) ||
                (name1 == canonical_map_value || name2 == canonical_map_value);

            if (!involves_canonical_name)
            {
                // Both are non-canonical and different - this is an error
                FAIL("Field name mismatch: '", name1, "' vs '", name2, "'");
            }
        }
    }
}

void compare_layouts(
    const sparrow::array& arr1,
    const sparrow::array& arr2
)
{
    const auto& proxy1 = sparrow::detail::array_access::get_arrow_proxy(arr1);
    const auto& proxy2 = sparrow::detail::array_access::get_arrow_proxy(arr2);

    // Compare basic properties
    REQUIRE_EQ(proxy1.format(), proxy2.format());
    REQUIRE_EQ(proxy1.length(), proxy2.length());
    REQUIRE_EQ(proxy1.null_count(), proxy2.null_count());
    REQUIRE_EQ(proxy1.offset(), proxy2.offset());
    REQUIRE_EQ(proxy1.n_buffers(), proxy2.n_buffers());
    REQUIRE_EQ(proxy1.n_children(), proxy2.n_children());
    REQUIRE_EQ(proxy1.flags(), proxy2.flags());

    compare_names(proxy1.name(), proxy2.name());
    // TODO compare metadata

    // Recursively compare children
    const auto& children1 = arr1.children();
    const auto& children2 = arr2.children();
    REQUIRE_EQ(children1.size(), children2.size());
    for (size_t i = 0; i < children1.size(); ++i)
    {
        compare_layouts(children1[i], children2[i]);
    }
}

void compare_record_batches(
    const std::vector<sparrow::record_batch>& record_batches_1,
    const std::vector<sparrow::record_batch>& record_batches_2
)
{
    REQUIRE_EQ(record_batches_1.size(), record_batches_2.size());
    for (size_t i = 0; i < record_batches_1.size(); ++i)
    {
        for (size_t y = 0; y < record_batches_1[i].nb_columns(); y++)
        {
            const auto& column_1 = record_batches_1[i].get_column(y);
            const auto& column_2 = record_batches_2[i].get_column(y);
            const auto col_name = column_1.name().value_or("NA");
            REQUIRE_EQ(column_1.data_type(), column_2.data_type());
            REQUIRE_EQ(column_1.size(), column_2.size());
            CHECK_EQ(record_batches_1[i].names()[y], record_batches_2[i].names()[y]);

            compare_layouts(column_1, column_2);

            for (size_t z = 0; z < column_1.size(); z++)
            {
                INFO("Comparing batch " << i << ", column " << y << " named :" << col_name << " , row " << z);
                const auto& column_1_value = column_1[z];
                const auto& column_2_value = column_2[z];
                CHECK_EQ(column_1_value, column_2_value);
            }

            // Additional check for buffer layout for view data types
            column_1.visit(
                [&](const auto& arr1)
                {
                    using array_type1 = std::decay_t<decltype(arr1)>;
                    if constexpr (sparrow::is_variable_size_binary_view_array<array_type1>)
                    {
                        using value_type1 = typename array_type1::value_type;
                        column_2.visit(
                            [&](const auto& arr2)
                            {
                                using array_type2 = std::decay_t<decltype(arr2)>;
                                if constexpr (sparrow::is_variable_size_binary_view_array<array_type2>)
                                {
                                    using value_type2 = typename array_type2::value_type;
                                    const auto& proxy1 = sparrow::detail::array_access::get_arrow_proxy(arr1);
                                    const auto& buffers1 = proxy1.buffers();

                                    const auto& proxy2 = sparrow::detail::array_access::get_arrow_proxy(arr2);
                                    const auto& buffers2 = proxy2.buffers();

                                    REQUIRE_EQ(buffers1.size(), buffers2.size());
                                    REQUIRE_GE(buffers1.size(), 3); // Minimum: validity, views, sizes_buffer (even if data buffers are empty)

                                    const auto& sizes_buffer_info1 = buffers1.back();
                                    const auto& sizes_buffer_info2 = buffers2.back();

                                    REQUIRE_EQ(sizes_buffer_info1.size(), sizes_buffer_info2.size());

                                    const size_t num_data_buffers1 = sizes_buffer_info1.size() / sizeof(int64_t);
                                    const size_t num_data_buffers2 = sizes_buffer_info2.size() / sizeof(int64_t);
                                    REQUIRE_EQ(num_data_buffers1, num_data_buffers2);

                                    REQUIRE_EQ(buffers1.size(), 3 + num_data_buffers1);
                                    REQUIRE_EQ(buffers2.size(), 3 + num_data_buffers2);

                                    if (!sizes_buffer_info1.empty())
                                    {
                                        const int64_t* sizes_buffer1 = reinterpret_cast<const int64_t*>(sizes_buffer_info1.data());
                                        const int64_t* sizes_buffer2 = reinterpret_cast<const int64_t*>(sizes_buffer_info2.data());
                                        for (size_t k = 0; k < num_data_buffers1; ++k)
                                        {
                                            CHECK_EQ(sizes_buffer1[k], sizes_buffer2[k]);
                                            CHECK_EQ(buffers1[k + 2].size(), buffers2[k + 2].size());
                                        }
                                    }

                                    // Also compare the contents of the buffers (validity, views, data, variadic buffer sizes)
                                    for (size_t k = 0; k < buffers1.size(); ++k)
                                    {
                                        REQUIRE_EQ(buffers1[k].size(), buffers2[k].size());
                                        if (buffers1[k].size() > 0)
                                        {
                                            CHECK(std::equal(buffers1[k].begin(), buffers1[k].end(), buffers2[k].begin()));
                                        }
                                    }
                                }
                                else
                                {
                                    FAIL("Incorrect array type");
                                }
                            }
                        );
                    }
                }
            );
        }
    }
}

struct Lz4CompressionParams
{
    static constexpr sparrow_ipc::CompressionType compression_type = sparrow_ipc::CompressionType::LZ4_FRAME;

    static const std::vector<std::filesystem::path>& files()
    {
        return files_paths_to_test_with_lz4_compression;
    }

    static constexpr const char* name = "LZ4";
};

struct ZstdCompressionParams
{
    static constexpr sparrow_ipc::CompressionType compression_type = sparrow_ipc::CompressionType::ZSTD;

    static const std::vector<std::filesystem::path>& files()
    {
        return files_paths_to_test_with_zstd_compression;
    }

    static constexpr const char* name = "ZSTD";
};

TEST_SUITE("Integration tests")
{
    TEST_CASE("Record batch equivalence across JSON and IPC streams")
    {
        for (const auto& file_path : files_paths_to_test)
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");
            const std::string test_name = "Testing " + file_path.filename().string();
            SUBCASE(test_name.c_str())
            {
                // Load the JSON file
                auto json_data = load_json_file(json_path);
                CHECK(json_data != nullptr);

                const size_t num_batches = get_number_of_batches(json_path);
                std::vector<sparrow::record_batch> record_batches_from_json;
                for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
                {
                    INFO("Processing batch " << batch_idx << " of " << num_batches);
                    record_batches_from_json.emplace_back(
                        sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                    );
                }

                // Load stream file
                std::filesystem::path stream_file_path = file_path;
                stream_file_path.replace_extension(".stream");
                std::ifstream stream_file(stream_file_path, std::ios::in | std::ios::binary);
                REQUIRE(stream_file.is_open());
                const std::vector<uint8_t> stream_data(
                    (std::istreambuf_iterator<char>(stream_file)),
                    (std::istreambuf_iterator<char>())
                );
                stream_file.close();

                // Process the stream file
                const auto record_batches_from_stream = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(stream_data)
                );

                std::vector<uint8_t> serialized_data;
                sparrow_ipc::memory_output_stream stream(serialized_data);
                sparrow_ipc::serializer serializer(stream);
                serializer << record_batches_from_json << sparrow_ipc::end_stream;
                const auto deserialized_serialized_data = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(serialized_data)
                );

                SUBCASE("Compare stream with JSON deserialization")
                {
                    compare_record_batches(record_batches_from_json, record_batches_from_stream);
                }
                SUBCASE("Compare record_batch de_serialization with stream deserialization")
                {
                    compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
                }
            }
        }
    }

    TEST_CASE_TEMPLATE(
        "Record batch equivalence across JSON and IPC streams - compressed",
        T,
        Lz4CompressionParams,
        ZstdCompressionParams
    )
    {
        for (const auto& file_path : T::files())
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");
            const std::string test_name = "Testing " + std::string(T::name) + " compression with "
                                          + file_path.filename().string();
            SUBCASE(test_name.c_str())
            {
                // Load the JSON file
                auto json_data = load_json_file(json_path);
                CHECK(json_data != nullptr);

                const size_t num_batches = get_number_of_batches(json_path);
                std::vector<sparrow::record_batch> record_batches_from_json;
                for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
                {
                    INFO("Processing batch " << batch_idx << " of " << num_batches);
                    record_batches_from_json.emplace_back(
                        sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                    );
                }

                // Load stream file
                std::filesystem::path stream_file_path = file_path;
                stream_file_path.replace_extension(".stream");
                std::ifstream stream_file(stream_file_path, std::ios::in | std::ios::binary);
                REQUIRE(stream_file.is_open());
                const std::vector<uint8_t> stream_data(
                    (std::istreambuf_iterator<char>(stream_file)),
                    (std::istreambuf_iterator<char>())
                );
                stream_file.close();

                // Process the stream file
                const auto record_batches_from_stream = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(stream_data)
                );

                std::vector<uint8_t> serialized_data;
                sparrow_ipc::memory_output_stream stream(serialized_data);
                sparrow_ipc::serializer serializer(stream, T::compression_type);
                serializer << record_batches_from_json << sparrow_ipc::end_stream;
                const auto deserialized_serialized_data = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(serialized_data)
                );
                SUBCASE("Compare stream with JSON deserialization - compressed")
                {
                    compare_record_batches(record_batches_from_json, record_batches_from_stream);
                }
                SUBCASE("Compare record_batch de_serialization with stream deserialization - compressed")
                {
                    compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
                }
            }
        }
    }

    TEST_CASE_TEMPLATE(
        "Round trip of classic test files serialization/deserialization using compression",
        T,
        Lz4CompressionParams,
        ZstdCompressionParams
    )
    {
        for (const auto& file_path : files_paths_to_test)
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");

            // Load the JSON file
            auto json_data = load_json_file(json_path);
            CHECK(json_data != nullptr);

            const size_t num_batches = get_number_of_batches(json_path);
            std::vector<sparrow::record_batch> record_batches_from_json;
            for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
            {
                INFO("Processing batch " << batch_idx << " of " << num_batches);
                record_batches_from_json.emplace_back(
                    sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                );
            }

            // Load stream file
            std::filesystem::path stream_file_path = file_path;
            stream_file_path.replace_extension(".stream");
            std::ifstream stream_file(stream_file_path, std::ios::in | std::ios::binary);
            REQUIRE(stream_file.is_open());
            const std::vector<uint8_t> stream_data(
                (std::istreambuf_iterator<char>(stream_file)),
                (std::istreambuf_iterator<char>())
            );
            stream_file.close();

            // Process the stream file
            const auto record_batches_from_stream = sparrow_ipc::deserialize_stream(
                std::span<const uint8_t>(stream_data)
            );

            // Serialize from json with compression
            std::vector<uint8_t> serialized_data;
            sparrow_ipc::memory_output_stream stream(serialized_data);
            sparrow_ipc::serializer serializer(stream, T::compression_type);
            serializer << record_batches_from_json << sparrow_ipc::end_stream;

            // Deserialize
            const auto deserialized_serialized_data = sparrow_ipc::deserialize_stream(
                std::span<const uint8_t>(serialized_data)
            );

            // Compare
            SUBCASE("Compare stream with JSON deserialization using compression")
            {
                compare_record_batches(record_batches_from_json, record_batches_from_stream);
            }
            SUBCASE("Compare record_batch de_serialization with stream deserialization using compression")
            {
                compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
            }
        }
    }
}
