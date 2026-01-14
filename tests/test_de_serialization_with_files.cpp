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
};

const std::vector<std::filesystem::path> files_paths_to_test_with_lz4_compression = {
    tests_resources_files_path_with_compression / "generated_lz4",
    tests_resources_files_path_with_compression / "generated_uncompressible_lz4",
};

const std::vector<std::filesystem::path> files_paths_to_test_with_zstd_compression = {
    tests_resources_files_path_with_compression / "generated_zstd",
    tests_resources_files_path_with_compression / "generated_uncompressible_zstd",
};

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
    TEST_CASE("Compare stream deserialization with JSON deserialization")
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
                compare_record_batches(record_batches_from_json, record_batches_from_stream);
            }
        }
    }

    TEST_CASE("Compare record_batch serialization with stream file")
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
                compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
            }
        }
    }

    TEST_CASE_TEMPLATE(
        "Compare record_batch serialization with stream file using compression",
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
                compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
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
            compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
        }
    }
}
