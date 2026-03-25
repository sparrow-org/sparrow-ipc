#pragma once

#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

#include "doctest/doctest.h"

namespace sparrow_ipc::test_utils
{
    inline const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

    inline const std::filesystem::path dictionary_fixture_base = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                        / "integration" / "cpp-21.0.0"
                                                        / "generated_dictionary";

    inline std::vector<uint8_t> read_binary_file(const std::filesystem::path& file_path)
    {
        std::ifstream file(file_path, std::ios::binary);
        REQUIRE(file);
        return std::vector<uint8_t>(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    }

    inline nlohmann::json load_json_file(const std::filesystem::path& json_path)
    {
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            throw std::runtime_error("Could not open file: " + json_path.string());
        }
        return nlohmann::json::parse(json_file);
    }

    inline size_t get_number_of_batches(const nlohmann::json& data)
    {
        return data["batches"].size();
    }

    inline size_t get_number_of_batches(const std::filesystem::path& json_path)
    {
        const nlohmann::json data = load_json_file(json_path);
        return data["batches"].size();
    }
} //namespace sparrow_ipc::test_utils
