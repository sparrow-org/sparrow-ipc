#pragma once

#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>

#include "doctest/doctest.h"

namespace
{
    const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

    const std::filesystem::path dictionary_fixture_base = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                          / "integration" / "cpp-21.0.0"
                                                          / "generated_dictionary";

    std::vector<uint8_t> read_binary_file(const std::filesystem::path& file_path)
    {
        std::ifstream file(file_path, std::ios::binary);
        REQUIRE(file.is_open());
        return std::vector<uint8_t>((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    }
}
