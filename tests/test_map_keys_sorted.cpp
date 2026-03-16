#include <fstream>
#include <span>
#include <string>
#include <vector>

#include <doctest/doctest.h>
#include <nlohmann/json.hpp>

#include <sparrow/layout/array_access.hpp>
#include <sparrow/map_array.hpp>
#include <sparrow/primitive_array.hpp>
#include <sparrow/record_batch.hpp>
#include <sparrow/variable_size_binary_array.hpp>
#include <sparrow/json_reader/json_parser.hpp>

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("map array")
    {
        TEST_CASE("Sorted map from JSON round-trip")
        {
            const std::filesystem::path json_path = std::filesystem::path(__FILE__).parent_path() / "data" / "map_array_sorted.json";
            std::ifstream json_file(json_path);
            REQUIRE(json_file.is_open());

            const nlohmann::json json_data = nlohmann::json::parse(json_file);
            json_file.close();

            const sp::record_batch rb = sp::json_reader::build_record_batch_from_json(json_data, 0);

            std::vector<uint8_t> buffer;
            memory_output_stream out_stream(buffer);
            serializer ser(out_stream);
            ser << rb << end_stream;

            const auto rb_des_vec = deserialize_stream(std::span<const uint8_t>(buffer));
            REQUIRE(!rb_des_vec.empty());
            auto& rb_des = rb_des_vec[0];

            const auto& col_des = rb_des.get_column("map_nullable");
            const auto& proxy_des = sp::detail::array_access::get_arrow_proxy(col_des);

            CHECK(proxy_des.flags().contains(sp::ArrowFlag::MAP_KEYS_SORTED));
        }

        TEST_CASE("Sorted map")
        {
            // Sorted keys
            sp::string_array keys_sorted(std::vector<std::string>{"a", "b", "c"});
            sp::primitive_array<int32_t> values_sorted({1, 2, 3});
            std::vector<int32_t> offsets_sorted{0, 3};

            sp::array keys_sorted_arr(std::move(keys_sorted));
            sp::array values_sorted_arr(std::move(values_sorted));
            sp::map_array map_sorted(std::move(keys_sorted_arr), std::move(values_sorted_arr), std::move(offsets_sorted));

            auto rb = sp::record_batch({
                {"sorted", sp::array(std::move(map_sorted))}
            });

            std::vector<uint8_t> buffer;
            memory_output_stream out_stream(buffer);
            serializer ser(out_stream);
            ser << rb << end_stream;

            const auto rb_des_vec = deserialize_stream(std::span<const uint8_t>(buffer));
            REQUIRE(!rb_des_vec.empty());
            auto& rb_des = rb_des_vec[0];

            const auto& col_sorted = rb_des.get_column("sorted");
            const auto& proxy_sorted = sp::detail::array_access::get_arrow_proxy(col_sorted);

            CHECK(proxy_sorted.flags().contains(sp::ArrowFlag::NULLABLE));
            CHECK(proxy_sorted.flags().contains(sp::ArrowFlag::MAP_KEYS_SORTED));
            CHECK_FALSE(proxy_sorted.flags().contains(sp::ArrowFlag::DICTIONARY_ORDERED));
        }

        TEST_CASE("Unsorted map")
        {
            // Unsorted keys
            sp::string_array keys_unsorted(std::vector<std::string>{"c", "a", "b"});
            sp::primitive_array<int32_t> values_unsorted({3, 1, 2});
            std::vector<int32_t> offsets_unsorted{0, 3};

            sp::array keys_unsorted_arr(std::move(keys_unsorted));
            sp::array values_unsorted_arr(std::move(values_unsorted));
            sp::map_array map_unsorted(std::move(keys_unsorted_arr), std::move(values_unsorted_arr), std::move(offsets_unsorted));

            auto rb = sp::record_batch({
                {"unsorted", sp::array(std::move(map_unsorted))}
            });

            std::vector<uint8_t> buffer;
            memory_output_stream out_stream(buffer);
            serializer ser(out_stream);
            ser << rb << end_stream;

            const auto rb_des_vec = deserialize_stream(std::span<const uint8_t>(buffer));
            REQUIRE(!rb_des_vec.empty());
            auto& rb_des = rb_des_vec[0];

            const auto& col_unsorted = rb_des.get_column("unsorted");
            const auto& proxy_unsorted = sp::detail::array_access::get_arrow_proxy(col_unsorted);

            CHECK(proxy_unsorted.flags().contains(sp::ArrowFlag::NULLABLE));
            CHECK_FALSE(proxy_unsorted.flags().contains(sp::ArrowFlag::MAP_KEYS_SORTED));
            CHECK_FALSE(proxy_unsorted.flags().contains(sp::ArrowFlag::DICTIONARY_ORDERED));
        }
    }
}
