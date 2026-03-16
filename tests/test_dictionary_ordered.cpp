#include <fstream>
#include <span>
#include <string>
#include <vector>

#include <doctest/doctest.h>
#include <nlohmann/json.hpp>

#include <sparrow/dictionary_encoded_array.hpp>
#include <sparrow/json_reader/json_parser.hpp>
#include <sparrow/record_batch.hpp>
#include <sparrow/variable_size_binary_array.hpp>
#include <sparrow/layout/array_access.hpp>

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("Dictionary Ordered support")
    {
        TEST_CASE("Dictionary ordered from JSON round-trip")
        {
            const std::filesystem::path json_path = std::filesystem::path(SPARROW_IPC_TESTS_DATA_DIR) / "ordered_dictionary.json";
            std::ifstream json_file(json_path);
            REQUIRE(json_file.is_open());

            //TODO move load_json_file to helpers and use it instead, other helpers?
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

            const auto& col_des = rb_des.get_column("ordered_dict");
            const auto& proxy_des = sp::detail::array_access::get_arrow_proxy(col_des);

            CHECK(proxy_des.flags().contains(sp::ArrowFlag::DICTIONARY_ORDERED));
        }

        TEST_CASE("Dictionary ordered round-trip")
        {
            using dict_array_t = sp::dictionary_encoded_array<int8_t>;

            sp::record_batch rb(
                {{"col", sp::array(dict_array_t(
                    dict_array_t::keys_buffer_type{0, 1, 2, 1},
                    sp::array(sp::string_array(std::vector<std::string>{"A", "B", "C"}))
                ))}}
            );

            // Set the DICTIONARY_ORDERED flag
            auto& col = rb.get_column("col");
            auto& proxy = sp::detail::array_access::get_arrow_proxy(col);
            proxy.set_flags({sp::ArrowFlag::DICTIONARY_ORDERED, sp::ArrowFlag::NULLABLE});

            std::vector<uint8_t> buffer;
            memory_output_stream out_stream(buffer);
            serializer ser(out_stream);
            ser << rb << end_stream;

            const auto rb_des_vec = deserialize_stream(std::span<const uint8_t>(buffer));
            REQUIRE(!rb_des_vec.empty());
            auto& rb_des = rb_des_vec[0];

            const auto& col_des = rb_des.get_column("col");
            const auto& proxy_des = sp::detail::array_access::get_arrow_proxy(col_des);

            CHECK(proxy_des.flags().contains(sp::ArrowFlag::DICTIONARY_ORDERED));
        }

        TEST_CASE("Dictionary NOT ordered round-trip")
        {
            using dict_array_t = sp::dictionary_encoded_array<int8_t>;

            sp::record_batch rb(
                {{"col", sp::array(dict_array_t(
                    dict_array_t::keys_buffer_type{0, 1, 2, 1},
                    sp::array(sp::string_array(std::vector<std::string>{"A", "B", "C"}))
                ))}}
            );

            // Ensure DICTIONARY_ORDERED flag is NOT set
            auto& col = rb.get_column("col");
            auto& proxy = sp::detail::array_access::get_arrow_proxy(col);
            proxy.set_flags({sp::ArrowFlag::NULLABLE});

            std::vector<uint8_t> buffer;
            memory_output_stream out_stream(buffer);
            serializer ser(out_stream);
            ser << rb << end_stream;

            const auto rb_des_vec = deserialize_stream(std::span<const uint8_t>(buffer));
            REQUIRE(!rb_des_vec.empty());
            auto& rb_des = rb_des_vec[0];

            const auto& col_des = rb_des.get_column("col");
            const auto& proxy_des = sp::detail::array_access::get_arrow_proxy(col_des);

            CHECK_FALSE(proxy_des.flags().contains(sp::ArrowFlag::DICTIONARY_ORDERED));
        }
    }
}
