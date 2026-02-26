#include "doctest/doctest.h"
#include "test_helper.hpp"

#include <sparrow/dictionary_encoded_array.hpp>
#include <sparrow/variable_size_binary_array.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"

TEST_SUITE("Dictionary support")
{
    TEST_CASE("Deserializes dictionary fixture stream")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        REQUIRE_FALSE(batches.empty());
        REQUIRE_EQ(batches.front().nb_columns(), size_t{3});
        REQUIRE_EQ(batches.front().nb_rows(), size_t{7});
    }

    TEST_CASE("Dictionary stream round-trip preserves values")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto input_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        std::vector<uint8_t> serialized_data;
        sparrow_ipc::memory_output_stream output_stream(serialized_data);
        sparrow_ipc::serializer serializer(output_stream);
        serializer << input_batches << sparrow_ipc::end_stream;

        const auto roundtrip_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(serialized_data));

        REQUIRE_EQ(input_batches.size(), roundtrip_batches.size());
        for (size_t batch_idx = 0; batch_idx < input_batches.size(); ++batch_idx)
        {
            const auto& lhs = input_batches[batch_idx];
            const auto& rhs = roundtrip_batches[batch_idx];
            REQUIRE_EQ(lhs.nb_columns(), rhs.nb_columns());
            REQUIRE_EQ(lhs.nb_rows(), rhs.nb_rows());

            for (size_t col_idx = 0; col_idx < lhs.nb_columns(); ++col_idx)
            {
                const auto& left_col = lhs.get_column(col_idx);
                const auto& right_col = rhs.get_column(col_idx);
                REQUIRE_EQ(left_col.size(), right_col.size());
                REQUIRE_EQ(left_col.data_type(), right_col.data_type());

                for (size_t row_idx = 0; row_idx < left_col.size(); ++row_idx)
                {
                    CHECK_EQ(left_col[row_idx], right_col[row_idx]);
                }
            }
        }
    }

    TEST_CASE("File footer contains dictionary blocks")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);

        const auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream output_stream(file_data);
        sparrow_ipc::stream_file_serializer file_serializer(output_stream);
        file_serializer << batches << sparrow_ipc::end_file;

        const auto* footer = sparrow_ipc::get_footer_from_file_data(std::span<const uint8_t>(file_data));
        REQUIRE_NE(footer, nullptr);
        REQUIRE_NE(footer->dictionaries(), nullptr);
        CHECK_GT(footer->dictionaries()->size(), 0);
    }

    TEST_CASE("Delta dictionary deserialization - spec example")
    {
        // Tests the spec example from the Arrow IPC format documentation:
        //   <SCHEMA>
        //   <DICTIONARY 0>  (0)"A" (1)"B" (2)"C"
        //   <RECORD BATCH 0>  0 1 2 1  -> ["A","B","C","B"]
        //   <DICTIONARY 0 DELTA>  (3)"D" (4)"E"
        //   <RECORD BATCH 1>  3 2 4 0  -> ["D","C","E","A"]
        //   EOS
        namespace sp = sparrow;
        using dict_array_t = sp::dictionary_encoded_array<int8_t>;

        // batch0: dict = ["A","B","C"], indices = [0,1,2,1]
        sp::record_batch batch0(
            {{"col", sp::array(dict_array_t(
                dict_array_t::keys_buffer_type{0, 1, 2, 1},
                sp::array(sp::string_array(std::vector<std::string>{"A", "B", "C"}))
            ))}}
        );

        // Extract the dict_id assigned to "col" at column index 0
        sparrow_ipc::dictionary_tracker tracker;
        const auto dict_infos = tracker.extract_dictionaries_from_batch(batch0);
        REQUIRE_EQ(dict_infos.size(), size_t{1});
        const int64_t dict_id = dict_infos[0].id;

        // batch1: indices = [3,2,4,0] referencing extended dict ["A","B","C","D","E"]
        sp::record_batch batch1(
            {{"col", sp::array(dict_array_t(
                dict_array_t::keys_buffer_type{3, 2, 4, 0},
                sp::array(sp::string_array(std::vector<std::string>{"A", "B", "C", "D", "E"}))
            ))}}
        );

        // delta dict: only the new values ["D","E"]
        sp::record_batch delta(
            {{"col", sp::array(sp::string_array(std::vector<std::string>{"D", "E"}))}}
        );

        // Build the IPC stream manually: SCHEMA + BASE_DICT + BATCH_0 + DELTA_DICT + BATCH_1 + EOS
        std::vector<uint8_t> bytes;
        sparrow_ipc::memory_output_stream mem_stream(bytes);
        sparrow_ipc::any_output_stream stream(mem_stream);

        sparrow_ipc::serialize_schema_message(batch0, stream);
        sparrow_ipc::serialize_dictionary_batch(dict_id, dict_infos[0].data, false, stream, std::nullopt, std::nullopt);
        sparrow_ipc::serialize_record_batch(batch0, stream, std::nullopt, std::nullopt);
        sparrow_ipc::serialize_dictionary_batch(dict_id, delta, true, stream, std::nullopt, std::nullopt);
        sparrow_ipc::serialize_record_batch(batch1, stream, std::nullopt, std::nullopt);
        stream.write(sparrow_ipc::end_of_stream);

        // Deserialize and verify
        const auto result = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(bytes));

        REQUIRE_EQ(result.size(), size_t{2});

        // batch0: indices [0,1,2,1] in base dict ["A","B","C"] -> ["A","B","C","B"]
        const auto& rb0 = result[0];
        REQUIRE_EQ(rb0.nb_rows(), size_t{4});
        CHECK_EQ(rb0.get_column(0)[1], rb0.get_column(0)[3]);   // "B" == "B"
        CHECK_NE(rb0.get_column(0)[0], rb0.get_column(0)[1]);   // "A" != "B"
        CHECK_NE(rb0.get_column(0)[1], rb0.get_column(0)[2]);   // "B" != "C"

        // batch1: indices [3,2,4,0] in extended dict ["A","B","C","D","E"] -> ["D","C","E","A"]
        const auto& rb1 = result[1];
        REQUIRE_EQ(rb1.nb_rows(), size_t{4});
        // rb1[1]="C" matches rb0[2]="C"
        CHECK_EQ(rb1.get_column(0)[1], rb0.get_column(0)[2]);
        // rb1[3]="A" matches rb0[0]="A"
        CHECK_EQ(rb1.get_column(0)[3], rb0.get_column(0)[0]);
        // rb1[0]="D" and rb1[2]="E" are delta values absent from rb0
        CHECK_NE(rb1.get_column(0)[0], rb1.get_column(0)[2]);   // "D" != "E"
        CHECK_NE(rb1.get_column(0)[0], rb0.get_column(0)[0]);   // "D" != "A"
        CHECK_NE(rb1.get_column(0)[0], rb0.get_column(0)[1]);   // "D" != "B"
        CHECK_NE(rb1.get_column(0)[0], rb0.get_column(0)[2]);   // "D" != "C"
        CHECK_NE(rb1.get_column(0)[2], rb0.get_column(0)[0]);   // "E" != "A"
        CHECK_NE(rb1.get_column(0)[2], rb0.get_column(0)[1]);   // "E" != "B"
        CHECK_NE(rb1.get_column(0)[2], rb0.get_column(0)[2]);   // "E" != "C"
    }
}
