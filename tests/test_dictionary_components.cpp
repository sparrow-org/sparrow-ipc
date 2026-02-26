#include <filesystem>
#include <vector>

#include <doctest/doctest.h>

#include <sparrow/utils/metadata.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/dictionary_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "test_helper.hpp"

namespace
{
    namespace sp = sparrow;

    sp::record_batch create_dictionary_values_batch()
    {
        return sp::record_batch(
            {{"dict_values", sp::array(sp::string_array(std::vector<std::string>{"alpha", "beta", "gamma"}))}}
        );
    }

    sp::record_batch create_non_dictionary_batch()
    {
        return sp::record_batch({{"values", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}});
    }

    std::vector<uint8_t> serialize_record_batch_without_schema(const sp::record_batch& record_batch)
    {
        std::vector<uint8_t> bytes;
        sparrow_ipc::memory_output_stream stream(bytes);
        sparrow_ipc::any_output_stream output(stream);

        sparrow_ipc::serialize_record_batch(record_batch, output, std::nullopt, std::nullopt);
        output.write(sparrow_ipc::end_of_stream);
        return bytes;
    }

    std::vector<uint8_t> serialize_dictionary_batch_without_schema(int64_t dict_id)
    {
        std::vector<uint8_t> bytes;
        sparrow_ipc::memory_output_stream stream(bytes);
        sparrow_ipc::any_output_stream output(stream);

        const auto dictionary_values = create_dictionary_values_batch();
        sparrow_ipc::serialize_dictionary_batch(dict_id, dictionary_values, false, output, std::nullopt, std::nullopt);
        output.write(sparrow_ipc::end_of_stream);
        return bytes;
    }

    std::vector<uint8_t> serialize_schema_then_undeclared_dictionary_batch(int64_t dict_id)
    {
        std::vector<uint8_t> bytes;
        sparrow_ipc::memory_output_stream stream(bytes);
        sparrow_ipc::any_output_stream output(stream);

        const auto schema_batch = create_non_dictionary_batch();
        sparrow_ipc::serialize_schema_message(schema_batch, output);

        const auto dictionary_values = create_dictionary_values_batch();
        sparrow_ipc::serialize_dictionary_batch(dict_id, dictionary_values, false, output, std::nullopt, std::nullopt);
        output.write(sparrow_ipc::end_of_stream);
        return bytes;
    }
}

TEST_SUITE("dictionary_components")
{
    TEST_CASE("dictionary_utils compute_fallback_dictionary_id is deterministic")
    {
        const auto h1 = sparrow_ipc::compute_fallback_dictionary_id("field_a", 0);
        const auto h2 = sparrow_ipc::compute_fallback_dictionary_id("field_a", 0);
        const auto h3 = sparrow_ipc::compute_fallback_dictionary_id("field_a", 1);
        const auto h4 = sparrow_ipc::compute_fallback_dictionary_id("field_b", 0);

        CHECK_EQ(h1, h2);
        CHECK_NE(h1, h3);
        CHECK_NE(h1, h4);
    }

    TEST_CASE("dictionary_utils parse_dictionary_metadata")
    {
        SUBCASE("without metadata")
        {
            ArrowSchema schema{};
            schema.metadata = nullptr;

            const auto metadata = sparrow_ipc::parse_dictionary_metadata(schema);
            CHECK_FALSE(metadata.id.has_value());
            CHECK_FALSE(metadata.is_ordered);
        }

        SUBCASE("with dictionary metadata")
        {
            const auto raw_metadata = sparrow::get_metadata_from_key_values(
                std::vector<sparrow::metadata_pair>{
                    {"ARROW:dictionary:id", "42"},
                    {"ARROW:dictionary:ordered", "true"}
                }
            );

            ArrowSchema schema{};
            schema.metadata = raw_metadata.c_str();

            const auto metadata = sparrow_ipc::parse_dictionary_metadata(schema);
            REQUIRE(metadata.id.has_value());
            CHECK_EQ(*metadata.id, int64_t{42});
            CHECK(metadata.is_ordered);
        }
    }

    TEST_CASE("dictionary_cache store and retrieval behavior")
    {
        sparrow_ipc::dictionary_cache cache;

        const auto first = create_dictionary_values_batch();
        cache.store_dictionary(7, first, false);
        CHECK(cache.contains(7));
        CHECK_EQ(cache.size(), size_t{1});

        const auto found = cache.get_dictionary(7);
        REQUIRE(found.has_value());
        CHECK_EQ(found->get().nb_columns(), size_t{1});

        SUBCASE("replacement overwrites")
        {
            auto replacement = sp::record_batch(
                {{"dict_values", sp::array(sp::string_array(std::vector<std::string>{"replacement"}))}}
            );
            cache.store_dictionary(7, std::move(replacement), false);

            const auto replaced = cache.get_dictionary(7);
            REQUIRE(replaced.has_value());
            CHECK_EQ(replaced->get().nb_rows(), size_t{1});
        }

        SUBCASE("delta with new id inserts")
        {
            const auto delta_new = create_dictionary_values_batch();
            cache.store_dictionary(8, delta_new, true);
            CHECK(cache.contains(8));
        }

        SUBCASE("delta with existing id appends values")
        {
            const auto delta_existing = create_dictionary_values_batch();
            cache.store_dictionary(7, delta_existing, true);

            const auto appended = cache.get_dictionary(7);
            REQUIRE(appended.has_value());
            CHECK_EQ(appended->get().nb_rows(), size_t{6});
        }

        SUBCASE("delta with existing id preserves null string values")
        {
            auto base_with_nulls = sp::record_batch({{"dict_values",
                                                     sp::array(sp::string_array(std::vector<sp::nullable<std::string>>{
                                                         sp::nullable<std::string>("alpha"),
                                                         sp::nullable<std::string>(),
                                                         sp::nullable<std::string>("gamma")
                                                     }))}});

            auto delta_with_nulls = sp::record_batch({{"dict_values",
                                                      sp::array(sp::string_array(std::vector<sp::nullable<std::string>>{
                                                          sp::nullable<std::string>(),
                                                          sp::nullable<std::string>("epsilon")
                                                      }))}});

            cache.store_dictionary(77, std::move(base_with_nulls), false);
            cache.store_dictionary(77, std::move(delta_with_nulls), true);

            const auto appended = cache.get_dictionary(77);
            REQUIRE(appended.has_value());
            CHECK_EQ(appended->get().nb_rows(), size_t{5});

            const auto& dict_values = appended->get().get_column(0);
            dict_values.visit([](const auto& impl) {
                if constexpr (sp::is_string_array_v<std::decay_t<decltype(impl)>>)
                {
                    REQUIRE(impl[0].has_value());
                    CHECK_EQ(impl[0].value(), "alpha");

                    CHECK_FALSE(impl[1].has_value());

                    REQUIRE(impl[2].has_value());
                    CHECK_EQ(impl[2].value(), "gamma");

                    CHECK_FALSE(impl[3].has_value());

                    REQUIRE(impl[4].has_value());
                    CHECK_EQ(impl[4].value(), "epsilon");
                }
            });
        }

        SUBCASE("delta with existing id and mismatched type throws")
        {
            auto mismatched = sp::record_batch(
                {{"dict_values", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}}
            );
            CHECK_THROWS_WITH_AS(
                cache.store_dictionary(7, std::move(mismatched), true),
                "Delta dictionary update has mismatched dictionary value types",
                std::runtime_error
            );
        }

        SUBCASE("invalid dictionary batch throws")
        {
            auto invalid = sp::record_batch(
                {{"a", sp::array(sp::primitive_array<int32_t>({1, 2}))},
                 {"b", sp::array(sp::primitive_array<int32_t>({3, 4}))}}
            );
            CHECK_THROWS_AS(cache.store_dictionary(9, std::move(invalid), false), std::invalid_argument);
        }

        cache.clear();
        CHECK_EQ(cache.size(), size_t{0});
    }

    TEST_CASE("dictionary_tracker extracts and tracks emitted dictionaries")
    {
        std::filesystem::path stream_file = dictionary_fixture_base;
        stream_file.replace_extension(".stream");
        const auto stream_data = read_binary_file(stream_file);
        const auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));

        REQUIRE_FALSE(batches.empty());

        sparrow_ipc::dictionary_tracker tracker;
        const auto extracted = tracker.extract_dictionaries_from_batch(batches.front());

        REQUIRE_FALSE(extracted.empty());
        for (const auto& dict : extracted)
        {
            CHECK_EQ(dict.data.nb_columns(), size_t{1});
            tracker.mark_emitted(dict.id);
        }

        const auto after_mark = tracker.extract_dictionaries_from_batch(batches.front());
        CHECK(after_mark.empty());

        tracker.reset();
        const auto after_reset = tracker.extract_dictionaries_from_batch(batches.front());
        CHECK_FALSE(after_reset.empty());
    }

    TEST_CASE("dictionary_tracker returns empty for non-dictionary record batch")
    {
        sparrow_ipc::dictionary_tracker tracker;
        const auto batch = create_non_dictionary_batch();
        const auto extracted = tracker.extract_dictionaries_from_batch(batch);
        CHECK(extracted.empty());
    }
}

TEST_SUITE("deserialize_failures")
{
    TEST_CASE("RecordBatch before schema throws")
    {
        const auto bytes = serialize_record_batch_without_schema(create_non_dictionary_batch());
        CHECK_THROWS_WITH_AS(
            (void) sparrow_ipc::deserialize_stream(std::span<const uint8_t>(bytes)),
            "RecordBatch encountered before Schema message.",
            std::runtime_error
        );
    }

    TEST_CASE("DictionaryBatch before schema throws")
    {
        const auto bytes = serialize_dictionary_batch_without_schema(123);
        CHECK_THROWS_WITH_AS(
            (void) sparrow_ipc::deserialize_stream(std::span<const uint8_t>(bytes)),
            "DictionaryBatch encountered before Schema message.",
            std::runtime_error
        );
    }

    TEST_CASE("DictionaryBatch not declared in schema throws")
    {
        const auto bytes = serialize_schema_then_undeclared_dictionary_batch(9876);
        CHECK_THROWS_WITH_AS(
            (void) sparrow_ipc::deserialize_stream(std::span<const uint8_t>(bytes)),
            "Dictionary with id 9876 is not declared in schema",
            std::runtime_error
        );
    }
}
