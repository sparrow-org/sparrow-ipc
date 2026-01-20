
#include <optional>
#include <ostream>  // Needed by doctest
#include <string_view>
#include <unordered_set>

#include <sparrow/arrow_interface/arrow_schema.hpp>

#include "sparrow/utils/metadata.hpp"
#include "sparrow/utils/repeat_container.hpp"

#include "doctest/doctest.h"
#include "metadata_sample.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"

using namespace std::string_literals;

void compare_arrow_schema(const ArrowSchema& schema, const ArrowSchema& schema_copy)
{
    CHECK_NE(&schema, &schema_copy);
    CHECK_EQ(std::string_view(schema.format), std::string_view(schema_copy.format));
    CHECK_EQ(std::string_view(schema.name), std::string_view(schema_copy.name));
    CHECK_EQ(std::string_view(schema.metadata), std::string_view(schema_copy.metadata));
    CHECK_EQ(schema.flags, schema_copy.flags);
    CHECK_EQ(schema.n_children, schema_copy.n_children);
    if (schema.n_children > 0)
    {
        REQUIRE_NE(schema.children, nullptr);
        REQUIRE_NE(schema_copy.children, nullptr);
        for (int64_t i = 0; i < schema.n_children; ++i)
        {
            CHECK_NE(schema.children[i], nullptr);
            compare_arrow_schema(*schema.children[i], *schema_copy.children[i]);
        }
    }
    else
    {
        CHECK_EQ(schema.children, nullptr);
        CHECK_EQ(schema_copy.children, nullptr);
    }

    if (schema.dictionary != nullptr)
    {
        REQUIRE_NE(schema_copy.dictionary, nullptr);
        compare_arrow_schema(*schema.dictionary, *schema_copy.dictionary);
    }
    else
    {
        CHECK_EQ(schema_copy.dictionary, nullptr);
    }
}

void check_empty(ArrowSchema& sch)
{
    CHECK_EQ(std::strcmp(sch.format, "n"), 0);
    CHECK_EQ(std::strcmp(sch.name, ""), 0);
    CHECK_EQ(std::strcmp(sch.metadata, ""), 0);
    CHECK_EQ(sch.flags, 0);
    CHECK_EQ(sch.n_children, 0);
    CHECK_EQ(sch.children, nullptr);
    CHECK_EQ(sch.dictionary, nullptr);
}

TEST_SUITE("C Data Interface")
{
    TEST_CASE("ArrowSchema")
    {
        SUBCASE("make_non_owning_arrow_schema")
        {
            ArrowSchema** children = new ArrowSchema*[2];
            children[0] = new ArrowSchema();
            children[1] = new ArrowSchema();

            const auto children_1_ptr = children[0];
            const auto children_2_ptr = children[1];

            auto dictionnary = new ArrowSchema();
            dictionnary->name = "dictionary";
            const std::string format = "format";
            const std::string name = "name";
            auto schema = sparrow_ipc::make_non_owning_arrow_schema(
                format.data(),
                name.data(),
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::DICTIONARY_ORDERED},
                2,
                children,
                dictionnary
            );

            const auto schema_format = std::string_view(schema.format);
            const bool format_eq = schema_format == format;
            CHECK(format_eq);
            const auto schema_name = std::string_view(schema.name);
            const bool name_eq = schema_name == name;
            CHECK(name_eq);
            sparrow_ipc::test_metadata(sparrow_ipc::metadata_sample, schema.metadata);
            CHECK_EQ(schema.flags, 1);
            CHECK_EQ(schema.n_children, 2);
            REQUIRE_NE(schema.children, nullptr);
            CHECK_EQ(schema.children[0], children_1_ptr);
            CHECK_EQ(schema.children[1], children_2_ptr);
            CHECK_EQ(schema.dictionary, dictionnary);
            const bool is_release_arrow_schema = schema.release
                                                 == &sparrow_ipc::release_non_owning_arrow_schema;
            CHECK(is_release_arrow_schema);
            CHECK_NE(schema.private_data, nullptr);
            schema.release(&schema);
        }

        SUBCASE("make_non_owning_arrow_schema no children, no dictionary, no name and metadata")
        {
            auto schema = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "",
                std::optional<std::vector<sparrow::metadata_pair>>{},
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::DICTIONARY_ORDERED},
                0,
                nullptr,
                nullptr
            );

            CHECK_EQ(std::string_view(schema.format), "format");
            CHECK_EQ(std::string_view(schema.name), "");
            CHECK_EQ(schema.metadata, nullptr);
            CHECK_EQ(schema.flags, 1);
            CHECK_EQ(schema.n_children, 0);
            CHECK_EQ(schema.children, nullptr);
            CHECK_EQ(schema.dictionary, nullptr);
            const bool is_release_arrow_schema = schema.release
                                                 == &sparrow_ipc::release_non_owning_arrow_schema;
            CHECK(is_release_arrow_schema);
            CHECK_NE(schema.private_data, nullptr);
            schema.release(&schema);
        }

        SUBCASE("ArrowSchema release")
        {
            ArrowSchema** children = new ArrowSchema*[2];
            children[0] = new ArrowSchema();
            children[1] = new ArrowSchema();

            auto schema = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "name",
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::DICTIONARY_ORDERED},
                2,
                children,
                new ArrowSchema()
            );

            schema.release(&schema);

            CHECK_EQ(schema.format, nullptr);
            CHECK_EQ(schema.name, nullptr);
            CHECK_EQ(schema.metadata, nullptr);
            CHECK_EQ(schema.children, nullptr);
            CHECK_EQ(schema.dictionary, nullptr);
            const bool is_nullptr = schema.release == nullptr;
            CHECK(is_nullptr);
            CHECK_EQ(schema.private_data, nullptr);
        }

        SUBCASE("ArrowSchema release no children, no dictionary, no name and metadata")
        {
            auto schema = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "",
                std::optional<std::vector<sparrow::metadata_pair>>{},
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::DICTIONARY_ORDERED},
                0,
                nullptr,
                nullptr
            );

            schema.release(&schema);

            CHECK_EQ(schema.format, nullptr);
            CHECK_EQ(schema.name, nullptr);
            CHECK_EQ(schema.metadata, nullptr);
            CHECK_EQ(schema.children, nullptr);
            CHECK_EQ(schema.dictionary, nullptr);
            const bool is_nullptr = schema.release == nullptr;
            CHECK(is_nullptr);
            CHECK_EQ(schema.private_data, nullptr);
        }

        SUBCASE("deep_copy_schema")
        {
            auto children = new ArrowSchema*[2];
            children[0] = new ArrowSchema();
            *children[0] = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "child1",
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::MAP_KEYS_SORTED},
                0,
                nullptr,
                nullptr
            );
            children[1] = new ArrowSchema();
            *children[1] = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "child2",
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE},
                0,
                nullptr,
                nullptr
            );

            auto dictionary = new ArrowSchema();
            *dictionary = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "dictionary",
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::MAP_KEYS_SORTED},
                0,
                nullptr,
                nullptr
            );
            auto schema = sparrow_ipc::make_non_owning_arrow_schema(
                "format",
                "name",
                sparrow_ipc::metadata_sample_opt,
                std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::DICTIONARY_ORDERED},
                2,
                children,
                dictionary
            );

            auto schema_copy = sparrow::copy_schema(schema);

            compare_arrow_schema(schema, schema_copy);

            schema_copy.release(&schema_copy);
            schema.release(&schema);
        }

        // SUBCASE("swap_schema")
        // {
        //     auto schema0 = test::make_arrow_schema(true);
        //     auto schema0_bkup = sparrow::copy_schema(schema0);

        //     auto schema1 = test::make_arrow_schema(false);
        //     auto schema1_bkup = sparrow::copy_schema(schema1);

        //     sparrow::swap(schema0, schema1);
        //     compare_arrow_schema(schema0, schema1_bkup);
        //     compare_arrow_schema(schema1, schema0_bkup);

        //     schema0.release(&schema0);
        //     schema1.release(&schema1);
        //     schema0_bkup.release(&schema0_bkup);
        //     schema1_bkup.release(&schema1_bkup);
        // }

        // SUBCASE("move_schema")
        // {
        //     auto src_schema = test::make_arrow_schema(true);
        //     auto control = sparrow::copy_schema(src_schema);

        //     auto dst_schema = sparrow::move_schema(std::move(src_schema));
        //     // check_empty(src_schema);
        //     compare_arrow_schema(dst_schema, control);

        //     auto dst2_schema = sparrow::move_schema(dst_schema);
        //     // check_empty(dst_schema);
        //     compare_arrow_schema(dst2_schema, control);
        //     dst2_schema.release(&dst2_schema);
        //     control.release(&control);
        // }
    }
}
