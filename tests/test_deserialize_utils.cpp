#include <doctest/doctest.h>

#include "sparrow_ipc/flatbuffers_generated/Schema_generated.h"

#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    TEST_SUITE("deserialize_utils")
    {
        TEST_CASE("get_fb_name")
        {
            SUBCASE("Object is null")
            {
                const org::apache::arrow::flatbuf::Field* field = nullptr;
                CHECK_EQ(utils::get_fb_name(field, "default_val"), "default_val");
            }

            SUBCASE("Object is not null, but name is null (missing)")
            {
                flatbuffers::FlatBufferBuilder builder;
                org::apache::arrow::flatbuf::FieldBuilder field_builder(builder);
                // No name() added
                auto field_offset = field_builder.Finish();
                builder.Finish(field_offset);

                auto field = flatbuffers::GetRoot<org::apache::arrow::flatbuf::Field>(builder.GetBufferPointer());
                CHECK_EQ(utils::get_fb_name(field, "default_val"), "default_val");
            }

            SUBCASE("Name is present and non-empty")
            {
                flatbuffers::FlatBufferBuilder builder;
                auto name_offset = builder.CreateString("test_field");
                org::apache::arrow::flatbuf::FieldBuilder field_builder(builder);
                field_builder.add_name(name_offset);
                auto field_offset = field_builder.Finish();
                builder.Finish(field_offset);

                auto field = flatbuffers::GetRoot<org::apache::arrow::flatbuf::Field>(builder.GetBufferPointer());
                CHECK_EQ(utils::get_fb_name(field, "default_val"), "test_field");
            }

            SUBCASE("Name is present but empty, allow_empty = true (default)")
            {
                flatbuffers::FlatBufferBuilder builder;
                auto name_offset = builder.CreateString("");
                org::apache::arrow::flatbuf::FieldBuilder field_builder(builder);
                field_builder.add_name(name_offset);
                auto field_offset = field_builder.Finish();
                builder.Finish(field_offset);

                auto field = flatbuffers::GetRoot<org::apache::arrow::flatbuf::Field>(builder.GetBufferPointer());
                CHECK_EQ(utils::get_fb_name(field, "default_val"), "");
            }

            SUBCASE("Name is present but empty, allow_empty = false")
            {
                flatbuffers::FlatBufferBuilder builder;
                auto name_offset = builder.CreateString("");
                org::apache::arrow::flatbuf::FieldBuilder field_builder(builder);
                field_builder.add_name(name_offset);
                auto field_offset = field_builder.Finish();
                builder.Finish(field_offset);

                auto field = flatbuffers::GetRoot<org::apache::arrow::flatbuf::Field>(builder.GetBufferPointer());
                CHECK_EQ(utils::get_fb_name(field, "default_val", false), "default_val");
            }
        }
    }
}
