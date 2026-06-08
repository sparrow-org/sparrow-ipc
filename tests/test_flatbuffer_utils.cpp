#include <doctest/doctest.h>
#include <detail/flatbuffer_utils.hpp>

#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    TEST_SUITE("flatbuffer_utils")
    {
        TEST_CASE("create_metadata")
        {
            flatbuffers::FlatBufferBuilder builder;

            SUBCASE("No metadata (nullptr)")
            {
                auto schema = create_test_arrow_schema("i");
                auto metadata_offset = create_metadata(builder, schema);
                CHECK_EQ(metadata_offset.o, 0);
            }

            SUBCASE("With metadata - basic test")
            {
                auto schema = create_test_arrow_schema_with_metadata("i");
                auto metadata_offset = create_metadata(builder, schema);
                // For now just check that it doesn't crash
                // TODO: Add proper metadata testing when sparrow metadata is properly handled
            }
        }

        TEST_CASE("create_field")
        {
            flatbuffers::FlatBufferBuilder builder;

            SUBCASE("Basic field creation")
            {
                auto schema = create_test_arrow_schema("i", "int_field", true);
                auto field_offset = create_field(builder, schema);
                CHECK_NE(field_offset.o, 0);
            }

            SUBCASE("Field with null name")
            {
                auto schema = create_test_arrow_schema("i", nullptr, false);
                auto field_offset = create_field(builder, schema);
                CHECK_NE(field_offset.o, 0);
            }

            SUBCASE("Non-nullable field")
            {
                auto schema = create_test_arrow_schema("i", "int_field", false);
                auto field_offset = create_field(builder, schema);
                CHECK_NE(field_offset.o, 0);
            }
        }

        TEST_CASE("create_children from ArrowSchema")
        {
            flatbuffers::FlatBufferBuilder builder;

            SUBCASE("No children")
            {
                auto schema = create_test_arrow_schema("i");
                auto children_offset = create_children(builder, schema);
                CHECK_EQ(children_offset.o, 0);
            }

            SUBCASE("With children")
            {
                auto parent_schema = create_test_arrow_schema("+s");
                auto child1 = new ArrowSchema(create_test_arrow_schema("i", "child1"));
                auto child2 = new ArrowSchema(create_test_arrow_schema("u", "child2"));

                ArrowSchema* children[] = {child1, child2};
                parent_schema.children = children;
                parent_schema.n_children = 2;

                auto children_offset = create_children(builder, parent_schema);
                CHECK_NE(children_offset.o, 0);

                // Clean up
                delete child1;
                delete child2;
            }

            SUBCASE("Null child pointer throws exception")
            {
                auto parent_schema = create_test_arrow_schema("+s");
                ArrowSchema* children[] = {nullptr};
                parent_schema.children = children;
                parent_schema.n_children = 1;

                CHECK_THROWS_AS(
                    const auto children_offset = create_children(builder, parent_schema),
                    std::invalid_argument
                );
            }
        }

        TEST_CASE("create_children from record_batch columns")
        {
            flatbuffers::FlatBufferBuilder builder;

            SUBCASE("With valid record batch")
            {
                auto record_batch = create_test_record_batch();
                auto children_offset = create_children(builder, record_batch);
                CHECK_NE(children_offset.o, 0);
            }

            SUBCASE("Empty record batch")
            {
                auto empty_batch = sp::record_batch({});

                auto children_offset = create_children(builder, empty_batch);
                CHECK_EQ(children_offset.o, 0);
            }
        }

        TEST_CASE("get_schema_message_builder")
        {
            SUBCASE("Valid record batch")
            {
                auto record_batch = create_test_record_batch();
                auto builder = get_schema_message_builder(record_batch);

                CHECK_GT(builder.GetSize(), 0);
                CHECK_NE(builder.GetBufferPointer(), nullptr);
            }
        }

        TEST_CASE("fill_fieldnodes")
        {
            SUBCASE("Single array without children")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);

                std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
                fill_fieldnodes(proxy, nodes);

                CHECK_EQ(nodes.size(), 1);
                CHECK_EQ(nodes[0].length(), 5);
                CHECK_EQ(nodes[0].null_count(), 0);
            }

            SUBCASE("Array with null values")
            {
                // For now, just test with a simple array without explicit nulls
                // Creating arrays with null values requires more complex sparrow setup
                auto array = sp::primitive_array<int32_t>({1, 2, 3});
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);

                std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
                fill_fieldnodes(proxy, nodes);

                CHECK_EQ(nodes.size(), 1);
                CHECK_EQ(nodes[0].length(), 3);
                CHECK_EQ(nodes[0].null_count(), 0);
            }
        }

        TEST_CASE("create_fieldnodes")
        {
            SUBCASE("Record batch with multiple columns")
            {
                auto record_batch = create_test_record_batch();
                auto nodes = create_fieldnodes(record_batch);

                CHECK_EQ(nodes.size(), 2);  // Two columns

                // Check the first column (integer array)
                CHECK_EQ(nodes[0].length(), 5);
                CHECK_EQ(nodes[0].null_count(), 0);

                // Check the second column (string array)
                CHECK_EQ(nodes[1].length(), 5);
                CHECK_EQ(nodes[1].null_count(), 0);
            }
        }

        void test_fill_buffers_variant(
            const std::function<void(const sparrow::arrow_proxy&, std::vector<org::apache::arrow::flatbuf::Buffer>&, int64_t&)>& fill_func)
        {
            auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);

            std::vector<org::apache::arrow::flatbuf::Buffer> buffers;
            int64_t offset = 0;
            fill_func(proxy, buffers, offset);

            CHECK_GT(buffers.size(), 0);
            CHECK_GT(offset, 0);

            // Verify offsets are aligned
            for (const auto& buffer : buffers)
            {
                CHECK_EQ(buffer.offset() % 8, 0);
            }
        }

        TEST_CASE("fill_buffers")
        {
            SUBCASE("Simple primitive array")
            {
                test_fill_buffers_variant([](const sparrow::arrow_proxy& proxy, std::vector<org::apache::arrow::flatbuf::Buffer>& buffers, int64_t& offset) {
                    fill_buffers(proxy, buffers, offset);
                });
            }
        }

        TEST_CASE_TEMPLATE("fill_compressed_buffers", Compression, Lz4Compression, ZstdCompression)
        {
            SUBCASE("Simple primitive array")
            {
                test_fill_buffers_variant([](const sparrow::arrow_proxy& proxy, std::vector<org::apache::arrow::flatbuf::Buffer>& buffers, int64_t& offset) {
                    CompressionCache cache;
                    fill_compressed_buffers(proxy, buffers, offset, Compression::type, cache);
                });
            }
        }

        void test_get_buffers_variant(const std::function<std::vector<org::apache::arrow::flatbuf::Buffer>(const sparrow::record_batch&)>& get_func)
        {
            auto record_batch = create_test_record_batch();
            auto buffers = get_func(record_batch);
            CHECK_GT(buffers.size(), 0);
            // Verify all offsets are properly calculated and aligned
            for (size_t i = 1; i < buffers.size(); ++i)
            {
                CHECK_GE(buffers[i].offset(), buffers[i - 1].offset() + buffers[i - 1].length());
            }
        }

        TEST_CASE("get_buffers")
        {
            SUBCASE("Record batch with multiple columns")
            {
                test_get_buffers_variant([](const sparrow::record_batch& record_batch) {
                    return get_buffers(record_batch);
                });
            }
        }

        TEST_CASE_TEMPLATE("get_compressed_buffers", Compression, Lz4Compression, ZstdCompression)
        {
            SUBCASE("Record batch with multiple columns")
            {
                test_get_buffers_variant([](const sparrow::record_batch& record_batch) {
                    CompressionCache cache;
                    return get_compressed_buffers(record_batch, Compression::type, cache);
                });
            }
        }

        TEST_CASE("get_flatbuffer_type")
        {
            flatbuffers::FlatBufferBuilder builder;
            SUBCASE("Null and Boolean types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::NA)).first,
                    org::apache::arrow::flatbuf::Type::Null
                );
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::BOOL)).first,
                    org::apache::arrow::flatbuf::Type::Bool
                );
            }

            SUBCASE("Integer types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT8)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // INT8
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT8)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // UINT8
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT16)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // INT16
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT16)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // UINT16
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT32)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // INT32
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT32)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // UINT32
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INT64)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // INT64
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::UINT64)).first,
                    org::apache::arrow::flatbuf::Type::Int
                );  // UINT64
            }

            SUBCASE("Floating Point types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::HALF_FLOAT)).first,
                    org::apache::arrow::flatbuf::Type::FloatingPoint
                );  // HALF_FLOAT
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::FLOAT)).first,
                    org::apache::arrow::flatbuf::Type::FloatingPoint
                );  // FLOAT
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DOUBLE)).first,
                    org::apache::arrow::flatbuf::Type::FloatingPoint
                );  // DOUBLE
            }

            SUBCASE("String and Binary types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::STRING)).first,
                    org::apache::arrow::flatbuf::Type::Utf8
                );  // STRING
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_STRING))
                        .first,
                    org::apache::arrow::flatbuf::Type::LargeUtf8
                );  // LARGE_STRING
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::BINARY)).first,
                    org::apache::arrow::flatbuf::Type::Binary
                );  // BINARY
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_BINARY))
                        .first,
                    org::apache::arrow::flatbuf::Type::LargeBinary
                );  // LARGE_BINARY
                CHECK_EQ(
                    get_flatbuffer_type(builder, "vu").first,
                    org::apache::arrow::flatbuf::Type::Utf8View
                );  // STRING_VIEW
                CHECK_EQ(
                    get_flatbuffer_type(builder, "vz").first,
                    org::apache::arrow::flatbuf::Type::BinaryView
                );  // BINARY_VIEW
            }

            SUBCASE("Date types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DATE_DAYS)).first,
                    org::apache::arrow::flatbuf::Type::Date
                );  // DATE_DAYS
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DATE_MILLISECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Date
                );  // DATE_MILLISECONDS
            }

            SUBCASE("Timestamp types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_SECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Timestamp
                );  // TIMESTAMP_SECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_MILLISECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Timestamp
                );  // TIMESTAMP_MILLISECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_MICROSECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Timestamp
                );  // TIMESTAMP_MICROSECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::TIMESTAMP_NANOSECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Timestamp
                );  // TIMESTAMP_NANOSECONDS
            }

            SUBCASE("Duration types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::DURATION_SECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Duration
                );  // DURATION_SECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::DURATION_MILLISECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Duration
                );  // DURATION_MILLISECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::DURATION_MICROSECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Duration
                );  // DURATION_MICROSECONDS
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::DURATION_NANOSECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Duration
                );  // DURATION_NANOSECONDS
            }

            SUBCASE("Interval types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INTERVAL_MONTHS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Interval
                );  // INTERVAL_MONTHS
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::INTERVAL_DAYS_TIME))
                        .first,
                    org::apache::arrow::flatbuf::Type::Interval
                );  // INTERVAL_DAYS_TIME
                CHECK_EQ(
                    get_flatbuffer_type(
                        builder,
                        sparrow::data_type_to_format(sparrow::data_type::INTERVAL_MONTHS_DAYS_NANOSECONDS)
                    )
                        .first,
                    org::apache::arrow::flatbuf::Type::Interval
                );  // INTERVAL_MONTHS_DAYS_NANOSECONDS
            }

            SUBCASE("Time types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIME_SECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Time
                );  // TIME_SECONDS
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIME_MILLISECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Time
                );  // TIME_MILLISECONDS
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIME_MICROSECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Time
                );  // TIME_MICROSECONDS
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::TIME_NANOSECONDS))
                        .first,
                    org::apache::arrow::flatbuf::Type::Time
                );  // TIME_NANOSECONDS
            }

            SUBCASE("List types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LIST)).first,
                    org::apache::arrow::flatbuf::Type::List
                );  // LIST
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::LARGE_LIST)).first,
                    org::apache::arrow::flatbuf::Type::LargeList
                );  // LARGE_LIST
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+vl").first,
                    org::apache::arrow::flatbuf::Type::ListView
                );  // LIST_VIEW
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+vL").first,
                    org::apache::arrow::flatbuf::Type::LargeListView
                );  // LARGE_LIST_VIEW
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+w:16").first,
                    org::apache::arrow::flatbuf::Type::FixedSizeList
                );                                                  // FIXED_SIZED_LIST
                CHECK_THROWS(static_cast<void>(get_flatbuffer_type(builder, "+w:")));  // Invalid FixedSizeList format
            }

            SUBCASE("Struct and Map types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::STRUCT)).first,
                    org::apache::arrow::flatbuf::Type::Struct_
                );  // STRUCT
                CHECK_EQ(
                    get_flatbuffer_type(builder, sparrow::data_type_to_format(sparrow::data_type::MAP)).first,
                    org::apache::arrow::flatbuf::Type::Map
                );  // MAP
            }

            SUBCASE("Union types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+ud:").first,
                    org::apache::arrow::flatbuf::Type::Union
                );  // DENSE_UNION
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+us:").first,
                    org::apache::arrow::flatbuf::Type::Union
                );  // SPARSE_UNION
            }

            SUBCASE("Run-End Encoded type")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, "+r").first,
                    org::apache::arrow::flatbuf::Type::RunEndEncoded
                );  // RUN_ENCODED
            }

            SUBCASE("Decimal types")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, "d:10,5").first,
                    org::apache::arrow::flatbuf::Type::Decimal
                );                                                   // DECIMAL (general)
                CHECK_THROWS(static_cast<void>(get_flatbuffer_type(builder, "d:10")));  // Invalid Decimal format
            }

            SUBCASE("Fixed Width Binary type")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, "w:32").first,
                    org::apache::arrow::flatbuf::Type::FixedSizeBinary
                );                                                 // FIXED_WIDTH_BINARY
                CHECK_THROWS(static_cast<void>(get_flatbuffer_type(builder, "w:")));  // Invalid FixedSizeBinary format
            }

            SUBCASE("Unsupported type returns Null")
            {
                CHECK_EQ(
                    get_flatbuffer_type(builder, "unsupported_format").first,
                    org::apache::arrow::flatbuf::Type::Null
                );
            }
        }

        TEST_CASE("get_record_batch_message_builder")
        {
            auto test_get_record_batch_message_builder = [](std::optional<CompressionType> compression)
            {
                auto record_batch = create_test_record_batch();
                CompressionCache cache;
                auto builder = get_record_batch_message_builder(record_batch, compression, cache);
                CHECK_GT(builder.GetSize(), 0);
                CHECK_NE(builder.GetBufferPointer(), nullptr);
            };

            SUBCASE("Valid record batch with field nodes and buffers (Without compression)")
            {
                test_get_record_batch_message_builder(std::nullopt);
            }

            SUBCASE("Valid record batch with field nodes and buffers (With LZ4 compression)")
            {
                test_get_record_batch_message_builder(CompressionType::LZ4_FRAME);
            }

            SUBCASE("Valid record batch with field nodes and buffers (With ZSTD compression)")
            {
                test_get_record_batch_message_builder(CompressionType::ZSTD);
            }
        }
    }
}
