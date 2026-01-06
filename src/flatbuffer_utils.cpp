#include "sparrow_ipc/flatbuffer_utils.hpp"

#include <string>

#include <sparrow/utils/ranges.hpp>

#include "compression_impl.hpp"
#include "sparrow_ipc/magic_values.hpp"

namespace sparrow_ipc
{
    namespace details
    {
        std::size_t get_nb_buffers_to_process(const std::string_view& format, const std::size_t orig_buffers_size)
        {
            // If the array data type is a view type, we should not consider the last buffer (corresponding to the variadic buffer sizes)
            if (const auto type = sparrow::format_to_data_type(format);
                type == sparrow::data_type::STRING_VIEW || type == sparrow::data_type::BINARY_VIEW)
            {
                auto num_buffers_to_process = (orig_buffers_size > 0) ? orig_buffers_size - 1 : 0;
                return num_buffers_to_process;
            }
            return orig_buffers_size;
        }
    }

    namespace
    {
        std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
        get_flatbuffer_timestamp_type(
            flatbuffers::FlatBufferBuilder& builder,
            std::string_view format_str,
            org::apache::arrow::flatbuf::TimeUnit time_unit)
        {
            const auto timezone = utils::get_substr_after_separator(format_str, ":");
            flatbuffers::Offset<flatbuffers::String> timezone_offset = 0;
            if (timezone.has_value() && !timezone.value().empty())
            {
                timezone_offset = builder.CreateString(timezone.value());
            }
            const auto timestamp_type = org::apache::arrow::flatbuf::CreateTimestamp(
                builder,
                time_unit,
                timezone_offset);
            return {org::apache::arrow::flatbuf::Type::Timestamp, timestamp_type.Union()};
        }
    }

    std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str)
    {
        const auto type = sparrow::format_to_data_type(format_str);
        switch (type)
        {
            case sparrow::data_type::NA:
            {
                const auto null_type = org::apache::arrow::flatbuf::CreateNull(builder);
                return {org::apache::arrow::flatbuf::Type::Null, null_type.Union()};
            }
            case sparrow::data_type::BOOL:
            {
                const auto bool_type = org::apache::arrow::flatbuf::CreateBool(builder);
                return {org::apache::arrow::flatbuf::Type::Bool, bool_type.Union()};
            }
            case sparrow::data_type::UINT8:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 8, false);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::INT8:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 8, true);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::UINT16:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 16, false);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::INT16:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 16, true);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::UINT32:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 32, false);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::INT32:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 32, true);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::UINT64:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 64, false);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::INT64:
            {
                const auto int_type = org::apache::arrow::flatbuf::CreateInt(builder, 64, true);
                return {org::apache::arrow::flatbuf::Type::Int, int_type.Union()};
            }
            case sparrow::data_type::HALF_FLOAT:
            {
                const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                    builder,
                    org::apache::arrow::flatbuf::Precision::HALF
                );
                return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
            }
            case sparrow::data_type::FLOAT:
            {
                const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                    builder,
                    org::apache::arrow::flatbuf::Precision::SINGLE
                );
                return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
            }
            case sparrow::data_type::DOUBLE:
            {
                const auto fp_type = org::apache::arrow::flatbuf::CreateFloatingPoint(
                    builder,
                    org::apache::arrow::flatbuf::Precision::DOUBLE
                );
                return {org::apache::arrow::flatbuf::Type::FloatingPoint, fp_type.Union()};
            }
            case sparrow::data_type::STRING:
            {
                const auto string_type = org::apache::arrow::flatbuf::CreateUtf8(builder);
                return {org::apache::arrow::flatbuf::Type::Utf8, string_type.Union()};
            }
            case sparrow::data_type::LARGE_STRING:
            {
                const auto large_string_type = org::apache::arrow::flatbuf::CreateLargeUtf8(builder);
                return {org::apache::arrow::flatbuf::Type::LargeUtf8, large_string_type.Union()};
            }
            case sparrow::data_type::BINARY:
            {
                const auto binary_type = org::apache::arrow::flatbuf::CreateBinary(builder);
                return {org::apache::arrow::flatbuf::Type::Binary, binary_type.Union()};
            }
            case sparrow::data_type::LARGE_BINARY:
            {
                const auto large_binary_type = org::apache::arrow::flatbuf::CreateLargeBinary(builder);
                return {org::apache::arrow::flatbuf::Type::LargeBinary, large_binary_type.Union()};
            }
            case sparrow::data_type::STRING_VIEW:
            {
                const auto string_view_type = org::apache::arrow::flatbuf::CreateUtf8View(builder);
                return {org::apache::arrow::flatbuf::Type::Utf8View, string_view_type.Union()};
            }
            case sparrow::data_type::BINARY_VIEW:
            {
                const auto binary_view_type = org::apache::arrow::flatbuf::CreateBinaryView(builder);
                return {org::apache::arrow::flatbuf::Type::BinaryView, binary_view_type.Union()};
            }
            case sparrow::data_type::DATE_DAYS:
            {
                const auto date_type = org::apache::arrow::flatbuf::CreateDate(
                    builder,
                    org::apache::arrow::flatbuf::DateUnit::DAY
                );
                return {org::apache::arrow::flatbuf::Type::Date, date_type.Union()};
            }
            case sparrow::data_type::DATE_MILLISECONDS:
            {
                const auto date_type = org::apache::arrow::flatbuf::CreateDate(
                    builder,
                    org::apache::arrow::flatbuf::DateUnit::MILLISECOND
                );
                return {org::apache::arrow::flatbuf::Type::Date, date_type.Union()};
            }
            case sparrow::data_type::TIMESTAMP_SECONDS:
            {
                return get_flatbuffer_timestamp_type(
                    builder,
                    format_str,
                    org::apache::arrow::flatbuf::TimeUnit::SECOND
                );
            }
            case sparrow::data_type::TIMESTAMP_MILLISECONDS:
            {
                return get_flatbuffer_timestamp_type(
                    builder,
                    format_str,
                    org::apache::arrow::flatbuf::TimeUnit::MILLISECOND
                );
            }
            case sparrow::data_type::TIMESTAMP_MICROSECONDS:
            {
                return get_flatbuffer_timestamp_type(
                    builder,
                    format_str,
                    org::apache::arrow::flatbuf::TimeUnit::MICROSECOND
                );
            }
            case sparrow::data_type::TIMESTAMP_NANOSECONDS:
            {
                return get_flatbuffer_timestamp_type(
                    builder,
                    format_str,
                    org::apache::arrow::flatbuf::TimeUnit::NANOSECOND
                );
            }
            case sparrow::data_type::DURATION_SECONDS:
            {
                const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::SECOND
                );
                return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
            }
            case sparrow::data_type::DURATION_MILLISECONDS:
            {
                const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::MILLISECOND
                );
                return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
            }
            case sparrow::data_type::DURATION_MICROSECONDS:
            {
                const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::MICROSECOND
                );
                return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
            }
            case sparrow::data_type::DURATION_NANOSECONDS:
            {
                const auto duration_type = org::apache::arrow::flatbuf::CreateDuration(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::NANOSECOND
                );
                return {org::apache::arrow::flatbuf::Type::Duration, duration_type.Union()};
            }
            case sparrow::data_type::INTERVAL_MONTHS:
            {
                const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                    builder,
                    org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH
                );
                return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
            }
            case sparrow::data_type::INTERVAL_DAYS_TIME:
            {
                const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                    builder,
                    org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME
                );
                return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
            }
            case sparrow::data_type::INTERVAL_MONTHS_DAYS_NANOSECONDS:
            {
                const auto interval_type = org::apache::arrow::flatbuf::CreateInterval(
                    builder,
                    org::apache::arrow::flatbuf::IntervalUnit::MONTH_DAY_NANO
                );
                return {org::apache::arrow::flatbuf::Type::Interval, interval_type.Union()};
            }
            case sparrow::data_type::TIME_SECONDS:
            {
                const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::SECOND,
                    32
                );
                return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
            }
            case sparrow::data_type::TIME_MILLISECONDS:
            {
                const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::MILLISECOND,
                    32
                );
                return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
            }
            case sparrow::data_type::TIME_MICROSECONDS:
            {
                const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::MICROSECOND,
                    64
                );
                return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
            }
            case sparrow::data_type::TIME_NANOSECONDS:
            {
                const auto time_type = org::apache::arrow::flatbuf::CreateTime(
                    builder,
                    org::apache::arrow::flatbuf::TimeUnit::NANOSECOND,
                    64
                );
                return {org::apache::arrow::flatbuf::Type::Time, time_type.Union()};
            }
            case sparrow::data_type::LIST:
            {
                const auto list_type = org::apache::arrow::flatbuf::CreateList(builder);
                return {org::apache::arrow::flatbuf::Type::List, list_type.Union()};
            }
            case sparrow::data_type::LARGE_LIST:
            {
                const auto large_list_type = org::apache::arrow::flatbuf::CreateLargeList(builder);
                return {org::apache::arrow::flatbuf::Type::LargeList, large_list_type.Union()};
            }
            case sparrow::data_type::LIST_VIEW:
            {
                const auto list_view_type = org::apache::arrow::flatbuf::CreateListView(builder);
                return {org::apache::arrow::flatbuf::Type::ListView, list_view_type.Union()};
            }
            case sparrow::data_type::LARGE_LIST_VIEW:
            {
                const auto large_list_view_type = org::apache::arrow::flatbuf::CreateLargeListView(builder);
                return {org::apache::arrow::flatbuf::Type::LargeListView, large_list_view_type.Union()};
            }
            case sparrow::data_type::FIXED_SIZED_LIST:
            {
                // FixedSizeList requires listSize. We need to parse the format_str.
                // Format: "+w:size"
                const auto list_size = utils::get_substr_as_int32(format_str, ":");
                if (!list_size.has_value())
                {
                    throw std::runtime_error(
                        "Failed to parse FixedSizeList size from format string: " + std::string(format_str)
                    );
                }

                const auto fixed_size_list_type = org::apache::arrow::flatbuf::CreateFixedSizeList(
                    builder,
                    list_size.value()
                );
                return {org::apache::arrow::flatbuf::Type::FixedSizeList, fixed_size_list_type.Union()};
            }
            case sparrow::data_type::STRUCT:
            {
                const auto struct_type = org::apache::arrow::flatbuf::CreateStruct_(builder);
                return {org::apache::arrow::flatbuf::Type::Struct_, struct_type.Union()};
            }
            case sparrow::data_type::MAP:
            {
                // not sorted keys
                const auto map_type = org::apache::arrow::flatbuf::CreateMap(builder, false);
                return {org::apache::arrow::flatbuf::Type::Map, map_type.Union()};
            }
            case sparrow::data_type::DENSE_UNION:
            {
                const auto union_type = org::apache::arrow::flatbuf::CreateUnion(
                    builder,
                    org::apache::arrow::flatbuf::UnionMode::Dense,
                    0
                );
                return {org::apache::arrow::flatbuf::Type::Union, union_type.Union()};
            }
            case sparrow::data_type::SPARSE_UNION:
            {
                const auto union_type = org::apache::arrow::flatbuf::CreateUnion(
                    builder,
                    org::apache::arrow::flatbuf::UnionMode::Sparse,
                    0
                );
                return {org::apache::arrow::flatbuf::Type::Union, union_type.Union()};
            }
            case sparrow::data_type::RUN_ENCODED:
            {
                const auto run_end_encoded_type = org::apache::arrow::flatbuf::CreateRunEndEncoded(builder);
                return {org::apache::arrow::flatbuf::Type::RunEndEncoded, run_end_encoded_type.Union()};
            }
            case sparrow::data_type::DECIMAL32:
            {
                return get_flatbuffer_decimal_type(builder, format_str, 32);
            }
            case sparrow::data_type::DECIMAL64:
            {
                return get_flatbuffer_decimal_type(builder, format_str, 64);
            }
            case sparrow::data_type::DECIMAL128:
            {
                return get_flatbuffer_decimal_type(builder, format_str, 128);
            }
            case sparrow::data_type::DECIMAL256:
            {
                return get_flatbuffer_decimal_type(builder, format_str, 256);
            }
            case sparrow::data_type::FIXED_WIDTH_BINARY:
            {
                // FixedSizeBinary requires byteWidth. We need to parse the format_str.
                // Format: "w:size"
                const auto byte_width = utils::get_substr_as_int32(format_str, ":");
                if (!byte_width.has_value())
                {
                    throw std::runtime_error(
                        "Failed to parse FixedWidthBinary size from format string: " + std::string(format_str)
                    );
                }

                const auto fixed_width_binary_type = org::apache::arrow::flatbuf::CreateFixedSizeBinary(
                    builder,
                    byte_width.value()
                );
                return {org::apache::arrow::flatbuf::Type::FixedSizeBinary, fixed_width_binary_type.Union()};
            }
            default:
            {
                throw std::runtime_error("Unsupported data type for serialization");
            }
        }
    }

    // Creates a Flatbuffers Decimal type from a format string
    // The format string is expected to be in the format "d:precision,scale" or "d:precision,scale,bitWidth
    std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>> get_flatbuffer_decimal_type(
        flatbuffers::FlatBufferBuilder& builder,
        std::string_view format_str,
        const int32_t bitWidth
    )
    {
        // Decimal requires precision and scale. We need to parse the format_str.
        // Format: "d:precision,scale" or "d:precision,scale,bitWidth"
        const auto parsed = utils::parse_decimal_format(format_str);
        if (!parsed.has_value())
        {
            throw std::runtime_error(
                "Failed to parse Decimal " + std::to_string(bitWidth)
                + " format string: " + std::string(format_str)
            );
        }

        const auto& [precision, scale, parsed_bitwidth] = parsed.value();

        // Use the bitWidth from the format string if provided, otherwise use the parameter
        const int32_t actual_bitwidth = parsed_bitwidth.value_or(bitWidth);
        const auto decimal_type = org::apache::arrow::flatbuf::CreateDecimal(
            builder,
            precision,
            scale,
            actual_bitwidth
        );
        return {org::apache::arrow::flatbuf::Type::Decimal, decimal_type.Union()};
    }

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
    create_metadata(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        if (arrow_schema.metadata == nullptr)
        {
            return 0;
        }

        const auto metadata_view = sparrow::key_value_view(arrow_schema.metadata);
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
        kv_offsets.reserve(metadata_view.size());
        for (const auto& [key, value] : metadata_view)
        {
            const auto key_offset = builder.CreateString(std::string(key));
            const auto value_offset = builder.CreateString(std::string(value));
            kv_offsets.push_back(org::apache::arrow::flatbuf::CreateKeyValue(builder, key_offset, value_offset));
        }
        return builder.CreateVector(kv_offsets);
    }

    ::flatbuffers::Offset<org::apache::arrow::flatbuf::Field> create_field(
        flatbuffers::FlatBufferBuilder& builder,
        const ArrowSchema& arrow_schema,
        std::optional<std::string_view> name_override
    )
    {
        flatbuffers::Offset<flatbuffers::String>
            fb_name_offset = name_override.has_value()
                                 ? builder.CreateString(name_override.value())
                                 : (arrow_schema.name == nullptr ? 0 : builder.CreateString(arrow_schema.name));
        const auto [type_enum, type_offset] = get_flatbuffer_type(builder, arrow_schema.format);
        auto fb_metadata_offset = create_metadata(builder, arrow_schema);
        const auto children = create_children(builder, arrow_schema);
        const auto fb_field = org::apache::arrow::flatbuf::CreateField(
            builder,
            fb_name_offset,
            (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
            type_enum,
            type_offset,
            0,  // TODO: support dictionary
            children,
            fb_metadata_offset
        );
        return fb_field;
    }

    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const ArrowSchema& arrow_schema)
    {
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> children_vec;
        children_vec.reserve(arrow_schema.n_children);
        for (size_t i = 0; i < arrow_schema.n_children; ++i)
        {
            if (arrow_schema.children[i] == nullptr)
            {
                throw std::invalid_argument("ArrowSchema has null child pointer");
            }
            const auto& child = *arrow_schema.children[i];
            flatbuffers::Offset<org::apache::arrow::flatbuf::Field> field = create_field(builder, child);
            children_vec.emplace_back(field);
        }
        return children_vec.empty() ? 0 : builder.CreateVector(children_vec);
    }

    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::Field>>>
    create_children(flatbuffers::FlatBufferBuilder& builder, const sparrow::record_batch& record_batch)
    {
        const auto& columns = record_batch.columns();
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> children_vec;
        children_vec.reserve(columns.size());
        const auto names = record_batch.names();
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto& arrow_schema = sparrow::detail::array_access::get_arrow_proxy(columns[i]).schema();
            flatbuffers::Offset<org::apache::arrow::flatbuf::Field> field = create_field(
                builder,
                arrow_schema,
                names[i]
            );
            children_vec.emplace_back(field);
        }
        return children_vec.empty() ? 0 : builder.CreateVector(children_vec);
    }

    flatbuffers::FlatBufferBuilder get_schema_message_builder(const sparrow::record_batch& record_batch)
    {
        flatbuffers::FlatBufferBuilder schema_builder;
        const auto fields_vec = create_children(schema_builder, record_batch);
        const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(
            schema_builder,
            org::apache::arrow::flatbuf::Endianness::Little,  // TODO: make configurable
            fields_vec
        );
        const auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
            schema_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            org::apache::arrow::flatbuf::MessageHeader::Schema,
            schema_offset.Union(),
            0,  // body length is 0 for schema messages
            0   // custom metadata
        );
        schema_builder.Finish(schema_message_offset);
        return schema_builder;
    }

    void fill_fieldnodes(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::FieldNode>& nodes
    )
    {
        nodes.emplace_back(arrow_proxy.length(), arrow_proxy.null_count());
        nodes.reserve(nodes.size() + arrow_proxy.n_children());
        for (const auto& child : arrow_proxy.children())
        {
            fill_fieldnodes(child, nodes);
        }
    }

    std::vector<org::apache::arrow::flatbuf::FieldNode>
    create_fieldnodes(const sparrow::record_batch& record_batch)
    {
        std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
        nodes.reserve(record_batch.nb_columns());
        for (const auto& column : record_batch.columns())
        {
            fill_fieldnodes(sparrow::detail::array_access::get_arrow_proxy(column), nodes);
        }
        return nodes;
    }

    void fill_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers,
        int64_t& offset
    )
    {
        details::fill_buffers_impl(
            arrow_proxy,
            flatbuf_buffers,
            offset,
            [](const auto& buffer)
            {
                return static_cast<int64_t>(buffer.size());
            }
        );
    }

    std::vector<org::apache::arrow::flatbuf::Buffer> get_buffers(const sparrow::record_batch& record_batch)
    {
        return details::get_buffers_impl(
            record_batch,
            [](const sparrow::arrow_proxy& proxy,
               std::vector<org::apache::arrow::flatbuf::Buffer>& buffers,
               int64_t& offset)
            {
                fill_buffers(proxy, buffers, offset);
            }
        );
    }

    void fill_compressed_buffers(
        const sparrow::arrow_proxy& arrow_proxy,
        std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_compressed_buffers,
        int64_t& offset,
        const CompressionType compression_type,
        CompressionCache& cache
    )
    {
        details::fill_buffers_impl(
            arrow_proxy,
            flatbuf_compressed_buffers,
            offset,
            [&](const auto& buffer)
            {
                return get_compressed_size(
                    compression_type,
                    std::span<const uint8_t>(buffer.data(), buffer.size()),
                    cache
                );
            }
        );
    }

    std::vector<org::apache::arrow::flatbuf::Buffer> get_compressed_buffers(
        const sparrow::record_batch& record_batch,
        const CompressionType compression_type,
        CompressionCache& cache
    )
    {
        return details::get_buffers_impl(
            record_batch,
            [&](const sparrow::arrow_proxy& proxy,
                std::vector<org::apache::arrow::flatbuf::Buffer>& buffers,
                int64_t& offset)
            {
                fill_compressed_buffers(proxy, buffers, offset, compression_type, cache);
            }
        );
    }

    int64_t calculate_body_size(
        const sparrow::arrow_proxy& arrow_proxy,
        std::optional<CompressionType> compression,
        std::optional<std::reference_wrapper<CompressionCache>> cache
    )
    {
        int64_t total_size = 0;
        const auto& buffers = arrow_proxy.buffers();
        auto nb_buffers = details::get_nb_buffers_to_process(arrow_proxy.schema().format, buffers.size());

        if (compression.has_value())
        {
            if (!cache)
            {
                throw std::invalid_argument("Compression type set but no cache is given.");
            }

            total_size = sparrow::ranges::accumulate(
                buffers | std::views::take(nb_buffers), int64_t{0},
                [&](int64_t acc, const auto& buffer) {
                    return acc + utils::align_to_8(get_compressed_size(
                        compression.value(),
                        std::span<const uint8_t>(buffer.data(), buffer.size()),
                        cache.value().get()
                    ));
                });
        }
        else
        {
            total_size = sparrow::ranges::accumulate(
                buffers | std::views::take(nb_buffers), int64_t{0},
                [&](int64_t acc, const auto& buffer) {
                    return acc + utils::align_to_8(buffer.size());
                });
        }

        for (const auto& child : arrow_proxy.children())
        {
            total_size += calculate_body_size(child, compression, cache);
        }
        return total_size;
    }

    int64_t calculate_body_size(
        const sparrow::record_batch& record_batch,
        std::optional<CompressionType> compression,
        std::optional<std::reference_wrapper<CompressionCache>> cache
    )
    {
        return sparrow::ranges::accumulate(
            record_batch.columns(),
            int64_t{0},
            [&](int64_t acc, const sparrow::array& arr)
            {
                const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(arr);
                return acc + calculate_body_size(arrow_proxy, compression, cache);
            }
        );
    }

    namespace
    {
        std::vector<int64_t> get_variadic_buffer_counts(const sparrow::record_batch& record_batch)
        {
            std::vector<int64_t> counts;
            for (const auto& column : record_batch.columns())
            {
                const auto& proxy = sparrow::detail::array_access::get_arrow_proxy(column);
                const auto format_str = proxy.schema().format;
                const auto type = sparrow::format_to_data_type(format_str);

                if (type == sparrow::data_type::BINARY_VIEW || type == sparrow::data_type::STRING_VIEW)
                {
                    // n_buffers includes the following buffers: validity buffer, views buffer, data buffers (variadic buffers if present for strings > 12 bytes) and buffer for variadic buffer sizes
                    int64_t n_buffers = proxy.n_buffers();
                    // The variable size binary view array should at least contain validity, views and variadic buffers size
                    if (n_buffers <= 2)
                    {
                        throw std::runtime_error(
                            "Variable size binary view array contains " + std::to_string(n_buffers) + " buffers!"
                        );
                    }

                    const auto& sizes_buffer = proxy.buffers().back();
                    const int64_t num_variadic_data_buffers = sizes_buffer.size() / sizeof(int64_t);

                    counts.push_back(num_variadic_data_buffers);
                }
            }
            return counts;
        }
    }

    flatbuffers::FlatBufferBuilder get_record_batch_message_builder(const sparrow::record_batch& record_batch,
                                                                    std::optional<CompressionType> compression,
                                                                    std::optional<std::reference_wrapper<CompressionCache>> cache)
    {
        flatbuffers::FlatBufferBuilder record_batch_builder;
        flatbuffers::Offset<org::apache::arrow::flatbuf::BodyCompression> compression_offset = 0;
        std::optional<std::vector<org::apache::arrow::flatbuf::Buffer>> compressed_buffers;
        if (compression)
        {
            if (!cache)
            {
                throw std::invalid_argument("Compression type set but no cache is given.");
            }
            compressed_buffers = get_compressed_buffers(record_batch, compression.value(), cache.value().get());
            compression_offset = org::apache::arrow::flatbuf::CreateBodyCompression(
                record_batch_builder,
                details::to_fb_compression_type(compression.value()),
                org::apache::arrow::flatbuf::BodyCompressionMethod::BUFFER
            );
        }
        const auto& buffers = compressed_buffers ? *compressed_buffers : get_buffers(record_batch);
        const std::vector<org::apache::arrow::flatbuf::FieldNode> nodes = create_fieldnodes(record_batch);
        const auto variadic_counts = get_variadic_buffer_counts(record_batch);
        auto nodes_offset = record_batch_builder.CreateVectorOfStructs(nodes);
        auto buffers_offset = record_batch_builder.CreateVectorOfStructs(buffers);
        auto variadic_counts_offset = record_batch_builder.CreateVector(variadic_counts);
        const auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(
            record_batch_builder,
            static_cast<int64_t>(record_batch.nb_rows()),
            nodes_offset,
            buffers_offset,
            compression_offset,
            variadic_counts_offset
        );

        const int64_t body_size = calculate_body_size(record_batch, compression, cache);
        const auto record_batch_message_offset = org::apache::arrow::flatbuf::CreateMessage(
            record_batch_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
            record_batch_offset.Union(),
            body_size,  // body length
            0           // custom metadata
        );
        record_batch_builder.Finish(record_batch_message_offset);
        return record_batch_builder;
    }

    const org::apache::arrow::flatbuf::Footer* get_footer_from_file_data(std::span<const uint8_t> file_data)
    {
        // Footer size is stored 4 bytes before the trailing magic
        const size_t footer_size_offset = file_data.size() - sparrow_ipc::arrow_file_magic_size
                                          - sizeof(int32_t);
        int32_t footer_size = 0;
        std::memcpy(&footer_size, file_data.data() + footer_size_offset, sizeof(int32_t));

        // Footer data starts at footer_size_offset - footer_size
        const size_t footer_offset = footer_size_offset - footer_size;

        return org::apache::arrow::flatbuf::GetFooter(file_data.data() + footer_offset);
    }
}
