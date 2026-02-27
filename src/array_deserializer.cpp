#include "array_deserializer.hpp"

#include <algorithm>
#include <stdexcept>

#include <sparrow/map_array.hpp>

#include "array_deserialization_dictionaries.hpp"
#include "bit_width.hpp"
#include "sparrow_ipc/deserialize_decimal_array.hpp"
#include "sparrow_ipc/deserialize_duration_array.hpp"
#include "sparrow_ipc/deserialize_fixed_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_interval_array.hpp"
#include "sparrow_ipc/deserialize_null_array.hpp"
#include "sparrow_ipc/deserialize_run_end_encoded_array.hpp"
#include "sparrow_ipc/deserialize_time_related_arrays.hpp"
#include "sparrow_ipc/dictionary_cache.hpp"

namespace sparrow_ipc
{
    void array_deserializer::initialize_deserializer_map()
    {
        if (!m_deserializer_map.empty())
        {
            return;
        }

        m_deserializer_map[org::apache::arrow::flatbuf::Type::Bool] = &deserialize_primitive<bool>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Int] = &deserialize_int;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::FloatingPoint] = &deserialize_float;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::FixedSizeBinary] = &deserialize_fixed_size_binary;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Binary] = &deserialize_variable_size_binary<
            sparrow::binary_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeBinary] = &deserialize_variable_size_binary<
            sparrow::big_binary_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Utf8] = &deserialize_variable_size_binary<
            sparrow::string_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeUtf8] = &deserialize_variable_size_binary<
            sparrow::big_string_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Interval] = &deserialize_interval;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Duration] = &deserialize_duration;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Timestamp] = &deserialize_timestamp;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Date] = &deserialize_date;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Time] = &deserialize_time;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Null] = &deserialize_null;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Decimal] = &deserialize_decimal;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::BinaryView] = &deserialize_variable_size_binary_view<
            sparrow::binary_view_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Utf8View] = &deserialize_variable_size_binary_view<
            sparrow::string_view_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::List] = &deserialize_list<sparrow::list_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeList] = &deserialize_list<sparrow::big_list_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::FixedSizeList] = &deserialize_fixed_size_list;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::ListView] = &deserialize_list_view<sparrow::list_view_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeListView] = &deserialize_list_view<
            sparrow::big_list_view_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Struct_] = &deserialize_struct;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Map] = &deserialize_map;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::RunEndEncoded] = &deserialize_run_end_encoded;
    }

    sparrow::array array_deserializer::deserialize(deserialization_context& context, const field_descriptor& field_desc)
    {
        const auto deserialize_impl = [&]() -> sparrow::array
        {
            if (field_desc.decode_dictionary_indices && field_desc.field.dictionary() != nullptr)
            {
                auto indices = deserialize_dictionary_indices(
                    context,
                    field_desc
                );


                if (field_desc.dictionaries == nullptr)
                {
                    return indices;
                }

                return apply_dictionary_encoding(std::move(indices), field_desc.field, *(field_desc.dictionaries));
            }

            initialize_deserializer_map();
            auto it = m_deserializer_map.find(field_desc.field.type_type());
            if (it == m_deserializer_map.end())
            {
                throw std::runtime_error(
                    "Unsupported field type: " + std::to_string(static_cast<int>(field_desc.field.type_type()))
                    + " for field '" + field_desc.name + "'"
                );
            }

            return it->second(context, field_desc);
        };

        return deserialize_impl();
    }

    sparrow::array array_deserializer::deserialize_int(deserialization_context& context, const field_descriptor& field_desc)
    {
        const auto* int_type = field_desc.field.type_as_Int();
        const auto bit_width = int_type->bitWidth();
        const bool is_signed = int_type->is_signed();

        if (is_signed)
        {
            switch (bit_width)
            {
                case BIT_WIDTH_8:  return deserialize_primitive<int8_t>(context, field_desc);
                case BIT_WIDTH_16: return deserialize_primitive<int16_t>(context, field_desc);
                case BIT_WIDTH_32: return deserialize_primitive<int32_t>(context, field_desc);
                case BIT_WIDTH_64: return deserialize_primitive<int64_t>(context, field_desc);
                default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
            }
        }
        else
        {
            switch (bit_width)
            {
                case BIT_WIDTH_8: return deserialize_primitive<uint8_t>(context, field_desc);
                case BIT_WIDTH_16: return deserialize_primitive<uint16_t>(context, field_desc);
                case BIT_WIDTH_32: return deserialize_primitive<uint32_t>(context, field_desc);
                case BIT_WIDTH_64: return deserialize_primitive<uint64_t>(context, field_desc);
                default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
            }
        }
    }

    sparrow::array array_deserializer::deserialize_float(deserialization_context& context, const field_descriptor& field_desc)
    {
        const auto* float_type = field_desc.field.type_as_FloatingPoint();
        const auto precision = float_type->precision();
        switch (precision)
        {
            case org::apache::arrow::flatbuf::Precision::HALF: return deserialize_primitive<sparrow::float16_t>(context, field_desc);
            case org::apache::arrow::flatbuf::Precision::SINGLE: return deserialize_primitive<float>(context, field_desc);
            case org::apache::arrow::flatbuf::Precision::DOUBLE: return deserialize_primitive<double>(context, field_desc);
            default: throw std::runtime_error("Unsupported floating point precision: " + std::to_string(static_cast<int>(precision)));
        }
    }

    sparrow::array array_deserializer::deserialize_fixed_size_binary(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this fixed-size binary array
        const auto* fixed_size_binary_field = field_desc.field.type_as_FixedSizeBinary();
        return sparrow::array(deserialize_fixed_width_binary_array(
            context, field_desc, fixed_size_binary_field->byteWidth()
        ));
    }

    sparrow::array array_deserializer::deserialize_decimal(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this decimal array
        const auto* decimal_field = field_desc.field.type_as_Decimal();
        const auto scale = decimal_field->scale();
        const auto precision = decimal_field->precision();
        const auto bit_width = decimal_field->bitWidth();
        switch (bit_width)
        {
            case 32:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<int32_t>>(
                    context, field_desc, scale, precision
                ));
            case 64:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<int64_t>>(
                    context, field_desc, scale, precision
                ));
            case 128:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<sparrow::int128_t>>(
                    context, field_desc, scale, precision
                ));
            case 256:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<sparrow::int256_t>>(
                    context, field_desc, scale, precision
                ));
            default:
                throw std::runtime_error("Unsupported decimal bit width: " + std::to_string(bit_width));
        }
    }

    sparrow::array array_deserializer::deserialize_null(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this null array
        return sparrow::array(deserialize_null_array(
            context, field_desc
        ));
    }

    sparrow::array array_deserializer::deserialize_date(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this date array
        const auto date_type = field_desc.field.type_as_Date();
        const auto date_unit = date_type->unit();
        switch (date_unit)
        {
            case org::apache::arrow::flatbuf::DateUnit::DAY: return sparrow::array(deserialize_date_array<sparrow::date_days>(context, field_desc));
            case org::apache::arrow::flatbuf::DateUnit::MILLISECOND: return sparrow::array(deserialize_date_array<sparrow::date_milliseconds>(context, field_desc));
            default: throw std::runtime_error("Unsupported date unit: " + std::to_string(static_cast<int>(date_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_interval(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this interval array
        const auto* interval_type = field_desc.field.type_as_Interval();
        const org::apache::arrow::flatbuf::IntervalUnit interval_unit = interval_type->unit();
        switch (interval_unit)
        {
            case org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH: return sparrow::array(deserialize_interval_array<sparrow::chrono::months>(context, field_desc));
            case org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME: return sparrow::array(deserialize_interval_array<sparrow::days_time_interval>(context, field_desc));
            case org::apache::arrow::flatbuf::IntervalUnit::MONTH_DAY_NANO: return sparrow::array(deserialize_interval_array<sparrow::month_day_nanoseconds_interval>(context, field_desc));
            default: throw std::runtime_error("Unsupported interval unit: " + std::to_string(static_cast<int>(interval_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_duration(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this duration array
        const auto* duration_type = field_desc.field.type_as_Duration();
        const org::apache::arrow::flatbuf::TimeUnit time_unit = duration_type->unit();
        switch (time_unit)
        {
            case org::apache::arrow::flatbuf::TimeUnit::SECOND: return sparrow::array(deserialize_duration_array<std::chrono::seconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND: return sparrow::array(deserialize_duration_array<std::chrono::milliseconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND: return sparrow::array(deserialize_duration_array<std::chrono::microseconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND: return sparrow::array(deserialize_duration_array<std::chrono::nanoseconds>(context, field_desc));
            default: throw std::runtime_error("Unsupported duration time unit: " + std::to_string(static_cast<int>(time_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_time(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this time array
        const auto time_type = field_desc.field.type_as_Time();
        const auto time_unit = time_type->unit();
        switch (time_unit)
        {
            case org::apache::arrow::flatbuf::TimeUnit::SECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_seconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_milliseconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_microseconds>(context, field_desc));
            case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_nanoseconds>(context, field_desc));
            default: throw std::runtime_error("Unsupported time unit: " + std::to_string(static_cast<int>(time_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_timestamp(deserialization_context& context, const field_descriptor& field_desc)
    {
        ++context.node_index;  // Consume one FieldNode for this timestamp array
        const auto timestamp_type = field_desc.field.type_as_Timestamp();
        const auto time_unit = timestamp_type->unit();
        const bool has_timezone = timestamp_type->timezone() != nullptr;

        if (has_timezone)
        {
            const std::string timezone = timestamp_type->timezone()->str();
            switch (time_unit)
            {
                case org::apache::arrow::flatbuf::TimeUnit::SECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_second>(context, field_desc, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_millisecond>(context, field_desc, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_microsecond>(context, field_desc, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_nanosecond>(context, field_desc, timezone));
                default:
                    throw std::runtime_error("Unsupported timestamp unit: " + std::to_string(static_cast<int>(time_unit)));
            }
        }
        else
        {
            switch (time_unit)
            {
                case org::apache::arrow::flatbuf::TimeUnit::SECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_seconds>(context, field_desc));
                case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_milliseconds>(context, field_desc));
                case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_microseconds>(context, field_desc));
                case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_nanoseconds>(context, field_desc));
                default:
                    throw std::runtime_error("Unsupported timestamp unit: " + std::to_string(static_cast<int>(time_unit)));
            }
        }
    }

    // Implementation of list related arrays (present here because of recursive call to
    // array_deserializer::deserialize)
    namespace
    {
        [[nodiscard]] sparrow::fixed_sized_list_array deserialize_fixed_size_list_array(
            deserialization_context& context, const field_descriptor& field_desc
        )
        {
            ++context.node_index;  // Consume one FieldNode for this fixed-size list array
            std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
            if (field_desc.nullable)
            {
                flags = {sparrow::ArrowFlag::NULLABLE};
            }

            const auto compression = context.record_batch.compression();
            std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;

            {
                std::span<const uint8_t> validity_buffer_span = utils::get_buffer(context.record_batch, context.body, context.buffer_index);

                if (compression)
                {
                    buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
                }
                else
                {
                    buffers.push_back(std::move(validity_buffer_span));
                }
            }

            const auto* child_field = field_desc.field.children()->Get(0);
            if (!child_field)
            {
                throw std::runtime_error("FixedSizeList array field has no child field.");
            }

            std::optional<std::vector<sparrow::metadata_pair>> child_metadata;
            if (child_field->custom_metadata())
            {
                child_metadata = to_sparrow_metadata(*child_field->custom_metadata());
            }

            const auto* fixed_size_list_type = field_desc.field.type_as_FixedSizeList();
            const int32_t list_size = fixed_size_list_type->listSize();
            const int64_t child_length = field_desc.length * list_size;

            deserialization_context child_context(
                context.record_batch,
                context.body,
                context.buffer_index,
                context.node_index,
                context.variadic_counts_idx
            );

            field_descriptor child_descriptor(
                child_length,
                child_field->name()->str(),
                std::move(child_metadata),
                child_field->nullable(),
                true,
                *child_field,
                field_desc.dictionaries
            );

            sparrow::array child_array = array_deserializer::deserialize(child_context, child_descriptor);

            const std::string format = "+w:" + std::to_string(list_size);
            const auto null_count = std::visit(
                [length = field_desc.length](const auto& arg) {
                    std::span<const uint8_t> span(arg.data(), arg.size());
                    return utils::get_bitmap_pointer_and_null_count(span, length).second;
                },
                buffers[0]
            );

            auto [child_arrow_array, child_arrow_schema] = sparrow::extract_arrow_structures(std::move(child_array));

            auto** schema_children = new ArrowSchema*[1];
            schema_children[0] = new ArrowSchema(std::move(child_arrow_schema));
            ArrowSchema schema = make_non_owning_arrow_schema(
                format,
                field_desc.name,
                field_desc.metadata,
                flags,
                1,
                schema_children,
                nullptr
            );

            auto** array_children = new ArrowArray*[1];
            array_children[0] = new ArrowArray(std::move(child_arrow_array));
            ArrowArray array = make_arrow_array<arrow_array_private_data>(
                field_desc.length,
                null_count,
                0,
                1,
                array_children,
                nullptr,
                std::move(buffers)
            );

            sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
            return sparrow::fixed_sized_list_array{std::move(ap)};
        }

        [[nodiscard]] sparrow::struct_array deserialize_struct_array(
            deserialization_context& context,
            const field_descriptor& field_desc
        )
        {
            ++context.node_index;  // Consume one FieldNode for this struct array
            std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
            if (field_desc.nullable)
            {
                flags = {sparrow::ArrowFlag::NULLABLE};
            }

            const auto compression = context.record_batch.compression();
            std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;

            {
                std::span<const uint8_t> validity_buffer_span = utils::get_buffer(context.record_batch, context.body, context.buffer_index);
                if (compression)
                {
                    buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
                }
                else
                {
                    buffers.push_back(std::move(validity_buffer_span));
                }
            }

            std::vector<sparrow::array> child_arrays;
            for (const auto* child_field : *field_desc.field.children())
            {
                if (!child_field)
                {
                    throw std::runtime_error("Struct array has a null child field.");
                }

                std::optional<std::vector<sparrow::metadata_pair>> child_metadata;
                if (child_field->custom_metadata())
                {
                    child_metadata = to_sparrow_metadata(*child_field->custom_metadata());
                }

                deserialization_context child_context(
                    context.record_batch,
                    context.body,
                    context.buffer_index,
                    context.node_index,
                    context.variadic_counts_idx
                );

                field_descriptor child_descriptor(
                    field_desc.length,
                    child_field->name()->str(),
                    std::move(child_metadata),
                    child_field->nullable(),
                    true,
                    *child_field,
                    field_desc.dictionaries
                );

                child_arrays.push_back(array_deserializer::deserialize(
                    child_context,
                    child_descriptor
                ));
            }

            const std::string_view format = sparrow::data_type_to_format(
                sparrow::detail::get_data_type_from_array<sparrow::struct_array>::get()
            );

            const size_t n_child_arrays = child_arrays.size();

            auto** schema_children = new ArrowSchema*[n_child_arrays];
            auto** array_children = new ArrowArray*[n_child_arrays];

            for (size_t i = 0; i < n_child_arrays; ++i)
            {
                auto [arr, schema] = sparrow::extract_arrow_structures(std::move(child_arrays[i]));

                schema_children[i] = new ArrowSchema(std::move(schema));
                array_children[i] = new ArrowArray(std::move(arr));
            }

            ArrowSchema schema = make_non_owning_arrow_schema(
                format,
                field_desc.name,
                field_desc.metadata,
                flags,
                n_child_arrays,
                schema_children,
                nullptr
            );

            const auto null_count = std::visit(
                [length = field_desc.length](const auto& arg) {
                    std::span<const uint8_t> span(arg.data(), arg.size());
                    return utils::get_bitmap_pointer_and_null_count(span, length).second;
                },
                buffers[0]
            );

            ArrowArray array = make_arrow_array<arrow_array_private_data>(
                field_desc.length,
                null_count,
                0,
                n_child_arrays,
                array_children,
                nullptr,
                std::move(buffers)
            );

            sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
            return sparrow::struct_array{std::move(ap)};
        }
    }

    sparrow::array array_deserializer::deserialize_fixed_size_list(
        deserialization_context& context, const field_descriptor& field_desc
    )
    {
        return sparrow::array(deserialize_fixed_size_list_array(context, field_desc));
    }

    sparrow::array array_deserializer::deserialize_struct(
        deserialization_context& context, const field_descriptor& field_desc
    )
    {
        return sparrow::array(deserialize_struct_array(context, field_desc));
    }

    sparrow::array array_deserializer::deserialize_map(
        deserialization_context& context, const field_descriptor& field_desc)
    {
        // TODO handle the keyssorted in flags (needs a custom test) when true
        return sparrow::array(array_deserializer::deserialize_list_array<sparrow::map_array>(context, field_desc));
    }

    sparrow::array array_deserializer::deserialize_run_end_encoded(
        deserialization_context& context, const field_descriptor& field_desc
    )
    {
        return sparrow::array(deserialize_run_end_encoded_array(context, field_desc));
    }
}
