#include "array_deserializer.hpp"

#include <stdexcept>

#include <sparrow/types/data_type.hpp>

#include "sparrow_ipc/deserialize_decimal_array.hpp"
#include "sparrow_ipc/deserialize_duration_array.hpp"
#include "sparrow_ipc/deserialize_fixed_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_interval_array.hpp"
#include "sparrow_ipc/deserialize_null_array.hpp"
#include "sparrow_ipc/deserialize_time_related_arrays.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // Integer bit width constants
        constexpr int32_t BIT_WIDTH_8 = 8;
        constexpr int32_t BIT_WIDTH_16 = 16;
        constexpr int32_t BIT_WIDTH_32 = 32;
        constexpr int32_t BIT_WIDTH_64 = 64;
    }

    array_deserializer::array_deserializer()
    {
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Bool] = &deserialize_primitive<bool>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Int] = &deserialize_int;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::FloatingPoint] = &deserialize_float;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::FixedSizeBinary] = &deserialize_fixed_size_binary;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Binary] = &deserialize_variable_size_binary<sparrow::binary_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeBinary] = &deserialize_variable_size_binary<sparrow::big_binary_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Utf8] = &deserialize_variable_size_binary<sparrow::string_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::LargeUtf8] = &deserialize_variable_size_binary<sparrow::big_string_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Interval] = &deserialize_interval;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Duration] = &deserialize_duration;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Timestamp] = &deserialize_timestamp;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Date] = &deserialize_date;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Time] = &deserialize_time;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Null] = &deserialize_null;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Decimal] = &deserialize_decimal;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::BinaryView] = &deserialize_variable_size_binary_view<sparrow::binary_view_array>;
        m_deserializer_map[org::apache::arrow::flatbuf::Type::Utf8View] = &deserialize_variable_size_binary_view<sparrow::string_view_array>;
    }

    sparrow::array array_deserializer::deserialize(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                  const std::span<const uint8_t>& body,
                                                  const std::string& name,
                                                  const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                  bool nullable,
                                                  size_t& buffer_index,
                                                  size_t& variadic_counts_idx,
                                                  const org::apache::arrow::flatbuf::Field& field) const
    {
        auto it = m_deserializer_map.find(field.type_type());
        if (it == m_deserializer_map.end())
        {
            throw std::runtime_error(
                "Unsupported field type: " + std::to_string(static_cast<int>(field.type_type()))
                + " for field '" + name + "'"
            );
        }
        return it->second(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
    }

    sparrow::array array_deserializer::deserialize_int(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                      const std::span<const uint8_t>& body,
                                                      const std::string& name,
                                                      const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                      bool nullable,
                                                      size_t& buffer_index,
                                                      size_t& variadic_counts_idx,
                                                      const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* int_type = field.type_as_Int();
        const auto bit_width = int_type->bitWidth();
        const bool is_signed = int_type->is_signed();

        if (is_signed)
        {
            switch (bit_width)
            {
                case BIT_WIDTH_8:  return deserialize_primitive<int8_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_16: return deserialize_primitive<int16_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_32: return deserialize_primitive<int32_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_64: return deserialize_primitive<int64_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
            }
        }
        else
        {
            switch (bit_width)
            {
                case BIT_WIDTH_8: return deserialize_primitive<uint8_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_16: return deserialize_primitive<uint16_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_32: return deserialize_primitive<uint32_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                case BIT_WIDTH_64: return deserialize_primitive<uint64_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
                default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
            }
        }
    }

    sparrow::array array_deserializer::deserialize_float(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                        const std::span<const uint8_t>& body,
                                                        const std::string& name,
                                                        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                        bool nullable,
                                                        size_t& buffer_index,
                                                        size_t& variadic_counts_idx,
                                                        const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* float_type = field.type_as_FloatingPoint();
        const auto precision = float_type->precision();
        switch (precision)
        {
            case org::apache::arrow::flatbuf::Precision::HALF: return deserialize_primitive<sparrow::float16_t>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
            case org::apache::arrow::flatbuf::Precision::SINGLE: return deserialize_primitive<float>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
            case org::apache::arrow::flatbuf::Precision::DOUBLE: return deserialize_primitive<double>(record_batch, body, name, metadata, nullable, buffer_index, variadic_counts_idx, field);
            default: throw std::runtime_error("Unsupported floating point precision: " + std::to_string(static_cast<int>(precision)));
        }
    }

    sparrow::array array_deserializer::deserialize_fixed_size_binary(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                 const std::span<const uint8_t>& body,
                                                 const std::string& name,
                                                 const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                 bool nullable,
                                                 size_t& buffer_index,
                                                 size_t&,
                                                 const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* fixed_size_binary_field = field.type_as_FixedSizeBinary();
        return sparrow::array(deserialize_fixed_width_binary_array(
            record_batch, body, name, metadata, nullable, buffer_index,
            fixed_size_binary_field->byteWidth()
        ));
    }

    sparrow::array array_deserializer::deserialize_decimal(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                          const std::span<const uint8_t>& body,
                                                          const std::string& name,
                                                          const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                          bool nullable,
                                                          size_t& buffer_index,
                                                          size_t&,
                                                          const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* decimal_field = field.type_as_Decimal();
        const auto scale = decimal_field->scale();
        const auto precision = decimal_field->precision();
        const auto bit_width = decimal_field->bitWidth();
        switch (bit_width)
        {
            case 32:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<int32_t>>(
                    record_batch, body, name, metadata, nullable, buffer_index, scale, precision
                ));
            case 64:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<int64_t>>(
                    record_batch, body, name, metadata, nullable, buffer_index, scale, precision
                ));
            case 128:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<sparrow::int128_t>>(
                    record_batch, body, name, metadata, nullable, buffer_index, scale, precision
                ));
            case 256:
                return sparrow::array(deserialize_decimal_array<sparrow::decimal<sparrow::int256_t>>(
                    record_batch, body, name, metadata, nullable, buffer_index, scale, precision
                ));
            default:
                throw std::runtime_error("Unsupported decimal bit width: " + std::to_string(bit_width));
        }
    }

    sparrow::array array_deserializer::deserialize_null(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                       const std::span<const uint8_t>& body,
                                                       const std::string& name,
                                                       const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                       bool nullable,
                                                       size_t& buffer_index,
                                                       size_t&,
                                                       const org::apache::arrow::flatbuf::Field&)
    {
        return sparrow::array(deserialize_null_array(
            record_batch, body, name, metadata, nullable, buffer_index
        ));
    }

    sparrow::array array_deserializer::deserialize_date(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                       const std::span<const uint8_t>& body,
                                                       const std::string& name,
                                                       const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                       bool nullable,
                                                       size_t& buffer_index,
                                                       size_t&,
                                                       const org::apache::arrow::flatbuf::Field& field)
    {
        const auto date_type = field.type_as_Date();
        const auto date_unit = date_type->unit();
        switch (date_unit)
        {
            case org::apache::arrow::flatbuf::DateUnit::DAY: return sparrow::array(deserialize_date_array<sparrow::date_days>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::DateUnit::MILLISECOND: return sparrow::array(deserialize_date_array<sparrow::date_milliseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            default: throw std::runtime_error("Unsupported date unit: " + std::to_string(static_cast<int>(date_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_interval(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                           const std::span<const uint8_t>& body,
                                                           const std::string& name,
                                                           const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                           bool nullable,
                                                           size_t& buffer_index,
                                                           size_t&,
                                                           const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* interval_type = field.type_as_Interval();
        const org::apache::arrow::flatbuf::IntervalUnit interval_unit = interval_type->unit();
        switch (interval_unit)
        {
            case org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH: return sparrow::array(deserialize_interval_array<sparrow::chrono::months>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME: return sparrow::array(deserialize_interval_array<sparrow::days_time_interval>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::IntervalUnit::MONTH_DAY_NANO: return sparrow::array(deserialize_interval_array<sparrow::month_day_nanoseconds_interval>(record_batch, body, name, metadata, nullable, buffer_index));
            default: throw std::runtime_error("Unsupported interval unit: " + std::to_string(static_cast<int>(interval_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_duration(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                           const std::span<const uint8_t>& body,
                                                           const std::string& name,
                                                           const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                           bool nullable,
                                                           size_t& buffer_index,
                                                           size_t&,
                                                           const org::apache::arrow::flatbuf::Field& field)
    {
        const auto* duration_type = field.type_as_Duration();
        const org::apache::arrow::flatbuf::TimeUnit time_unit = duration_type->unit();
        switch (time_unit)
        {
            case org::apache::arrow::flatbuf::TimeUnit::SECOND: return sparrow::array(deserialize_duration_array<std::chrono::seconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND: return sparrow::array(deserialize_duration_array<std::chrono::milliseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND: return sparrow::array(deserialize_duration_array<std::chrono::microseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND: return sparrow::array(deserialize_duration_array<std::chrono::nanoseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            default: throw std::runtime_error("Unsupported duration time unit: " + std::to_string(static_cast<int>(time_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_time(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                       const std::span<const uint8_t>& body,
                                                       const std::string& name,
                                                       const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                       bool nullable,
                                                       size_t& buffer_index,
                                                       size_t&,
                                                       const org::apache::arrow::flatbuf::Field& field)
    {
        const auto time_type = field.type_as_Time();
        const auto time_unit = time_type->unit();
        switch (time_unit)
        {
            case org::apache::arrow::flatbuf::TimeUnit::SECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_seconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_milliseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_microseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND: return sparrow::array(deserialize_time_array<sparrow::chrono::time_nanoseconds>(record_batch, body, name, metadata, nullable, buffer_index));
            default: throw std::runtime_error("Unsupported time unit: " + std::to_string(static_cast<int>(time_unit)));
        }
    }

    sparrow::array array_deserializer::deserialize_timestamp(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                            const std::span<const uint8_t>& body,
                                                            const std::string& name,
                                                            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                            bool nullable,
                                                            size_t& buffer_index,
                                                            size_t&,
                                                            const org::apache::arrow::flatbuf::Field& field)
    {
        const auto timestamp_type = field.type_as_Timestamp();
        const auto time_unit = timestamp_type->unit();
        const bool has_timezone = timestamp_type->timezone() != nullptr;

        if (has_timezone)
        {
            const std::string timezone = timestamp_type->timezone()->str();
            switch (time_unit)
            {
                case org::apache::arrow::flatbuf::TimeUnit::SECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_second>(record_batch, body, name, metadata, nullable, buffer_index, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_millisecond>(record_batch, body, name, metadata, nullable, buffer_index, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_microsecond>(record_batch, body, name, metadata, nullable, buffer_index, timezone));
                case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND:
                    return sparrow::array(deserialize_timestamp_array<sparrow::timestamp_nanosecond>(record_batch, body, name, metadata, nullable, buffer_index, timezone));
                default:
                    throw std::runtime_error("Unsupported timestamp unit: " + std::to_string(static_cast<int>(time_unit)));
            }
        }
        else
        {
            switch (time_unit)
            {
                case org::apache::arrow::flatbuf::TimeUnit::SECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_seconds>(record_batch, body, name, metadata, nullable, buffer_index));
                case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_milliseconds>(record_batch, body, name, metadata, nullable, buffer_index));
                case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_microseconds>(record_batch, body, name, metadata, nullable, buffer_index));
                case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND:
                    return sparrow::array(deserialize_timestamp_without_timezone_array<sparrow::zoned_time_without_timezone_nanoseconds>(record_batch, body, name, metadata, nullable, buffer_index));
                default:
                    throw std::runtime_error("Unsupported timestamp unit: " + std::to_string(static_cast<int>(time_unit)));
            }
        }
    }
}
