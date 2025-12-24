#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/date_array.hpp>
#include <sparrow/time_array.hpp>
#include <sparrow/timestamp_array.hpp>
#include <sparrow/timestamp_without_timezone_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/deserialize_array_impl.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::date_array<T> deserialize_date_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_simple_array<sparrow::date_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index,
            std::nullopt
        );
    }

    template <typename T>
    [[nodiscard]] sparrow::timestamp_array<T> deserialize_timestamp_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        const std::string& timezone
    )
    {
        std::string format = std::string(data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::timestamp_array<T>>::get()
        )) + timezone;

        return detail::deserialize_simple_array<sparrow::timestamp_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index,
            std::move(format)
        );
    }

    template <typename T>
    [[nodiscard]] sparrow::timestamp_without_timezone_array<T> deserialize_timestamp_without_timezone_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_simple_array<sparrow::timestamp_without_timezone_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index,
            std::nullopt
        );
    }

    template <typename T>
    [[nodiscard]] sparrow::time_array<T> deserialize_time_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_simple_array<sparrow::time_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index,
            std::nullopt
        );
    }
}
