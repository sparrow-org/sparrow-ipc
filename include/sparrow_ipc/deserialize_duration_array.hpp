#pragma once

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/duration_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/deserialize_array_impl.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::duration_array<T> deserialize_duration_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_simple_array<sparrow::duration_array, T>(
            record_batch,
            body,
            name,
            metadata,
            nullable,
            buffer_index
        );
    }
}
