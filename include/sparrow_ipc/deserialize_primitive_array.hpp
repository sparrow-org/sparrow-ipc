#pragma once

#include <optional>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/primitive_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/deserialize_array_impl.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::primitive_array<T> deserialize_primitive_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    )
    {
        return detail::deserialize_simple_array<sparrow::primitive_array, T>(
            record_batch,
            body,
            length,
            name,
            metadata,
            nullable,
            buffer_index
        );
    }
}
