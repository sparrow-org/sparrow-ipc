#pragma once

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/fixed_width_binary_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    [[nodiscard]] sparrow::fixed_width_binary_array deserialize_fixed_width_binary_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        int32_t byte_width
    );
}
