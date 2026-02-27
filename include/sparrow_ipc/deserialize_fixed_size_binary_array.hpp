#pragma once

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/fixed_width_binary_array.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialization_context.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    [[nodiscard]] sparrow::fixed_width_binary_array deserialize_fixed_width_binary_array(
        deserialization_context& context,
        const field_descriptor& field_desc,
        int32_t byte_width
    );
}
