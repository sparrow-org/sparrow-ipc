#pragma once

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/interval_array.hpp>

#include "sparrow_ipc/deserialization_context.hpp"
#include "sparrow_ipc/deserialize_array_impl.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] sparrow::interval_array<T> deserialize_interval_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    )
    {
        return detail::deserialize_simple_array<sparrow::interval_array, T>(
            context,
            field_desc
        );
    }
}
