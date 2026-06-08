#pragma once

#include <sparrow/array.hpp>

namespace sparrow_ipc
{
    struct deserialization_context;
    struct field_descriptor;

    /**
     * @brief Deserialize a union array from IPC format.
     *
     * Union arrays represent a nested type where each value can be of different types.
     * The array consists of:
     * - Type ID buffer: An array of 8-bit integers indicating the type of each value.
     * - Offsets buffer (dense union only): An array of 32-bit integers indicating the
     *   offset into the corresponding child array.
     * - Child arrays: One for each possible type in the union.
     *
     * There are two modes for union arrays:
     * 1. Sparse Union: Each child array has the same length as the union array.
     * 2. Dense Union: Each child array can have a different length, and the offsets
     *    buffer is used to locate the value within the child array.
     *
     * @param context The deserialization context.
     * @param field_desc The field descriptor.
     *
     * @return The deserialized union array
     */
    [[nodiscard]] sparrow::array deserialize_union_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    );
}
