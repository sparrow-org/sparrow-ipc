#pragma once

#include <sparrow/run_end_encoded_array.hpp>

namespace sparrow_ipc
{
    struct deserialization_context;
    struct field_descriptor;

    /**
     * @brief Deserialize a run-end encoded array from IPC format.
     *
     * Run-end encoded arrays compress data by storing run end positions and corresponding values.
     * The array has two child arrays:
     * 1. Run ends (acc_lengths): positions where runs end (cumulative)
     * 2. Encoded values: the actual values for each run
     *
     * The run-end encoded array has no buffers at the parent level - only two child arrays.
     * The children should have the same encoded length (number of runs).
     *
     * @param context The deserialization context.
     * @param field_desc The field descriptor.
     *
     * @return The deserialized run-end encoded array
     */
    [[nodiscard]] sparrow::run_end_encoded_array deserialize_run_end_encoded_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    );
}
