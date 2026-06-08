#pragma once

#include <vector>

#include <flatbuffers/flatbuffers.h>

#include <sparrow/utils/metadata.hpp>

#include "sparrow_ipc/flatbuffers_generated/Schema_generated.h"

namespace sparrow_ipc
{
    /**
     * @brief Converts FlatBuffers metadata to Sparrow metadata format.
     *
     * This function takes a FlatBuffers vector containing key-value pairs from Apache Arrow
     * format and converts them into a vector of Sparrow metadata pairs. Each key-value pair
     * from the FlatBuffers structure is extracted and stored as a sparrow::metadata_pair.
     *
     * @param metadata A FlatBuffers vector containing KeyValue pairs from Apache Arrow format
     * @return std::vector<sparrow::metadata_pair> A vector of Sparrow metadata pairs containing
     *         the converted key-value data
     *
     * @note The function reserves space in the output vector to match the input size for
     *       optimal memory allocation performance.
     */
    std::vector<sparrow::metadata_pair> to_sparrow_metadata(
        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>& metadata
    );
}
