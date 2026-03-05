#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Deserializes an Arrow IPC stream from binary data into a vector of record batches.
     *
     * This function processes an Arrow IPC stream format, extracting schema information
     * and record batch data. It handles encapsulated messages sequentially, first expecting
     * a Schema message followed by one or more RecordBatch messages.
     *
     * @param data A span of bytes containing the serialized Arrow IPC stream data
     *
     * @return std::vector<sparrow::record_batch> A vector containing all deserialized record batches
     *
     * @throws std::runtime_error If:
     *         - A RecordBatch message is encountered before a Schema message
     *         - A RecordBatch message header is missing or invalid
     *         - Unsupported message types are encountered (Tensor, DictionaryBatch, SparseTensor)
     *         - An unknown message header type is encountered
     *
     * @note The function processes messages until an end-of-stream marker is detected
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<sparrow::record_batch>
    deserialize_stream(std::span<const uint8_t> data);
}
