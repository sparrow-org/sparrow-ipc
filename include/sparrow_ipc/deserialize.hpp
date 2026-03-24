#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Result of stream deserialization containing optional schema and record batches.
     */
    struct record_batch_stream
    {
        std::optional<sparrow::record_batch> schema;
        std::vector<sparrow::record_batch> batches;
    };

    /**
     * @brief Deserializes an Arrow IPC stream into a record_batch_stream.
     *
     * This function returns both the schema (as an optional record batch) and the
     * record batches found in the stream.
     *
     * @param data A span of bytes containing the serialized Arrow IPC stream data
     * @return record_batch_stream Containing schema and batches
     */
    [[nodiscard]] SPARROW_IPC_API record_batch_stream
    deserialize_stream_to_record_batches(std::span<const uint8_t> data);

    /**
     * @brief Deserializes an Arrow IPC stream from binary data into a vector of record batches.
    ...
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

    /**
     * @brief Deserializes Arrow IPC file format into a record_batch_stream.
     *
     * Reads an Arrow IPC file format which consists of:
     * 1. Magic bytes "ARROW1" with padding (8 bytes)
     * 2. Stream format data (schema + record batches)
     * 3. Footer containing metadata
     * 4. Footer size (int32)
     * 5. Trailing magic bytes "ARROW1" (6 bytes)
     *
     * @param data A span of bytes containing the serialized Arrow IPC file data
     *
     * @return record_batch_stream Containing schema and batches
     *
     * @throws std::runtime_error If:
     *         - The file magic bytes are incorrect
     *         - The footer is missing or invalid
     *         - Record batch deserialization fails
     *
     * @note The function validates the file structure including magic bytes at both start and end
     */
    [[nodiscard]] SPARROW_IPC_API record_batch_stream
    deserialize_file(std::span<const uint8_t> data);
}
