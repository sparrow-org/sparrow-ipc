#pragma once

#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a record batch schema into a binary message format.
     *
     * This function creates a serialized schema message by combining continuation bytes,
     * a length prefix, the flatbuffer schema data, and padding to ensure 8-byte alignment.
     * The resulting format follows the Arrow IPC specification for schema messages.
     *
     * @param record_batch The record batch containing the schema to be serialized
     * @param stream The output stream where the serialized schema message will be written
     */
    SPARROW_IPC_API void
    serialize_schema_message(const sparrow::record_batch& record_batch, any_output_stream& stream);
    
    /**
     * @brief Calculates the total serialized size of a schema message.
     *
     * This function computes the complete size that would be produced by serialize_schema_message(),
     * including:
     * - Continuation bytes (4 bytes)
     * - Message length prefix (4 bytes)
     * - FlatBuffer schema message data
     * - Padding to 8-byte alignment
     *
     * @param record_batch The record batch containing the schema to be measured
     * @return The total size in bytes that the serialized schema message would occupy
     */
    [[nodiscard]] SPARROW_IPC_API std::size_t
    calculate_schema_message_size(const sparrow::record_batch& record_batch);

    /**
     * @brief Calculates the total serialized size of a record batch message.
     *
     * This function computes the complete size that would be produced by serialize_record_batch(),
     * including:
     * - Continuation bytes (4 bytes)
     * - Message length prefix (4 bytes)
     * - FlatBuffer record batch metadata
     * - Padding to 8-byte alignment after metadata
     * - Body data with 8-byte alignment between buffers
     *
     * @param record_batch The record batch to be measured.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache to store and retrieve compressed buffer sizes, avoiding recompression.
     * If compression is given, cache should be set as well.
     * @return The total size in bytes that the serialized record batch would occupy.
     */
    [[nodiscard]] SPARROW_IPC_API std::size_t
    calculate_record_batch_message_size(const sparrow::record_batch& record_batch,
                                        std::optional<CompressionType> compression = std::nullopt,
                                        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt);

    /**
     * @brief Calculates the total serialized size of a dictionary batch message.
     *
     * This function computes the complete size that would be produced by
     * serialize_dictionary_batch(), including:
     * - Continuation bytes (4 bytes)
     * - Message length prefix (4 bytes)
     * - FlatBuffer dictionary batch metadata
     * - Padding to 8-byte alignment after metadata
     * - Body data with 8-byte alignment between buffers
     *
     * @param dictionary_id The unique identifier for the dictionary.
     * @param record_batch A single-column record batch containing dictionary values.
     * @param is_delta Whether this dictionary batch is a delta update.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache to store and retrieve compressed buffer sizes, avoiding recompression.
     * If compression is given, cache should be set as well.
     * @return The total size in bytes that the serialized dictionary batch would occupy.
     */
    [[nodiscard]] SPARROW_IPC_API std::size_t
    calculate_dictionary_batch_message_size(
        int64_t dictionary_id,
        const sparrow::record_batch& record_batch,
        bool is_delta,
        std::optional<CompressionType> compression = std::nullopt,
        std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt
    );

    /**
     * @brief Calculates the total serialized size for a collection of record batches.
     *
     * This function computes the complete size that would be produced by serializing
     * a schema message followed by all record batch messages in the collection.
     *
     * @tparam R Range type containing sparrow::record_batch objects.
     * @param record_batches Collection of record batches to be measured.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache to store and retrieve compressed buffer sizes, avoiding recompression.
     * If compression is given, cache should be set as well.
     * @return The total size in bytes for the complete serialized output.
     * @throws std::invalid_argument if record batches have inconsistent schemas.
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    [[nodiscard]] std::size_t calculate_total_serialized_size(const R& record_batches,
                                                              std::optional<CompressionType> compression = std::nullopt,
                                                              std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt)
    {
        if (record_batches.empty())
        {
            return 0;
        }

        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument("Record batches have inconsistent schemas");
        }

        // Calculate schema message size (only once)
        auto it = std::ranges::begin(record_batches);
        std::size_t total_size = calculate_schema_message_size(*it);

        // Calculate record batch message sizes
        for (const auto& record_batch : record_batches)
        {
            total_size += calculate_record_batch_message_size(record_batch, compression, cache);
        }

        return total_size;
    }

    /**
     * @brief Fills the body vector with serialized data from an arrow proxy and its children.
     *
     * This function recursively processes an arrow proxy by:
     * 1. Iterating through all buffers in the proxy and appending their data to the body vector
     * 2. Adding padding bytes (zeros) after each buffer to align data to 8-byte boundaries
     * 3. Recursively processing all child proxies in the same manner
     *
     * The function ensures proper memory alignment by padding each buffer's data to the next
     * 8-byte boundary, which is typically required for efficient memory access and Arrow
     * format compliance.
     *
     * @param arrow_proxy The arrow proxy containing buffers and potential child proxies to serialize.
     * @param stream The output stream where the serialized body data will be written.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache for compressed buffers to avoid recompression if compression is enabled.
     * If compression is given, cache should be set as well.
     * @throws std::invalid_argument if compression is given but not cache.
     */
    SPARROW_IPC_API void fill_body(const sparrow::arrow_proxy& arrow_proxy, any_output_stream& stream,
                                   std::optional<CompressionType> compression = std::nullopt,
                                   std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt);

    /**
     * @brief Generates a serialized body from a record batch.
     *
     * This function iterates through all columns in the provided record batch,
     * extracts their Arrow proxy representations, and serializes them into a
     * single byte vector that forms the body of the serialized data.
     *
     * @param record_batch The record batch containing columns to be serialized.
     * @param stream The output stream where the serialized body will be written.
     * @param compression Optional: The compression type to use when serializing.
     * @param cache Optional: A cache for compressed buffers to avoid recompression if compression is enabled.
     * If compression is given, cache should be set as well.
     */
    SPARROW_IPC_API void generate_body(const sparrow::record_batch& record_batch, any_output_stream& stream,
                                       std::optional<CompressionType> compression = std::nullopt,
                                       std::optional<std::reference_wrapper<CompressionCache>> cache = std::nullopt);

    SPARROW_IPC_API std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb);
}
