#pragma once

#include <cstdint>
#include <span>
#include <utility>
#include <variant>

#include <sparrow/buffer/buffer.hpp>

#include "Message_generated.h"

namespace sparrow_ipc::utils
{
    /**
     * @brief Extracts bitmap pointer and null count from a validity buffer span.
     *
     * This function calculates the number of null values represented by the bitmap.
     *
     * @param validity_buffer_span The validity buffer as a byte span.
     * @param length The Arrow RecordBatch length (number of values in the array).
     *
     * @return A pair containing:
     *         - First: Pointer to the bitmap data (nullptr if buffer is empty)
     *         - Second: Count of null values in the bitmap (0 if buffer is empty)
     *
     * @note If the bitmap buffer is empty, returns {nullptr, 0}
     * @note The returned pointer is a non-const cast of the original const data
     */
    [[nodiscard]] std::pair<std::uint8_t*, int64_t>
    get_bitmap_pointer_and_null_count(std::span<const uint8_t> validity_buffer_span, const int64_t length);

    /**
     * @brief Extracts a buffer from a RecordBatch's body.
     *
     * This function retrieves a buffer span from the specified index in the RecordBatch's
     * buffer list and increments the index.
     *
     * @param record_batch The Arrow RecordBatch containing buffer metadata.
     * @param body The raw buffer data as a byte span.
     * @param buffer_index The index of the buffer to retrieve. This value is incremented by the function.
     *
     * @return A `std::span<const uint8_t>` viewing the extracted buffer data.
     * @throws std::runtime_error if the buffer metadata indicates a buffer that exceeds the body size.
     */
    [[nodiscard]] std::span<const uint8_t> get_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index
    );

    /**
     * @brief Retrieves a decompressed buffer or a view of the original buffer.
     *
     * This function either decompresses the provided buffer span, if compression is specified,
     * or returns a view of the original buffer without modification.
     *
     * @param buffer_span A span of raw buffer data to be decompressed, or returned as-is if no decompression
     * is needed.
     * @param compression The compression algorithm to use. If nullptr, no decompression is performed.
     *
     * @return A `std::variant` containing either:
     *         - A `sparrow::buffer<std::uint8_t>` with the decompressed data, or
     *         - A `std::span<const std::uint8_t>` providing a view of the original `buffer_span` if no
     * decompression occurred.
     */
    [[nodiscard]] std::variant<sparrow::buffer<std::uint8_t>, std::span<const std::uint8_t>>
    get_decompressed_buffer(
        std::span<const uint8_t> buffer_span,
        const org::apache::arrow::flatbuf::BodyCompression* compression
    );
}
