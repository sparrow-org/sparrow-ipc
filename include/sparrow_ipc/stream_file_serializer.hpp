#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/dictionary_iteration.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/serializer_reserve.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Represents a block entry in the Arrow IPC file footer.
     *
     * Each block describes the location and size of a record batch in the file.
     */
    struct record_batch_block
    {
        int64_t offset;           ///< Offset from the start of the file to the record batch message
        int32_t metadata_length;  ///< Length of the metadata (FlatBuffer message)
        int64_t body_length;      ///< Length of the record batch body (data buffers)
    };

    /**
     * @brief Writes the Arrow IPC file footer.
     *
     * @param record_batch A record batch containing the schema for the footer
     * @param record_batch_blocks Vector of block information for each record batch
     * @param stream The output stream to write the footer to
     * @return The size of the footer in bytes
     */
    SPARROW_IPC_API size_t write_footer(
        const sparrow::record_batch& record_batch,
        const std::vector<record_batch_block>& dictionary_blocks,
        const std::vector<record_batch_block>& record_batch_blocks,
        any_output_stream& stream
    );

    /**
     * @brief A class for serializing Apache Arrow record batches to the IPC file format.
     *
     * The stream_file_serializer class provides functionality to serialize single or multiple
     * record batches into the Arrow IPC file format suitable for storage. It ensures schema
     * consistency across multiple record batches and optimizes memory allocation by
     * pre-calculating required buffer sizes.
     *
     * @details The stream_file_serializer follows the Arrow IPC file format specification:
     * - File header magic bytes (ARROW1 + padding)
     * - Stream format data (schema + record batches + end-of-stream marker)
     * - Footer (FlatBuffer containing schema and empty record batch blocks)
     * - Footer size (int32)
     * - Trailing magic bytes (ARROW1)
     *
     * The class validates that all record batches have consistent schemas and throws
     * std::invalid_argument if inconsistencies are detected.
     *
     * @note Unlike the stream serializer, the file serializer automatically writes the
     *       complete file format (including header and footer) when end() is called or
     *       when the destructor is invoked.
     */
    class SPARROW_IPC_API stream_file_serializer
    {
    public:

        /**
         * @brief Constructs a stream_file_serializer object with a reference to a stream.
         *
         * @tparam TStream The type of the stream to be used for serialization.
         * @param stream Reference to the stream object that will be used for serialization operations.
         *               The serializer stores a pointer to this stream for later use.
         * @param compression Optional compression type to apply to record batch bodies.
         */
        template <writable_stream TStream>
        stream_file_serializer(TStream& stream, std::optional<CompressionType> compression = std::nullopt)
            : m_stream(stream)
            , m_compression(compression)
        {
        }

        /**
         * @brief Constructs a stream_file_serializer object with a reference to a stream and a schema.
         *
         * This constructor allows establishing the schema for the file immediately, which is
         * useful when the number of record batches is zero or when the schema is known upfront.
         *
         * @tparam TStream The type of the stream to be used for serialization.
         * @param stream Reference to the stream object that will be used for serialization operations.
         * @param schema_batch A record batch containing the schema for the file. The data in this
         *                     batch is NOT written to the file; only its schema is used.
         * @param compression Optional compression type to apply to record batch bodies.
         */
        template <writable_stream TStream>
        stream_file_serializer(
            TStream& stream,
            const sparrow::record_batch& schema_batch,
            std::optional<CompressionType> compression = std::nullopt
        )
            : m_stream(stream)
            , m_compression(compression)
        {
            // Write file header magic
            m_stream.write(arrow_file_header_magic);
            m_stream.add_padding();
            m_header_written = true;

            // Establish schema
            m_schema_received = true;
            m_first_record_batch = schema_batch;
            m_dtypes = get_column_dtypes(schema_batch);
            serialize_schema_message(schema_batch, m_stream);
        }

        /**
         * @brief Destructor for the stream_file_serializer.
         *
         * Ensures proper cleanup by calling end() if the serializer has not been
         * explicitly ended. This guarantees that the complete file format (including
         * footer and trailing magic bytes) is written before the object is destroyed.
         */
        ~stream_file_serializer();

        /**
         * @brief Writes a single record batch to the file.
         *
         * @param rb The record batch to write to the file
         * @throws std::runtime_error if the serializer has been ended
         * @throws std::invalid_argument if the record batch schema doesn't match the established schema
         */
        void write(const sparrow::record_batch& rb);

        /**
         * @brief Writes a collection of record batches to the file.
         *
         * This method efficiently adds multiple record batches to the serialization stream
         * by first calculating the total required size and reserving memory space to minimize
         * reallocations during the append operations.
         *
         * @tparam R The type of the record batch collection (must be iterable)
         * @param record_batches A collection of record batches to append to the file
         * @throws std::runtime_error if the serializer has been ended
         * @throws std::invalid_argument if any record batch schema doesn't match
         *
         * The method performs the following operations:
         * 1. Writes file header magic bytes (if first write)
         * 2. Calculates the total size needed for all record batches
         * 3. Reserves the required memory space in the stream
         * 4. Writes schema message (if first write)
         * 5. Iterates through each record batch and writes it to the stream
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        void write(const R& record_batches)
        {
            CompressionCache compressed_buffers_cache;
            if (std::ranges::empty(record_batches))
            {
                return;
            }

            if (m_ended)
            {
                throw std::runtime_error("Cannot write to a file serializer that has been ended");
            }

            // Write file header magic on first write
            if (!m_header_written)
            {
                m_stream.write(arrow_file_header_magic);
                m_stream.add_padding();
                m_header_written = true;
            }

            // NOTE `reserve_function` is making us store a cache for the compressed buffers at this level.
            // The benefit of capacity allocation should be evaluated vs storing a cache of compressed buffers
            // of record batches.
            const auto reserve_function = [&record_batches, &compressed_buffers_cache, this]()
            {
                return calculate_serializer_reserve_size(
                    record_batches,
                    m_stream.size(),
                    m_schema_received,
                    m_compression,
                    m_dict_tracker,
                    compressed_buffers_cache
                );
            };

            m_stream.reserve(reserve_function);

            if (!m_schema_received)
            {
                m_schema_received = true;
                m_first_record_batch = *record_batches.begin();
                m_dtypes = get_column_dtypes(*record_batches.begin());
                serialize_schema_message(*record_batches.begin(), m_stream);
            }

            for (const auto& rb : record_batches)
            {
                if (get_column_dtypes(rb) != m_dtypes)
                {
                    throw std::invalid_argument("Record batch schema does not match file serializer schema");
                }

                for_each_pending_dictionary(rb, m_dict_tracker, [&](const dictionary_info& dict_info)
                {
                    if (m_dict_tracker.is_emitted(dict_info.id) && !dict_info.is_delta)
                    {
                        throw std::runtime_error(
                            "Arrow file format does not support multiple non-delta dictionary batches "
                            "for the same dictionary id"
                        );
                    }

                    const int64_t dict_offset = static_cast<int64_t>(m_stream.size());
                    const auto dict_block_info = serialize_dictionary_batch(
                        dict_info.id,
                        dict_info.data,
                        dict_info.is_delta,
                        m_stream,
                        m_compression,
                        compressed_buffers_cache
                    );
                    m_dictionary_blocks.emplace_back(
                        dict_offset,
                        dict_block_info.metadata_length,
                        dict_block_info.body_length
                    );
                });

                // Offset is from the start of the file to the record batch message
                const int64_t offset = static_cast<int64_t>(m_stream.size());

                // Serialize and get block info
                const auto info = serialize_record_batch(rb, m_stream, m_compression, compressed_buffers_cache);

                m_record_batch_blocks.emplace_back(offset, info.metadata_length, info.body_length);
            }
        }

        /**
         * @brief Appends a record batch using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * record batches to the file serializer. It delegates to the write() method
         * and returns a reference to the serializer to enable method chaining.
         *
         * @param rb The record batch to append to the file
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if the record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * stream_file_serializer ser(stream);
         * ser << batch1 << batch2 << batch3 << end_file;
         */
        stream_file_serializer& operator<<(const sparrow::record_batch& rb)
        {
            write(rb);
            return *this;
        }

        /**
         * @brief Appends a range of record batches using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * multiple record batches to the file serializer at once. It delegates to the
         * write() method and returns a reference to the serializer to enable method chaining.
         *
         * @tparam R The type of the record batch collection (must be an input range)
         * @param record_batches A range of record batches to append to the file
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if any record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * stream_file_serializer ser(stream);
         * std::vector<sparrow::record_batch> batches = {batch1, batch2, batch3};
         * ser << batches << another_batch << end_file;
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        stream_file_serializer& operator<<(const R& record_batches)
        {
            write(record_batches);
            return *this;
        }

        /**
         * @brief Stream manipulator operator for functions like end_file.
         *
         * This operator enables the use of manipulator functions (similar to std::endl)
         * with the file serializer. It accepts a function pointer that takes and returns
         * a reference to a stream_file_serializer.
         *
         * @param manip A function pointer to a manipulator function
         * @return A reference to this serializer for method chaining
         *
         * @example
         * stream_file_serializer ser(stream);
         * ser << batch1 << batch2 << end_file;
         */
        stream_file_serializer& operator<<(stream_file_serializer& (*manip)(stream_file_serializer&) )
        {
            return manip(*this);
        }

        /**
         * @brief Finalizes the file serialization by writing footer and trailing magic bytes.
         *
         * This method completes the Arrow IPC file format by:
         * 1. Writing the end-of-stream marker
         * 2. Writing the footer (FlatBuffer containing schema)
         * 3. Writing the footer size (int32)
         * 4. Writing the trailing magic bytes (ARROW1)
         *
         * It can be called multiple times safely as it tracks whether the file has
         * already been ended to prevent duplicate operations.
         *
         * @note This method is idempotent - calling it multiple times has no additional effect.
         * @post After calling this method, m_ended will be set to true.
         * @throws std::runtime_error if no record batches have been written
         */
        void end();

        bool m_header_written{false};
        bool m_schema_received{false};
        std::optional<sparrow::record_batch> m_first_record_batch;
        std::vector<sparrow::data_type> m_dtypes;
        any_output_stream m_stream;
        bool m_ended{false};
        std::optional<CompressionType> m_compression;
        dictionary_tracker m_dict_tracker;
        std::vector<record_batch_block> m_dictionary_blocks;
        std::vector<record_batch_block> m_record_batch_blocks;
    };

    /**
     * @brief Stream manipulator to finalize the Arrow IPC file format.
     *
     * This manipulator can be used with the << operator to end the file serialization
     * and write the complete file format footer.
     *
     * @param serializer The file serializer to finalize
     * @return Reference to the serializer for method chaining
     *
     * @example
     * stream_file_serializer ser(stream);
     * ser << batch1 << batch2 << end_file;
     */
    inline stream_file_serializer& end_file(stream_file_serializer& serializer)
    {
        serializer.end();
        return serializer;
    }
}
