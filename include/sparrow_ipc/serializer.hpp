#pragma once

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/dictionary_iteration.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/serializer_reserve.hpp"

namespace sparrow_ipc
{
    /**
     * @brief A class for serializing Apache Arrow record batches to an output stream.
     *
     * The serializer class provides functionality to serialize single or multiple record batches
     * into a binary format suitable for storage or transmission. It ensures schema consistency
     * across multiple record batches and optimizes memory allocation by pre-calculating required
     * buffer sizes.
     *
     * @details The serializer supports two main usage patterns:
     * 1. Construction with a collection of record batches for batch serialization
     * 2. Construction with a single record batch followed by incremental appends
     *
     * The class validates that all record batches have consistent schemas and throws
     * std::invalid_argument if inconsistencies are detected or if an empty collection
     * is provided.
     *
     * Memory efficiency is achieved through:
     * - Pre-calculation of total serialization size
     * - Stream reservation to minimize memory reallocations
     * - Lazy evaluation of size calculations using lambda functions
     */
    class SPARROW_IPC_API serializer
    {
    public:

        /**
         * @brief Constructs a serializer object with a reference to a stream.
         *
         * @tparam TStream The type of the stream to be used for serialization.
         * @param stream Reference to the stream object that will be used for serialization operations.
         *               The serializer stores a pointer to this stream for later use.
         * @param compression Optional: The compression type to use for record batch bodies.
         */
        template <writable_stream TStream>
        serializer(TStream& stream, std::optional<CompressionType> compression = std::nullopt)
            : m_stream(stream)
            , m_compression(compression)
        {
        }

        /**
         * @brief Constructs a serializer object with a reference to a stream and a schema.
         *
         * This constructor allows establishing the schema for the stream immediately, which is
         * useful when the number of record batches is zero or when the schema is known upfront.
         *
         * @tparam TStream The type of the stream to be used for serialization.
         * @param stream Reference to the stream object that will be used for serialization operations.
         * @param schema_batch A record batch containing the schema for the stream. The data in this
         *                     batch is NOT written to the stream; only its schema is used.
         * @param compression Optional: The compression type to use for record batch bodies.
         */
        template <writable_stream TStream>
        serializer(
            TStream& stream,
            const sparrow::record_batch& schema_batch,
            std::optional<CompressionType> compression = std::nullopt
        )
            : m_stream(stream)
            , m_compression(compression)
            , m_schema_received(true)
            , m_dtypes(get_column_dtypes(schema_batch))
        {
            serialize_schema_message(schema_batch, m_stream);
        }

        /**
         * @brief Destructor for the serializer.
         *
         * Ensures proper cleanup by calling end() if the serializer has not been
         * explicitly ended. This guarantees that any pending data is flushed and
         * resources are properly released before the object is destroyed.
         */
        ~serializer();

        /**
         * Writes a record batch to the serializer.
         *
         * @param rb The record batch to write to the serializer
         */
        void write(const sparrow::record_batch& rb);

        /**
         * @brief Writes a collection of record batches to the stream.
         *
         * This method efficiently adds multiple record batches to the serialization stream
         * by first calculating the total required size and reserving memory space to minimize
         * reallocations during the append operations.
         *
         * @tparam R The type of the record batch collection (must be iterable)
         * @param record_batches A collection of record batches to append to the stream
         *
         * The method performs the following operations:
         * 1. Calculates the total size needed for all record batches
         * 2. Reserves the required memory space in the stream
         * 3. Iterates through each record batch and adds it to the stream
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
                throw std::runtime_error("Cannot append to a serializer that has been ended");
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
                m_dtypes = get_column_dtypes(*record_batches.begin());
                serialize_schema_message(*record_batches.begin(), m_stream);
            }

            for (const auto& rb : record_batches)
            {
                if (get_column_dtypes(rb) != m_dtypes)
                {
                    throw std::invalid_argument("Record batch schema does not match serializer schema");
                }

                for_each_pending_dictionary(rb, m_dict_tracker, [&](const dictionary_info& dict_info)
                {
                    serialize_dictionary_batch(
                        dict_info.id,
                        dict_info.data,
                        dict_info.is_delta,
                        m_stream,
                        m_compression,
                        compressed_buffers_cache
                    );
                });

                serialize_record_batch(rb, m_stream, m_compression, compressed_buffers_cache);
            }
        }

        /**
         * @brief Appends a record batch using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * record batches to the serializer. It delegates to the append() method
         * and returns a reference to the serializer to enable method chaining.
         *
         * @param rb The record batch to append to the serializer
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if the record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * serializer ser(initial_batch, stream);
         * ser << batch1 << batch2 << batch3;
         */
        serializer& operator<<(const sparrow::record_batch& rb)
        {
            write(rb);
            return *this;
        }

        /**
         * @brief Appends a range of record batches using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * multiple record batches to the serializer at once. It delegates to the
         * append() method and returns a reference to the serializer to enable method chaining.
         *
         * @tparam R The type of the record batch collection (must be an input range)
         * @param record_batches A range of record batches to append to the serializer
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if any record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * serializer ser(initial_batch, stream);
         * std::vector<sparrow::record_batch> batches = {batch1, batch2, batch3};
         * ser << batches << another_batch;
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        serializer& operator<<(const R& record_batches)
        {
            write(record_batches);
            return *this;
        }

        /**
         * @brief Stream manipulator operator for functions like end_stream.
         *
         * This operator enables the use of manipulator functions (similar to std::endl)
         * with the serializer. It accepts a function pointer that takes and returns
         * a reference to a serializer.
         *
         * @param manip A function pointer to a manipulator function
         * @return A reference to this serializer for method chaining
         *
         * @example
         * serializer ser(stream);
         * ser << batch1 << batch2 << end_stream;
         */
        serializer& operator<<(serializer& (*manip)(serializer&) )
        {
            return manip(*this);
        }

        /**
         * @brief Finalizes the serialization process by writing end-of-stream marker.
         *
         * This method writes an end-of-stream marker to the output stream and flushes
         * any buffered data. It can be called multiple times safely as it tracks
         * whether the stream has already been ended to prevent duplicate operations.
         *
         * @note This method is idempotent - calling it multiple times has no additional effect.
         * @post After calling this method, m_ended will be set to true.
         */
        void end();

    private:

        static std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb);

        bool m_schema_received{false};
        std::vector<sparrow::data_type> m_dtypes;
        any_output_stream m_stream;
        bool m_ended{false};
        std::optional<CompressionType> m_compression;
        dictionary_tracker m_dict_tracker;
    };

    inline serializer& end_stream(serializer& serializer)
    {
        serializer.end();
        return serializer;
    }
}
