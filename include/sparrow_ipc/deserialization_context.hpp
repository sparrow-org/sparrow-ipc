#pragma once

#include <optional>
#include <span>
#include <string>
#include <vector>

#include "Message_generated.h"
#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Encapsulates the context required for deserialization.
     *
     * This struct groups parameters that represent the current state and source
     * data for the deserialization process.
     *
     * @param record_batch The Flatbuffer RecordBatch containing the data.
     * @param body The raw byte buffer of the message body.
     * @param buffer_index The current index into the buffer list of the RecordBatch.
     * @param node_index This index tracks the FieldNode being processed in the RecordBatch's depth-first traversal.
     * It is advanced for each FieldNode consumed.
     * @param variadic_counts_idx The current index into the list of variadic buffers (used with view data types).
     */
    struct deserialization_context
    {
        const org::apache::arrow::flatbuf::RecordBatch& record_batch;
        std::span<const uint8_t> body;
        size_t& buffer_index;
        size_t& node_index;
        size_t& variadic_counts_idx;

        deserialization_context(
            const org::apache::arrow::flatbuf::RecordBatch& record_batch,
            std::span<const uint8_t> body,
            size_t& buffer_index,
            size_t& node_index,
            size_t& variadic_counts_idx)
            : record_batch(record_batch)
            , body(body)
            , buffer_index(buffer_index)
            , node_index(node_index)
            , variadic_counts_idx(variadic_counts_idx)
        {}
    };

    /**
     * @brief Encapsulates the description of a field to be deserialized.
     *
     * This struct groups parameters that describe the specific field
     * that is currently being processed by the deserializer.
     *
     * @param length The number of elements in the array to deserialize.
     * @param name The name of the field.
     * @param metadata The metadata associated with the field.
     * @param nullable Whether the field is nullable.
     * @param decode_dictionary_indices Whether dictionary-annotated fields should be read from their
     * physical index buffers (true) or interpreted as logical dictionary values (false).
     * @param field The Flatbuffer Field object describing the array to deserialize.
     * @param dictionaries Cache for dictionaries.
     */
    struct field_descriptor
    {
        int64_t length;
        std::string name;
        std::optional<std::vector<sparrow::metadata_pair>> metadata;
        bool nullable;
        bool decode_dictionary_indices;
        const org::apache::arrow::flatbuf::Field& field;
        const dictionary_cache* dictionaries = nullptr;

        field_descriptor(
            int64_t length,
            std::string name,
            std::optional<std::vector<sparrow::metadata_pair>> metadata,
            bool nullable,
            bool decode_dictionary_indices,
            const org::apache::arrow::flatbuf::Field& field,
            const dictionary_cache* dictionaries)
            : length(length)
            , name(std::move(name))
            , metadata(std::move(metadata))
            , nullable(nullable)
            , decode_dictionary_indices(decode_dictionary_indices)
            , field(field)
            , dictionaries(dictionaries)
        {}
    };
} // namespace sparrow_ipc
