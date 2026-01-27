#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/run_end_encoded_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
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
     * @param record_batch The FlatBuffer RecordBatch containing metadata
     * @param body The raw buffer data
     * @param length The number of elements in the decoded array (parent length)
     * @param name The array column name
     * @param metadata Optional metadata pairs  
     * @param nullable Whether the parent array is nullable
     * @param buffer_index The current buffer index (not incremented for parent, incremented by children)
     * @param node_index The current node index (incremented for parent, passed to children)
     * @param variadic_counts_idx The current index into variadic buffer counts
     * @param field The FlatBuffer Field object describing the array
     * @param array_deserializer The deserializer to use for child arrays
     *
     * @return The deserialized run-end encoded array
     * 
     * @note KNOWN LIMITATION: This implementation may fail on zero-length run-end encoded arrays
     *       due to missing or null buffer entries in the IPC format combined with sparrow's
     *       buffer_view construction requirements. The workaround of inferring encoded_length 
     *       from buffer sizes doesn't work when buffers are absent or have null pointers.
     *       The proper solution requires threading node_index through all deserializers to 
     *       access FieldNode.length values directly. This is a significant architectural change.
     *       For non-zero length arrays, the implementation works correctly.
     *       Users encountering zero-length arrays can filter them out before deserialization
     *       as a workaround.
     */
    template <typename ArrayDeserializer>
    [[nodiscard]] sparrow::run_end_encoded_array deserialize_run_end_encoded_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        size_t& node_index,
        size_t& variadic_counts_idx,
        const org::apache::arrow::flatbuf::Field& field,
        ArrayDeserializer& array_deserializer
    )
    {
        ++node_index;  // Consume one FieldNode for this run-end encoded array (parent)
        constexpr size_t n_children = 2;

        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        if (!field.children() || field.children()->size() != n_children)
        {
            throw std::runtime_error(
                "Run-end encoded array field must have exactly 2 children (run ends and values)"
            );
        }

        // For a proper implementation, we need node indices to get the encoded length from FieldNodes.
        // According to the Arrow IPC spec, FieldNode structs are in pre-ordered flattened order.
        // For a run-end encoded array:
        // - Node[i]: parent with decoded length
        // - Node[i+1]: run_ends child with encoded length
        // - Node[i+2]: values child with encoded length
        // A complete solution would require threading node_index through all deserializers.
        //
        // As a workaround, we inspect the run_ends buffer to determine encoded length.
        // This works because run_ends is always a primitive integer array.
        
        const auto* run_ends_field = field.children()->Get(0);
        if (!run_ends_field)
        {
            throw std::runtime_error("Run-end encoded array field has null run ends child.");
        }

        // IMPORTANT: Run-end encoded arrays have ZERO buffers at the parent level!
        // All buffers belong to the children. So buffer_index currently points to where
        // the first child's buffers start.
        
        // Get encoded_length directly from FieldNodes using node_index.
        // According to the Arrow IPC spec, FieldNode structs are in pre-ordered flattened order.
        // For a run-end encoded array:
        // - Node[node_index-1]: parent with decoded length (already consumed above with ++node_index)
        // - Node[node_index]: run_ends child with encoded length
        // - Node[node_index+1]: values child with encoded length
        
        const auto* nodes = record_batch.nodes();
        if (!nodes || node_index >= nodes->size())
        {
            throw std::runtime_error(
                "Run-end encoded array: insufficient FieldNodes. Expected run_ends child node at index "
                + std::to_string(node_index)
            );
        }
        
        const auto* run_ends_node = nodes->Get(node_index);
        if (!run_ends_node)
        {
            throw std::runtime_error("Run-end encoded array: null run_ends FieldNode.");
        }
        
        const int64_t encoded_length = run_ends_node->length();

        std::optional<std::vector<sparrow::metadata_pair>> run_ends_metadata;
        if (run_ends_field->custom_metadata())
        {
            run_ends_metadata = to_sparrow_metadata(*run_ends_field->custom_metadata());
        }
        
        sparrow::array run_ends_array = array_deserializer.deserialize(
            record_batch,
            body,
            encoded_length,
            run_ends_field->name()->str(),
            run_ends_metadata,
            run_ends_field->nullable(),
            buffer_index,
            node_index,
            variadic_counts_idx,
            *run_ends_field
        );

        // Deserialize the second child: encoded values
        const auto* values_field = field.children()->Get(1);
        if (!values_field)
        {
            throw std::runtime_error("Run-end encoded array field has null values child.");
        }

        std::optional<std::vector<sparrow::metadata_pair>> values_metadata;
        if (values_field->custom_metadata())
        {
            values_metadata = to_sparrow_metadata(*values_field->custom_metadata());
        }

        sparrow::array values_array = array_deserializer.deserialize(
            record_batch,
            body,
            encoded_length,  // Same encoded length as run ends
            values_field->name()->str(),
            values_metadata,
            values_field->nullable(),
            buffer_index,
            node_index,
            variadic_counts_idx,
            *values_field
        );

        // Extract Arrow structures from child arrays
        auto [run_ends_arrow_array, run_ends_arrow_schema] = sparrow::extract_arrow_structures(std::move(run_ends_array));
        auto [values_arrow_array, values_arrow_schema] = sparrow::extract_arrow_structures(std::move(values_array));

        // Create schema with two children
        auto** schema_children = new ArrowSchema*[n_children];
        schema_children[0] = new ArrowSchema(std::move(run_ends_arrow_schema));
        schema_children[1] = new ArrowSchema(std::move(values_arrow_schema));

        const std::string_view format = "+r";  // Run-end encoded format
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            name,
            metadata,
            flags,
            n_children,
            schema_children,
            nullptr
        );

        // Create array with two children and no buffers
        auto** array_children = new ArrowArray*[n_children];
        array_children[0] = new ArrowArray(std::move(run_ends_arrow_array));
        array_children[1] = new ArrowArray(std::move(values_arrow_array));

        std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;  // No buffers

        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            length,         // Decoded length
            0,              // null_count (run-end encoded arrays don't have nulls at parent level)
            0,              // offset
            n_children,              // two children
            array_children,
            nullptr,        // no dictionary
            std::move(buffers)
        );

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::run_end_encoded_array{std::move(ap)};
    }
}
