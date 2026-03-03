#pragma once

#include <cstddef>
#include <optional>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/run_end_encoded_array.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialization_context.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

class array_deserializer;

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
     * @param context The deserialization context.
     * @param field_desc The field descriptor.
     *
     * @return The deserialized run-end encoded array
     */
    [[nodiscard]] sparrow::run_end_encoded_array deserialize_run_end_encoded_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    )
    {
        ++context.node_index;  // Consume one FieldNode for this run-end encoded array (parent)
        constexpr size_t n_children = 2;

        if (!field_desc.field.children() || field_desc.field.children()->size() != n_children)
        {
            throw std::runtime_error(
                "Run-end encoded array field must have exactly 2 children (run ends and values)"
            );
        }

        const auto* run_ends_field = field_desc.field.children()->Get(0);
        if (!run_ends_field)
        {
            throw std::runtime_error("Run-end encoded array field has null run ends child.");
        }
        
        const auto* nodes = context.record_batch.nodes();
        if (!nodes || context.node_index >= nodes->size())
        {
            throw std::runtime_error(
                "Run-end encoded array: insufficient FieldNodes. Expected run_ends child node at index "
                + std::to_string(context.node_index)
            );
        }
        
        const auto* run_ends_node = nodes->Get(context.node_index);
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
        
        deserialization_context run_ends_context(
            context.record_batch,
            context.body,
            context.buffer_index,
            context.node_index,
            context.variadic_counts_idx
        );

        field_descriptor run_ends_desc(
            encoded_length,
            run_ends_field->name()->str(),
            std::move(run_ends_metadata),
            utils::get_sparrow_flags(*run_ends_field),
            true,
            *run_ends_field,
            field_desc.dictionaries
        );

        sparrow::array run_ends_array = array_deserializer::deserialize(
            run_ends_context,
            run_ends_desc
        );

        // Deserialize the second child: encoded values
        const auto* values_field = field_desc.field.children()->Get(1);
        if (!values_field)
        {
            throw std::runtime_error("Run-end encoded array field has null values child.");
        }

        std::optional<std::vector<sparrow::metadata_pair>> values_metadata;
        if (values_field->custom_metadata())
        {
            values_metadata = to_sparrow_metadata(*values_field->custom_metadata());
        }

        deserialization_context values_context(
            context.record_batch,
            context.body,
            context.buffer_index,
            context.node_index,
            context.variadic_counts_idx
        );

        field_descriptor values_desc(
            encoded_length,  // Same encoded length as run ends
            values_field->name()->str(),
            std::move(values_metadata),
            utils::get_sparrow_flags(*values_field),
            true,
            *values_field,
            field_desc.dictionaries
        );

        sparrow::array values_array = array_deserializer::deserialize(
            values_context,
            values_desc
        );

        auto [run_ends_arrow_array, run_ends_arrow_schema] = sparrow::extract_arrow_structures(std::move(run_ends_array));
        auto [values_arrow_array, values_arrow_schema] = sparrow::extract_arrow_structures(std::move(values_array));

        auto** schema_children = new ArrowSchema*[n_children];
        schema_children[0] = new ArrowSchema(std::move(run_ends_arrow_schema));
        schema_children[1] = new ArrowSchema(std::move(values_arrow_schema));

        const std::string_view format = "+r"; // TODO: Use sparrow::data_type_to_format(sparrow::data_type::RUN_ENCODED); when it will be available on sparrow
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            field_desc.name,
            field_desc.metadata,
            field_desc.flags,
            n_children,
            schema_children,
            nullptr
        );

        auto** array_children = new ArrowArray*[n_children];
        array_children[0] = new ArrowArray(std::move(run_ends_arrow_array));
        array_children[1] = new ArrowArray(std::move(values_arrow_array));


        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            field_desc.length,
            0,
            0,
            n_children,
            array_children,
            nullptr,
             std::vector<arrow_array_private_data::optionally_owned_buffer>{} // No buffers at parent level
        );

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::run_end_encoded_array{std::move(ap)};
    }
}
