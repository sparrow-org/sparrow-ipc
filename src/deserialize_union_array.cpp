#include "sparrow_ipc/deserialize_union_array.hpp"

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/union_array.hpp>

#include "array_deserializer.hpp"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialization_context.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    sparrow::array deserialize_union_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    )
    {
        ++context.node_index;  // Consume one FieldNode for this union array
        const auto* union_type = field_desc.field.type_as_Union();
        const auto mode = union_type->mode();

        const auto compression = context.record_batch.compression();
        std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;

        // Type ID buffer
        {
            std::span<const uint8_t> type_id_buffer = utils::get_buffer(context.record_batch, context.body, context.buffer_index);
            if (compression)
            {
                buffers.push_back(utils::get_decompressed_buffer(type_id_buffer, compression));
            }
            else
            {
                buffers.push_back(std::move(type_id_buffer));
            }
        }

        // Offset buffer (for dense union)
        if (mode == org::apache::arrow::flatbuf::UnionMode::Dense)
        {
            std::span<const uint8_t> offset_buffer = utils::get_buffer(context.record_batch, context.body, context.buffer_index);
            if (compression)
            {
                buffers.push_back(utils::get_decompressed_buffer(offset_buffer, compression));
            }
            else
            {
                buffers.push_back(std::move(offset_buffer));
            }
        }

        const auto n_child_arrays = field_desc.field.children() ? field_desc.field.children()->size() : 0;
        const auto* type_ids = union_type->typeIds();

        if (type_ids && type_ids->size() != static_cast<flatbuffers::uoffset_t>(n_child_arrays))
        {
            throw std::runtime_error("Union typeIds size does not match number of children");
        }

        std::string format = (mode == org::apache::arrow::flatbuf::UnionMode::Dense) ? "+ud:" : "+us:";
        if (type_ids)
        {
            for (size_t i = 0; i < type_ids->size(); ++i)
            {
                format += std::to_string(type_ids->Get(i)) + ",";
            }
            format.pop_back();
        }
        else
        {
            // Default type IDs: 0, 1, 2... to have a valid format
            for (size_t i = 0; i < n_child_arrays; ++i)
            {
                format += std::to_string(i) + ",";
            }
            if (n_child_arrays > 0)
            {
                format.pop_back();
            }
        }

        std::vector<sparrow::array> child_arrays;
        child_arrays.reserve(n_child_arrays);

        for (const auto* child_field : *field_desc.field.children())
        {
            if (!child_field)
            {
                throw std::runtime_error("Union array has a null child field.");
            }

            const int64_t child_length = [&] {
                if (!context.record_batch.nodes() || context.node_index >= context.record_batch.nodes()->size())
                {
                    throw std::runtime_error(
                        "Could not retrieve child length from FieldNode metadata. "
                        "FieldNode not found at expected index."
                    );
                }
                const int64_t length = context.record_batch.nodes()->Get(context.node_index)->length();
                if (mode == org::apache::arrow::flatbuf::UnionMode::Sparse && length != field_desc.length)
                {
                    throw std::runtime_error("Sparse union child length must match union length");
                }
                return length;
            }();

            std::optional<std::vector<sparrow::metadata_pair>> child_metadata;
            if (child_field->custom_metadata())
            {
                child_metadata = to_sparrow_metadata(*child_field->custom_metadata());
            }

            deserialization_context child_context(
                context.record_batch,
                context.body,
                context.buffer_index,
                context.node_index,
                context.variadic_counts_idx
            );

            field_descriptor child_descriptor(
                child_length,
                utils::get_fb_name(child_field),
                std::move(child_metadata),
                utils::get_sparrow_flags(*child_field),
                true,
                *child_field,
                field_desc.dictionaries
            );

            child_arrays.push_back(array_deserializer::deserialize(
                child_context,
                child_descriptor
            ));
        }

        auto** schema_children = new ArrowSchema*[n_child_arrays];
        auto** array_children = new ArrowArray*[n_child_arrays];

        for (size_t i = 0; i < n_child_arrays; ++i)
        {
            auto [arr, schema] = sparrow::extract_arrow_structures(std::move(child_arrays[i]));
            schema_children[i] = new ArrowSchema(std::move(schema));
            array_children[i] = new ArrowArray(std::move(arr));
        }

        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            field_desc.name,
            field_desc.metadata,
            field_desc.flags,
            n_child_arrays,
            schema_children,
            nullptr
        );

        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            field_desc.length,
            0, // null_count
            0,
            n_child_arrays,
            array_children,
            nullptr,
            std::move(buffers)
        );

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        if (mode == org::apache::arrow::flatbuf::UnionMode::Dense)
        {
            return sparrow::array(sparrow::dense_union_array(std::move(ap)));
        }
        else
        {
            return sparrow::array(sparrow::sparse_union_array(std::move(ap)));
        }
    }
}
