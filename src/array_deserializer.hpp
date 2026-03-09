#pragma once

#include <functional>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <sparrow/array.hpp>
#include <sparrow/list_array.hpp>
#include <sparrow/struct_array.hpp>
#include <sparrow/types/data_type.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialization_context.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_view_array.hpp"

namespace sparrow_ipc
{
    /**
     * @brief A class for deserializing Arrow arrays from Flatbuffers messages.
     *
     * This class uses a map to deserialize different types of Arrow
     * arrays based on the field type specified in the schema. It is designed to be
     * extensible, allowing new deserializers to be added by registering them in the map.
     */
    class array_deserializer
    {
    public:
        /**
         * @brief A function pointer type for the deserializer function.
         *
         * This defines the signature for all functions that can deserialize a specific
         * Arrow array type.
         */
        using deserializer_func = std::function<sparrow::array(
            deserialization_context&,
            const field_descriptor&
        )>;

        /**
         * @brief Deserializes an array based on its field description.
         *
         * This is the main entry point of the deserializer. It looks up the appropriate
         * deserialization function from the map based on the field's type and invokes it.
         *
         * @param context The deserialization context.
         * @param field_desc The field descriptor.
         * @return A `sparrow::array` containing the deserialized data.
         * @throws std::runtime_error if the field type is not supported.
         */
        static sparrow::array deserialize(deserialization_context& context, const field_descriptor& field_desc);
    private:

        inline static std::unordered_map<org::apache::arrow::flatbuf::Type, deserializer_func> m_deserializer_map;

        /**
         * @bried Populate the map with function pointers to the static
         * deserialization methods for each supported Arrow data type.
         */
        static void initialize_deserializer_map();

        template<typename T>
        static sparrow::array deserialize_primitive(deserialization_context& context, const field_descriptor& field_desc)
        {
            ++context.node_index;  // Consume one FieldNode for this primitive array
            return sparrow::array(deserialize_primitive_array<T>(
                context, field_desc
            ));
        }

        template<typename T>
        static sparrow::array deserialize_variable_size_binary(deserialization_context& context, const field_descriptor& field_desc)
        {
            ++context.node_index;  // Consume one FieldNode for this binary array
            return sparrow::array(deserialize_variable_size_binary_array<T>(
                context, field_desc
            ));
        }

        template<typename T>
        static sparrow::array deserialize_variable_size_binary_view(deserialization_context& context, const field_descriptor& field_desc)
        {
            ++context.node_index;  // Consume one FieldNode for this binary view array
            const auto* variadic_counts = context.record_batch.variadicBufferCounts();
            int64_t data_buffers_size = 0;
            if (variadic_counts && context.variadic_counts_idx < variadic_counts->size())
            {
                data_buffers_size = variadic_counts->Get(context.variadic_counts_idx++);
            }
            return sparrow::array(deserialize_variable_size_binary_view_array<T>(
                context, field_desc, data_buffers_size
            ));
        }

        // TODO refactor the 'list' related fcts when testing with recursive is handled
        template <typename T>
        [[nodiscard]] static T deserialize_list_array(
            deserialization_context& context,
            const field_descriptor& field_desc)
        {
            ++context.node_index;  // Consume one FieldNode for this list array

            const auto compression = context.record_batch.compression();
            std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
            constexpr auto nb_buffers = 2;
            buffers.reserve(nb_buffers);

            auto validity_buffer_span = utils::get_buffer(context.record_batch, context.body, context.buffer_index);
            auto offsets_buffer_span = utils::get_buffer(context.record_batch, context.body, context.buffer_index);

            using offset_type = typename T::offset_type;
            int64_t child_length = 0;

            if (compression)
            {
                auto processed_offsets = utils::get_decompressed_buffer(offsets_buffer_span, compression);
                child_length = std::visit(
                    [length = field_desc.length](const auto& arg) -> int64_t {
                        const auto* offsets_ptr = reinterpret_cast<const offset_type*>(arg.data());
                        return offsets_ptr[length];
                    },
                    processed_offsets
                );

                buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
                buffers.push_back(std::move(processed_offsets));
            }
            else
            {
                const auto offsets = reinterpret_cast<const offset_type*>(offsets_buffer_span.data());
                child_length = offsets[field_desc.length];

                buffers.push_back(std::move(validity_buffer_span));
                buffers.push_back(std::move(offsets_buffer_span));
            }

            const auto null_count = std::visit(
                [length = field_desc.length](const auto& arg) {
                    std::span<const uint8_t> span(arg.data(), arg.size());
                    return utils::get_bitmap_pointer_and_null_count(span, length).second;
                },
                buffers[0]
            );

            // Deserialize child array
            const auto* child_field = field_desc.field.children()->Get(0);
            if (!child_field)
            {
                throw std::runtime_error("List array field has no child field.");
            }

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

            sparrow::array child_array = array_deserializer::deserialize(
                child_context,
                child_descriptor
            );

            const std::string_view format = sparrow::data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());

            auto [child_arrow_array, child_arrow_schema] = sparrow::extract_arrow_structures(
                std::move(child_array)
            );

            auto** schema_children = new ArrowSchema*[1];
            schema_children[0] = new ArrowSchema(std::move(child_arrow_schema));
            ArrowSchema schema = make_non_owning_arrow_schema(
                format,
                field_desc.name,
                field_desc.metadata,
                field_desc.flags,
                1,  // one child
                schema_children,
                nullptr
            );

            auto** array_children = new ArrowArray*[1];
            array_children[0] = new ArrowArray(std::move(child_arrow_array));
            ArrowArray array = make_arrow_array<arrow_array_private_data>(
                field_desc.length,
                null_count,
                0,
                1,
                array_children,
                nullptr,
                std::move(buffers)
            );

            sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
            return T{std::move(ap)};
        }

        template <typename T>
        static sparrow::array deserialize_list(
            deserialization_context& context,
            const field_descriptor& field_desc)
        {
            return sparrow::array(deserialize_list_array<T>(
                context, field_desc
            ));
        }

        template <typename T>
        [[nodiscard]] static T deserialize_list_view_array(
            deserialization_context& context,
            const field_descriptor& field_desc)
        {
            ++context.node_index;  // Consume one FieldNode for this list view array

            const int64_t child_length = [&] {
                if (!context.record_batch.nodes() || context.node_index >= context.record_batch.nodes()->size())
                {
                    throw std::runtime_error(
                        "Could not retrieve child length from FieldNode metadata. "
                        "FieldNode not found at expected index."
                    );
                }
                return context.record_batch.nodes()->Get(context.node_index)->length();
            }();

            const auto compression = context.record_batch.compression();

            auto process_buffer = [&](auto&& buffer_span)
            {
                return compression ? utils::get_decompressed_buffer(buffer_span, compression)
                                   : std::move(buffer_span);
            };

            // Process buffers
            auto processed_validity = process_buffer(utils::get_buffer(context.record_batch, context.body, context.buffer_index));
            auto processed_offsets = process_buffer(utils::get_buffer(context.record_batch, context.body, context.buffer_index));
            auto processed_sizes = process_buffer(utils::get_buffer(context.record_batch, context.body, context.buffer_index));

            {
                using offset_type = typename T::offset_type;
                using size_type = typename T::list_size_type;

                auto get_buffer_data_ptr = [](const arrow_array_private_data::optionally_owned_buffer& buf)
                {
                    return std::visit(
                        [](const auto& arg)
                        {
                            return arg.data();
                        },
                        buf
                    );
                };

                const auto* offsets_ptr = reinterpret_cast<const offset_type*>(
                    get_buffer_data_ptr(processed_offsets)
                );
                const auto* sizes_ptr = reinterpret_cast<const size_type*>(get_buffer_data_ptr(processed_sizes));

                // Validate offsets + sizes
                for (int64_t i = 0; i < field_desc.length; ++i)
                {
                    const int64_t offset = static_cast<int64_t>(offsets_ptr[i]);
                    const int64_t size = static_cast<int64_t>(sizes_ptr[i]);

                    if (offset < 0)
                    {
                        throw std::runtime_error("Negative offset detected in list view array.");
                    }

                    if (size < 0)
                    {
                        throw std::runtime_error("Negative size detected in list view array.");
                    }

                    if (offset > child_length || size > child_length - offset)
                    {
                        throw std::runtime_error("Offset + size exceeds child length derived from metadata.");
                    }
                }
            }

            std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
            constexpr auto nb_buffers = 3;
            buffers.reserve(nb_buffers);

            // Store buffers
            buffers.push_back(std::move(processed_validity));
            buffers.push_back(std::move(processed_offsets));
            buffers.push_back(std::move(processed_sizes));

            const auto null_count = std::visit(
                [length = field_desc.length](const auto& arg) {
                    return utils::get_bitmap_pointer_and_null_count({arg.data(), arg.size()}, length).second;
                },
                buffers[0]
            );

            const auto* child_field = field_desc.field.children()->Get(0);
            if (!child_field)
            {
                throw std::runtime_error("List view array field has no child field.");
            }

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

            sparrow::array child_array = array_deserializer::deserialize(
                child_context,
                child_descriptor
            );

            const std::string_view format = sparrow::data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());

            auto [child_arrow_array, child_arrow_schema] = sparrow::extract_arrow_structures(std::move(child_array));

            auto** schema_children = new ArrowSchema*[1];
            schema_children[0] = new ArrowSchema(std::move(child_arrow_schema));
            ArrowSchema schema = make_non_owning_arrow_schema(
                format,
                field_desc.name,
                field_desc.metadata,
                field_desc.flags,
                1,  // one child
                schema_children,
                nullptr
            );

            auto** array_children = new ArrowArray*[1];
            array_children[0] = new ArrowArray(std::move(child_arrow_array));
            ArrowArray array = make_arrow_array<arrow_array_private_data>(
                field_desc.length,
                null_count,
                0,
                1,
                array_children,
                nullptr,
                std::move(buffers)
            );

            sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
            return T{std::move(ap)};
        }

        template <typename T>
        static sparrow::array deserialize_list_view(
            deserialization_context& context,
            const field_descriptor& field_desc)
        {
            return sparrow::array(deserialize_list_view_array<T>(
                context, field_desc
            ));
        }

        static sparrow::array deserialize_int(deserialization_context& context,
                                              const field_descriptor& field_desc);

        static sparrow::array deserialize_float(deserialization_context& context,
                                                const field_descriptor& field_desc);

        static sparrow::array deserialize_fixed_size_binary(deserialization_context& context,
                                                            const field_descriptor& field_desc);

        static sparrow::array deserialize_decimal(deserialization_context& context,
                                                  const field_descriptor& field_desc);

        static sparrow::array deserialize_null(deserialization_context& context,
                                               const field_descriptor& field_desc);

        static sparrow::array deserialize_date(deserialization_context& context,
                                               const field_descriptor& field_desc);

        static sparrow::array deserialize_interval(deserialization_context& context,
                                                   const field_descriptor& field_desc);

        static sparrow::array deserialize_duration(deserialization_context& context,
                                                   const field_descriptor& field_desc);

        static sparrow::array deserialize_time(deserialization_context& context,
                                               const field_descriptor& field_desc);

        static sparrow::array deserialize_timestamp(deserialization_context& context,
                                                    const field_descriptor& field_desc);

        static sparrow::array deserialize_fixed_size_list(deserialization_context& context,
                                                          const field_descriptor& field_desc);

        static sparrow::array deserialize_struct(deserialization_context& context,
                                                 const field_descriptor& field_desc);

        static sparrow::array deserialize_map(deserialization_context& context,
                                              const field_descriptor& field_desc);

        static sparrow::array deserialize_run_end_encoded(deserialization_context& context,
                                                          const field_descriptor& field_desc);

        static sparrow::array deserialize_union(deserialization_context& context,
                                                const field_descriptor& field_desc);
    };
}
