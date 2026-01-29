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

#include "Message_generated.h"

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_view_array.hpp"
#include "sparrow_ipc/metadata.hpp"

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
            const org::apache::arrow::flatbuf::RecordBatch&,
            const std::span<const uint8_t>&,
            const int64_t,
            const std::string&,
            const std::optional<std::vector<sparrow::metadata_pair>>&,
            bool,
            size_t&,
            size_t&,
            size_t&,
            const org::apache::arrow::flatbuf::Field&
        )>;

        /**
         * @brief Deserializes an array based on its field description.
         *
         * This is the main entry point of the deserializer. It looks up the appropriate
         * deserialization function from the map based on the field's type and invokes it.
         *
         * @param record_batch The Flatbuffer RecordBatch containing the data.
         * @param body The raw byte buffer of the message body.
         * @param length The number of elements in the array to deserialize.
         * @param name The name of the field.
         * @param metadata The metadata associated with the field.
         * @param nullable Whether the field is nullable.
         * @param buffer_index The current index into the buffer list of the RecordBatch.
         * @param variadic_counts_idx The current index into the list of variadic buffers (used with view data types).
         * @param field The Flatbuffer Field object describing the array to deserialize.
         * @return A `sparrow::array` containing the deserialized data.
         * @throws std::runtime_error if the field type is not supported.
         */
        static sparrow::array deserialize(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                   const std::span<const uint8_t>& body,
                                   const int64_t length,
                                   const std::string& name,
                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                   bool nullable,
                                   size_t& buffer_index,
                                   size_t& node_index,
                                   size_t& variadic_counts_idx,
                                   const org::apache::arrow::flatbuf::Field& field);
    private:
        inline static std::unordered_map<org::apache::arrow::flatbuf::Type, deserializer_func> m_deserializer_map;

        /**
         * @bried Populate the map with function pointers to the static
         * deserialization methods for each supported Arrow data type.
         */
        static void initialize_deserializer_map();

        template<typename T>
        static sparrow::array deserialize_primitive(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                    const std::span<const uint8_t>& body,
                                                    const int64_t length,
                                                    const std::string& name,
                                                    const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                    bool nullable,
                                                    size_t& buffer_index,
                                                    size_t& node_index,
                                                    size_t& variadic_counts_idx,
                                                    const org::apache::arrow::flatbuf::Field&)
        {
            ++node_index;  // Consume one FieldNode for this primitive array
            return sparrow::array(deserialize_primitive_array<T>(
                record_batch, body, length, name, metadata, nullable, buffer_index
            ));
        }

        template<typename T>
        static sparrow::array deserialize_variable_size_binary(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                               const std::span<const uint8_t>& body,
                                                               const int64_t length,
                                                               const std::string& name,
                                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                               bool nullable,
                                                               size_t& buffer_index,
                                                               size_t& node_index,
                                                               size_t& variadic_counts_idx,
                                                               const org::apache::arrow::flatbuf::Field&)
        {
            ++node_index;  // Consume one FieldNode for this binary array
            return sparrow::array(deserialize_variable_size_binary_array<T>(
                record_batch, body, length, name, metadata, nullable, buffer_index
            ));
        }

        template<typename T>
        static sparrow::array deserialize_variable_size_binary_view(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                               const std::span<const uint8_t>& body,
                                                               const int64_t length,
                                                               const std::string& name,
                                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                               bool nullable,
                                                               size_t& buffer_index,
                                                               size_t& node_index,
                                                               size_t& variadic_counts_idx,
                                                               const org::apache::arrow::flatbuf::Field&)
        {
            ++node_index;  // Consume one FieldNode for this binary view array
            const auto* variadic_counts = record_batch.variadicBufferCounts();
            int64_t data_buffers_size = 0;
            if (variadic_counts && variadic_counts_idx < variadic_counts->size())
            {
                data_buffers_size = variadic_counts->Get(variadic_counts_idx++);
            }
            return sparrow::array(deserialize_variable_size_binary_view_array<T>(
                record_batch, body, length, name, metadata, nullable, buffer_index, data_buffers_size
            ));
        }

        // TODO refactor the 'list' related fcts when testing with recursive is handled
        template <typename T>
        [[nodiscard]] static T deserialize_list_array(
            const org::apache::arrow::flatbuf::RecordBatch& record_batch,
            const std::span<const uint8_t>& body,
            const int64_t length,
            const std::string& name,
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
            bool nullable,
            size_t& buffer_index,
            size_t& node_index,
            size_t& variadic_counts_idx,
            const org::apache::arrow::flatbuf::Field& field)
        {
            ++node_index;  // Consume one FieldNode for this list array
            // Set up flags based on nullable
            std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
            if (nullable)
            {
                flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
            }

            const auto compression = record_batch.compression();
            std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
            constexpr auto nb_buffers = 2;
            buffers.reserve(nb_buffers);

            auto validity_buffer_span = utils::get_buffer(record_batch, body, buffer_index);
            auto offsets_buffer_span = utils::get_buffer(record_batch, body, buffer_index);

            using offset_type = typename T::offset_type;
            int64_t child_length;

            if (compression)
            {
                auto processed_offsets = utils::get_decompressed_buffer(offsets_buffer_span, compression);
                child_length = std::visit(
                    [length](const auto& arg) -> int64_t {
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
                child_length = offsets[length];

                buffers.push_back(std::move(validity_buffer_span));
                buffers.push_back(std::move(offsets_buffer_span));
            }

            const auto null_count = std::visit(
                [length](const auto& arg) {
                    std::span<const uint8_t> span(arg.data(), arg.size());
                    return utils::get_bitmap_pointer_and_null_count(span, length).second;
                },
                buffers[0]
            );

            // Deserialize child array
            const auto* child_field = field.children()->Get(0);
            if (!child_field)
            {
                throw std::runtime_error("List array field has no child field.");
            }

            std::optional<std::vector<sparrow::metadata_pair>> child_metadata;
            if (child_field->custom_metadata())
            {
                child_metadata = to_sparrow_metadata(*child_field->custom_metadata());
            }

            sparrow::array child_array = array_deserializer::deserialize(
                record_batch,
                body,
                child_length,
                child_field->name()->str(),
                child_metadata,
                child_field->nullable(),
                buffer_index,
                node_index,
                variadic_counts_idx,
                *child_field
            );

            const std::string_view format = sparrow::data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());

            auto [child_arrow_array, child_arrow_schema] = sparrow::extract_arrow_structures(std::move(child_array));

            auto** schema_children = new ArrowSchema*[1];
            schema_children[0] = new ArrowSchema(std::move(child_arrow_schema));
            ArrowSchema schema = make_non_owning_arrow_schema(
                format,
                name,
                metadata,
                flags,
                1, // one child
                schema_children,
                nullptr
            );

            auto** array_children = new ArrowArray*[1];
            array_children[0] = new ArrowArray(std::move(child_arrow_array));
            ArrowArray array = make_arrow_array<arrow_array_private_data>(
                length,
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

        template<typename T>
        static sparrow::array deserialize_list(
            const org::apache::arrow::flatbuf::RecordBatch& record_batch,
            const std::span<const uint8_t>& body,
            const int64_t length,
            const std::string& name,
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
            bool nullable,
            size_t& buffer_index,
            size_t& node_index,
            size_t& variadic_counts_idx,
            const org::apache::arrow::flatbuf::Field& field)
        {
            return sparrow::array(deserialize_list_array<T>(
                record_batch, body, length, name, metadata, nullable, buffer_index, node_index, variadic_counts_idx, field
            ));
        }

        static sparrow::array deserialize_int(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                              const std::span<const uint8_t>& body,
                                              const int64_t length,
                                              const std::string& name,
                                              const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                              bool nullable,
                                              size_t& buffer_index,
                                              size_t& node_index,
                                              size_t& variadic_counts_idx,
                                              const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_float(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                const std::span<const uint8_t>& body,
                                                const int64_t length,
                                                const std::string& name,
                                                const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                bool nullable,
                                                size_t& buffer_index,
                                                size_t& node_index,
                                                size_t& variadic_counts_idx,
                                                const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_fixed_size_binary(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                            const std::span<const uint8_t>& body,
                                                            const int64_t length,
                                                            const std::string& name,
                                                            const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                            bool nullable,
                                                            size_t& buffer_index,
                                                            size_t& node_index,
                                                            size_t& variadic_counts_idx,
                                                            const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_decimal(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                  const std::span<const uint8_t>& body,
                                                  const int64_t length,
                                                  const std::string& name,
                                                  const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                  bool nullable,
                                                  size_t& buffer_index,
                                                  size_t& node_index,
                                                  size_t& variadic_counts_idx,
                                                  const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_null(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const int64_t length,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               size_t& node_index,
                                               size_t& variadic_counts_idx,
                                               const org::apache::arrow::flatbuf::Field&);

        static sparrow::array deserialize_date(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const int64_t length,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               size_t& node_index,
                                               size_t& variadic_counts_idx,
                                               const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_interval(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                   const std::span<const uint8_t>& body,
                                                   const int64_t length,
                                                   const std::string& name,
                                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                   bool nullable,
                                                   size_t& buffer_index,
                                                   size_t& node_index,
                                                   size_t& variadic_counts_idx,
                                                   const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_duration(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                   const std::span<const uint8_t>& body,
                                                   const int64_t length,
                                                   const std::string& name,
                                                   const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                   bool nullable,
                                                   size_t& buffer_index,
                                                   size_t& node_index,
                                                   size_t& variadic_counts_idx,
                                                   const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_time(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                               const std::span<const uint8_t>& body,
                                               const int64_t length,
                                               const std::string& name,
                                               const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                               bool nullable,
                                               size_t& buffer_index,
                                               size_t& node_index,
                                               size_t& variadic_counts_idx,
                                               const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_timestamp(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                    const std::span<const uint8_t>& body,
                                                    const int64_t length,
                                                    const std::string& name,
                                                    const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                    bool nullable,
                                                    size_t& buffer_index,
                                                    size_t& node_index,
                                                    size_t& variadic_counts_idx,
                                                    const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_fixed_size_list(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                          const std::span<const uint8_t>& body,
                                                          const int64_t length,
                                                          const std::string& name,
                                                          const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                          bool nullable,
                                                          size_t& buffer_index,
                                                          size_t& node_index,
                                                          size_t& variadic_counts_idx,
                                                          const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_struct(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                 const std::span<const uint8_t>& body,
                                                 const int64_t length,
                                                 const std::string& name,
                                                 const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                 bool nullable,
                                                 size_t& buffer_index,
                                                 size_t& node_index,
                                                 size_t& variadic_counts_idx,
                                                 const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_map(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                              const std::span<const uint8_t>& body,
                                              const int64_t length,
                                              const std::string& name,
                                              const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                              bool nullable,
                                              size_t& buffer_index,
                                              size_t& node_index,
                                              size_t& variadic_counts_idx,
                                              const org::apache::arrow::flatbuf::Field& field);

        static sparrow::array deserialize_run_end_encoded(const org::apache::arrow::flatbuf::RecordBatch& record_batch,
                                                          const std::span<const uint8_t>& body,
                                                          const int64_t length,
                                                          const std::string& name,
                                                          const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
                                                          bool nullable,
                                                          size_t& buffer_index,
                                                          size_t& node_index,
                                                          size_t& variadic_counts_idx,
                                                          const org::apache::arrow::flatbuf::Field& field);
    };
}
