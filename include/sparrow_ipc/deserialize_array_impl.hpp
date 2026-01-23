#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc::detail
{
    /**
     * @brief Generic implementation for deserializing non-owning arrays with simple layout.
     *
     * This function provides the common deserialization logic for array types that have
     * a validity buffer and a single data buffer (e.g., primitive_array, interval_array).
     *
     * @tparam ArrayType The array type template (e.g., sparrow::primitive_array)
     * @tparam T The element type
     *
     * @param record_batch The FlatBuffer RecordBatch containing metadata
     * @param body The raw buffer data
     * @param length The number of elements in the array to deserialize
     * @param name The array column name
     * @param metadata Optional metadata pairs
     * @param nullable Whether the array is nullable
     * @param buffer_index The current buffer index (incremented by this function)
     *
     * @return The deserialized array of type ArrayType<T>
     */
    template <template<typename...> class ArrayType, typename T>
    [[nodiscard]] ArrayType<T> deserialize_simple_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        std::optional<std::string> format_override = std::nullopt
    )
    {
        const std::string_view format = format_override.has_value()
            ? *format_override
            : data_type_to_format(sparrow::detail::get_data_type_from_array<ArrayType<T>>::get());
        
        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }
        
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            name,
            metadata,
            flags,
            0,
            nullptr,
            nullptr
        );

        const auto compression = record_batch.compression();
        std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
        constexpr auto nb_buffers = 2;
        buffers.reserve(nb_buffers);
        {
            auto validity_buffer_span = utils::get_buffer(record_batch, body, buffer_index);
            auto data_buffer_span = utils::get_buffer(record_batch, body, buffer_index);

            if (compression)
            {
                buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
                buffers.push_back(utils::get_decompressed_buffer(data_buffer_span, compression));
            }
            else
            {
                buffers.push_back(std::move(validity_buffer_span));
                buffers.push_back(std::move(data_buffer_span));
            }
        }

        const auto null_count = std::visit(
            [length](const auto& arg) {
                std::span<const uint8_t> span(arg.data(), arg.size());
                return utils::get_bitmap_pointer_and_null_count(span, length).second;
            },
            buffers[0]
        );

        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            length,
            null_count,
            0,
            0,
            nullptr,
            nullptr,
            std::move(buffers)
        );

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return ArrayType<T>{std::move(ap)};
    }
}
