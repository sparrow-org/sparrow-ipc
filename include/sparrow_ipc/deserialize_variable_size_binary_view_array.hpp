#pragma once

#include <span>
#include <unordered_set>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/variable_size_binary_view_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] T deserialize_variable_size_binary_view_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        const int64_t data_buffers_size
    )
    {
        const std::string_view format = sparrow::data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());

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
        const auto nb_buffers = data_buffers_size + 3;
        buffers.reserve(nb_buffers);

        {
            auto validity_buffer_span = utils::get_buffer(record_batch, body, buffer_index);
            auto views_buffer_span = utils::get_buffer(record_batch, body, buffer_index);

            if (compression)
            {
                buffers.push_back(utils::get_decompressed_buffer(validity_buffer_span, compression));
                buffers.push_back(utils::get_decompressed_buffer(views_buffer_span, compression));
            }
            else
            {
                buffers.push_back(std::move(validity_buffer_span));
                buffers.push_back(std::move(views_buffer_span));
            }
        }

        std::vector<int64_t> variadic_buffer_sizes;
        variadic_buffer_sizes.reserve(data_buffers_size);

        auto push_buffer = [&](auto&& buffer)
        {
            variadic_buffer_sizes.push_back(static_cast<int64_t>(buffer.size()));
            buffers.push_back(std::forward<decltype(buffer)>(buffer));
        };

        for (auto i = 0; i < data_buffers_size; ++i)
        {
            auto data_buffer_span = utils::get_buffer(record_batch, body, buffer_index);

            if (compression)
            {
                auto decompressed = utils::get_decompressed_buffer(data_buffer_span, compression);
                std::visit(
                    [&](auto&& buf) { push_buffer(buf); },
                    std::move(decompressed));
            }
            else
            {
                push_buffer(data_buffer_span);
            }
        }

        buffers.push_back(
            sparrow::buffer<uint8_t>(
                std::vector<uint8_t>(
                    reinterpret_cast<const uint8_t*>(variadic_buffer_sizes.data()),
                    reinterpret_cast<const uint8_t*>(variadic_buffer_sizes.data() + variadic_buffer_sizes.size())
                ),
                sparrow::buffer<uint8_t>::default_allocator{}
            )
        );

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
        return T{std::move(ap)};
    }
}
