#include "sparrow_ipc/deserialize_fixed_size_binary_array.hpp"

#include <unordered_set>

namespace sparrow_ipc
{
    sparrow::fixed_width_binary_array deserialize_fixed_width_binary_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        int32_t byte_width
    )
    {
        const std::string format = "w:" + std::to_string(byte_width);
        
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
        return sparrow::fixed_width_binary_array{std::move(ap)};
    }
}
