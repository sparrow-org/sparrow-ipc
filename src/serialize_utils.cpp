#include <algorithm>
#include <stdexcept>

#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"


namespace sparrow_ipc
{
    void fill_body(const sparrow::arrow_proxy& arrow_proxy, any_output_stream& stream,
                   std::optional<CompressionType> compression,
                   std::optional<std::reference_wrapper<CompressionCache>> cache)
    {
        const auto& buffers = arrow_proxy.buffers();
        auto nb_buffers = details::get_nb_buffers_to_process(arrow_proxy.schema().format, buffers.size());

        std::for_each(buffers.begin(), buffers.begin() + nb_buffers, [&](const auto& buffer)
        {
            if (compression.has_value())
            {
                if (!cache)
                {
                    throw std::invalid_argument("Compression type set but no cache is given.");
                }
                auto compressed_buffer_with_header = compress(compression.value(), std::span<const uint8_t>(buffer.data(), buffer.size()), cache.value().get());
                stream.write(compressed_buffer_with_header);
            }
            else
            {
                stream.write(buffer);
            }
            stream.add_padding();
        });

        std::for_each(arrow_proxy.children().begin(), arrow_proxy.children().end(), [&](const auto& child) {
            fill_body(child, stream, compression, cache);
        });
    }

    void generate_body(const sparrow::record_batch& record_batch, any_output_stream& stream,
                       std::optional<CompressionType> compression,
                       std::optional<std::reference_wrapper<CompressionCache>> cache)
    {
        std::ranges::for_each(record_batch.columns(), [&](const auto& column) {
            const auto& arrow_proxy = sparrow::detail::array_access::get_arrow_proxy(column);
            fill_body(arrow_proxy, stream, compression, cache);
        });
    }

    std::size_t calculate_schema_message_size(const sparrow::record_batch& record_batch)
    {
        // Build the schema message to get its exact size
        flatbuffers::FlatBufferBuilder schema_builder = get_schema_message_builder(record_batch);
        const flatbuffers::uoffset_t schema_len = schema_builder.GetSize();

        // Calculate total size:
        // - Continuation bytes (4)
        // - Message length prefix (4)
        // - FlatBuffer schema message data
        // - Padding to 8-byte alignment
        std::size_t total_size = continuation.size() + sizeof(uint32_t) + schema_len;
        return utils::align_to_8(total_size);
    }

    std::size_t calculate_record_batch_message_size(const sparrow::record_batch& record_batch,
                                                    std::optional<CompressionType> compression,
                                                    std::optional<std::reference_wrapper<CompressionCache>> cache)
    {
        // Build the record batch message to get its exact metadata size
        flatbuffers::FlatBufferBuilder record_batch_builder = get_record_batch_message_builder(record_batch, compression, cache);
        const flatbuffers::uoffset_t record_batch_len = record_batch_builder.GetSize();

        const std::size_t actual_body_size = static_cast<std::size_t>(calculate_body_size(record_batch, compression, cache));

        // Calculate total size:
        // - Continuation bytes (4)
        // - Message length prefix (4)
        // - FlatBuffer record batch metadata
        // - Padding after metadata to 8-byte alignment
        // - Body data (already aligned)
        std::size_t metadata_size = continuation.size() + sizeof(uint32_t) + record_batch_len;
        metadata_size = utils::align_to_8(metadata_size);

        return metadata_size + actual_body_size;
    }

    std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb)
    {
        std::vector<sparrow::data_type> dtypes;
        dtypes.reserve(rb.nb_columns());
        std::ranges::transform(
            rb.columns(),
            std::back_inserter(dtypes),
            [](const auto& col)
            {
                return col.data_type();
            }
        );
        return dtypes;
    }
}
