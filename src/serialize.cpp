#include "sparrow_ipc/serialize.hpp"

#include <cstdint>
#include <optional>

#include "detail/flatbuffer_utils.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    void common_serialize(const flatbuffers::FlatBufferBuilder& builder, any_output_stream& stream)
    {
        stream.write(continuation);
        const flatbuffers::uoffset_t size = builder.GetSize();
        const int32_t size_with_padding = utils::align_to_8(static_cast<int32_t>(size));
        const std::span<const uint8_t> size_span(
            reinterpret_cast<const uint8_t*>(&size_with_padding),
            sizeof(int32_t)
        );
        stream.write(size_span);
        stream.write(std::span(builder.GetBufferPointer(), size));
        stream.add_padding();
    }

    void serialize_schema_message(const sparrow::record_batch& record_batch, any_output_stream& stream)
    {
        common_serialize(get_schema_message_builder(record_batch), stream);
    }

    serialized_record_batch_info serialize_record_batch(
        const sparrow::record_batch& record_batch,
        any_output_stream& stream,
        std::optional<CompressionType> compression,
        std::optional<std::reference_wrapper<CompressionCache>> cache
    )
    {
        // Build and serialize metadata
        flatbuffers::FlatBufferBuilder builder = get_record_batch_message_builder(record_batch, compression, cache);

        // Calculate metadata length for the Block in the footer
        // According to Arrow spec, metadata_length must be a multiple of 8.
        // The encapsulated message format is:
        //   - continuation (4 bytes)
        //   - size prefix (4 bytes)
        //   - flatbuffer metadata (flatbuffer_size bytes)
        //   - padding to 8-byte boundary
        //
        // Arrow's WriteMessage returns metadata_length = align_to_8(8 + flatbuffer_size)
        // which INCLUDES the continuation bytes.

        // Write metadata
        common_serialize(builder, stream);

        // Track position before body to calculate body length
        const size_t body_start = stream.size();

        // Write body
        generate_body(record_batch, stream, compression, cache);

        const auto body_length = static_cast<int64_t>(stream.size() - body_start);
        const flatbuffers::uoffset_t flatbuffer_size = builder.GetSize();
        const size_t prefix_size = continuation.size() + sizeof(uint32_t);  // 8 bytes
        const auto metadata_length = static_cast<int32_t>(utils::align_to_8(prefix_size + flatbuffer_size));
        return {.metadata_length = metadata_length, .body_length = body_length};
    }

    serialized_record_batch_info serialize_dictionary_batch(
        int64_t dictionary_id,
        const sparrow::record_batch& record_batch,
        bool is_delta,
        any_output_stream& stream,
        std::optional<CompressionType> compression,
        std::optional<std::reference_wrapper<CompressionCache>> cache
    )
    {
        // Build and serialize metadata
        flatbuffers::FlatBufferBuilder builder = get_dictionary_batch_message_builder(
            dictionary_id,
            record_batch,
            is_delta,
            compression,
            cache
        );

        // Write metadata (continuation + length prefix + flatbuffer + padding)
        common_serialize(builder, stream);

        // Track position before body to calculate body length
        const size_t body_start = stream.size();

        // Write body (same as record batch)
        generate_body(record_batch, stream, compression, cache);

        const auto body_length = static_cast<int64_t>(stream.size() - body_start);
        const flatbuffers::uoffset_t flatbuffer_size = builder.GetSize();
        const size_t prefix_size = continuation.size() + sizeof(uint32_t);  // 8 bytes
        const auto metadata_length = static_cast<int32_t>(utils::align_to_8(prefix_size + flatbuffer_size));
        return {.metadata_length = metadata_length, .body_length = body_length};
    }
}
