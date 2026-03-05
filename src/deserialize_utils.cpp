#include "sparrow_ipc/deserialize_utils.hpp"

#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>

#include "compression_impl.hpp"

namespace sparrow_ipc::utils
{
    std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        std::span<const uint8_t> validity_buffer_span,
        const int64_t length
    )
    {
        if (validity_buffer_span.empty())
        {
            return {nullptr, 0};
        }
        auto ptr = const_cast<uint8_t*>(validity_buffer_span.data());
        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
            ptr,
            static_cast<size_t>(length)
        };
        return {ptr, bitmap_view.null_count()};
    }

    std::span<const uint8_t> get_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index
    )
    {
        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        if (body.size() < (buffer_metadata->offset() + buffer_metadata->length()))
        {
            throw std::runtime_error("Buffer metadata exceeds body size");
        }
        return body.subspan(buffer_metadata->offset(), buffer_metadata->length());
    }

    std::variant<sparrow::buffer<uint8_t>, std::span<const uint8_t>> get_decompressed_buffer(
        std::span<const uint8_t> buffer_span,
        const org::apache::arrow::flatbuf::BodyCompression* compression
    )
    {
        if (compression && !buffer_span.empty())
        {
            return decompress(sparrow_ipc::details::from_fb_compression_type(compression->codec()), buffer_span);
        }
        else
        {
            return buffer_span;
        }
    }
}
