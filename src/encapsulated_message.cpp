#include "detail/encapsulated_message.hpp"

#include <stdexcept>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    encapsulated_message::encapsulated_message(std::span<const uint8_t> data)
        : m_data(data)
    {
    }

    const org::apache::arrow::flatbuf::Message* encapsulated_message::flat_buffer_message() const
    {
        const uint8_t* message_ptr = m_data.data() + (sizeof(uint32_t) * 2);  // 4 bytes continuation + 4
                                                                              // bytes metadata size
        return org::apache::arrow::flatbuf::GetMessage(message_ptr);
    }

    size_t encapsulated_message::metadata_length() const
    {
        return *(reinterpret_cast<const uint32_t*>(m_data.data() + sizeof(uint32_t)));
        
    }

    [[nodiscard]] std::variant<
        const org::apache::arrow::flatbuf::Schema*,
        const org::apache::arrow::flatbuf::RecordBatch*,
        const org::apache::arrow::flatbuf::Tensor*,
        const org::apache::arrow::flatbuf::DictionaryBatch*,
        const org::apache::arrow::flatbuf::SparseTensor*>
    encapsulated_message::metadata() const
    {
        const auto schema_message = flat_buffer_message();
        switch (schema_message->header_type())
        {
            case org::apache::arrow::flatbuf::MessageHeader::Schema:
            {
                return schema_message->header_as_Schema();
            }
            case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
            {
                return schema_message->header_as_RecordBatch();
            }
            case org::apache::arrow::flatbuf::MessageHeader::Tensor:
            {
                return schema_message->header_as_Tensor();
            }
            case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
            {
                return schema_message->header_as_DictionaryBatch();
            }
            case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
            {
                return schema_message->header_as_SparseTensor();
            }
            default:
                throw std::runtime_error("Unknown message header type.");
        }
    }

    const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
    encapsulated_message::custom_metadata() const
    {
        return flat_buffer_message()->custom_metadata();
    }

    size_t encapsulated_message::body_length() const
    {
        return static_cast<size_t>(flat_buffer_message()->bodyLength());
    }

    std::span<const uint8_t> encapsulated_message::body() const
    {
        const size_t offset = sizeof(uint32_t) * 2  // 4 bytes continuation + 4 bytes metadata size
                              + metadata_length();
        const size_t padded_offset = utils::align_to_8(offset);  // Round up to 8-byte boundary
        if (m_data.size() < padded_offset + body_length())
        {
            throw std::runtime_error("Data size is smaller than expected from metadata.");
        }
        return m_data.subspan(padded_offset, body_length());
    }

    size_t encapsulated_message::total_length() const
    {
        const size_t offset = sizeof(uint32_t) * 2  // 4 bytes continuation + 4 bytes metadata size
                              + metadata_length();
        const size_t padded_offset = utils::align_to_8(offset);  // Round up to 8-byte boundary
        return padded_offset + body_length();
    }

    std::span<const uint8_t> encapsulated_message::as_span() const
    {
        return m_data;
    }

    std::pair<encapsulated_message, std::span<const uint8_t>>
    extract_encapsulated_message(std::span<const uint8_t> data)
    {
        if (data.size() < 8)
        {
            throw std::invalid_argument("Buffer is too small to contain a valid message.");
        }
        const std::span<const uint8_t> continuation_span = data.subspan(0, 4);
        if (!is_continuation(continuation_span))
        {
            throw std::runtime_error("Buffer should start with continuation bytes, expected a valid message.");
        }
        encapsulated_message message(data);
        std::span<const uint8_t> rest = data.subspan(message.total_length());
        return {std::move(message), std::move(rest)};
    }
}
