#pragma once

#include <cstdint>
#include <variant>
#include <span>

#include "sparrow_ipc/flatbuffers_generated/Message_generated.h"

namespace sparrow_ipc
{
    class encapsulated_message
    {
    public:

        encapsulated_message(std::span<const uint8_t> data);

        [[nodiscard]] const org::apache::arrow::flatbuf::Message* flat_buffer_message() const;

        [[nodiscard]] size_t metadata_length() const;

        [[nodiscard]] std::variant<
            const org::apache::arrow::flatbuf::Schema*,
            const org::apache::arrow::flatbuf::RecordBatch*,
            const org::apache::arrow::flatbuf::Tensor*,
            const org::apache::arrow::flatbuf::DictionaryBatch*,
            const org::apache::arrow::flatbuf::SparseTensor*>
        metadata() const;

        [[nodiscard]] const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
        custom_metadata() const;

        [[nodiscard]] size_t body_length() const;

        [[nodiscard]] std::span<const uint8_t> body() const;

        [[nodiscard]] size_t total_length() const;

        [[nodiscard]] std::span<const uint8_t> as_span() const;

    private:

        std::span<const uint8_t> m_data;
    };

    [[nodiscard]] std::pair<encapsulated_message, std::span<const uint8_t>>
    extract_encapsulated_message(std::span<const uint8_t> buf_ptr);
}
