#include "sparrow_ipc/deserialize.hpp"

#include "array_deserializer.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // End-of-stream marker size in bytes
        constexpr size_t END_OF_STREAM_MARKER_SIZE = 8;
    }

    /**
     * @brief Deserializes arrays from an Apache Arrow RecordBatch using the provided schema.
     *
     * This function processes each field in the schema and deserializes the corresponding
     * data from the RecordBatch into sparrow::array objects. It handles various Arrow data
     * types including primitive types (bool, integers, floating point), binary data, string
     * data, fixed-size binary data, and interval types.
     *
     * @param record_batch The Apache Arrow FlatBuffer RecordBatch containing the serialized data
     * @param schema The Apache Arrow FlatBuffer Schema defining the structure and types of the data
     * @param encapsulated_message The message containing the binary data buffers
     * @param field_metadata Metadata associated with each field in the schema
     *
     * @return std::vector<sparrow::array> A vector of deserialized arrays, one for each field in the schema
     *
     * @throws std::runtime_error If an unsupported data type, integer bit width, floating point precision,
     *         or interval unit is encountered
     *
     * @note The function maintains a buffer index that is incremented as it processes each field
     *       to correctly map data buffers to their corresponding arrays.
     */
    std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata
    )
    {
        static const array_deserializer arr_deserializer;

        const size_t num_fields = schema.fields() == nullptr ? 0 : static_cast<size_t>(schema.fields()->size());
        std::vector<sparrow::array> arrays;
        if (num_fields == 0)
        {
            return arrays;
        }
        arrays.reserve(num_fields);
        size_t field_idx = 0;
        size_t buffer_index = 0;
        size_t variadic_counts_idx = 0;
        for (const auto field : *(schema.fields()))
        {
            if (!field)
            {
                throw std::runtime_error("Invalid null field.");
            }
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata = field_metadata[field_idx++];
            const std::string name = field->name() == nullptr ? "" : field->name()->str();
            const bool nullable = field->nullable();
            const auto field_type = field->type_type();

            arrays.emplace_back(arr_deserializer.deserialize(
                record_batch,
                encapsulated_message.body(),
                name,
                metadata,
                nullable,
                buffer_index,
                variadic_counts_idx,
                *field
            ));
        }
        return arrays;
    }

    std::vector<sparrow::record_batch> deserialize_stream(std::span<const uint8_t> data)
    {
        const org::apache::arrow::flatbuf::Schema* schema = nullptr;
        std::vector<sparrow::record_batch> record_batches;
        std::vector<std::string> field_names;
        std::vector<bool> fields_nullable;
        std::vector<sparrow::data_type> field_types;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;

        while (!data.empty())
        {
            // Check for end-of-stream marker
            if (data.size() >= END_OF_STREAM_MARKER_SIZE
                && is_end_of_stream(data.subspan(0, END_OF_STREAM_MARKER_SIZE)))
            {
                break;
            }

            const auto [encapsulated_message, rest] = extract_encapsulated_message(data);
            const org::apache::arrow::flatbuf::Message* message = encapsulated_message.flat_buffer_message();

            if (message == nullptr)
            {
                throw std::invalid_argument("Extracted flatbuffers message is null.");
            }

            switch (message->header_type())
            {
                case org::apache::arrow::flatbuf::MessageHeader::Schema:
                {
                    schema = message->header_as_Schema();
                    const size_t size = schema->fields() == nullptr
                                            ? 0
                                            : static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_nullable.reserve(size);
                    fields_metadata.reserve(size);
                    if (schema->fields() == nullptr)
                    {
                        break;
                    }
                    for (const auto field : *(schema->fields()))
                    {
                        if (field != nullptr && field->name() != nullptr)
                        {
                            field_names.emplace_back(field->name()->str());
                        }
                        else
                        {
                            field_names.emplace_back("_unnamed_");
                        }
                        fields_nullable.push_back(field->nullable());
                        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                            fb_custom_metadata = field->custom_metadata();
                        std::optional<std::vector<sparrow::metadata_pair>>
                            metadata = fb_custom_metadata == nullptr
                                           ? std::nullopt
                                           : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
                        fields_metadata.push_back(std::move(metadata));
                    }
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("RecordBatch encountered before Schema message.");
                    }
                    const auto* record_batch = message->header_as_RecordBatch();
                    if (record_batch == nullptr)
                    {
                        throw std::runtime_error("RecordBatch message header is null.");
                    }
                    std::vector<sparrow::array> arrays = get_arrays_from_record_batch(
                        *record_batch,
                        *schema,
                        encapsulated_message,
                        fields_metadata
                    );
                    auto names_copy = field_names;
                    sparrow::record_batch sp_record_batch(std::move(names_copy), std::move(arrays));
                    record_batches.emplace_back(std::move(sp_record_batch));
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::Tensor:
                case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
                case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
                    throw std::runtime_error(
                        "Unsupported message type: Tensor, DictionaryBatch, or SparseTensor"
                    );
                default:
                    throw std::runtime_error("Unknown message header type.");
            }
            data = rest;
        }
        return record_batches;
    }
}
