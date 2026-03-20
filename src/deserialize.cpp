#include "sparrow_ipc/deserialize.hpp"

#include <optional>
#include <unordered_map>

#include "array_deserializer.hpp"

#include <sparrow/c_interface.hpp>

#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // End-of-stream marker size in bytes
        constexpr size_t END_OF_STREAM_MARKER_SIZE = 8;

        void collect_dictionary_fields(
            const org::apache::arrow::flatbuf::Field& field,
            std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*>& dictionary_fields
        )
        {
            if (const auto* dictionary = field.dictionary(); dictionary != nullptr)
            {
                const int64_t dictionary_id = dictionary->id();
                if (dictionary_fields.contains(dictionary_id))
                {
                    throw std::runtime_error(
                        "Duplicate dictionary id " + std::to_string(dictionary_id)
                        + " declared in schema"
                    );
                }
                dictionary_fields[dictionary_id] = &field;
            }

            const auto* children = field.children();
            if (children == nullptr)
            {
                return;
            }

            for (const auto* child : *children)
            {
                if (child != nullptr)
                {
                    collect_dictionary_fields(*child, dictionary_fields);
                }
            }
        }

        sparrow::record_batch deserialize_dictionary_batch(
            const org::apache::arrow::flatbuf::DictionaryBatch& dictionary_batch,
            const encapsulated_message& encapsulated_message,
            const std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*>& dictionary_fields
        )
        {
            const org::apache::arrow::flatbuf::RecordBatch* dict_record_batch = dictionary_batch.data();
            if (dict_record_batch == nullptr)
            {
                throw std::runtime_error("DictionaryBatch message has null RecordBatch data");
            }

            const auto field_it = dictionary_fields.find(dictionary_batch.id());
            if (field_it == dictionary_fields.end())
            {
                throw std::runtime_error(
                    "Dictionary with id " + std::to_string(dictionary_batch.id())
                    + " is not declared in schema"
                );
            }

            const org::apache::arrow::flatbuf::Field* field = field_it->second;
            const std::string field_name = utils::get_fb_name(field, "__dictionary__", false);

            size_t buffer_index = 0;
            size_t node_index = 0;
            size_t variadic_counts_idx = 0;
            
            deserialization_context context(
                *dict_record_batch,
                encapsulated_message.body(),
                buffer_index,
                node_index,
                variadic_counts_idx
            );

            field_descriptor field_desc(
                dict_record_batch->length(),
                std::move(field_name),
                std::nullopt,
                utils::get_sparrow_flags(*field),
                false,
                *field,
                nullptr // dictionaries
            );

            auto values = array_deserializer::deserialize(
                context,
                field_desc
            );

            std::vector<std::string> names;
            names.reserve(1);
            names.emplace_back(field_name);
            std::vector<sparrow::array> arrays;
            arrays.reserve(1);
            arrays.emplace_back(std::move(values));
            return sparrow::record_batch(std::move(names), std::move(arrays));
        }

        std::vector<sparrow::array> get_empty_arrays_from_schema(
            const org::apache::arrow::flatbuf::Schema& schema,
            const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& fields_metadata,
            const dictionary_cache& dictionaries
        )
        {
            const size_t num_fields = schema.fields() == nullptr ? 0 : static_cast<size_t>(schema.fields()->size());
            if (num_fields == 0)
            {
                return {};
            }

            // Provide a dummy body that is non-empty so that reading offsets[0] works and is 0.
            static const std::vector<uint8_t> dummy_body(4096, 0);

            // Create a dummy record batch with 0 length and enough empty buffers/nodes
            flatbuffers::FlatBufferBuilder builder;

            // Provide many dummy nodes and buffers so that get_buffer and node increments don't fail
            std::vector<org::apache::arrow::flatbuf::FieldNode> dummy_nodes_data(512, org::apache::arrow::flatbuf::FieldNode(0, 0));
            auto nodes_offset = builder.CreateVectorOfStructs(dummy_nodes_data);

            std::vector<org::apache::arrow::flatbuf::Buffer> dummy_buffers_data;
            dummy_buffers_data.reserve(512);
            for (size_t i = 0; i < 512; ++i)
            {
                dummy_buffers_data.emplace_back(static_cast<int64_t>(i*8), 8);
            }
            auto buffers_offset = builder.CreateVectorOfStructs(dummy_buffers_data);

            auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(builder, 0, nodes_offset, buffers_offset);
            builder.Finish(record_batch_offset);
            const auto* dummy_record_batch = ::flatbuffers::GetRoot<org::apache::arrow::flatbuf::RecordBatch>(builder.GetBufferPointer());

            std::vector<sparrow::array> arrays;
            arrays.reserve(num_fields);

            for (size_t i = 0; i < num_fields; ++i)
            {
                const auto field = schema.fields()->Get(i);
                size_t buffer_index = 0;
                size_t node_index = 0;
                size_t variadic_counts_idx = 0;
                deserialization_context context(
                    *dummy_record_batch,
                    dummy_body,
                    buffer_index,
                    node_index,
                    variadic_counts_idx
                );

                // For schema generation, we DON'T decode dictionaries because we might not have them yet.
                // This will return the indices array, but with the correct names and metadata.
                bool decode_dicts = true;
                if (field && field->dictionary() != nullptr)
                {
                    if (!dictionaries.get_dictionary(field->dictionary()->id()).has_value())
                    {
                        decode_dicts = false;
                    }
                }

                field_descriptor field_desc(
                    0,
                    utils::get_fb_name(field),
                    fields_metadata[i],
                    utils::get_sparrow_flags(*field),
                    decode_dicts,
                    *field,
                    &dictionaries
                );

                try
                {
                    arrays.emplace_back(array_deserializer::deserialize(context, field_desc));
                }
                catch (...)
                {
                    // Fallback to null array if something goes wrong during dummy generation
                    arrays.emplace_back(sparrow::null_array(0));
                }
            }
            return arrays;
        }
    }

    std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata,
        const dictionary_cache& dictionaries
    )
    {
        const size_t num_fields = schema.fields() == nullptr ? 0 : static_cast<size_t>(schema.fields()->size());
        std::vector<sparrow::array> arrays;
        if (num_fields == 0)
        {
            return arrays;
        }
        arrays.reserve(num_fields);
        size_t field_idx = 0;
        size_t buffer_index = 0;
        size_t node_index = 0;
        size_t variadic_counts_idx = 0;

        deserialization_context context(
            record_batch,
            encapsulated_message.body(),
            buffer_index,
            node_index,
            variadic_counts_idx
        );

        for (const auto field : *(schema.fields()))
        {
            if (!field)
            {
                throw std::runtime_error("Invalid null field.");
            }

            field_descriptor field_desc(
                record_batch.length(),
                utils::get_fb_name(field),
                field_metadata[field_idx++],
                utils::get_sparrow_flags(*field),
                true,
                *field,
                &dictionaries
            );

            arrays.emplace_back(array_deserializer::deserialize(context, field_desc));
        }
        return arrays;
    }

    record_batch_stream deserialize_stream_to_record_batches(std::span<const uint8_t> data)
    {
        const org::apache::arrow::flatbuf::Schema* schema = nullptr;
        record_batch_stream result;
        std::vector<std::string> field_names;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;
        std::unordered_map<int64_t, const org::apache::arrow::flatbuf::Field*> dictionary_fields;
        dictionary_cache dictionaries;

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
                    dictionary_fields.clear();
                    field_names.clear();
                    fields_metadata.clear();
                    const size_t size = schema->fields() == nullptr
                                            ? 0
                                            : static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_metadata.reserve(size);
                    if (schema->fields() != nullptr)
                    {
                        for (const auto field : *(schema->fields()))
                        {
                            field_names.emplace_back(utils::get_fb_name(field, "_unnamed_"));
                            const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                                fb_custom_metadata = field ? field->custom_metadata() : nullptr;
                            std::optional<std::vector<sparrow::metadata_pair>>
                                metadata = fb_custom_metadata == nullptr
                                               ? std::nullopt
                                               : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
                            fields_metadata.push_back(std::move(metadata));

                            if (field != nullptr)
                            {
                                collect_dictionary_fields(*field, dictionary_fields);
                            }
                        }
                    }

                    // Populate result.schema immediately
                    auto empty_arrays = get_empty_arrays_from_schema(*schema, fields_metadata, dictionaries);
                    auto names_copy = field_names;
                    result.schema = sparrow::record_batch(std::move(names_copy), std::move(empty_arrays));
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
                        fields_metadata,
                        dictionaries
                    );
                    auto names_copy = field_names;
                    sparrow::record_batch sp_record_batch(std::move(names_copy), std::move(arrays));
                    // TODO in the case of non empty record batches, we should update the schema with the right one with dict and all (first record batch?)
                    // or maybe just nullopt? and make it optional instead of copying every time
                    result.schema = sp_record_batch;
                    result.batches.emplace_back(std::move(sp_record_batch));
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("DictionaryBatch encountered before Schema message.");
                    }

                    const auto* dictionary_batch = message->header_as_DictionaryBatch();
                    if (dictionary_batch == nullptr)
                    {
                        throw std::runtime_error("DictionaryBatch message header is null.");
                    }

                    auto dictionary_data = deserialize_dictionary_batch(
                        *dictionary_batch,
                        encapsulated_message,
                        dictionary_fields
                    );

                    dictionaries.store_dictionary(
                        dictionary_batch->id(),
                        std::move(dictionary_data),
                        dictionary_batch->isDelta()
                    );
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::Tensor:
                case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
                    throw std::runtime_error(
                        "Unsupported message type: Tensor or SparseTensor"
                    );
                default:
                    throw std::runtime_error("Unknown message header type.");
            }
            data = rest;
        }
        return result;
    }

    std::vector<sparrow::record_batch> deserialize_stream(std::span<const uint8_t> data)
    {
        return deserialize_stream_to_record_batches(data).batches;
    }
}
