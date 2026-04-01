#include "sparrow_ipc/deserialize.hpp"

#include <optional>
#include <unordered_map>

#include "array_deserializer.hpp"

#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>
#include <sparrow/arrow_interface/arrow_schema.hpp>
#include <sparrow/c_interface.hpp>
#include <sparrow/utils/mp_utils.hpp>
#include <sparrow/utils/repeat_container.hpp>

#include "sparrow_ipc/deserialize_utils.hpp"
#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // End-of-stream marker size in bytes
        constexpr size_t end_of_stream_marker_size = 8;

        [[nodiscard]] std::vector<sparrow::buffer<uint8_t>> make_empty_buffers(const ArrowSchema& schema)
        {
            switch (sparrow::format_to_data_type(schema.format))
            {
                case sparrow::data_type::NA:
                case sparrow::data_type::RUN_ENCODED:
                    return {};
                case sparrow::data_type::BOOL:
                case sparrow::data_type::UINT8:
                case sparrow::data_type::INT8:
                case sparrow::data_type::UINT16:
                case sparrow::data_type::INT16:
                case sparrow::data_type::UINT32:
                case sparrow::data_type::INT32:
                case sparrow::data_type::UINT64:
                case sparrow::data_type::INT64:
                case sparrow::data_type::HALF_FLOAT:
                case sparrow::data_type::FLOAT:
                case sparrow::data_type::DOUBLE:
                case sparrow::data_type::DATE_DAYS:
                case sparrow::data_type::DATE_MILLISECONDS:
                case sparrow::data_type::TIMESTAMP_SECONDS:
                case sparrow::data_type::TIMESTAMP_MILLISECONDS:
                case sparrow::data_type::TIMESTAMP_MICROSECONDS:
                case sparrow::data_type::TIMESTAMP_NANOSECONDS:
                case sparrow::data_type::TIME_SECONDS:
                case sparrow::data_type::TIME_MILLISECONDS:
                case sparrow::data_type::TIME_MICROSECONDS:
                case sparrow::data_type::TIME_NANOSECONDS:
                case sparrow::data_type::DURATION_SECONDS:
                case sparrow::data_type::DURATION_MILLISECONDS:
                case sparrow::data_type::DURATION_MICROSECONDS:
                case sparrow::data_type::DURATION_NANOSECONDS:
                case sparrow::data_type::INTERVAL_MONTHS:
                case sparrow::data_type::INTERVAL_DAYS_TIME:
                case sparrow::data_type::INTERVAL_MONTHS_DAYS_NANOSECONDS:
                case sparrow::data_type::DECIMAL32:
                case sparrow::data_type::DECIMAL64:
                case sparrow::data_type::DECIMAL128:
                case sparrow::data_type::DECIMAL256:
                case sparrow::data_type::FIXED_WIDTH_BINARY:
                    return {make_zeroed_buffer(0), make_zeroed_buffer(0)};
                case sparrow::data_type::FIXED_SIZED_LIST:
                case sparrow::data_type::STRUCT:
                    return {make_zeroed_buffer(0)};
                case sparrow::data_type::STRING:
                case sparrow::data_type::BINARY:
                    return {make_zeroed_buffer(0), make_zero_offset_buffer<std::int32_t>(), make_zeroed_buffer(0)};
                case sparrow::data_type::LIST:
                case sparrow::data_type::MAP:
                    return {make_zeroed_buffer(0), make_zero_offset_buffer<std::int32_t>()};
                case sparrow::data_type::LARGE_STRING:
                case sparrow::data_type::LARGE_BINARY:
                    return {make_zeroed_buffer(0), make_zero_offset_buffer<std::int64_t>(), make_zeroed_buffer(0)};
                case sparrow::data_type::LARGE_LIST:
                    return {make_zeroed_buffer(0), make_zero_offset_buffer<std::int64_t>()};
                case sparrow::data_type::LIST_VIEW:
                    return {make_zeroed_buffer(0), make_typed_zeroed_buffer<std::int32_t>(0),
                            make_typed_zeroed_buffer<std::int32_t>(0)};
                case sparrow::data_type::LARGE_LIST_VIEW:
                    return {make_zeroed_buffer(0), make_typed_zeroed_buffer<std::int64_t>(0),
                            make_typed_zeroed_buffer<std::int64_t>(0)};
                case sparrow::data_type::STRING_VIEW:
                case sparrow::data_type::BINARY_VIEW:
                    return {make_zeroed_buffer(0), make_zeroed_buffer(0), make_typed_zeroed_buffer<std::int64_t>(0)};
                case sparrow::data_type::SPARSE_UNION:
                    return {make_zeroed_buffer(0)};
                case sparrow::data_type::DENSE_UNION:
                    return {make_zeroed_buffer(0), make_typed_zeroed_buffer<std::int32_t>(0)};
            }

            sparrow::mpl::unreachable();
        }

        ArrowArray make_empty_arrow_array(const ArrowSchema& schema)
        {
            std::vector<ArrowArray> child_arrays;
            child_arrays.reserve(static_cast<std::size_t>(schema.n_children));
            for (int64_t i = 0; i < schema.n_children; ++i)
            {
                child_arrays.push_back(make_empty_arrow_array(*schema.children[i]));
            }

            ArrowArray* dictionary = nullptr;
            if (schema.dictionary != nullptr)
            {
                dictionary = new ArrowArray(make_empty_arrow_array(*schema.dictionary));
            }

            const auto child_count = child_arrays.size();
            return sparrow::make_arrow_array(
                0,
                0,
                0,
                make_empty_buffers(schema),
                make_owned_children(std::move(child_arrays)),
                sparrow::repeat_view<bool>(true, child_count),
                dictionary,
                dictionary != nullptr
            );
        }

        ArrowSchema to_arrow_schema(
            const org::apache::arrow::flatbuf::Field& field,
            bool include_dictionary,
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata_override = std::nullopt
        )
        {
            // For dictionary values, there is no metadata
            const std::string name = include_dictionary ? utils::get_fb_name(&field) : "__dictionary__";
            auto metadata = include_dictionary
                                ? (metadata_override.has_value() ? metadata_override
                                                                 : (field.custom_metadata()
                                                                        ? std::make_optional(to_sparrow_metadata(*field.custom_metadata()))
                                                                        : std::nullopt))
                                : std::nullopt;

            auto flags = utils::get_sparrow_flags(field);

            if (include_dictionary && field.dictionary())
            {
                const auto* dict_meta = field.dictionary();
                const std::string index_format = get_index_format(dict_meta->indexType());

                // Recurse for values, but don't include dictionary again.
                // This call will skip this if block and go to the format/children logic below,
                // effectively putting the children into the dictionary schema.
                ArrowSchema value_schema = to_arrow_schema(field, false);

                ArrowSchema* dictionary = new ArrowSchema(std::move(value_schema));

                return sparrow::make_arrow_schema(
                    index_format,
                    name,
                    metadata,
                    flags,
                    nullptr,
                    std::vector<bool>{},
                    dictionary,
                    true
                );
            }

            const std::string format = get_format(field);
            std::vector<ArrowSchema> child_schemas;
            if (field.children() && field.children()->size() > 0)
            {
                const auto child_count = field.children()->size();
                child_schemas.reserve(child_count);
                for (size_t i = 0; i < child_count; ++i)
                {
                    const auto* child = field.children()->Get(i);
                    if (child != nullptr)
                    {
                        child_schemas.push_back(to_arrow_schema(*child, true));
                    }
                }
            }

            const auto child_count = child_schemas.size();
            return sparrow::make_arrow_schema(
                format,
                name,
                metadata,
                flags,
                make_owned_children(std::move(child_schemas)),
                sparrow::repeat_view<bool>(true, child_count),
                nullptr,
                false
            );
        }

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

            std::vector<sparrow::array> arrays;
            arrays.reserve(num_fields);

            for (size_t i = 0; i < num_fields; ++i)
            {
                const auto* field = schema.fields()->Get(i);
                if (field == nullptr)
                {
                    continue;
                }

                bool include_dictionary = false;
                if (field->dictionary() != nullptr)
                {
                    if (dictionaries.get_dictionary(field->dictionary()->id()).has_value())
                    {
                        include_dictionary = true;
                    }
                }

                ArrowSchema arrow_schema = to_arrow_schema(*field, include_dictionary, fields_metadata[i]);
                ArrowArray arrow_array = make_empty_arrow_array(arrow_schema);
                arrays.emplace_back(std::move(arrow_array), std::move(arrow_schema));
            }
            return arrays;
        }
    }

    /**
     * @brief Deserializes arrays from an Apache Arrow RecordBatch using the provided schema and dictionary cache.
     *
     * This function processes each field in the schema and deserializes the corresponding
     * data from the RecordBatch into sparrow::array objects. It handles various Arrow data
     * types including primitive types (bool, integers, floating point), binary data, string
     * data, fixed-size binary data, interval types, and dictionary-encoded types.
     *
     * @param record_batch The Apache Arrow FlatBuffer RecordBatch containing the serialized data.
     * @param schema The Apache Arrow FlatBuffer Schema defining the structure and types of the data.
     * @param encapsulated_message The message containing the binary data buffers.
     * @param field_metadata Metadata associated with each field in the schema.
     * @param dictionaries The cache containing pre-deserialized dictionary arrays.
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
            if (data.size() >= end_of_stream_marker_size
                && is_end_of_stream(data.subspan(0, end_of_stream_marker_size)))
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
                    if (result.batches.empty())
                    {
                        // TODO maybe use slice_view to avoid deep copy of the whole record batch?
                        // Set schema to the first record batch if present
                        result.schema = sp_record_batch;
                    }
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

    record_batch_stream deserialize_file(std::span<const uint8_t> data)
    {
        // Validate minimum file size
        // Magic (8) + Footer size (4) + Magic (6) = 18 bytes minimum
        constexpr size_t min_file_size = 18;
        if (data.size() < min_file_size)
        {
            throw std::runtime_error("File is too small to be a valid Arrow file");
        }

        // Check magic bytes at the beginning
        if (!is_arrow_file_magic(data.subspan(0, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at start");
        }

        // Check magic bytes at the end
        const size_t trailing_magic_offset = data.size() - arrow_file_magic_size;
        if (!is_arrow_file_magic(data.subspan(trailing_magic_offset, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at end");
        }

        // Read footer size (4 bytes before the trailing magic)
        const size_t footer_size_offset = data.size() - arrow_file_magic_size - sizeof(int32_t);
        int32_t footer_size = 0;
        std::memcpy(&footer_size, data.data() + footer_size_offset, sizeof(int32_t));

        if (footer_size <= 0 || static_cast<size_t>(footer_size) > data.size() - min_file_size)
        {
            throw std::runtime_error("Invalid footer size in Arrow file");
        }

        // Calculate the end of the stream data (before footer)
        const size_t footer_offset = footer_size_offset - footer_size;

        // Extract the stream portion (from after header magic to before footer)
        // Stream data starts after the 8-byte header magic
        const size_t stream_start = arrow_file_header_magic.size();
        const size_t stream_length = footer_offset - stream_start;

        auto stream_data = data.subspan(stream_start, stream_length);

        // Use deserialize_stream_to_record_batches to parse the stream format data
        // This handles schema message, record batches, and end-of-stream marker
        return deserialize_stream_to_record_batches(stream_data);
    }
}
