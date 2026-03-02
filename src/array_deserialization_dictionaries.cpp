#include "array_deserialization_dictionaries.hpp"

#include <algorithm>
#include <string>

#include "array_deserializer.hpp"

namespace sparrow_ipc
{
    void attach_dictionary_to_field_if_needed(
        ArrowArray& array,
        ArrowSchema& schema,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    )
    {
        if (field.dictionary() == nullptr || array.dictionary != nullptr || schema.dictionary != nullptr)
        {
            return;
        }

        const auto dictionary_id = field.dictionary()->id();
        const auto dictionary_batch = dictionaries.get_dictionary(dictionary_id);
        if (!dictionary_batch.has_value())
        {
            throw std::runtime_error(
                "Dictionary with id " + std::to_string(dictionary_id)
                + " not found when decoding dictionary-encoded field"
            );
        }

        const auto& dictionary_columns = dictionary_batch->get().columns();
        if (dictionary_columns.size() != 1)
        {
            throw std::runtime_error("Dictionary batch must have exactly one column");
        }

        const auto& dictionary_proxy = sparrow::detail::array_access::get_arrow_proxy(dictionary_columns.front());
        auto dictionary_arrow_array = sparrow::copy_array(dictionary_proxy.array(), dictionary_proxy.schema());
        auto dictionary_arrow_schema = sparrow::copy_schema(dictionary_proxy.schema());

        resolve_child_dictionaries_in_place(dictionary_arrow_array, dictionary_arrow_schema, field, dictionaries);

        array.dictionary = new ArrowArray(std::move(dictionary_arrow_array));
        schema.dictionary = new ArrowSchema(std::move(dictionary_arrow_schema));
    }

    void resolve_child_dictionaries_in_place(
        ArrowArray& array,
        ArrowSchema& schema,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    )
    {
        const auto* children = field.children();
        if (children == nullptr || array.children == nullptr || schema.children == nullptr)
        {
            return;
        }

        const auto child_count = std::min(
            static_cast<size_t>(children->size()),
            static_cast<size_t>(array.n_children)
        );
        for (size_t i = 0; i < child_count; ++i)
        {
            const auto* child_field = children->Get(i);
            ArrowArray* child_array = array.children[i];
            ArrowSchema* child_schema = schema.children[i];
            if (child_field == nullptr || child_array == nullptr || child_schema == nullptr)
            {
                continue;
            }

            attach_dictionary_to_field_if_needed(*child_array, *child_schema, *child_field, dictionaries);
            resolve_child_dictionaries_in_place(*child_array, *child_schema, *child_field, dictionaries);
        }
    }

    sparrow::array apply_dictionary_encoding(
        sparrow::array index_array,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    )
    {
        if (field.dictionary() == nullptr)
        {
            return index_array;
        }

        auto [index_arrow_array, index_arrow_schema] = sparrow::extract_arrow_structures(std::move(index_array));
        attach_dictionary_to_field_if_needed(index_arrow_array, index_arrow_schema, field, dictionaries);

        return {std::move(index_arrow_array), std::move(index_arrow_schema)};
    }

    sparrow::array deserialize_dictionary_indices(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const std::span<const uint8_t>& body,
        int64_t length,
        const std::string& name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index,
        size_t& node_index,
        const org::apache::arrow::flatbuf::Field& field
    )
    {
        const auto* dictionary = field.dictionary();
        if (dictionary == nullptr || dictionary->indexType() == nullptr)
        {
            throw std::runtime_error("Dictionary-encoded field is missing indexType");
        }

        ++node_index;

        const auto* index_type = dictionary->indexType();
        const auto bit_width = index_type->bitWidth();
        const bool is_signed = index_type->is_signed();

        const auto deserialize_for = [&]<class T>() -> sparrow::array
        {
            return sparrow::array(
                deserialize_primitive_array<T>(record_batch, body, length, name, metadata, nullable, buffer_index)
            );
        };

        switch (bit_width)
        {
            case sizeof(int8_t) * 8:
                return is_signed ? deserialize_for.template operator()<int8_t>()
                                 : deserialize_for.template operator()<uint8_t>();
            case sizeof(int16_t) * 8:
                return is_signed ? deserialize_for.template operator()<int16_t>()
                                 : deserialize_for.template operator()<uint16_t>();
            case sizeof(int32_t) * 8:
                return is_signed ? deserialize_for.template operator()<int32_t>()
                                 : deserialize_for.template operator()<uint32_t>();
            case sizeof(int64_t) * 8:
                return is_signed ? deserialize_for.template operator()<int64_t>()
                                 : deserialize_for.template operator()<uint64_t>();
            default:
                throw std::runtime_error("Unsupported dictionary index bit width: " + std::to_string(bit_width));
        }
    }
}