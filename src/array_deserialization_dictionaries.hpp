#pragma once

#include <sparrow/array_api.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>

#include "Message_generated.h"
#include "Schema_generated.h"
#include "sparrow_ipc/dictionary_cache.hpp"

namespace sparrow_ipc
{
    void resolve_child_dictionaries_in_place(
        ArrowArray& array,
        ArrowSchema& schema,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    );

    void attach_dictionary_to_field_if_needed(
        ArrowArray& array,
        ArrowSchema& schema,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    );

    sparrow::array apply_dictionary_encoding(
        sparrow::array index_array,
        const org::apache::arrow::flatbuf::Field& field,
        const dictionary_cache& dictionaries
    );

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
    );
}