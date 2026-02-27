#pragma once

#include <sparrow/array_api.hpp>
#include <sparrow/arrow_interface/arrow_array.hpp>

#include "Message_generated.h"
#include "Schema_generated.h"

#include "sparrow_ipc/deserialization_context.hpp"
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
        deserialization_context& context,
        const field_descriptor& field_desc
    );
}
