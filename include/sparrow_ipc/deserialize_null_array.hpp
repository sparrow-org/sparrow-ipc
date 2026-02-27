#include <sparrow/null_array.hpp>

#include "sparrow_ipc/deserialization_context.hpp"

namespace sparrow_ipc
{
    [[nodiscard]] sparrow::null_array deserialize_null_array(
        deserialization_context& context,
        const field_descriptor& field_desc
    );
}
