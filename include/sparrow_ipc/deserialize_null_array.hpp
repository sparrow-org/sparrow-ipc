#include <sparrow/null_array.hpp>

#include "Message_generated.h"

namespace sparrow_ipc
{
    [[nodiscard]] sparrow::null_array deserialize_null_array(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t& buffer_index
    );
}
