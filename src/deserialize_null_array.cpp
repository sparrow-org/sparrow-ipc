#include "sparrow_ipc/deserialize_null_array.hpp"

#include <unordered_set>
#include <sparrow/null_array.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"

namespace sparrow_ipc
{
    sparrow::null_array deserialize_null_array(
        const org::apache::arrow::flatbuf::RecordBatch&,
        std::span<const uint8_t>,
        const int64_t length,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        bool nullable,
        size_t&
    )
    {
        const std::string_view format = sparrow::data_type_to_format(
            sparrow::detail::get_data_type_from_array<sparrow::null_array>::get()
        );

        // Set up flags based on nullable
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags;
        if (nullable)
        {
            flags = std::unordered_set<sparrow::ArrowFlag>{sparrow::ArrowFlag::NULLABLE};
        }

        ArrowSchema schema = make_non_owning_arrow_schema(format, name, metadata, flags, 0, nullptr, nullptr);
        std::vector<arrow_array_private_data::optionally_owned_buffer> buffers;
        ArrowArray array = make_arrow_array<arrow_array_private_data>(
            length,
            length,
            0,
            0,
            nullptr,
            nullptr,
            std::move(buffers)
        );
        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::null_array{std::move(ap)};
    }
}
