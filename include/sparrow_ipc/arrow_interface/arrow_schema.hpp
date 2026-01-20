#pragma once

#include <optional>
#include <unordered_set>

#include <sparrow/c_interface.hpp>
#include <sparrow/utils/contracts.hpp>
#include <sparrow/utils/metadata.hpp>

#include "sparrow_ipc/arrow_interface/arrow_schema/private_data.hpp"
#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    SPARROW_IPC_API void release_non_owning_arrow_schema(ArrowSchema* schema);

    template <sparrow::input_metadata_container M = std::vector<sparrow::metadata_pair>>
    void fill_non_owning_arrow_schema(
        ArrowSchema& schema,
        std::string_view format,
        std::string_view name,
        std::optional<M> metadata,
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags,
        size_t children_count,
        ArrowSchema** children,
        ArrowSchema* dictionary
    )
    {
        schema.flags = 0;
        if (flags.has_value())
        {
            for (const auto& flag : *flags)
            {
                schema.flags |= static_cast<int64_t>(flag);
            }
        }
        schema.n_children = static_cast<int64_t>(children_count);

        std::optional<std::string> metadata_str = metadata.has_value()
                                                      ? std::make_optional(
                                                            sparrow::get_metadata_from_key_values(*metadata)
                                                        )
                                                      : std::nullopt;

        schema.private_data = new non_owning_arrow_schema_private_data(format, name, std::move(metadata_str));

        const auto private_data = static_cast<non_owning_arrow_schema_private_data*>(schema.private_data);
        schema.format = private_data->format_ptr();
        schema.name = private_data->name_ptr();
        schema.metadata = private_data->metadata_ptr();
        schema.children = children;
        schema.dictionary = dictionary;
        schema.release = release_non_owning_arrow_schema;
    }

    template <sparrow::input_metadata_container M = std::vector<sparrow::metadata_pair>>
    [[nodiscard]] ArrowSchema make_non_owning_arrow_schema(
        std::string_view format,
        std::string_view name,
        std::optional<M> metadata,
        std::optional<std::unordered_set<sparrow::ArrowFlag>> flags,
        size_t children_count,
        ArrowSchema** children,
        ArrowSchema* dictionary
    )
    {
        ArrowSchema schema{};
        fill_non_owning_arrow_schema(schema, format, name, metadata, flags, children_count, children, dictionary);
        return schema;
    }
}
