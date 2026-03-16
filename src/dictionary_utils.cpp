#include "sparrow_ipc/dictionary_utils.hpp"

#include <functional>
#include <string>

#include <sparrow/utils/metadata.hpp>

namespace sparrow_ipc
{
    namespace
    {
        constexpr uint64_t hash_combine_golden_ratio_64 = 0x9e3779b97f4a7c15ULL;
    }

    int64_t compute_fallback_dictionary_id(std::string_view field_name, size_t field_index)
    {
        const auto field_hash = std::hash<std::string_view>{}(field_name);
        const auto index_hash = std::hash<size_t>{}(field_index + 1);
        const auto combined = field_hash
                              ^ (index_hash + hash_combine_golden_ratio_64 + (field_hash << 6)
                                 + (field_hash >> 2));
        return static_cast<int64_t>(combined);
    }

    dictionary_metadata parse_dictionary_metadata(const ArrowSchema& schema)
    {
        dictionary_metadata metadata;
        metadata.is_ordered = (schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::DICTIONARY_ORDERED)) != 0;

        if (schema.metadata == nullptr)
        {
            return metadata;
        }

        const auto metadata_view = sparrow::key_value_view(schema.metadata);
        for (const auto& [key, value] : metadata_view)
        {
            if (key == "ARROW:dictionary:id") // TODO check if this is from spec?
            {
                try
                {
                    metadata.id = std::stoll(std::string(value));
                }
                catch (const std::exception& e)
                {
                    throw std::runtime_error(
                        "Failed to parse dictionary ID from metadata value '" + std::string(value)
                        + "': " + e.what()
                    );
                }
            }
        }

        return metadata;
    }
}
