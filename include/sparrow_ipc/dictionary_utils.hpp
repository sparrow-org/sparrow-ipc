#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include <sparrow/c_interface.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    struct SPARROW_IPC_API dictionary_metadata
    {
        std::optional<int64_t> id;
        bool is_ordered = false;
    };

    [[nodiscard]] SPARROW_IPC_API int64_t
    compute_fallback_dictionary_id(std::string_view field_name, size_t field_index);

    [[nodiscard]] SPARROW_IPC_API dictionary_metadata
    parse_dictionary_metadata(const ArrowSchema& schema);
}
