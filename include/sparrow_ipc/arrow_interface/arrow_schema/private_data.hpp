
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    class non_owning_arrow_schema_private_data
    {
    public:

        SPARROW_IPC_API non_owning_arrow_schema_private_data(
            std::string_view format,
            std::string_view name,
            std::optional<std::string> metadata
        );

        [[nodiscard]] SPARROW_IPC_API const char* format_ptr() const noexcept;
        [[nodiscard]] SPARROW_IPC_API const char* name_ptr() const noexcept;
        [[nodiscard]] SPARROW_IPC_API const char* metadata_ptr() const noexcept;

    private:

        std::string m_format;
        std::string m_name;
        std::optional<std::string> m_metadata;
    };
}
