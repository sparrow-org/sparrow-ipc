#pragma once
#include <concepts>
#include <cstdint>
#include <span>
#include <variant>
#include <vector>

#include <sparrow/buffer/buffer.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    template <typename T>
    concept ArrowPrivateData = requires(T& t) {
        { t.buffers_ptrs() } -> std::same_as<const void**>;
        { t.n_buffers() } -> std::convertible_to<std::size_t>;
    };

    class arrow_array_private_data
    {
    public:

        using optionally_owned_buffer = std::variant<sparrow::buffer<uint8_t>, std::span<const uint8_t>>;
        SPARROW_IPC_API explicit arrow_array_private_data(std::vector<optionally_owned_buffer>&& buffers);

        [[nodiscard]] SPARROW_IPC_API const void** buffers_ptrs() noexcept;
        [[nodiscard]] SPARROW_IPC_API std::size_t n_buffers() const noexcept;

    private:

        std::vector<optionally_owned_buffer> m_buffers;
        std::vector<const void*> m_buffer_pointers;
    };
}
