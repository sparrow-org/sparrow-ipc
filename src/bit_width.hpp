#pragma once

#include <cstddef>
#include <cstdint>

namespace sparrow_ipc
{
    constexpr size_t BIT_WIDTH_8 = sizeof(std::int8_t) * 8;
    constexpr size_t BIT_WIDTH_16 = sizeof(std::int16_t) * 8;
    constexpr size_t BIT_WIDTH_32 = sizeof(std::int32_t) * 8;
    constexpr size_t BIT_WIDTH_64 = sizeof(std::int64_t) * 8;
}