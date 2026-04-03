#pragma once

#include <cstdint>

#include "sparrow_ipc/flatbuffers_generated/Message_generated.h"

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc
{
    namespace details
    {
        constexpr auto CompressionHeaderSize = sizeof(std::int64_t);

        org::apache::arrow::flatbuf::CompressionType to_fb_compression_type(CompressionType compression_type);
        CompressionType from_fb_compression_type(org::apache::arrow::flatbuf::CompressionType compression_type);
    }
}
