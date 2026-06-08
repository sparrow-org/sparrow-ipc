// Test: ensure no FlatBuffer headers or types leak into the public API.
//
// The preprocessor guards below fire a compile-time #error if any of the
// tested public headers transitively include flatbuffers/flatbuffers.h
// (which defines FLATBUFFERS_VERSION_MAJOR in flatbuffers/base.h).
//
// A successful compilation of this file is a passing test.

#include <doctest/doctest.h>

#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked before umbrella header"
#endif
#include "sparrow_ipc.hpp"
#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked via umbrella header (sparrow_ipc.hpp)"
#endif

#include "sparrow_ipc/serializer.hpp"
#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked via serializer.hpp"
#endif

#include "sparrow_ipc/stream_file_serializer.hpp"
#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked via stream_file_serializer.hpp"
#endif

#include "sparrow_ipc/chunk_memory_serializer.hpp"
#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked via chunk_memory_serializer.hpp"
#endif

#include "sparrow_ipc/deserialize.hpp"
#ifdef FLATBUFFERS_VERSION_MAJOR
#    error "FLATBUFFERS LEAK: flatbuffers leaked via deserialize.hpp"
#endif

#include "sparrow_ipc/deserializer.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/chunk_memory_output_stream.hpp"
#include "sparrow_ipc/dictionary_cache.hpp"
#include "sparrow_ipc/dictionary_tracker.hpp"
#include "sparrow_ipc/dictionary_utils.hpp"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/utils.hpp"
#include "sparrow_ipc/serializer_reserve.hpp"
#include "sparrow_ipc/dictionary_iteration.hpp"

TEST_SUITE("no_flatbuffer_leak")
{
    TEST_CASE("public API does not expose FlatBuffer types")
    {
        CHECK(true);
    }
}
