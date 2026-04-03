#pragma once

// Serialization
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"
#include "sparrow_ipc/chunk_memory_serializer.hpp"

// Deserialization
#include "sparrow_ipc/deserializer.hpp"
#include "sparrow_ipc/deserialize_decimal_array.hpp"
#include "sparrow_ipc/deserialize_duration_array.hpp"
#include "sparrow_ipc/deserialize_fixed_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_interval_array.hpp"
#include "sparrow_ipc/deserialize_null_array.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_run_end_encoded_array.hpp"
#include "sparrow_ipc/deserialize_time_related_arrays.hpp"
#include "sparrow_ipc/deserialize_union_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_view_array.hpp"

// Utilities
#include "sparrow_ipc/dictionary_utils.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
