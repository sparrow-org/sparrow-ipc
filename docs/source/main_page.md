Sparrow-IPC                             {#mainpage}
===========

**!!!Sparrow-IPC is still under development and is not ready for production use!!!**


**!!!The documentation is still under development and may be incomplete or contain errors!!!**

Introduction
------------

Sparrow-IPC provides high-performance serialization and deserialization of record batches, adhering to both [Sparrow](https://github.com/man-group/sparrow) and [Apache Arrow IPC specifications](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).

Sparrow-IPC requires a modern C++ compiler supporting C++20:

| Compiler    | Version         |
| ----------- | --------------- |
| Clang       | 18 or higher    |
| GCC         | 11.2 or higher  |
| Apple Clang | 16 or higher    |
| MSVC        | 19.41 or higher |

This software is licensed under the BSD-3-Clause license. See the [LICENSE](https://github.com/sparrow-org/sparrow-ipc/blob/main/LICENSE) file for details.

Getting Started
---------------

### Quick Example

```cpp
#include <vector>
#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>
#include <sparrow/record_batch.hpp>

namespace sp = sparrow;
namespace sp_ipc = sparrow_ipc;

// Serialize record batches
std::vector<uint8_t> serialize(const std::vector<sp::record_batch>& batches)
{
    std::vector<uint8_t> stream_data;
    sp_ipc::memory_output_stream stream(stream_data);
    sp_ipc::serializer serializer(stream);
    serializer << batches << sp_ipc::end_stream;
    return stream_data;
}

// Deserialize record batches
std::vector<sp::record_batch> deserialize(const std::vector<uint8_t>& stream_data)
{
    return sp_ipc::deserialize_stream(stream_data);
}
```

Documentation
-------------

- @ref serialization "Serialization and Deserialization" - How to serialize and deserialize record batches
- @ref dev_build "Development Build" - How to build the project for development
