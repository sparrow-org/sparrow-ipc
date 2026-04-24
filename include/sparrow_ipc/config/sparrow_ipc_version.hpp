#pragma once

namespace sparrow_ipc
{
    constexpr int SPARROW_IPC_VERSION_MAJOR = 1;
    constexpr int SPARROW_IPC_VERSION_MINOR = 0;
    constexpr int SPARROW_IPC_VERSION_PATCH = 2;

    // Binary version
    // See the following URL for explanations about the binary versionning
    // https://www.gnu.org/software/libtool/manual/html_node/Updating-version-info.html#Updating-version-info
    constexpr int SPARROW_IPC_BINARY_CURRENT = 4;
    constexpr int SPARROW_IPC_BINARY_REVISION = 2;
    constexpr int SPARROW_IPC_BINARY_AGE = 0;

    static_assert(SPARROW_IPC_BINARY_AGE <= SPARROW_IPC_BINARY_CURRENT, "SPARROW_IPC_BINARY_AGE cannot be greater than SPARROW_IPC_BINARY_CURRENT");
}
