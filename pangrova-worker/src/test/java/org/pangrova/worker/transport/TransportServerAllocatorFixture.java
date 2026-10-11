package org.pangrova.worker.transport;

import org.apache.arrow.memory.BufferAllocator;

/** Access to the actual transport owner for cross-module allocation integration tests. */
public final class TransportServerAllocatorFixture {
    private TransportServerAllocatorFixture() {}

    public static BufferAllocator allocator(TransportServer server) {
        return server.allocator();
    }
}
