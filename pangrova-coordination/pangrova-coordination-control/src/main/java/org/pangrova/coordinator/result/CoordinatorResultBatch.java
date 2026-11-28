package org.pangrova.coordinator.result;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Owns one bounded columnar result and its allocation until its sole reader exits. */
public final class CoordinatorResultBatch implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final BufferAllocator allocator;
    private final long bytes;
    private boolean rootClosed;
    private boolean closed;

    CoordinatorResultBatch(VectorSchemaRoot root, BufferAllocator allocator) {
        this.root = root;
        this.allocator = allocator;
        this.bytes = allocator.getAllocatedMemory();
    }

    /** Stages the owned batch without transferring its mutable vectors to the caller. */
    void streamTo(ServerStreamListener listener) {
        listener.setUseZeroCopy(false);
        listener.start(root);
        listener.putNext();
    }

    public long bytes() { return bytes; }

    @Override
    public synchronized void close() {
        if (closed) return;
        if (!rootClosed) {
            root.close();
            rootClosed = true;
        }
        allocator.close();
        closed = true;
    }
}
