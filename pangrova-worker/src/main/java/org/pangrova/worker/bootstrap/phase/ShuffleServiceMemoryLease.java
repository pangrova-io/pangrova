package org.pangrova.worker.bootstrap.phase;

import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;

/**
 * Process-lifetime memory reservation for Native Flight retention and transit.
 * Both accounting domains must grant the full amount before the Worker can
 * register tasks; a partial grant is rolled back, never silently accepted.
 */
public final class ShuffleServiceMemoryLease implements AutoCloseable {

    private final NodeMemoryPool nodePool;
    private final MemoryArbitrator arbitrator;
    private final long bytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ShuffleServiceMemoryLease(
            NodeMemoryPool nodePool, MemoryArbitrator arbitrator,
            long bytes) {
        this.nodePool = nodePool;
        this.arbitrator = arbitrator;
        this.bytes = bytes;
    }

    public static ShuffleServiceMemoryLease reserve(
            String workerId, long retentionBytes, long transitBytes,
            NodeMemoryPool nodePool, MemoryArbitrator arbitrator) {
        if (workerId == null || workerId.isBlank() || nodePool == null || arbitrator == null ||
                retentionBytes <= 0 || transitBytes <= 0) {
            throw new IllegalArgumentException("Shuffle service requires a valid memory budget and Worker identity");
        }
        final long requested = Math.addExact(retentionBytes, transitBytes);
        long nodeGranted = nodePool.tryReserve(requested);
        if (nodeGranted != requested) {
            nodePool.free(nodeGranted);
            throw new IllegalStateException("Insufficient NodePool memory for Native shuffle service");
        }
        long arbGranted = 0;
        try {
            // Pool reservations have process lifetime; query reservations
            // would be reclaimed by the stale-query sweeper after 60 min.
            arbGranted = arbitrator.reserve(nodePool, requested);
            if (arbGranted != requested) {
                throw new IllegalStateException("Insufficient shared arbitrator memory for Native shuffle service");
            }
            return new ShuffleServiceMemoryLease(nodePool, arbitrator, requested);
        } catch (RuntimeException | Error failure) {
            arbitrator.release(nodePool, arbGranted);
            nodePool.free(nodeGranted);
            throw failure;
        }
    }

    public long bytes() {
        return bytes;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            arbitrator.release(nodePool, bytes);
            nodePool.free(bytes);
        }
    }
}
