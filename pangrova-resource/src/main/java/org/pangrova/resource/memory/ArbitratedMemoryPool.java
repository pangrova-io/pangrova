package org.pangrova.resource.memory;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * A memory pool that delegates all reserve/release operations to a
 * {@link MemoryArbitrator}, ensuring a single authoritative ledger
 * for global memory accounting.
 *
 * <p>This pool does not maintain independent counters — the arbitrator
 * is the source of truth. Local tracking in the parent {@link MemoryPool}
 * is used only for per-pool metrics visibility.</p>
 *
 * <p>Concurrency strategy: inherits AtomicLong CAS from MemoryPool base;
 * arbitrator calls are themselves thread-safe.</p>
 */
@ThreadSafe
public final class ArbitratedMemoryPool extends MemoryPool {

    private final MemoryArbitrator arbitrator;

    /**
     * Creates an arbitrated pool.
     *
     * @param name       human-readable pool name
     * @param maxBytes   maximum bytes this pool may use
     * @param arbitrator the arbitrator that owns the global budget
     */
    ArbitratedMemoryPool(String name, long maxBytes, MemoryArbitrator arbitrator) {
        super(name, maxBytes);
        this.arbitrator = arbitrator;
    }

    /**
     * Reserves bytes through the arbitrator, then tracks locally for metrics.
     *
     * @param bytes requested byte count
     * @return actual bytes reserved (may be less than requested)
     */
    @Override
    public long tryReserve(long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        long localCap = getMaxBytes() - getUsedBytes();
        long capped = Math.min(bytes, localCap);
        if (capped <= 0) {
            return 0;
        }
        long allocated = arbitrator.reserve(this, capped);
        if (allocated > 0) {
            super.tryReserve(allocated);
        }
        return allocated;
    }

    /**
     * Releases bytes through the arbitrator, then updates local tracking.
     *
     * @param bytes byte count to release
     * @throws DoubleReleaseException if bytes exceeds current usage
     */
    @Override
    public void free(long bytes) {
        if (bytes <= 0) {
            return;
        }
        super.free(bytes);
        arbitrator.release(this, bytes);
    }

    /**
     * Returns the arbitrator backing this pool.
     */
    public MemoryArbitrator getArbitrator() {
        return arbitrator;
    }
}
