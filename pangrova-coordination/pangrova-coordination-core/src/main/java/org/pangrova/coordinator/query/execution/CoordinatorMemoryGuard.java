package org.pangrova.coordinator.query.execution;

import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Tracks coordinator-side TypedRow memory allocation and enforces a
 * configurable upper bound. Fails fast with
 * {@link PangrovaErrorCode#COORDINATOR_MEMORY_EXCEEDED} when the threshold
 * is breached, preventing coordinator OOM from unbounded data processing.
 *
 * <p>Concurrency strategy: a single {@link AtomicLong} updated via CAS
 * ({@code addAndGet}). No locks, no I/O on the hot path.</p>
 */
@ThreadSafe
public final class CoordinatorMemoryGuard {

    /** Default memory ceiling: 256 MB. */
    static final long DEFAULT_MAX_BYTES = 256L * 1024 * 1024;

    private final AtomicLong allocatedBytes = new AtomicLong();
    private final long maxBytes;

    public CoordinatorMemoryGuard() {
        this(DEFAULT_MAX_BYTES);
    }

    /** Creates a coordinator memory guard with a positive byte limit. */
    public CoordinatorMemoryGuard(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    /**
     * Records a new allocation. If the cumulative allocation exceeds
     * {@code maxBytes}, the allocation is rolled back and an exception
     * is thrown immediately (fail-fast circuit breaker).
     *
     * @param bytes number of bytes being allocated (must be positive)
     * @throws PangrovaException with {@link PangrovaErrorCode#COORDINATOR_MEMORY_EXCEEDED}
     */
    public void trackAllocation(long bytes) {
        if (bytes <= 0) {
            return;
        }
        long current = allocatedBytes.addAndGet(bytes);
        if (current > maxBytes) {
            allocatedBytes.addAndGet(-bytes);
            throw new PangrovaException(PangrovaErrorCode.COORDINATOR_MEMORY_EXCEEDED,
                    "Coordinator TypedRow allocation exceeded limit: "
                            + current + " > " + maxBytes);
        }
    }

    /**
     * Records a deallocation, decrementing the tracked byte count.
     *
     * @param bytes number of bytes being released (must be positive)
     */
    public void trackDeallocation(long bytes) {
        if (bytes <= 0) {
            return;
        }
        allocatedBytes.addAndGet(-bytes);
    }

    /**
     * Returns the current allocation in bytes, for observability.
     */
    public long currentAllocation() {
        return allocatedBytes.get();
    }

    /**
     * Returns the configured maximum allocation in bytes.
     */
    public long maxBytes() {
        return maxBytes;
    }
}
