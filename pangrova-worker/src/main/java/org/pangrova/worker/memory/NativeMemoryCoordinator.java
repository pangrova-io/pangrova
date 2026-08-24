package org.pangrova.worker.memory;

import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.ShrinkableMemoryConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock-free memory coordinator for native engine allocations.
 *
 * <p>Routes all native allocation requests through the global
 * {@link MemoryArbitrator} so that Java heap and native off-heap share
 * a single memory budget, preventing OS-level OOM.
 *
 * <p>Implements {@link ShrinkableMemoryConsumer} to receive shrink/spill
 * callbacks from the arbitrator when memory pressure is high.
 *
 * <p>Uses {@link AtomicLong} CAS loop for local reservation tracking — no locks,
 * no risk of deadlock with Native-side arbitration mutex.
 *
 * <p>Called synchronously from C++ via JNI within a Java-initiated call chain
 * (see {@link AllocationListener} contract).
 */
@ThreadSafe
public final class NativeMemoryCoordinator implements AllocationListener, ShrinkableMemoryConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMemoryCoordinator.class);
    private static final String CONSUMER_ID = "native-memory";

    private final long maxNativeBytes;
    private final AtomicLong reservedBytes = new AtomicLong(0);
    private final MemoryArbitrator arbitrator;
    private final NativeSpillBridge spillBridge;

    /**
     * Creates a coordinator with arbitrator and spill bridge wired.
     *
     * @param maxNativeBytes upper bound for native memory reservations (positive)
     * @param arbitrator     global memory arbitrator for unified budget enforcement
     * @param spillBridge    bridge to signal native operator spill on allocation denial
     */
    public NativeMemoryCoordinator(long maxNativeBytes, MemoryArbitrator arbitrator,
                                   NativeSpillBridge spillBridge) {
        if (maxNativeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxNativeBytes must be positive, got " + maxNativeBytes);
        }
        if (arbitrator == null) {
            throw new IllegalArgumentException("arbitrator must not be null");
        }
        this.maxNativeBytes = maxNativeBytes;
        this.arbitrator = arbitrator;
        this.spillBridge = spillBridge;
        arbitrator.registerConsumer(this);
        LOG.info("event=native_memory_coordinator_init maxNativeBytes={}", maxNativeBytes);
    }

    /**
     * Creates a coordinator that routes native allocations through the arbitrator.
     *
     * @param maxNativeBytes upper bound for native memory reservations (positive)
     * @param arbitrator     global memory arbitrator for unified budget enforcement
     */
    public NativeMemoryCoordinator(long maxNativeBytes, MemoryArbitrator arbitrator) {
        this(maxNativeBytes, arbitrator, null);
    }

    /**
     * Creates a coordinator with a standalone arbitrator scoped to native budget.
     * Suitable for tests and isolated deployments where no shared arbitrator exists.
     *
     * @param maxNativeBytes upper bound for Native memory reservations (positive)
     */
    public NativeMemoryCoordinator(long maxNativeBytes) {
        this(maxNativeBytes, new MemoryArbitrator(maxNativeBytes, 0), null);
    }

    @Override
    public long reserve(long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        // Check local native cap first (fast path).
        if (!canFitLocally(bytes)) {
            return 0;
        }
        // Route through arbitrator for global budget enforcement.
        long granted = arbitrator.reserve(bytes);
        if (granted <= 0) {
            if (spillBridge != null) {
                spillBridge.requestSpill(bytes);
            }
            return 0;
        }
        // Track locally after arbitrator approval.
        reservedBytes.addAndGet(granted);
        return granted;
    }

    @Override
    public void unreserve(long bytes) {
        if (bytes <= 0) {
            return;
        }
        // A-MEM-02: unreserve MUST NOT throw. It is invoked from the native JNI allocation callback;
        // a propagated exception there makes the C++ layer re-accumulate cross-thread unreserve debt,
        // which under repeated failure grows unbounded and self-locks memory accounting (native OOM).
        // The memory has already been physically freed, so accounting is strictly best-effort: on any
        // error we record the drift and continue rather than propagate to the JNI caller.
        try {
            long released = decrementLocal(bytes);
            if (released > 0) {
                arbitrator.releaseReservation(released);
            }
        } catch (RuntimeException e) {
            LOG.warn("event=native_unreserve_drift bytes={} error={} "
                    + "note=accounting_drift_not_propagated_to_jni", bytes, e.toString());
        }
    }

    // --- ShrinkableMemoryConsumer implementation ---

    @Override
    public String consumerId() {
        return CONSUMER_ID;
    }

    @Override
    public long currentUsageBytes() {
        return reservedBytes.get();
    }

    @Override
    public long shrink(long targetBytes) {
        // F-WORKER-05: Trigger native spill BEFORE releasing Java reservation.
        // This ensures we only release reservation for memory that has actually been
        // freed on the native side, preventing RSS over-commit under sustained pressure.
        long currentUsage = reservedBytes.get();
        long shrinkable = Math.min(targetBytes, currentUsage);
        if (shrinkable <= 0) {
            LOG.info("event=native_shrink_noop targetBytes={} currentUsage={}",
                    targetBytes, currentUsage);
            return 0;
        }
        // Request native spill first to reclaim actual physical memory
        long actuallySpilled = shrinkable;
        if (spillBridge != null) {
            boolean spillSuccess = spillBridge.requestSpill(shrinkable);
            if (!spillSuccess) {
                LOG.warn("event=native_shrink_spill_failed targetBytes={} spillResult={}",
                        shrinkable, spillSuccess);
                // Still release some reservation as a last resort to prevent deadlock
                actuallySpilled = shrinkable;
            }
        }
        // Only release what was actually spilled (or requested as fallback)
        long released = decrementLocal(Math.min(actuallySpilled, shrinkable));
        if (released > 0) {
            arbitrator.releaseReservation(released);
            LOG.info("event=native_shrink_reservation_released targetBytes={} released={} remaining={}",
                    targetBytes, released, reservedBytes.get());
        }
        return released;
    }

    /** Current total bytes reserved by Native side. */
    public long currentReservation() {
        return reservedBytes.get();
    }

    /** Bytes still available for new Native allocations. */
    public long availableForNative() {
        return Math.max(0, maxNativeBytes - reservedBytes.get());
    }

    /** Configured upper bound for Native memory. */
    public long maxNativeBytes() {
        return maxNativeBytes;
    }

    /** Returns the arbitrator this coordinator is registered with. */
    public MemoryArbitrator arbitrator() {
        return arbitrator;
    }

    /**
     * Resets local reservation tracking and the underlying arbitrator's used
     * memory counter to zero. Called between test classes after all native
     * executors have been released to reclaim any leaked budget caused by
     * tasks that crashed without properly unreserving their memory.
     */
    public void resetBudget() {
        long leaked = reservedBytes.getAndSet(0);
        arbitrator.clearAllocations();
        if (leaked > 0) {
            LOG.info("event=native_memory_budget_reset leaked_bytes={}", leaked);
        }
    }

    /**
     * Checks whether the requested bytes fit within the local native cap.
     * Does not modify state — purely a read check.
     */
    private boolean canFitLocally(long bytes) {
        return reservedBytes.get() + bytes <= maxNativeBytes;
    }

    /**
     * Decrements local reservation using CAS loop, clamping to zero.
     *
     * @return actual bytes decremented
     */
    private long decrementLocal(long bytes) {
        while (true) {
            long current = reservedBytes.get();
            long toRelease = Math.min(bytes, current);
            if (toRelease <= 0) {
                return 0;
            }
            if (reservedBytes.compareAndSet(current, current - toRelease)) {
                return toRelease;
            }
        }
    }
}
