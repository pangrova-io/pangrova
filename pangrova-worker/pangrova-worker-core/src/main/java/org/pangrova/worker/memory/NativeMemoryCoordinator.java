package org.pangrova.worker.memory;

import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.ShrinkableMemoryConsumer;
import org.pangrova.worker.nativebridge.memory.AllocationListener;
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
 * <p>Uses {@link AtomicLong} CAS loops to hold local capacity during global
 * arbitration — no locks across Native-side reclaim callbacks.
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
    /** Committed plus in-flight requests; protects the local cap during arbitration. */
    private final AtomicLong localBudgetUsed = new AtomicLong(0);
    private final MemoryArbitrator arbitrator;
    private final NodeMemoryPool nodePool;
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
        this(maxNativeBytes, arbitrator, null, spillBridge);
    }

    /** Creates a callback that charges actual Native bytes to both node and global budgets. */
    public NativeMemoryCoordinator(long maxNativeBytes, MemoryArbitrator arbitrator,
                                   NodeMemoryPool nodePool, NativeSpillBridge spillBridge) {
        if (maxNativeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxNativeBytes must be positive, got " + maxNativeBytes);
        }
        if (arbitrator == null) {
            throw new IllegalArgumentException("arbitrator must not be null");
        }
        this.maxNativeBytes = maxNativeBytes;
        this.arbitrator = arbitrator;
        this.nodePool = nodePool;
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
        if (!reserveLocalBudget(bytes)) {
            return 0;
        }
        long granted = 0;
        long nodeReserved = 0;
        try {
            if (nodePool != null) {
                nodeReserved = nodePool.tryReserve(bytes);
                if (nodeReserved != bytes) {
                    if (spillBridge != null) {
                        spillBridge.requestSpill(bytes - nodeReserved);
                    }
                    return 0;
                }
            }
            // Local capacity is held throughout arbitration. Another driver
            // cannot pass the same cap while this call is waiting for spill.
            granted = arbitrator.reserve(bytes);
            if (granted <= 0) {
                if (spillBridge != null) {
                    spillBridge.requestSpill(bytes);
                }
                return 0;
            }
            reservedBytes.addAndGet(granted);
            return granted;
        } finally {
            if (nodeReserved > granted) {
                nodePool.free(nodeReserved - granted);
            }
            // Keep only the committed portion; roll back denials and partial grants.
            // The callback cannot unreserve this new allocation before reserve returns.
            localBudgetUsed.addAndGet(granted - bytes);
        }
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
                localBudgetUsed.addAndGet(-released);
                try {
                    arbitrator.releaseReservation(released);
                } finally {
                    if (nodePool != null) {
                        nodePool.free(released);
                    }
                }
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
        // Native shrinkCapacity reports actual releases through the existing
        // AllocationListener.unreserve callback. A request is not a release.
        long currentUsage = reservedBytes.get();
        long shrinkable = Math.min(targetBytes, currentUsage);
        if (shrinkable <= 0) {
            LOG.info("event=native_shrink_noop targetBytes={} currentUsage={}",
                    targetBytes, currentUsage);
            return 0;
        }
        if (spillBridge == null) {
            return 0;
        }
        if (!spillBridge.requestSpill(shrinkable)) {
            LOG.warn("event=native_shrink_spill_failed targetBytes={}", shrinkable);
            return 0;
        }
        // Native shrinkCapacity invokes AllocationListener.unreserve for each
        // actually freed byte. Never decrement here: that would double-credit
        // the shared arbitrator (or credit a merely signalled spill).
        return Math.max(0, Math.min(shrinkable, currentUsage - reservedBytes.get()));
    }

    /** Current total bytes reserved by Native side. */
    public long currentReservation() {
        return reservedBytes.get();
    }

    /** Bytes still available for new Native allocations. */
    public long availableForNative() {
        return Math.max(0, maxNativeBytes - localBudgetUsed.get());
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
     * Releases only this callback's residual reservation after native tasks
     * have quiesced. Never clear the shared arbitrator: service and task
     * leases may still be live between test classes.
     */
    public void resetBudget() {
        long leaked = reservedBytes.getAndSet(0);
        if (leaked > 0) {
            localBudgetUsed.addAndGet(-leaked);
            // Only the reservation owned by this callback may be released.
            // Clearing the shared arbitrator would erase process-lifetime
            // shuffle credits and live task grants without freeing memory.
            arbitrator.releaseReservation(leaked);
            if (nodePool != null) {
                nodePool.free(leaked);
            }
            LOG.info("event=native_memory_budget_reset leaked_bytes={}", leaked);
        }
    }

    /** Holds full requested capacity until global arbitration commits or denies it. */
    private boolean reserveLocalBudget(long bytes) {
        while (true) {
            long current = localBudgetUsed.get();
            if (bytes > maxNativeBytes - current) {
                return false;
            }
            if (localBudgetUsed.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
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
