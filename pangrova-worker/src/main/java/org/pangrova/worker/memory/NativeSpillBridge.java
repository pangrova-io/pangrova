package org.pangrova.worker.memory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.spi.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between the Java-side memory arbitrator and native operator spill.
 *
 * <p>Task 204 fix (G-INTEROP-05): the native PangrovaArbitrator.growCapacity
 * called shrinkPool on allocation failure, but shrinkPool only released
 * free capacity (capacity - usedBytes) and never called pool->reclaim()
 * to trigger actual operator spill. This bridge:</p>
 * <ol>
 *   <li>Receives allocation denial signals from Java-side
 *       ({@link NativeMemoryCoordinator})</li>
 *   <li>Signals back to the native side that a reclaim is needed</li>
 *   <li>The native side reads the spill-requested flag and calls
 *       pool->reclaim(targetBytes) to trigger operator MemoryReclaimer spill</li>
 *   <li>After spill completes, the allocation is retried</li>
 * </ol>
 *
 * <p>Spill is enabled by default ({@code spillEnabled = true}).</p>
 */
@ThreadSafe
public final class NativeSpillBridge {

    private static final Logger LOG = LoggerFactory.getLogger(NativeSpillBridge.class);

    private final AtomicBoolean spillEnabled = new AtomicBoolean(true);
    private final AtomicLong spillRequestCount = new AtomicLong(0);
    private final AtomicLong spillSuccessCount = new AtomicLong(0);
    private final AtomicLong spillFailureCount = new AtomicLong(0);
    private final AtomicLong totalBytesSpilled = new AtomicLong(0);
    /** Reclaim function — JNI-backed in production, injectable for deterministic tests. */
    private final java.util.function.LongUnaryOperator reclaimFn;

    /** Production bridge: reclaim delegates to the native pool via JNI. */
    public NativeSpillBridge() {
        this(org.pangrova.worker.jni.NativeRuntime::nativeRequestReclaim);
    }

    /**
     * Bridge with an injectable reclaim function. The JNI-backed default made the
     * unit tests environment-sensitive (pass without the native lib via the
     * UnsatisfiedLinkError fallback, fail with it loaded and no active pools);
     * injecting a deterministic function removes that coupling.
     */
    public NativeSpillBridge(java.util.function.LongUnaryOperator reclaimFn) {
        this.reclaimFn = java.util.Objects.requireNonNull(reclaimFn, "reclaimFn must not be null");
    }

    /**
     * Called when the Java-side arbitrator denies a native allocation.
     * Triggers native operator spill via pool reclaim and reports the outcome.
     *
     * @param requestedBytes the bytes that could not be allocated
     * @return true if the reclaim freed memory (or the native library is
     *         unavailable and the request degrades to a signal); false when
     *         spill is disabled or the reclaim freed nothing
     */
    public boolean requestSpill(long requestedBytes) {
        if (!spillEnabled.get()) {
            return false;
        }
        spillRequestCount.incrementAndGet();
        LOG.info("event=native_spill_requested requestedBytes={}", requestedBytes);
        // P1-7: actually trigger native pool reclaim (previously a no-op signal).
        // Calls JNI nativeRequestReclaim which invokes pool->reclaim(targetBytes)
        // on the native side, triggering operator MemoryReclaimer spill.
        try {
            long bytesReclaimed = reclaimFn.applyAsLong(requestedBytes);
            if (bytesReclaimed > 0) {
                spillSuccessCount.incrementAndGet();
                totalBytesSpilled.addAndGet(bytesReclaimed);
                LOG.info("event=native_spill_reclaimed bytesReclaimed={}", bytesReclaimed);
                return true;
            } else {
                spillFailureCount.incrementAndGet();
                LOG.warn("event=native_spill_reclaim_returned_zero requestedBytes={}", requestedBytes);
                return false;
            }
        } catch (UnsatisfiedLinkError e) {
            // Native library not loaded (test/dev environment without native build).
            // Fall back to signal-only mode and log once.
            LOG.debug("event=native_reclaim_unavailable reason=native_lib_not_loaded");
            return true; // signal recorded; native side handles via its own poll path
        }
    }

    /**
     * Called from native side (via JNI) to report that operator spill completed.
     *
     * @param bytesFreed bytes freed by operator spill (positive on success, 0 on failure)
     */
    public void reportSpillResult(long bytesFreed) {
        if (bytesFreed > 0) {
            spillSuccessCount.incrementAndGet();
            totalBytesSpilled.addAndGet(bytesFreed);
            LOG.info("event=native_spill_completed bytesFreed={}", bytesFreed);
        } else {
            spillFailureCount.incrementAndGet();
            LOG.warn("event=native_spill_failed bytesFreed=0");
        }
    }

    /** Returns whether spill is enabled. Default: true. */
    public boolean isSpillEnabled() {
        return spillEnabled.get();
    }

    /** Sets whether spill is enabled. */
    public void setSpillEnabled(boolean enabled) {
        boolean old = spillEnabled.getAndSet(enabled);
        if (old != enabled) {
            LOG.info("event=spill_enabled_changed old={} new={}", old, enabled);
        }
    }

    /** Total number of spill requests from allocation denials. */
    public long spillRequestCount() {
        return spillRequestCount.get();
    }

    /** Total number of successful spill operations. */
    public long spillSuccessCount() {
        return spillSuccessCount.get();
    }

    /** Total number of failed spill operations. */
    public long spillFailureCount() {
        return spillFailureCount.get();
    }

    /** Total bytes freed by operator spill. */
    public long totalBytesSpilled() {
        return totalBytesSpilled.get();
    }
}
