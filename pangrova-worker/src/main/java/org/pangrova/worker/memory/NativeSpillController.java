package org.pangrova.worker.memory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Requests native operator spill on behalf of the Java memory arbitrator.
 *
 * <p>Receives allocation denials from {@link NativeMemoryCoordinator} and calls
 * {@code NativeRuntime.nativeRequestReclaim}, which invokes
 * {@code arbitrator->shrinkCapacity(targetBytes, allowSpill=true, allowAbort=false)}.
 * The arbitrator first returns unused pool capacity, then drives
 * {@code pool->reclaim()} so operators can spill used bytes. The caller retries
 * the allocation afterwards.</p>
 *
 * <p>Starts enabled; {@code NativeRuntimeBootstrap} disables it when no spill
 * directory is configured, matching the native init gate.</p>
 */
@ThreadSafe
public final class NativeSpillController {

    private static final Logger LOG = LoggerFactory.getLogger(NativeSpillController.class);

    private final AtomicBoolean spillEnabled = new AtomicBoolean(true);
    private final AtomicLong spillRequestCount = new AtomicLong(0);
    private final AtomicLong spillSuccessCount = new AtomicLong(0);
    private final AtomicLong spillFailureCount = new AtomicLong(0);
    private final AtomicLong totalBytesSpilled = new AtomicLong(0);
    /** Reclaim function — JNI-backed in production, injectable for deterministic tests. */
    private final java.util.function.LongUnaryOperator reclaimFn;

    /** Uses the reclaim operation supplied by the Worker runtime owner. */
    public NativeSpillController(java.util.function.LongUnaryOperator reclaimFn) {
        this.reclaimFn = java.util.Objects.requireNonNull(reclaimFn, "reclaimFn must not be null");
    }

    /**
     * Called when the Java-side arbitrator denies a native allocation.
     * Triggers native operator spill via pool reclaim and reports the outcome.
     *
     * @param requestedBytes the bytes that could not be allocated
     * @return true only if native reclaim freed memory; false when spill is
     *         disabled, native is unavailable, or reclaim freed nothing
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
            LOG.warn("event=native_reclaim_unavailable reason=native_lib_not_loaded");
            spillFailureCount.incrementAndGet();
            return false;
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
