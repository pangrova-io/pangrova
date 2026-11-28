package org.pangrova.planner.validation;

import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Tracks the ratio of plans that required native fallback (partial or full
 * JVM execution) versus plans that were fully offloaded to the native engine.
 *
 * <p>Thread-safe via {@link AtomicLong} counters. The fallback rate is
 * computed as {@code fallbackCount / totalCount} and is the value exported
 * as the {@code pangrova.planner.fallback.rate} metric.</p>
 *
 * <p>Task 198 + 201: provides the fallback-rate metric required by
 * G-EXEC-02 / G-INTEROP-02 gap analysis.</p>
 */
@ThreadSafe
public final class FallbackRateTracker {

    private final AtomicLong totalPlans = new AtomicLong(0);
    private final AtomicLong fallbackPlans = new AtomicLong(0);
    private final AtomicLong nativeUnsupportedPlans = new AtomicLong(0);

    /** Records a plan that was fully native (no fallback needed). */
    public void recordFullyNative() {
        totalPlans.incrementAndGet();
    }

    /** Records a plan that had partial native support (some operators unsupported). */
    public void recordPartialFallback() {
        totalPlans.incrementAndGet();
        fallbackPlans.incrementAndGet();
    }

    /** Records a plan that was entirely unsupported by the native engine. */
    public void recordNativeUnsupported() {
        totalPlans.incrementAndGet();
        nativeUnsupportedPlans.incrementAndGet();
    }

    /** Returns the fallback rate as a ratio in [0.0, 1.0]. */
    public double fallbackRate() {
        long total = totalPlans.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) fallbackPlans.get() / total;
    }

    /** Returns the total number of plans validated. */
    public long totalPlans() {
        return totalPlans.get();
    }

    /** Returns the number of plans that triggered fallback. */
    public long fallbackPlans() {
        return fallbackPlans.get();
    }

    /** Returns the number of plans entirely unsupported by native engine. */
    public long nativeUnsupportedPlans() {
        return nativeUnsupportedPlans.get();
    }

    /** Resets all counters (for testing). */
    public void reset() {
        totalPlans.set(0);
        fallbackPlans.set(0);
        nativeUnsupportedPlans.set(0);
    }
}
