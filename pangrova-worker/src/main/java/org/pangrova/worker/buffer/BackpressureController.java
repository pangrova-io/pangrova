package org.pangrova.worker.buffer;

import org.pangrova.worker.enums.BackpressureAction;

import java.time.Duration;

public final class BackpressureController {

    public record BackpressureResult(BackpressureAction action, Duration delay) {}

    private static final int DEFAULT_MAX_PENDING = 1000;
    private static final int DEFAULT_HIGH_WATER_MARK = 800;
    private static final int DEFAULT_LOW_WATER_MARK = 500;

    private final int maxPending;
    private final int highWaterMark;
    private final int lowWaterMark;

    /** Creates a controller with default water marks. */
    public BackpressureController() {
        this(DEFAULT_MAX_PENDING, DEFAULT_HIGH_WATER_MARK, DEFAULT_LOW_WATER_MARK);
    }

    /** Creates a controller with explicit water marks; requires low &lt; high &lt; maxPending. */
    public BackpressureController(int maxPending, int highWaterMark, int lowWaterMark) {
        if (highWaterMark <= lowWaterMark) {
            throw new IllegalArgumentException(
                    "highWaterMark must be > lowWaterMark, got high=" + highWaterMark + " low=" + lowWaterMark);
        }
        if (maxPending <= highWaterMark) {
            throw new IllegalArgumentException(
                    "maxPending must be > highWaterMark, got max=" + maxPending + " high=" + highWaterMark);
        }
        this.maxPending = maxPending;
        this.highWaterMark = highWaterMark;
        this.lowWaterMark = lowWaterMark;
    }

    /** Evaluates the pending count against the water marks and returns the action to take. */
    public BackpressureResult checkBackpressure(int pendingCount) {
        if (pendingCount >= maxPending) {
            return new BackpressureResult(BackpressureAction.REJECT, Duration.ZERO);
        }
        if (pendingCount >= highWaterMark) {
            return new BackpressureResult(BackpressureAction.SLOW_DOWN, computeDelay(pendingCount));
        }
        return new BackpressureResult(BackpressureAction.PROCEED, Duration.ZERO);
    }

    private Duration computeDelay(int pendingCount) {
        double ratio = (double) (pendingCount - lowWaterMark) / (highWaterMark - lowWaterMark);
        long delayMs = (long) (ratio * 1000);
        return Duration.ofMillis(delayMs);
    }
}
