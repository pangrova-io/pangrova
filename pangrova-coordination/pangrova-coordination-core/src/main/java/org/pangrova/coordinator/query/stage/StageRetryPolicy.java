package org.pangrova.coordinator.query.stage;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Immutable retry policy for a Stage.
 * Determines whether a failed Stage should be retried based on
 * the number of attempts already made.
 */
@ThreadSafe
public record StageRetryPolicy(int maxRetries) {

    /** Default policy: up to 2 retries. */
    public static final StageRetryPolicy DEFAULT = new StageRetryPolicy(2);

    /** No-retry policy. */
    public static final StageRetryPolicy NO_RETRY = new StageRetryPolicy(0);

    public StageRetryPolicy {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        }
    }

    /**
     * Returns true if the stage can be retried given the current attempt count.
     *
     * @param currentAttempt zero-based attempt count (0 = first attempt)
     */
    public boolean canRetry(int currentAttempt) {
        return currentAttempt < maxRetries;
    }
}
