package org.pangrova.common.concurrency;

/**
 * Exponential backoff calculator for polling and retry loops.
 *
 * <p>Each call to {@link #next()} returns the current backoff value in milliseconds
 * and doubles the internal state, capped at the configured maximum. Call {@link #reset()}
 * to restart the sequence from the initial value.
 *
 * <p><strong>NOT thread-safe</strong> — this class is designed for single-thread
 * polling or retry loops. Do not share instances across threads without external
 * synchronization.
 *
 * <h3>Usage examples</h3>
 * <pre>{@code
 * // Poll backoff: 1 → 2 → 4 → 8 → 16 → 16 → ...
 * BackoffStrategy poll = BackoffStrategy.poll(16);
 *
 * // Retry backoff: 1000 → 2000 → 4000 → ... → 30000
 * BackoffStrategy retry = BackoffStrategy.retry(1000, 30000);
 * }</pre>
 */
public final class BackoffStrategy {

    private final long initialMs;
    private final long maxMs;
    private long currentMs;

    /**
     * Creates a new backoff strategy with the given initial and maximum values.
     *
     * @param initialMs the initial backoff value in milliseconds
     * @param maxMs     the maximum backoff value in milliseconds (cap)
     */
    public BackoffStrategy(long initialMs, long maxMs) {
        this.initialMs = initialMs;
        this.maxMs = maxMs;
        this.currentMs = initialMs;
    }

    /**
     * Returns the current backoff value and advances the internal state.
     *
     * <p>The returned value is the current backoff in milliseconds. After this call,
     * the internal value is doubled, capped at {@code maxMs}.
     *
     * @return the current backoff value in milliseconds
     */
    public long next() {
        long value = currentMs;
        currentMs = Math.min(currentMs * 2, maxMs);
        return value;
    }

    /**
     * Resets the backoff to the initial value.
     *
     * <p>After this call, the next invocation of {@link #next()} will return
     * {@code initialMs}.
     */
    public void reset() {
        currentMs = initialMs;
    }

    /**
     * Creates a poll-oriented backoff starting at 1 ms.
     *
     * <p>Suitable for tight polling loops where the initial delay should be
     * minimal (e.g., native batch polling, shuffle drain).
     *
     * @param maxMs the maximum backoff value in milliseconds
     * @return a new {@link BackoffStrategy} with {@code initialMs = 1}
     */
    public static BackoffStrategy poll(long maxMs) {
        return new BackoffStrategy(1, maxMs);
    }

    /**
     * Creates a retry-oriented backoff with explicit initial and maximum values.
     *
     * <p>Suitable for retry loops where the initial delay is meaningful
     * (e.g., registration retry, status report retry).
     *
     * @param initialMs the initial backoff value in milliseconds
     * @param maxMs     the maximum backoff value in milliseconds
     * @return a new {@link BackoffStrategy}
     */
    public static BackoffStrategy retry(long initialMs, long maxMs) {
        return new BackoffStrategy(initialMs, maxMs);
    }
}
