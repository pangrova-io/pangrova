package org.pangrova.common.concurrent;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Retries a commit operation with exponential backoff and jitter when an
 * Optimistic Concurrency Control (OCC) conflict is detected.
 *
 * <p>Designed for wrapping optimistic-concurrency-control (OCC) commit calls that
 * may throw on version conflicts under concurrent writes. The caller supplies
 * a predicate that identifies retryable exceptions (e.g., CommitFailedException).
 *
 * <p>Jitter is applied as a random fraction (0.5–1.0×) of the computed delay
 * to prevent thundering herd when multiple writers retry simultaneously.
 *
 * <p>Thread-safe: each invocation of {@link #execute(Runnable)} uses its own
 * backoff state. The instance itself is immutable and shareable.
 */
@ThreadSafe
public final class RetryingCommitWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingCommitWrapper.class);

    private static final long DEFAULT_INITIAL_DELAY_MS = 100;
    private static final long DEFAULT_MAX_DELAY_MS = 5000;
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final double JITTER_MIN_FACTOR = 0.5;

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final int maxRetries;
    private final Predicate<Exception> retryable;

    private RetryingCommitWrapper(long initialDelayMs, long maxDelayMs,
                                  int maxRetries, Predicate<Exception> retryable) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.maxRetries = maxRetries;
        this.retryable = retryable;
    }

    /**
     * Creates a wrapper with default parameters (initial=100ms, max=5s, maxRetries=5)
     * and the given retryable predicate.
     *
     * @param retryable predicate that returns true for exceptions that indicate
     *                  an OCC conflict and should trigger a retry
     * @return a new immutable wrapper instance
     */
    public static RetryingCommitWrapper withDefaults(Predicate<Exception> retryable) {
        return new RetryingCommitWrapper(
                DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_DELAY_MS,
                DEFAULT_MAX_RETRIES, retryable);
    }

    /**
     * Creates a wrapper with custom parameters.
     *
     * @param initialDelayMs initial backoff delay in milliseconds
     * @param maxDelayMs     maximum backoff delay in milliseconds
     * @param maxRetries     maximum number of retry attempts
     * @param retryable      predicate identifying retryable exceptions
     * @return a new immutable wrapper instance
     */
    public static RetryingCommitWrapper create(long initialDelayMs, long maxDelayMs,
                                               int maxRetries, Predicate<Exception> retryable) {
        if (initialDelayMs <= 0) {
            throw new IllegalArgumentException("initialDelayMs must be positive: " + initialDelayMs);
        }
        if (maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("maxDelayMs must be >= initialDelayMs");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative: " + maxRetries);
        }
        return new RetryingCommitWrapper(initialDelayMs, maxDelayMs, maxRetries, retryable);
    }

    /**
     * Executes the given commit action, retrying on retryable exceptions with
     * exponential backoff and jitter.
     *
     * @param commitAction the commit operation to execute
     * @throws PangrovaException if all retry attempts are exhausted
     * @throws RuntimeException if a non-retryable exception is thrown
     */
    public void execute(Runnable commitAction) {
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelayMs);
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                commitAction.run();
                if (attempt > 0) {
                    LOG.info("event=occ_commit_succeeded attempt={}", attempt + 1);
                }
                return;
            } catch (Exception e) {
                if (!retryable.test(e)) {
                    throw rethrow(e);
                }
                lastException = e;
                if (attempt == maxRetries) {
                    break;
                }
                long baseDelay = backoff.next();
                long jitteredDelay = applyJitter(baseDelay);
                LOG.warn("event=occ_commit_conflict attempt={} nextRetryMs={} error={}",
                        attempt + 1, jitteredDelay, e.getMessage());
                sleep(jitteredDelay);
            }
        }

        throw new PangrovaException(
                PangrovaErrorCode.RESILIENCE_RETRY_EXHAUSTED,
                "OCC commit failed after " + (maxRetries + 1) + " attempts",
                lastException);
    }

    /**
     * Applies jitter to the base delay. The jittered value is uniformly
     * distributed in [baseDelay * 0.5, baseDelay].
     */
    static long applyJitter(long baseDelay) {
        double jitterFactor = JITTER_MIN_FACTOR
                + ThreadLocalRandom.current().nextDouble() * (1.0 - JITTER_MIN_FACTOR);
        return Math.max(1, (long) (baseDelay * jitterFactor));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PangrovaException(
                    PangrovaErrorCode.RESILIENCE_RETRY_EXHAUSTED,
                    "OCC commit retry interrupted", e);
        }
    }

    private static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException re) {
            throw re;
        }
        throw new RuntimeException(e);
    }
}
