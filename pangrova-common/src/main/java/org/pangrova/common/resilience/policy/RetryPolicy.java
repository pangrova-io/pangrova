package org.pangrova.common.resilience.policy;

import org.pangrova.common.resilience.policy.ErrorClassifier.ErrorCategory;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff retry policy with jitter, driven by error category.
 */
public class RetryPolicy {

    private static final long DEFAULT_INITIAL_BACKOFF_MS = 100;
    private static final long DEFAULT_MAX_BACKOFF_MS = 10_000;
    private static final double DEFAULT_MULTIPLIER = 2.0;
    private static final double DEFAULT_JITTER_FACTOR = 0.50;

    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double multiplier;
    private final double jitterFactor;
    private final ErrorClassifier classifier;

    public RetryPolicy() {
        this(DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_MS, DEFAULT_MULTIPLIER, new ErrorClassifier());
    }

    public RetryPolicy(long initialBackoffMs, long maxBackoffMs, double multiplier, ErrorClassifier classifier) {
        this(initialBackoffMs, maxBackoffMs, multiplier, DEFAULT_JITTER_FACTOR, classifier);
    }

    /** Full constructor with explicit jitter factor. */
    public RetryPolicy(long initialBackoffMs, long maxBackoffMs, double multiplier,
                        double jitterFactor, ErrorClassifier classifier) {
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
        this.classifier = classifier;
    }

    public boolean shouldRetry(ErrorCategory category, int attemptCount) {
        return shouldRetry(category, attemptCount, true);
    }

    /**
     * Determines whether to retry the operation, taking idempotency into account.
     * Non-idempotent operations are NOT retried on TIMEOUT (server may have already
     * processed the request), only on NETWORK_ERROR (connection never established).
     *
     * @param idempotent true if the operation is safe to retry without side effects
     */
    public boolean shouldRetry(ErrorCategory category, int attemptCount, boolean idempotent) {
        if (!classifier.isRetryable(category)) {
            return false;
        }
        // Refuse retry on TIMEOUT for non-idempotent operations — server may have
        // already processed the request (e.g., task creation, DML writes).
        if (!idempotent && category == ErrorCategory.TIMEOUT) {
            return false;
        }
        return attemptCount < classifier.maxRetries(category);
    }

    /**
     * Computes backoff with jitter: base * (1 ± jitterFactor).
     * Jitter prevents thundering herd when multiple clients retry simultaneously.
     */
    public Duration getBackoff(int attemptCount) {
        long base = (long) (initialBackoffMs * Math.pow(multiplier, attemptCount));
        long capped = Math.min(base, maxBackoffMs);
        long jitterRange = (long) (capped * jitterFactor);
        long jitter = jitterRange > 0
                ? ThreadLocalRandom.current().nextLong(-jitterRange, jitterRange + 1)
                : 0;
        return Duration.ofMillis(Math.max(0, capped + jitter));
    }
}
