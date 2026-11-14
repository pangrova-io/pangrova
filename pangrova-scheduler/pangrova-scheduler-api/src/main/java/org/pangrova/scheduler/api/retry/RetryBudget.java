package org.pangrova.scheduler.api.retry;

import org.pangrova.scheduler.api.enums.FaultTolerancePolicy;

import java.time.Duration;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Configures retry limits and backoff for fault-tolerant execution.
 *
 * <p>A {@code RetryBudget} is bound to a {@link FaultTolerancePolicy} and governs
 * how aggressively the scheduler retries failed tasks or stages before giving up
 * and failing the query. It describes limits at three granularities:
 * <ul>
 *   <li><b>Per-task:</b> maximum attempts for a single task instance.</li>
 *   <li><b>Per-stage:</b> maximum total task retries across all tasks in one stage.</li>
 *   <li><b>Per-query:</b> maximum total retries across all stages in a query.</li>
 * </ul>
 *
 * <p>Backoff is exponential with a configurable initial delay and cap.</p>
 *
 * <h3>Industry reference</h3>
 * <ul>
 *   <li>Trino: {@code retry-policy}, {@code task-retry-attempts-per-task=4},
 *       {@code retry-initial-delay=10s}, {@code retry-max-delay=1m}.</li>
 *   <li>Spark: {@code spark.task.maxFailures=4},
 *       {@code spark.stage.maxConsecutiveAttempts=4}.</li>
 * </ul>
 */
@Experimental(reason = "G-SCHED-07 fault-tolerant execution retry budget")
public final class RetryBudget {

    /** Sensible defaults aligned with Trino FTE. */
    public static final int DEFAULT_MAX_RETRIES_PER_TASK = 4;
    public static final int DEFAULT_MAX_RETRIES_PER_STAGE = 8;
    public static final int DEFAULT_MAX_RETRIES_PER_QUERY = 16;
    public static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofSeconds(10);
    public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofMinutes(1);
    public static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

    private final int maxRetriesPerTask;
    private final int maxRetriesPerStage;
    private final int maxRetriesPerQuery;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double backoffMultiplier;

    private RetryBudget(Builder builder) {
        this.maxRetriesPerTask = builder.maxRetriesPerTask;
        this.maxRetriesPerStage = builder.maxRetriesPerStage;
        this.maxRetriesPerQuery = builder.maxRetriesPerQuery;
        this.initialBackoff = builder.initialBackoff;
        this.maxBackoff = builder.maxBackoff;
        this.backoffMultiplier = builder.backoffMultiplier;
    }

    /** Creates a budget with default limits. */
    public static RetryBudget withDefaults() {
        return builder().build();
    }

    /** Creates a builder for custom retry configuration. */
    public static Builder builder() {
        return new Builder();
    }

    /** Maximum retry attempts for a single task instance before failing it. */
    public int maxRetriesPerTask() {
        return maxRetriesPerTask;
    }

    /** Maximum total task retries within a single stage. */
    public int maxRetriesPerStage() {
        return maxRetriesPerStage;
    }

    /** Maximum total retries across all stages in a query. */
    public int maxRetriesPerQuery() {
        return maxRetriesPerQuery;
    }

    /** Initial delay before the first retry attempt. */
    public Duration initialBackoff() {
        return initialBackoff;
    }

    /** Upper cap on backoff delay. */
    public Duration maxBackoff() {
        return maxBackoff;
    }

    /** Multiplier applied to backoff after each retry (exponential). */
    public double backoffMultiplier() {
        return backoffMultiplier;
    }

    /**
     * Computes the backoff delay for the given attempt number (1-based).
     *
     * @param attempt the retry attempt number, starting from 1
     * @return the delay to wait before this attempt
     */
    public Duration computeBackoff(int attempt) {
        if (attempt <= 1) {
            return initialBackoff;
        }
        long delayMs = initialBackoff.toMillis();
        for (int i = 1; i < attempt; i++) {
            delayMs = (long) (delayMs * backoffMultiplier);
            if (delayMs >= maxBackoff.toMillis()) {
                return maxBackoff;
            }
        }
        return Duration.ofMillis(delayMs);
    }

    @Override
    public String toString() {
        return "RetryBudget{"
            + "maxRetriesPerTask=" + maxRetriesPerTask
            + ", maxRetriesPerStage=" + maxRetriesPerStage
            + ", maxRetriesPerQuery=" + maxRetriesPerQuery
            + ", initialBackoff=" + initialBackoff
            + ", maxBackoff=" + maxBackoff
            + ", backoffMultiplier=" + backoffMultiplier
            + '}';
    }

    /**
     * Builder for {@link RetryBudget}.
     */
    public static final class Builder {
        private int maxRetriesPerTask = DEFAULT_MAX_RETRIES_PER_TASK;
        private int maxRetriesPerStage = DEFAULT_MAX_RETRIES_PER_STAGE;
        private int maxRetriesPerQuery = DEFAULT_MAX_RETRIES_PER_QUERY;
        private Duration initialBackoff = DEFAULT_INITIAL_BACKOFF;
        private Duration maxBackoff = DEFAULT_MAX_BACKOFF;
        private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

        private Builder() {
        }

        /** Configures the maximum retries for one task. */
        public Builder maxRetriesPerTask(int value) {
            if (value < 0) {
                throw new IllegalArgumentException(
                        "maxRetriesPerTask must be non-negative, got: " + value);
            }
            this.maxRetriesPerTask = value;
            return this;
        }

        /** Configures the maximum retries across one stage. */
        public Builder maxRetriesPerStage(int value) {
            if (value < 0) {
                throw new IllegalArgumentException(
                        "maxRetriesPerStage must be non-negative, got: " + value);
            }
            this.maxRetriesPerStage = value;
            return this;
        }

        /** Configures the maximum retries across one query. */
        public Builder maxRetriesPerQuery(int value) {
            if (value < 0) {
                throw new IllegalArgumentException(
                        "maxRetriesPerQuery must be non-negative, got: " + value);
            }
            this.maxRetriesPerQuery = value;
            return this;
        }

        /** Configures the delay before the first retry. */
        public Builder initialBackoff(Duration value) {
            if (value == null || value.isNegative()) {
                throw new IllegalArgumentException(
                        "initialBackoff must be non-null and non-negative");
            }
            this.initialBackoff = value;
            return this;
        }

        /** Configures the maximum delay between retries. */
        public Builder maxBackoff(Duration value) {
            if (value == null || value.isNegative()) {
                throw new IllegalArgumentException(
                        "maxBackoff must be non-null and non-negative");
            }
            this.maxBackoff = value;
            return this;
        }

        /** Configures the exponential multiplier applied after each retry. */
        public Builder backoffMultiplier(double value) {
            if (value < 1.0) {
                throw new IllegalArgumentException(
                        "backoffMultiplier must be >= 1.0, got: " + value);
            }
            this.backoffMultiplier = value;
            return this;
        }

        /** Builds an immutable retry budget. */
        public RetryBudget build() {
            if (maxBackoff.compareTo(initialBackoff) < 0) {
                throw new IllegalStateException(
                        "maxBackoff (" + maxBackoff + ") must be >= initialBackoff (" + initialBackoff + ")");
            }
            return new RetryBudget(this);
        }
    }
}
