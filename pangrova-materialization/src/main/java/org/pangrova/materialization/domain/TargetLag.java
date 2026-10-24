package org.pangrova.materialization.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * Freshness target of a scheduled materialization.
 *
 * <p>Target lag is a staleness objective, not a refresh interval. Manual
 * objects have no target lag at all; the ambiguous {@code PT0S} convention is
 * rejected.
 *
 * @param value positive freshness target
 */
public record TargetLag(Duration value) {

    public TargetLag {
        if (value == null) {
            throw new IllegalArgumentException("target lag must not be null");
        }
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("target lag must be positive");
        }
        if (value.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException("target lag must not exceed 365 days");
        }
    }

    /**
     * Returns the instant by which a refresh started at {@code now} must have completed.
     *
     * @param now start instant
     * @return freshness deadline
     */
    public Instant deadlineFrom(Instant now) {
        return now.plus(value);
    }
}
