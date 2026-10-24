package org.pangrova.materialization.usecase;

import java.time.Duration;

/**
 * Retention policy of physical generation collection.
 *
 * @param retention minimum age of a settled generation before collection
 * @param batchSize maximum generations collected per pass
 */
public record GcPolicy(Duration retention, int batchSize) {

    public GcPolicy {
        if (retention == null || retention.isNegative()) {
            throw new IllegalArgumentException("retention must not be negative");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
    }

    /** Default policy: one day of retention, 64 generations per pass. */
    public static GcPolicy defaults() {
        return new GcPolicy(Duration.ofDays(1), 64);
    }
}
