package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.enums.ShuffleRetentionPolicy;

import java.util.Objects;

/**
 * A reference to a unit of committed shuffle data held on a specific worker (G-RES-08).
 *
 * <p>This diagnostic value lacks worker incarnation, exchange generation and admission/reader
 * ownership. It must not independently authorize decommission or retry. Retention is a
 * classification, not an effective failure-survival guarantee.
 *
 * @param queryId   the owning query
 * @param stageId   the producing stage
 * @param workerId  the worker holding this data
 * @param retention the retention policy governing this data's lifecycle
 * @param sizeBytes estimated size of the shuffle data in bytes (best-effort; zero if unknown)
 * @param consumed  whether all downstream consumers have finished reading this data
 */
@Experimental(reason = "G-RES-08: shuffle data tracking for safe scale-down")
public record ShuffleDataRef(
        String queryId,
        int stageId,
        String workerId,
        ShuffleRetentionPolicy retention,
        long sizeBytes,
        boolean consumed) {

    public ShuffleDataRef {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(retention, "retention must not be null");
        if (queryId.isBlank() || workerId.isBlank() || stageId < 0) {
            throw new IllegalArgumentException("shuffle reference requires query, worker and non-negative stage");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative, got: " + sizeBytes);
        }
    }

    /** Returns true if this data is live (not yet fully consumed by downstream stages). */
    public boolean isLive() {
        return !consumed;
    }

    /** Returns its retention classification; this alone does not prove failure survival or loss. */
    public boolean isEphemeral() {
        return retention == ShuffleRetentionPolicy.EPHEMERAL;
    }
}
