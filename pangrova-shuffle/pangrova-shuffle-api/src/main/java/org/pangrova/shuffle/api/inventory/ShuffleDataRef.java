package org.pangrova.shuffle.api.inventory;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.shuffle.api.capability.ShuffleRetentionPolicy;

import java.util.Objects;

/**
 * A reference to a unit of committed shuffle data held on a specific worker (G-RES-08).
 *
 * <p>The scheduler uses these references to decide whether a worker can be safely decommissioned.
 * A shuffle data ref is "live" if at least one downstream stage has not yet consumed it. The
 * scheduler must not decommission a worker that holds live EPHEMERAL shuffle data without first
 * either migrating the data or marking affected stages for re-execution.
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
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative, got: " + sizeBytes);
        }
    }

    /** Returns true if this data is live (not yet fully consumed by downstream stages). */
    public boolean isLive() {
        return !consumed;
    }

    /** Returns true if this data would be lost on worker shutdown. */
    public boolean isEphemeral() {
        return retention == ShuffleRetentionPolicy.EPHEMERAL;
    }
}
