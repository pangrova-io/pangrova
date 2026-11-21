package org.pangrova.coordinator.query.execution.split;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Time-limited lease for a split assigned to a worker.
 * If the worker fails to complete the split before the lease expires,
 * the split is returned to the pending queue for reassignment.
 *
 * @param splitIndex    the split's index within the stage
 * @param workerId      the worker holding this lease
 * @param leaseExpiryMs absolute timestamp when the lease expires
 * @param split         the split assignment
 */
public record SplitLease(
        int splitIndex,
        String workerId,
        long leaseExpiryMs,
        SplitAssignment split
) {

    public SplitLease {
        checkNotNull(workerId, "workerId must not be null");
        checkNotNull(split, "split must not be null");
    }

    /** Returns true if this lease has expired. */
    public boolean isExpired() {
        return System.currentTimeMillis() > leaseExpiryMs;
    }
}
