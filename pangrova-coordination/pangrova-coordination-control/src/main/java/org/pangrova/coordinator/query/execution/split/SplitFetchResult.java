package org.pangrova.coordinator.query.execution.split;

import org.pangrova.query.api.task.QuerySplit;

import java.util.List;

/**
 * Result of a split fetch operation carrying the outcome, splits, and
 * diagnostic context for structured error reporting.
 */
public record SplitFetchResult(
        SplitFetchOutcome outcome,
        List<QuerySplit> splits,
        String failureReason
) {
    public SplitFetchResult {
        splits = splits != null ? List.copyOf(splits) : List.of();
    }

    public static SplitFetchResult success(List<QuerySplit> splits) {
        return new SplitFetchResult(SplitFetchOutcome.SUCCESS, splits, null);
    }

    public static SplitFetchResult failed(String reason) {
        return new SplitFetchResult(SplitFetchOutcome.FAILED, List.of(), reason);
    }

    public static SplitFetchResult limitReached(List<QuerySplit> splits, String reason) {
        return new SplitFetchResult(SplitFetchOutcome.LIMIT_REACHED, splits, reason);
    }

    /**
     * Terminal outcome of a split enumeration operation.
     */
    public enum SplitFetchOutcome {
        SUCCESS,
        FAILED,
        LIMIT_REACHED
    }
}
