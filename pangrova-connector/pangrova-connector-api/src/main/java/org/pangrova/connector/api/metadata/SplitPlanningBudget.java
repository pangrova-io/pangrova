package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;

import java.util.concurrent.TimeUnit;

/** The remaining stage capacity and monotonic deadline available to one split source. */
@Experimental
public record SplitPlanningBudget(long deadlineNanos, int remainingSplits, FileGranularity fileGranularity) {
    /** Whether the consumer can execute byte ranges independently within one data object. */
    public enum FileGranularity { WHOLE_OBJECT, BYTE_RANGES }

    public static final long DEFAULT_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_MAX_SPLITS = 100_000;

    public SplitPlanningBudget(long deadlineNanos, int remainingSplits) {
        this(deadlineNanos, remainingSplits, FileGranularity.WHOLE_OBJECT);
    }

    public SplitPlanningBudget {
        java.util.Objects.requireNonNull(fileGranularity, "fileGranularity");
        if (remainingSplits < 0) throw new IllegalArgumentException("Split capacity must not be negative");
    }

    /** Starts a new standalone enumeration with the standard stage limits. */
    public static SplitPlanningBudget standard() {
        return new SplitPlanningBudget(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS),
                DEFAULT_MAX_SPLITS);
    }

    /** Returns whether the monotonic deadline has elapsed. */
    public boolean expired() {
        return System.nanoTime() - deadlineNanos >= 0;
    }

    /** Validates a complete or partial count before the source allocates or publishes splits. */
    public void check(long splitCount) {
        if (expired()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT,
                    "Split enumeration timed out with " + splitCount + " splits planned by the source");
        }
        if (splitCount > remainingSplits) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                    "Split source needs " + splitCount + " splits with " + remainingSplits + " stage slots left");
        }
    }
}
