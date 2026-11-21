package org.pangrova.query.api.event;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import java.util.Optional;

/**
 * Emitted once when a split reaches a terminal state (completed or failed).
 *
 * <p>Carries split-level timing and I/O metrics. The event is dispatched
 * asynchronously by the task execution thread, so listeners must not assume
 * any ordering relative to {@link QueryCompletedEvent} for the same query.</p>
 *
 * @param queryId           unique identifier of the owning query
 * @param stageId           stage within the query that processed this split
 * @param splitId           unique identifier of the split within the stage
 * @param catalogName       catalog the split belongs to (e.g., {@code "iceberg_lakehouse"})
 * @param startTimeEpochMillis wall-clock time when split processing started
 * @param endTimeEpochMillis   wall-clock time when split processing ended
 * @param processedRows     number of rows read from this split
 * @param processedBytes    number of bytes read from this split
 * @param failureMessage    failure detail if the split failed, else empty
 */
@Experimental(reason = "G-SPI-08: event listener plugin SPI")
public record SplitCompletedEvent(
        String queryId,
        String stageId,
        String splitId,
        String catalogName,
        long startTimeEpochMillis,
        long endTimeEpochMillis,
        long processedRows,
        long processedBytes,
        Optional<String> failureMessage) {

    public SplitCompletedEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(stageId, "stageId must not be null");
        Objects.requireNonNull(splitId, "splitId must not be null");
        catalogName = catalogName == null ? "" : catalogName;
        failureMessage = failureMessage == null ? Optional.empty() : failureMessage;
    }

    /** @return true when the split completed successfully */
    public boolean succeeded() {
        return failureMessage.isEmpty();
    }

    /** @return wall-clock duration in milliseconds */
    public long durationMillis() {
        return endTimeEpochMillis - startTimeEpochMillis;
    }
}
