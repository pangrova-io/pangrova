package org.pangrova.metadata.dynamictable;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable record of a single Dynamic Table refresh execution.
 *
 * <p>Persisted by {@link RefreshHistoryStore} for audit, diagnostics,
 * and information_schema queries (R23).</p>
 *
 * @param refreshId      unique identifier for this refresh execution (required)
 * @param dtName         Dynamic Table name (required)
 * @param tenantId       owning tenant identifier (required)
 * @param triggerType    how the refresh was initiated (required)
 * @param startTime      refresh start timestamp (required)
 * @param endTime        refresh end timestamp (required)
 * @param status         outcome of the refresh (required)
 * @param processedRows  number of rows processed during this refresh
 * @param bytesWritten   number of bytes written to the target table
 * @param fromCheckpoint source checkpoint at refresh start (required)
 * @param toCheckpoint   source checkpoint at refresh end (required)
 * @param errorMessage   error details when status is FAILURE (nullable)
 */
public record RefreshHistoryEntry(
        String refreshId,
        String dtName,
        String tenantId,
        TriggerType triggerType,
        Instant startTime,
        Instant endTime,
        RefreshStatus status,
        long processedRows,
        long bytesWritten,
        String fromCheckpoint,
        String toCheckpoint,
        String errorMessage
) {

    public RefreshHistoryEntry {
        Objects.requireNonNull(refreshId, "refreshId must not be null");
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(triggerType, "triggerType must not be null");
        Objects.requireNonNull(startTime, "startTime must not be null");
        Objects.requireNonNull(endTime, "endTime must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(fromCheckpoint, "fromCheckpoint must not be null");
        Objects.requireNonNull(toCheckpoint, "toCheckpoint must not be null");
    }
}
