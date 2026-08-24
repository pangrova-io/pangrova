package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.coordinator.dynamictable.write.DynamicTableWriteResult;
import org.pangrova.metadata.dynamictable.RefreshStatus;

import java.util.Objects;

/**
 * Immutable result of a single Dynamic Table refresh execution.
 *
 * <p>Captures the outcome status, processing statistics, and the new
 * checkpoint produced by the refresh pipeline. Use the static factories
 * {@link #noOp()} and {@link #success(DynamicTableWriteResult, String, long)} to
 * construct instances.</p>
 *
 * @param status        outcome of the refresh (SUCCESS, NO_OP, FAILURE, CANCELLED)
 * @param processedRows number of rows processed during this refresh
 * @param bytesWritten  number of bytes written to the target table
 * @param newCheckpoint opaque checkpoint after this refresh (nullable for NO_OP)
 * @param durationMs    wall-clock duration of the refresh in milliseconds
 */
public record RefreshResult(
        RefreshStatus status,
        long processedRows,
        long bytesWritten,
        String newCheckpoint,
        long durationMs
) {

    public RefreshResult {
        Objects.requireNonNull(status, "status must not be null");
        if (processedRows < 0) {
            throw new IllegalArgumentException("processedRows must be non-negative");
        }
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative");
        }
        if (durationMs < 0) {
            throw new IllegalArgumentException("durationMs must be non-negative");
        }
    }

    /** No changes detected — refresh skipped. */
    public static RefreshResult noOp() {
        return new RefreshResult(RefreshStatus.NO_OP, 0, 0, null, 0);
    }

    /** Successful refresh with write statistics. */
    public static RefreshResult success(DynamicTableWriteResult writeResult,
                                        String newCheckpoint,
                                        long durationMs) {
        Objects.requireNonNull(writeResult, "writeResult must not be null");
        return new RefreshResult(
                RefreshStatus.SUCCESS,
                writeResult.rowsWritten(),
                writeResult.bytesWritten(),
                newCheckpoint,
                durationMs);
    }

    /** Failed refresh with duration. */
    public static RefreshResult failure(long durationMs) {
        return new RefreshResult(RefreshStatus.FAILURE, 0, 0, null, durationMs);
    }
}
