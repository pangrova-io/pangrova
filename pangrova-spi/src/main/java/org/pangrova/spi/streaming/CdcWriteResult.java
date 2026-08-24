package org.pangrova.spi.streaming;

import java.util.Objects;

/**
 * Immutable result of a CDC batch write operation.
 *
 * <p>Returned by {@link CdcWriteSink#writeBatch} to report write metrics.
 * Part of the <em>declarative incremental data transformation</em> subsystem.</p>
 *
 * @param rowsWritten       number of rows written in this batch (must be non-negative)
 * @param bytesWritten      estimated bytes written (must be non-negative)
 * @param dataFilesProduced number of data files produced (must be non-negative)
 * @param targetTable       fully qualified target table name (must not be null)
 */
public record CdcWriteResult(
        long rowsWritten,
        long bytesWritten,
        int dataFilesProduced,
        String targetTable
) {

    /**
     * Compact constructor enforcing non-negative values and non-null target table.
     */
    public CdcWriteResult {
        if (rowsWritten < 0) {
            throw new IllegalArgumentException("rowsWritten must be non-negative, got: " + rowsWritten);
        }
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative, got: " + bytesWritten);
        }
        if (dataFilesProduced < 0) {
            throw new IllegalArgumentException("dataFilesProduced must be non-negative, got: " + dataFilesProduced);
        }
        Objects.requireNonNull(targetTable, "targetTable must not be null");
    }
}
