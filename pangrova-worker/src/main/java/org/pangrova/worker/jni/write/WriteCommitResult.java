package org.pangrova.worker.jni.write;

import java.util.List;

/**
 * Result returned by {@link NativeWriter#commitWrite(long)} after a
 * write transaction's file materialization phase completes.
 *
 * <p>Contains commit metadata fragments produced by
 * {@link org.pangrova.spi.connector.data.ConnectorPageSink#finishWithMetadata()}.
 * The coordinator aggregates these fragments from all workers and performs
 * a single atomic catalog commit.</p>
 *
 * @param rowsWritten      total number of rows written to data files
 * @param bytesWritten     total bytes written to storage
 * @param filePathsCreated storage file paths created during the write (never null, may be empty)
 * @param commitMetadata   serialized commit metadata fragments for coordinator-side commit (never null)
 */
public record WriteCommitResult(
        long rowsWritten,
        long bytesWritten,
        List<String> filePathsCreated,
        byte[] commitMetadata) {

    /**
     * Compact constructor enforcing defensive copy and non-null invariants.
     */
    public WriteCommitResult {
        filePathsCreated = filePathsCreated != null ? List.copyOf(filePathsCreated) : List.of();
        commitMetadata = commitMetadata != null ? commitMetadata : new byte[0];
    }
}
