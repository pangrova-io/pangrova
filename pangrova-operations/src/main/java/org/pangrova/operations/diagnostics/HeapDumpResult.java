package org.pangrova.operations.diagnostics;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable result of a heap dump operation.
 */
public record HeapDumpResult(String filePath, long fileSizeBytes, Instant timestamp) {

    public HeapDumpResult {
        Objects.requireNonNull(filePath, "filePath");
        Objects.requireNonNull(timestamp, "timestamp");
        if (fileSizeBytes < 0) {
            throw new IllegalArgumentException("fileSizeBytes must be non-negative");
        }
    }
}
