package org.pangrova.operations.diagnostics;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable result of a thread dump operation.
 */
public record ThreadDumpResult(int threadCount, String dump, Instant timestamp) {

    public ThreadDumpResult {
        Objects.requireNonNull(dump, "dump");
        Objects.requireNonNull(timestamp, "timestamp");
        if (threadCount < 0) {
            throw new IllegalArgumentException("threadCount must be non-negative");
        }
    }
}
