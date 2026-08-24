package org.pangrova.metadata.sync;

import java.time.Instant;
import java.util.List;

/**
 * Result of a metadata synchronization operation.
 */
public record SyncResult(
        String catalogName,
        String syncType,
        Instant syncTime,
        int changesApplied,
        int errors,
        List<String> errorMessages,
        long durationMs) {

    public SyncResult {
        errorMessages = errorMessages == null ? List.of() : List.copyOf(errorMessages);
    }
}
