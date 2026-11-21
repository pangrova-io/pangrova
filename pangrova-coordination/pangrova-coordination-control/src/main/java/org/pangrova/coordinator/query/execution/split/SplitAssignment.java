package org.pangrova.coordinator.query.execution.split;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.List;

/**
 * Represents a single split assignment with locality metadata.
 *
 * @param splitIndex    zero-based index within the stage's split list
 * @param splitData     serialized split data (opaque bytes or JSON)
 * @param dataLocations hostnames where the split's data resides (for locality)
 * @param filePaths     file paths for cache affinity routing
 * @param enqueueTimeMs timestamp when the split was enqueued (for steal-delay)
 */
public record SplitAssignment(
        int splitIndex,
        String splitData,
        List<String> dataLocations,
        List<String> filePaths,
        long enqueueTimeMs
) {

    public SplitAssignment {
        checkArgument(splitIndex >= 0, "splitIndex must be non-negative");
        checkNotNull(splitData, "splitData must not be null");
        dataLocations = dataLocations != null ? List.copyOf(dataLocations) : List.of();
        filePaths = filePaths != null ? List.copyOf(filePaths) : List.of();
        if (enqueueTimeMs <= 0) {
            enqueueTimeMs = System.currentTimeMillis();
        }
    }

    /** Convenience constructor without file paths and auto-timestamp. */
    public SplitAssignment(int splitIndex, String splitData, List<String> dataLocations) {
        this(splitIndex, splitData, dataLocations, List.of(), System.currentTimeMillis());
    }
}
