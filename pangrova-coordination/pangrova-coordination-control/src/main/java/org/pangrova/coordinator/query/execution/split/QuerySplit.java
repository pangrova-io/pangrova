package org.pangrova.coordinator.query.execution.split;

import java.util.List;
import java.util.Objects;
import org.pangrova.protocol.v2.DataSplit;

/**
 * Canonical split shared by stage binding, placement and task construction.
 * Locations are cache-affinity hints; access is governed by the split's canonical identity.
 */
public record QuerySplit(DataSplit data, List<String> filePaths, long estimatedScanBytes) {
    public QuerySplit(DataSplit data, List<String> filePaths) {
        this(data, filePaths, 0L);
    }

    public QuerySplit {
        Objects.requireNonNull(data, "data");
        filePaths = filePaths != null ? List.copyOf(filePaths) : List.of();
        if (estimatedScanBytes < 0) {
            throw new IllegalArgumentException("Estimated scan bytes must be non-negative");
        }
    }
}
