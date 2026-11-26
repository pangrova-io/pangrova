package org.pangrova.planner.analysis;

import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.util.List;

/** Exact exchanged build keys and the selected Iceberg scan they can restrict. */
public record StageRuntimeFilter(int buildStageId, String buildExchangeId,
        int probeStageId, ConnectorReadBinding probeRead, List<ProbeKey> keys) {
    public StageRuntimeFilter {
        keys = List.copyOf(keys);
        if (keys.isEmpty()) throw new IllegalArgumentException("Runtime-filter build keys are required");
    }

    public record ProbeKey(int buildOrdinal, int fieldId, String column) {}

    public static final long MAX_BUILD_ROWS = 50_000;
    private static final long LARGE_SCAN_BYTES = 128L * 1024 * 1024;
    private static final long MANY_SCAN_FILES = 32;

    /** A probe must offer enough physical work to justify bounded build materialization. */
    public static boolean enoughProbeWork(TableStatistics statistics) {
        return statistics.dataSize() >= LARGE_SCAN_BYTES || statistics.sourceWorkUnits() >= MANY_SCAN_FILES;
    }
}
