package org.pangrova.planner.analysis;

import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.TableStatistics;

/** Exact exchanged build key and the selected Iceberg scan field it can restrict. */
public record StageRuntimeFilter(int buildStageId, String buildExchangeId, int buildKeyOrdinal,
        int probeStageId, ConnectorReadBinding probeRead, int probeFieldId, String probeColumn) {
    public static final long MAX_BUILD_ROWS = 50_000;
    private static final long LARGE_SCAN_BYTES = 128L * 1024 * 1024;
    private static final long MANY_SCAN_FILES = 32;

    /** A probe must offer enough physical work to justify bounded build materialization. */
    public static boolean enoughProbeWork(TableStatistics statistics) {
        return statistics.dataSize() >= LARGE_SCAN_BYTES || statistics.sourceWorkUnits() >= MANY_SCAN_FILES;
    }
}
