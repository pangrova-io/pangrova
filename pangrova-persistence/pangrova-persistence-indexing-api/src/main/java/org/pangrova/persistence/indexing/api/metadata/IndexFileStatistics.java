package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.List;
import java.util.Map;

/** Tenant and snapshot scoped pruning statistics. */
@Experimental
public interface IndexFileStatistics {
    /** Atomically replaces each complete file/snapshot statistics set within the exact tenant and table. */
    default void batchInsertFileStatistics(
            String tenantId, String catalog, String schema, String table,
            List<org.pangrova.connector.api.index.statistics.DataFileStatistics> statistics) {
        throw new UnsupportedOperationException("Snapshot-scoped file statistics unavailable");
    }

    /**
     * Deletes file column statistics for the specified data file paths.
     * Called when files are removed (OVERWRITE/COMPACT) to prevent stale statistics accumulation.
     *
     * @param catalog   catalog name
     * @param tenantId authenticated owning tenant; never inferred from a catalog or file path
     * @param schema    schema name
     * @param table     table name
     * @param dataFilePaths paths of removed data files
     */
    default void deleteFileStatistics(String tenantId, String catalog, String schema, String table,
                                      List<String> dataFilePaths) {
        throw new UnsupportedOperationException("Snapshot-scoped file statistics unavailable");
    }

    /**
     * Returns column-level statistics for the specified data files and columns.
     * Returns dataFilePath → list of column statistics for the exact tenant and source snapshot only.
     * Unscoped historical rows are not query authority and must not be returned.
     */
    default Map<String, List<org.pangrova.connector.api.index.statistics.FileColumnStatistics>> getFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotId,
            List<String> dataFilePaths, List<String> columns) {
        throw new UnsupportedOperationException("Snapshot-scoped file statistics unavailable");
    }
}
