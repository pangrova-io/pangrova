package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Builds {@link TableStatistics} for a table's selected snapshot, pinning the
 * snapshot for the active query and classifying failures through the catalog
 * exception classifier.
 */
final class IcebergTableStatisticsReader {

    private IcebergTableStatisticsReader() {}

    /** Reads table statistics for the selected snapshot of the given table. */
    static TableStatistics read(IcebergCatalogTables catalogTables,
                                IcebergTableHandle handle,
                                ConnectorSession session,
                                IcebergSnapshotPinManager pinManager,
                                CatalogExceptionClassifier exceptionClassifier) {
        String tableId = handle.schemaName() + "." + handle.tableName();
        try {
            Table icebergTable = catalogTables.resolveTable(handle);
            Snapshot snapshot = IcebergReadBindings.selectedSnapshot(icebergTable, handle);
            if (snapshot == null) {
                return new TableStatistics(0, 0, 0, java.util.Map.of());
            }
            // Pin the snapshot for the current query to ensure consistent reads
            String queryId = session.getProperties().get("queryId");
            if (queryId != null) {
                pinManager.pinSnapshotForQuery(queryId, handle.schemaName(), handle.tableName(), snapshot.snapshotId());
            }
            return IcebergStatisticsCollector.buildStatistics(snapshot, icebergTable, handle);
        } catch (Exception e) {
            throw exceptionClassifier.handleAndClassify(e, "getTableStatistics", tableId);
        }
    }
}
