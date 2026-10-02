package org.pangrova.coordinator.clustering;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.statement.TableResolver;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.parser.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.clustering.ClusteringCapability;
import org.pangrova.spi.connector.clustering.ClusteringDescriptor;
import org.pangrova.spi.connector.clustering.ClusteringMode;
import org.pangrova.spi.connector.clustering.ClusteringState;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Executes clustering DDL statements:
 * SET CLUSTERING ORDER, RECLUSTER TABLE, SHOW CLUSTERING STATUS.
 *
 * <p>Resolves connector clustering capability via CatalogRegistry,
 * delegates metadata persistence to ClusteringMetadataStore,
 * scheduling to ClusteringScheduler, and status reporting to
 * ClusteringStatusReporter.</p>
 */
public final class ClusteringDdlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ClusteringDdlExecutor.class);

    private final ClusteringDdlContext deps;

    ClusteringDdlExecutor(ClusteringDdlContext deps) {
        this.deps = deps;
    }

    /**
     * Handles ALTER TABLE t SET CLUSTERING ORDER (col1, col2) [MODE ...].
     * Validates connector support, stores descriptor, returns confirmation.
     */
    public List<TypedRow> executeSetClusteringOrder(SetClusteringOrderStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ClusteringCapability capability = resolveCapability(tableRef, context);
        ClusteringMode mode = parseMode(stmt.mode());

        if (!capability.supportsMode(mode)) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Connector does not support clustering mode: " + mode);
        }

        var spiTableName = toSpiTableName(tableRef);
        ClusteringDescriptor descriptor = new ClusteringDescriptor(
                spiTableName, stmt.columnNames(), mode, ClusteringState.PENDING);

        deps.metadataStore().store(descriptor);
        LOG.info("action=set_clustering_order table={} columns={} mode={}",
                tableRef, stmt.columnNames(), mode);

        return List.of(TypedRow.builder()
                .putString("result", "CLUSTERING ORDER SET")
                .putString("table", tableRef.toString())
                .putString("mode", mode.name())
                .build());
    }

    /**
     * Handles RECLUSTER TABLE t [WHERE ...].
     * Submits an async recluster task and returns the task ID immediately.
     * Users can poll progress via SHOW CLUSTERING STATUS.
     */
    public List<TypedRow> executeReclusterTable(ReclusterTableStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ClusteringCapability capability = resolveCapability(tableRef, context);
        var spiTableName = toSpiTableName(tableRef);

        ClusteringDescriptor descriptor = deps.metadataStore().find(spiTableName)
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_INDEX_NOT_FOUND,
                        "No clustering order defined for table " + tableRef));

        String taskId = java.util.UUID.randomUUID().toString();
        deps.scheduler().scheduleManualRecluster(descriptor, capability);
        LOG.info("action=recluster_submitted table={} taskId={}", tableRef, taskId);

        return List.of(TypedRow.builder()
                .putString("result", "RECLUSTER SUBMITTED")
                .putString("table", tableRef.toString())
                .putString("task_id", taskId)
                .build());
    }

    /**
     * Handles SHOW CLUSTERING STATUS FOR TABLE t.
     * Returns clustering ratio, file counts, and state.
     */
    public List<TypedRow> executeShowClusteringStatus(ShowClusteringStatusStatement stmt, CoordinatorSessionContext context) {
        QualifiedTableName tableRef = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ClusteringCapability capability = resolveCapability(tableRef, context);

        ClusteringStatus status = deps.statusReporter().getStatus(
                toSpiTableName(tableRef), capability);

        return List.of(TypedRow.builder()
                .putString("table", tableRef.toString())
                .putString("clustering_ratio",
                        String.format("%.2f%%", status.clusteringRatio() * 100))
                .putInt("total_files", status.totalFiles())
                .putInt("clustered_files", status.clusteredFiles())
                .putString("mode", status.mode() != null ? status.mode().name() : "NONE")
                .putString("columns", String.join(", ", status.columns()))
                .putString("state", status.state().name())
                .build());
    }

    // --- Private helpers ---

    private ClusteringCapability resolveCapability(QualifiedTableName tableRef, CoordinatorSessionContext context) {
        CatalogHandle handle = deps.catalogRegistry().getCatalog(tableRef.catalog());
        ConnectorSession session = TableResolver.buildSession(context, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata metadata = handle.getMetadata(session);
        return metadata.getClusteringCapability()
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                        "Connector for catalog '" + tableRef.catalog()
                                + "' does not support clustering"));
    }

    private static ClusteringMode parseMode(String modeStr) {
        if (modeStr == null || modeStr.isBlank()) {
            return ClusteringMode.INTERLEAVE;
        }
        try {
            return ClusteringMode.valueOf(modeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Unknown clustering mode: " + modeStr);
        }
    }

    private static org.pangrova.spi.connector.clustering.ClusteringTableRef toSpiTableName(
            QualifiedTableName ref) {
        return new org.pangrova.spi.connector.clustering.ClusteringTableRef(
                ref.catalog(), ref.schema(), ref.table());
    }
}
