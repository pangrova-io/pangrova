package org.pangrova.connector.spi.clustering;

import org.pangrova.connector.api.clustering.ClusteringMode;
import org.pangrova.connector.api.clustering.ClusteringRequest;
import org.pangrova.connector.api.clustering.ClusteringResult;
import org.pangrova.connector.api.clustering.ClusteringTableRef;
import org.pangrova.connector.api.clustering.FileClusteringStats;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * SPI contract for connector-level clustering capability.
 *
 * <p>Connectors that support data recluster (Iceberg RewriteDataFiles,
 * Paimon CompactAction) implement this interface. Pangrova orchestrates
 * the recluster lifecycle; the connector executes the actual I/O.</p>
 *
 * <p>Implementations MUST:</p>
 * <ul>
 *   <li>Execute recluster atomically (all-or-nothing file rewrite)</li>
 *   <li>Be thread-safe (concurrent recluster on different tables)</li>
 *   <li>Return accurate file paths in ClusteringResult</li>
 * </ul>
 */
@Experimental
public interface ClusteringCapability {

    /**
     * Returns true if this connector supports the given clustering mode.
     */
    boolean supportsMode(ClusteringMode mode);

    /**
     * Collects per-file column statistics for recluster planning.
     */
    List<FileClusteringStats> collectFileStats(
            ClusteringTableRef tableName,
            List<String> columnNames);

    /**
     * Executes recluster: reads files, sorts by mode, rewrites atomically.
     */
    ClusteringResult recluster(ClusteringRequest request);
}
