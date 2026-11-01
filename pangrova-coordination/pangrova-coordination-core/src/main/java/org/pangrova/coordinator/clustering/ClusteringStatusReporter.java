package org.pangrova.coordinator.clustering;

import org.pangrova.spi.connector.clustering.ClusteringCapability;
import org.pangrova.spi.connector.clustering.ClusteringDescriptor;
import org.pangrova.spi.connector.clustering.ClusteringState;
import org.pangrova.spi.connector.clustering.ColumnMinMax;
import org.pangrova.spi.connector.clustering.FileClusteringStats;
import org.pangrova.spi.connector.clustering.ClusteringTableRef;

import java.util.List;

/**
 * Reports clustering status for SHOW CLUSTERING STATUS.
 * Computes clustering ratio from file statistics collected via connector.
 */
public final class ClusteringStatusReporter {

    private final ClusteringMetadataStore metadataStore;

    public ClusteringStatusReporter(ClusteringMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Builds a clustering status snapshot for the given table.
     *
     * @param tableName  target table
     * @param capability connector clustering capability for collecting stats
     * @return clustering status with ratio, file counts, and state
     */
    public ClusteringStatus getStatus(
            ClusteringTableRef tableName, ClusteringCapability capability) {
        ClusteringDescriptor descriptor = metadataStore.find(tableName)
                .orElse(null);
        if (descriptor == null) {
            return new ClusteringStatus(
                    1.0, 0, 0, null, null, List.of(), ClusteringState.NONE);
        }

        List<FileClusteringStats> fileStats =
                capability.collectFileStats(tableName, descriptor.columnNames());
        double ratio = computeClusteringRatio(fileStats, descriptor.columnNames());
        int clusteredFiles = (int) Math.round(ratio * fileStats.size());

        return new ClusteringStatus(
                ratio, fileStats.size(), clusteredFiles,
                null, descriptor.mode(), descriptor.columnNames(), descriptor.state());
    }

    /**
     * Computes clustering ratio measuring how well data files are clustered.
     * A perfectly clustered table has ratio = 1.0 (no overlap between files).
     * A completely random table has ratio approaching 0.0.
     *
     * <p>For each clustering column, computes the average overlap depth:
     * how many other files overlap with each file's min/max range.
     * ratio = 1.0 - avgOverlap / fileCount</p>
     *
     * @param fileStats        per-file column statistics
     * @param clusteringColumns columns to evaluate
     * @return ratio in [0.0, 1.0]
     */
    static double computeClusteringRatio(
            List<FileClusteringStats> fileStats, List<String> clusteringColumns) {
        if (fileStats.size() <= 1) {
            return 1.0;
        }

        double totalOverlapDepth = 0.0;
        for (String col : clusteringColumns) {
            double colOverlap = computeColumnOverlap(fileStats, col);
            totalOverlapDepth += colOverlap / fileStats.size();
        }

        double avgOverlap = totalOverlapDepth / clusteringColumns.size();
        return Math.max(0.0, 1.0 - avgOverlap / fileStats.size());
    }

    private static double computeColumnOverlap(
            List<FileClusteringStats> fileStats, String col) {
        double colOverlap = 0.0;
        for (int i = 0; i < fileStats.size(); i++) {
            ColumnMinMax fBounds = findColumnBounds(fileStats.get(i), col);
            if (fBounds == null) {
                continue;
            }
            int overlapCount = 0;
            for (int j = 0; j < fileStats.size(); j++) {
                if (i == j) {
                    continue;
                }
                ColumnMinMax gBounds = findColumnBounds(fileStats.get(j), col);
                if (gBounds != null && rangesOverlap(fBounds, gBounds)) {
                    overlapCount++;
                }
            }
            colOverlap += overlapCount;
        }
        return colOverlap;
    }

    private static ColumnMinMax findColumnBounds(
            FileClusteringStats stats, String columnName) {
        for (ColumnMinMax cm : stats.columnStats()) {
            if (cm.columnName().equals(columnName)) {
                return cm;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static boolean rangesOverlap(ColumnMinMax a, ColumnMinMax b) {
        if (a.min() == null || a.max() == null || b.min() == null || b.max() == null) {
            return false;
        }
        Comparable<Object> aMin = (Comparable<Object>) a.min();
        Comparable<Object> aMax = (Comparable<Object>) a.max();
        Comparable<Object> bMin = (Comparable<Object>) b.min();
        Comparable<Object> bMax = (Comparable<Object>) b.max();
        return aMin.compareTo((Object) bMax) <= 0 && bMin.compareTo((Object) aMax) <= 0;
    }
}
