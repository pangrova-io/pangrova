package org.pangrova.indexing.pruning;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.pruning.TopKBoundary;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Prunes splits for ORDER BY ... LIMIT queries using runtime Top-K boundaries.
 *
 * <p>Orders splits by min/max of the ORDER BY column so the top-k heap fills
 * with good values early. Once a boundary is reported by a worker, prunes
 * remaining splits where the file's range cannot contribute to the top-k.</p>
 */
public final class TopKSplitPruner {

    /**
     * Orders splits by the min (ASC) or max (DESC) of the ORDER BY column.
     */
    @SuppressWarnings("unchecked")
    public List<ConnectorSplit> orderByStatistics(
            List<ConnectorSplit> splits,
            Map<String, List<FileColumnStatistics>> statsMap,
            String orderByColumn, boolean descending) {
        List<ConnectorSplit> sorted = new ArrayList<>(splits);
        sorted.sort(Comparator.comparing(split -> {
            Comparable<Object> sortKey = extractSortKey(
                    split, statsMap, orderByColumn, descending);
            return sortKey != null ? sortKey : (Comparable<Object>) (Comparable<?>) Long.MAX_VALUE;
        }));
        if (descending) {
            java.util.Collections.reverse(sorted);
        }
        return List.copyOf(sorted);
    }

    /**
     * Prunes splits where the file's range cannot contribute to the top-k result.
     * For DESC: prune files where max < boundary.
     * For ASC: prune files where min > boundary.
     */
    @SuppressWarnings("unchecked")
    public List<ConnectorSplit> applyBoundary(
            List<ConnectorSplit> remaining,
            Map<String, List<FileColumnStatistics>> statsMap,
            TopKBoundary boundary) {
        List<ConnectorSplit> result = new ArrayList<>(remaining.size());
        for (ConnectorSplit split : remaining) {
            if (!canPrune(split, statsMap, boundary)) {
                result.add(split);
            }
        }
        return List.copyOf(result);
    }

    @SuppressWarnings("unchecked")
    private static boolean canPrune(ConnectorSplit split,
                                    Map<String, List<FileColumnStatistics>> statsMap,
                                    TopKBoundary boundary) {
        FileColumnStatistics stats = findStats(split, statsMap, boundary.orderByColumn());
        if (stats == null) {
            return false;
        }
        Comparable<Object> boundaryVal = (Comparable<Object>) boundary.boundaryValue();
        Comparable<Object> edge = boundary.descending()
                ? (Comparable<Object>) stats.maxValue()
                : (Comparable<Object>) stats.minValue();
        if (edge == null) {
            return false;
        }
        return boundary.descending()
                ? edge.compareTo(boundaryVal) < 0
                : edge.compareTo(boundaryVal) > 0;
    }

    @SuppressWarnings("unchecked")
    private static Comparable<Object> extractSortKey(
            ConnectorSplit split,
            Map<String, List<FileColumnStatistics>> statsMap,
            String column, boolean descending) {
        FileColumnStatistics stats = findStats(split, statsMap, column);
        if (stats == null) return null;
        return descending
                ? (Comparable<Object>) stats.maxValue()
                : (Comparable<Object>) stats.minValue();
    }

    private static FileColumnStatistics findStats(
            ConnectorSplit split,
            Map<String, List<FileColumnStatistics>> statsMap,
            String column) {
        for (String path : split.dataFilePaths()) {
            List<FileColumnStatistics> colStats = statsMap.get(path);
            if (colStats == null) continue;
            for (FileColumnStatistics s : colStats) {
                if (column.equals(s.columnName())) return s;
            }
        }
        return null;
    }
}
