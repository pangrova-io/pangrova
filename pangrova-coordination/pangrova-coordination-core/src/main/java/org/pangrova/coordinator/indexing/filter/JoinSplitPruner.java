package org.pangrova.coordinator.indexing.filter;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.pruning.JoinPruningSummary;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Prunes probe-side splits using build-side value summaries from JOIN operations.
 *
 * <p>After the hash join build phase completes, the worker reports a
 * {@link JoinPruningSummary} with the build-side min/max range. Probe-side
 * splits whose join column range is completely disjoint from the build side
 * can be safely skipped.</p>
 */
public final class JoinSplitPruner {

    /**
     * Prunes probe-side splits where the file's join column range is
     * completely disjoint from the build-side range.
     */
    @SuppressWarnings("unchecked")
    public List<ConnectorSplit> applyBuildSideSummary(
            List<ConnectorSplit> probeSplits,
            Map<String, List<FileColumnStatistics>> probeStats,
            JoinPruningSummary summary) {
        Comparable<Object> buildMin = (Comparable<Object>) summary.buildSideMin();
        Comparable<Object> buildMax = (Comparable<Object>) summary.buildSideMax();
        if (buildMin == null || buildMax == null) {
            return probeSplits;
        }
        List<ConnectorSplit> result = new ArrayList<>(probeSplits.size());
        for (ConnectorSplit split : probeSplits) {
            if (!isDisjoint(split, probeStats, summary.joinColumn(), buildMin, buildMax)) {
                result.add(split);
            }
        }
        return List.copyOf(result);
    }

    @SuppressWarnings("unchecked")
    private static boolean isDisjoint(ConnectorSplit split,
                                      Map<String, List<FileColumnStatistics>> probeStats,
                                      String joinColumn,
                                      Comparable<Object> buildMin, Comparable<Object> buildMax) {
        FileColumnStatistics stats = findStats(split, probeStats, joinColumn);
        if (stats == null) {
            return false;
        }
        Comparable<Object> fileMin = (Comparable<Object>) stats.minValue();
        Comparable<Object> fileMax = (Comparable<Object>) stats.maxValue();
        if (fileMin == null || fileMax == null) {
            return false;
        }
        return fileMax.compareTo(buildMin) < 0 || fileMin.compareTo(buildMax) > 0;
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
