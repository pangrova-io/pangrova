package org.pangrova.indexing.pruning;

import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.RangeQuery;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.pangrova.connector.api.enums.ColumnComparisonOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * L0 pruning: evaluates predicates against file-level column min/max statistics.
 *
 * <p>Pure metadata evaluation — zero I/O. A file can be safely skipped when
 * its column statistics prove that no rows can satisfy the query predicates.</p>
 */
final class L0StatisticsPruner {

    private static final Logger LOG = LoggerFactory.getLogger(L0StatisticsPruner.class);

    private L0StatisticsPruner() {}

    /**
     * Attempts L0 pruning for a split covering multiple data files.
     * The split can only be pruned if ALL its data files are prunable.
     */
    static boolean tryPruneAllFiles(
            List<String> filePaths,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        if (ctx.fileStatistics() == null) {
            return false;
        }
        for (String path : filePaths) {
            List<FileColumnStatistics> fileStats = ctx.fileStatistics().get(path);
            if (fileStats == null || fileStats.isEmpty()) {
                return false;
            }
            if (!canPruneFile(path, indexHints, fileStats)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Evaluates predicates against file-level column min/max statistics.
     * Returns true if the file can be safely skipped.
     */
    private static boolean canPruneFile(
            String dataFilePath,
            List<IndexHint> indexHints,
            List<FileColumnStatistics> fileStats) {
        Map<String, FileColumnStatistics> statsByColumn = new HashMap<>();
        for (FileColumnStatistics stat : fileStats) {
            statsByColumn.put(stat.columnName(), stat);
        }
        for (IndexHint hint : indexHints) {
            IndexPredicate predicate = hint.predicate();
            FileColumnStatistics colStats = statsByColumn.get(predicate.column());
            if (colStats == null) {
                continue;
            }
            if (canSkipByStatistics(predicate, colStats)) {
                LOG.debug("event=l0_prune_skip file={} column={} predicate={}",
                        dataFilePath, predicate.column(),
                        predicate.getClass().getSimpleName());
                return true;
            }
        }
        return false;
    }

    static boolean canSkipByStatistics(IndexPredicate predicate, FileColumnStatistics stats) {
        if (predicate instanceof EqualTo eq) {
            return stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, eq.value());
        }
        if (predicate instanceof RangeQuery rq) {
            return canSkipRange(rq, stats);
        }
        if (predicate instanceof InList inList) {
            return canSkipAllInListValues(inList, stats);
        }
        return false;
    }

    private static boolean canSkipRange(RangeQuery rq, FileColumnStatistics stats) {
        boolean skipLower = rq.lower() != null && stats.noRowsSatisfy(
                rq.lowerInclusive() ? ColumnComparisonOperator.GREATER_THAN_OR_EQUAL
                        : ColumnComparisonOperator.GREATER_THAN, rq.lower());
        boolean skipUpper = rq.upper() != null && stats.noRowsSatisfy(
                rq.upperInclusive() ? ColumnComparisonOperator.LESS_THAN_OR_EQUAL
                        : ColumnComparisonOperator.LESS_THAN, rq.upper());
        return skipLower || skipUpper;
    }

    private static boolean canSkipAllInListValues(InList inList, FileColumnStatistics stats) {
        for (Comparable<?> val : inList.values()) {
            if (!stats.noRowsSatisfy(ColumnComparisonOperator.EQUAL, val)) {
                return false;
            }
        }
        return true;
    }
}
