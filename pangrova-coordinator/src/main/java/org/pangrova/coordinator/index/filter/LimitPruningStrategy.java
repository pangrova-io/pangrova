package org.pangrova.coordinator.index.filter;

import org.pangrova.planner.index.IndexHint;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.RangeQuery;
import org.pangrova.spi.index.statistics.FileColumnStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics.ComparisonOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LIMIT-aware pruning: when a query has {@code LIMIT k}, reduces the result
 * set to only fully-matching files whose total row count satisfies the limit.
 *
 * <p>A file "fully matches" when its column statistics prove that ALL rows
 * satisfy ALL query predicates — meaning no further filtering is needed.</p>
 */
final class LimitPruningStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(LimitPruningStrategy.class);

    private LimitPruningStrategy() {}

    /**
     * Applies LIMIT pruning to the filtered split list.
     *
     * @return a reduced list if enough fully-matching rows exist, otherwise the original list
     */
    static List<ConnectorSplit> apply(
            List<ConnectorSplit> filtered,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        if (ctx.limitK() <= 0 || ctx.fileStatistics() == null) {
            return filtered;
        }
        return tryLimitPrune(filtered, indexHints, ctx);
    }

    private static List<ConnectorSplit> tryLimitPrune(
            List<ConnectorSplit> filtered, List<IndexHint> indexHints, SplitEvalContext ctx) {
        List<ConnectorSplit> fullyMatching = new ArrayList<>();
        long totalRows = 0;
        for (ConnectorSplit split : filtered) {
            long splitRows = countFullyMatchingRows(split, indexHints, ctx);
            if (splitRows >= 0) {
                fullyMatching.add(split);
                totalRows += splitRows;
            }
        }
        if (totalRows >= ctx.limitK() && !fullyMatching.isEmpty()) {
            LOG.debug("event=limit_pruning_applied limitK={} fullyMatchingFiles={} totalRows={}",
                    ctx.limitK(), fullyMatching.size(), totalRows);
            return List.copyOf(fullyMatching);
        }
        return filtered;
    }

    /** Returns total rows if all files fully match, or -1 if not. */
    private static long countFullyMatchingRows(ConnectorSplit split, List<IndexHint> indexHints,
                                               SplitEvalContext ctx) {
        List<String> paths = split.dataFilePaths();
        if (paths.isEmpty()) {
            return -1;
        }
        long splitRows = 0;
        for (String path : paths) {
            List<FileColumnStatistics> fileStats = ctx.fileStatistics().get(path);
            if (fileStats == null || fileStats.isEmpty()
                    || !allPredicatesFullyMatch(indexHints, fileStats)) {
                return -1;
            }
            splitRows += fileStats.get(0).rowCount();
        }
        return splitRows;
    }

    private static boolean allPredicatesFullyMatch(
            List<IndexHint> hints,
            List<FileColumnStatistics> fileStats) {
        Map<String, FileColumnStatistics> byColumn = new HashMap<>();
        for (FileColumnStatistics s : fileStats) {
            byColumn.put(s.columnName(), s);
        }
        for (IndexHint hint : hints) {
            FileColumnStatistics colStats = byColumn.get(hint.predicate().column());
            if (colStats == null) {
                return false;
            }
            if (!allRowsSatisfyPredicate(hint.predicate(), colStats)) {
                return false;
            }
        }
        return true;
    }

    private static boolean allRowsSatisfyPredicate(
            IndexPredicate predicate, FileColumnStatistics stats) {
        if (predicate instanceof EqualTo eq) {
            return stats.allRowsSatisfy(ComparisonOp.EQUAL, eq.value());
        }
        if (predicate instanceof RangeQuery rq) {
            boolean lowerOk = rq.lower() == null || stats.allRowsSatisfy(
                    rq.lowerInclusive() ? ComparisonOp.GREATER_THAN_OR_EQUAL
                            : ComparisonOp.GREATER_THAN, rq.lower());
            boolean upperOk = rq.upper() == null || stats.allRowsSatisfy(
                    rq.upperInclusive() ? ComparisonOp.LESS_THAN_OR_EQUAL
                            : ComparisonOp.LESS_THAN, rq.upper());
            return lowerOk && upperOk;
        }
        return false;
    }
}
