package org.pangrova.connector.iceberg.read;

import org.pangrova.spi.index.statistics.FileColumnStatistics;
import org.pangrova.spi.index.statistics.RowGroupSkipInfo;
import org.pangrova.spi.index.statistics.RowGroupStatistics;
import org.pangrova.spi.index.statistics.RowGroupStatistics.ColumnStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Evaluates filter predicates against Parquet row-group-level min/max statistics
 * to determine which row groups can be skipped entirely.
 *
 * <p>This is the sub-file counterpart to {@code L0StatisticsPruner} (file-level)
 * and {@code ConstraintBasedSplitFilter} (split-level). Where those operate on
 * file-level statistics to skip entire files/splits, this pruner operates on
 * per-row-group column statistics (zone maps) to skip individual row groups
 * within a file.</p>
 *
 * <p>Thread-safe: stateless, safe to share across threads.</p>
 *
 * <h3>Supported predicate types</h3>
 * <ul>
 *   <li>EQUAL: skip if value outside [min, max]</li>
 *   <li>LESS_THAN / LESS_THAN_OR_EQUAL: skip if all values above threshold</li>
 *   <li>GREATER_THAN / GREATER_THAN_OR_EQUAL: skip if all values below threshold</li>
 *   <li>RANGE (min/max bounds): skip if no overlap with row group range</li>
 * </ul>
 */
public final class RowGroupPruner {

    private static final Logger LOG = LoggerFactory.getLogger(RowGroupPruner.class);

    private RowGroupPruner() {}

    /**
     * A filter predicate for row-group pruning. Represents a column constraint
     * with comparison operator and value.
     *
     * @param column column name the predicate applies to
     * @param op     comparison operator
     * @param value  comparison value
     */
    public record Predicate(
            String column,
            FileColumnStatistics.ComparisonOp op,
            Comparable<?> value) {}

    /**
     * A range predicate with lower and upper bounds.
     *
     * @param column         column name
     * @param lowerBound     lower bound value (null for unbounded)
     * @param upperBound     upper bound value (null for unbounded)
     * @param lowerInclusive true if lower bound is inclusive
     * @param upperInclusive true if upper bound is inclusive
     */
    public record RangePredicate(
            String column,
            Comparable<?> lowerBound,
            Comparable<?> upperBound,
            boolean lowerInclusive,
            boolean upperInclusive) {}

    /**
     * Evaluates simple predicates against row-group statistics and returns
     * a skip info indicating which row groups can be skipped.
     *
     * <p>A row group is skipped only when ALL predicates prove it contains
     * no matching rows (conjunctive/AND semantics). If any predicate cannot
     * be evaluated (missing statistics), the row group is conservatively retained.</p>
     *
     * @param filePath       data file path (for logging)
     * @param rowGroupStats  per-row-group statistics
     * @param predicates     filter predicates to evaluate
     * @return skip info with the pruning decision
     */
    public static RowGroupSkipInfo prune(
            String filePath,
            List<RowGroupStatistics> rowGroupStats,
            List<Predicate> predicates) {
        if (rowGroupStats == null || rowGroupStats.isEmpty() || predicates == null || predicates.isEmpty()) {
            return RowGroupSkipInfo.NONE;
        }

        int totalGroups = rowGroupStats.size();
        BitSet skipped = new BitSet(totalGroups);
        int skippedCount = 0;

        for (RowGroupStatistics rgStats : rowGroupStats) {
            if (canSkipRowGroup(rgStats, predicates)) {
                skipped.set(rgStats.rowGroupIndex());
                skippedCount++;
            }
        }

        if (skippedCount > 0) {
            LOG.info("event=row_group_prune file={} total={} skipped={} retained={}",
                    filePath, totalGroups, skippedCount, totalGroups - skippedCount);
        }

        return new RowGroupSkipInfo(filePath, totalGroups, skipped);
    }

    /**
     * Evaluates range predicates against row-group statistics.
     * A range predicate skips a row group when the row group's column range
     * does not overlap with the predicate's range.
     *
     * @param filePath       data file path (for logging)
     * @param rowGroupStats  per-row-group statistics
     * @param predicates     range predicates to evaluate
     * @return skip info with the pruning decision
     */
    public static RowGroupSkipInfo pruneByRange(
            String filePath,
            List<RowGroupStatistics> rowGroupStats,
            List<RangePredicate> predicates) {
        if (rowGroupStats == null || rowGroupStats.isEmpty() || predicates == null || predicates.isEmpty()) {
            return RowGroupSkipInfo.NONE;
        }

        int totalGroups = rowGroupStats.size();
        BitSet skipped = new BitSet(totalGroups);
        int skippedCount = 0;

        for (RowGroupStatistics rgStats : rowGroupStats) {
            if (canSkipByRange(rgStats, predicates)) {
                skipped.set(rgStats.rowGroupIndex());
                skippedCount++;
            }
        }

        if (skippedCount > 0) {
            LOG.info("event=row_group_range_prune file={} total={} skipped={} retained={}",
                    filePath, totalGroups, skippedCount, totalGroups - skippedCount);
        }

        return new RowGroupSkipInfo(filePath, totalGroups, skipped);
    }

    /**
     * Combined pruning: evaluates both simple predicates and range predicates.
     * A row group is skipped if ANY predicate (simple or range) proves it
     * contains no matching rows.
     */
    public static RowGroupSkipInfo pruneAll(
            String filePath,
            List<RowGroupStatistics> rowGroupStats,
            List<Predicate> simplePredicates,
            List<RangePredicate> rangePredicates) {
        if (!hasContent(rowGroupStats)) {
            return RowGroupSkipInfo.NONE;
        }

        boolean hasSimple = hasContent(simplePredicates);
        boolean hasRange = hasContent(rangePredicates);
        if (!hasSimple && !hasRange) {
            return RowGroupSkipInfo.NONE;
        }

        int totalGroups = rowGroupStats.size();
        BitSet skipped = new BitSet(totalGroups);
        int skippedCount = markSkippable(
                rowGroupStats, hasSimple, simplePredicates, hasRange, rangePredicates, skipped);

        if (skippedCount > 0) {
            LOG.info("event=row_group_combined_prune file={} total={} skipped={} retained={}",
                    filePath, totalGroups, skippedCount, totalGroups - skippedCount);
        }

        return new RowGroupSkipInfo(filePath, totalGroups, skipped);
    }

    private static boolean hasContent(List<?> values) {
        return values != null && !values.isEmpty();
    }

    private static int markSkippable(
            List<RowGroupStatistics> rowGroupStats,
            boolean hasSimple,
            List<Predicate> simplePredicates,
            boolean hasRange,
            List<RangePredicate> rangePredicates,
            BitSet skipped) {
        int skippedCount = 0;
        for (RowGroupStatistics rgStats : rowGroupStats) {
            if (skippableByAny(rgStats, hasSimple, simplePredicates, hasRange, rangePredicates)) {
                skipped.set(rgStats.rowGroupIndex());
                skippedCount++;
            }
        }
        return skippedCount;
    }

    private static boolean skippableByAny(
            RowGroupStatistics rgStats,
            boolean hasSimple,
            List<Predicate> simplePredicates,
            boolean hasRange,
            List<RangePredicate> rangePredicates) {
        if (hasSimple && canSkipRowGroup(rgStats, simplePredicates)) {
            return true;
        }
        return hasRange && canSkipByRange(rgStats, rangePredicates);
    }

    /**
     * Returns true if the row group can be skipped based on simple predicates.
     * Any single predicate that proves no rows match is sufficient to skip.
     */
    static boolean canSkipRowGroup(RowGroupStatistics rgStats, List<Predicate> predicates) {
        Map<String, ColumnStats> stats = rgStats.columnStats();
        for (Predicate predicate : predicates) {
            ColumnStats colStats = stats.get(predicate.column());
            if (colStats == null || !colStats.hasBounds()) {
                continue;
            }
            if (colStats.noRowsSatisfy(predicate.op(), predicate.value())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the row group can be skipped based on range predicates.
     */
    @SuppressWarnings("unchecked")
    static boolean canSkipByRange(RowGroupStatistics rgStats, List<RangePredicate> predicates) {
        Map<String, ColumnStats> stats = rgStats.columnStats();
        for (RangePredicate predicate : predicates) {
            ColumnStats colStats = stats.get(predicate.column());
            if (colStats == null || !colStats.hasBounds()) {
                continue;
            }
            if (rangeDoesNotOverlap(colStats, predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether a row group's column range [rgMin, rgMax] has no overlap
     * with the predicate range [lower, upper].
     */
    @SuppressWarnings("unchecked")
    private static boolean rangeDoesNotOverlap(ColumnStats colStats, RangePredicate predicate) {
        Comparable<Object> rgMin = (Comparable<Object>) colStats.minValue();
        Comparable<Object> rgMax = (Comparable<Object>) colStats.maxValue();
        return entirelyBelowLower(rgMax, predicate) || entirelyAboveUpper(rgMin, predicate);
    }

    /** Checks if the row group range is entirely below the predicate's lower bound. */
    @SuppressWarnings("unchecked")
    private static boolean entirelyBelowLower(Comparable<Object> rgMax, RangePredicate predicate) {
        if (predicate.lowerBound() == null) {
            return false;
        }
        Comparable<Object> lower = (Comparable<Object>) predicate.lowerBound();
        // inclusive lower: skip when rgMax < lower; exclusive lower: skip when rgMax <= lower
        int boundary = predicate.lowerInclusive() ? -1 : 0;
        return rgMax.compareTo(lower) <= boundary;
    }

    /** Checks if the row group range is entirely above the predicate's upper bound. */
    @SuppressWarnings("unchecked")
    private static boolean entirelyAboveUpper(Comparable<Object> rgMin, RangePredicate predicate) {
        if (predicate.upperBound() == null) {
            return false;
        }
        Comparable<Object> upper = (Comparable<Object>) predicate.upperBound();
        // inclusive upper: skip when rgMin > upper; exclusive upper: skip when rgMin >= upper
        int boundary = predicate.upperInclusive() ? 1 : 0;
        return rgMin.compareTo(upper) >= boundary;
    }
}
