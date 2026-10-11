package org.pangrova.worker.execution.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.ColumnStatistics;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.SplitConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters splits using constraint predicates (partition values and file-level
 * column statistics) carried in the typed v2 {@link FileSplitDetail}.
 *
 * <p>Evaluates query predicates (from dynamic filters) against the split's own
 * metadata to skip irrelevant splits before native execution. This complements
 * {@link DynamicFilterApplicator} by leveraging file-level min/max statistics
 * for non-partition columns.</p>
 *
 * <p>Thread safety: stateless — safe to share across threads.</p>
 */
public final class ConstraintBasedSplitFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ConstraintBasedSplitFilter.class);

    private ConstraintBasedSplitFilter() {}

    /**
     * Result of constraint-based split filtering.
     *
     * @param retainedSplits splits that passed constraint evaluation
     * @param totalSplits    original split count
     * @param skippedSplits  number of splits skipped by constraint evaluation
     */
    public record FilterResult(
            List<DataSplit> retainedSplits,
            int totalSplits,
            int skippedSplits
    ) {
        /** Freezes the retained split membership for this filtering decision. */
        public FilterResult {
            retainedSplits = List.copyOf(retainedSplits);
        }
    }

    /**
     * Filters splits using their constraint predicates against the provided
     * dynamic filter payloads.
     *
     * <p>For each split, evaluates the split's column statistics (min/max)
     * against MIN_MAX dynamic filters. A split is skipped if its column
     * statistics prove the split cannot contain matching rows.</p>
     *
     * @param splits   decoded SplitInfo messages
     * @param filters  dynamic filter payloads carrying query predicates
     * @param queryId  query identifier for logging
     * @param stageId  stage identifier for logging
     * @return filter result with retained splits and skip metrics
     */
    public static FilterResult filter(List<DataSplit> splits,
                                       List<DynamicFilterPayload> filters,
                                       String queryId, int stageId) {
        if (splits == null || splits.isEmpty()) {
            return new FilterResult(splits == null ? List.of() : splits, 0, 0);
        }

        List<MinMaxPredicate> predicates = extractMinMaxPredicates(filters);
        if (predicates.isEmpty()) {
            return new FilterResult(splits, splits.size(), 0);
        }

        return applyPredicates(splits, predicates, queryId, stageId);
    }

    private static FilterResult applyPredicates(List<DataSplit> splits,
                                                List<MinMaxPredicate> predicates,
                                                String queryId, int stageId) {
        List<DataSplit> retained = new ArrayList<>(splits.size());
        int skipped = 0;
        for (DataSplit split : splits) {
            if (passesConstraint(split, predicates)) {
                retained.add(split);
            } else {
                skipped++;
            }
        }

        if (skipped > 0) {
            LOG.info("event=constraint_split_filter queryId={} stageId={} total={} skipped={} retained={}",
                    queryId, stageId, splits.size(), skipped, retained.size());
        }
        return new FilterResult(List.copyOf(retained), splits.size(), skipped);
    }

    /**
     * Evaluates a single split's constraint against the predicates.
     * Returns true if the split should be retained (might contain matching data).
     */
    static boolean passesConstraint(DataSplit split, List<MinMaxPredicate> predicates) {
        // Partition values belong to the canonical DataSplit envelope, not to
        // connector-specific FileSplitDetail. They must be evaluated even when
        // the file has no optional column-statistics constraint.
        if (failsPartitionConstraint(
                DataSplitProtoCodec.stringPartitionValues(split), predicates)) {
            return false;
        }

        FileSplitDetail detail = DataSplitProtoCodec.fileDetail(split);
        if (!detail.hasSplitConstraint()) {
            return true;
        }
        SplitConstraint constraint = detail.getSplitConstraint();

        List<ColumnStatistics> stats = constraint.getColumnStatisticsList();
        if (stats.isEmpty()) {
            return true;
        }

        return !failsStatsConstraint(buildStatsMap(stats), predicates);
    }

    private static boolean failsPartitionConstraint(
            Map<String, String> partitionValues, List<MinMaxPredicate> predicates) {
        for (MinMaxPredicate predicate : predicates) {
            String partitionValue = partitionValues.get(predicate.column());
            if (partitionValue != null && !predicate.matches(partitionValue)) {
                return true;
            }
        }
        return false;
    }

    private static boolean failsStatsConstraint(
            Map<String, ColumnStatistics> statsMap, List<MinMaxPredicate> predicates) {
        for (MinMaxPredicate predicate : predicates) {
            ColumnStatistics colStats = statsMap.get(predicate.column());
            if (colStats == null) {
                continue;
            }
            if (!predicate.overlaps(colStats.getMinValue(), colStats.getMaxValue())) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, ColumnStatistics> buildStatsMap(
            List<ColumnStatistics> stats) {
        var map = new java.util.HashMap<String, ColumnStatistics>(stats.size());
        for (ColumnStatistics stat : stats) {
            map.put(stat.getColumnName(), stat);
        }
        return map;
    }

    private static List<MinMaxPredicate> extractMinMaxPredicates(
            List<DynamicFilterPayload> filters) {
        List<MinMaxPredicate> predicates = new ArrayList<>();
        if (filters == null) {
            return predicates;
        }
        for (DynamicFilterPayload payload : filters) {
            RuntimeFilter entry = TypedDynamicFilter.parseSingle(payload);
            if (entry.hasRange()) {
                predicates.add(new MinMaxPredicate(entry.getColumnName(), entry));
            }
        }
        return predicates;
    }

    /**
     * A MIN_MAX predicate extracted from a dynamic filter payload.
     * Used to evaluate against split constraint column statistics.
     */
    record MinMaxPredicate(String column, RuntimeFilter entry) {

        /**
         * Returns true if the given value falls within [min, max].
         */
        boolean matches(String value) {
            if (value == null) {
                return true;
            }
            return TypedDynamicFilter.rangeMatches(entry, value);
        }

        /**
         * Returns true if the range [splitMin, splitMax] overlaps with [min, max].
         * If either split bound is empty, conservatively returns true.
         */
        boolean overlaps(String splitMin, String splitMax) {
            if (splitMin == null || splitMin.isEmpty()
                    || splitMax == null || splitMax.isEmpty()) {
                return true;
            }
            return TypedDynamicFilter.rangeOverlaps(entry, splitMin, splitMax);
        }
    }
}
