package org.pangrova.worker.execution.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v1.ColumnStatisticsInfo;
import org.pangrova.protocol.v1.SplitConstraintInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters splits using constraint predicates (partition values and file-level
 * column statistics) carried in {@link SplitConstraintInfo}.
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
            List<SplitInfo> retainedSplits,
            int totalSplits,
            int skippedSplits
    ) {}

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
    public static FilterResult filter(List<SplitInfo> splits,
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

    private static FilterResult applyPredicates(List<SplitInfo> splits,
                                                List<MinMaxPredicate> predicates,
                                                String queryId, int stageId) {
        List<SplitInfo> retained = new ArrayList<>(splits.size());
        int skipped = 0;
        for (SplitInfo split : splits) {
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
    static boolean passesConstraint(SplitInfo split, List<MinMaxPredicate> predicates) {
        SplitConstraintInfo constraint = split.getSplitConstraint();
        if (constraint.equals(SplitConstraintInfo.getDefaultInstance())) {
            return true;
        }

        if (failsPartitionConstraint(constraint.getPartitionValuesMap(), predicates)) {
            return false;
        }

        List<ColumnStatisticsInfo> stats = constraint.getColumnStatisticsList();
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
            Map<String, ColumnStatisticsInfo> statsMap, List<MinMaxPredicate> predicates) {
        for (MinMaxPredicate predicate : predicates) {
            ColumnStatisticsInfo colStats = statsMap.get(predicate.column());
            if (colStats == null) {
                continue;
            }
            if (!predicate.overlaps(colStats.getMinValue(), colStats.getMaxValue())) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, ColumnStatisticsInfo> buildStatsMap(
            List<ColumnStatisticsInfo> stats) {
        var map = new java.util.HashMap<String, ColumnStatisticsInfo>(stats.size());
        for (ColumnStatisticsInfo stat : stats) {
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
            if ("MIN_MAX".equals(payload.filterType())) {
                MinMaxPredicate predicate = MinMaxPredicate.parse(payload);
                if (predicate != null) {
                    predicates.add(predicate);
                }
            }
        }
        return predicates;
    }

    /**
     * A MIN_MAX predicate extracted from a dynamic filter payload.
     * Used to evaluate against split constraint column statistics.
     */
    record MinMaxPredicate(String column, String min, String max) {

        static MinMaxPredicate parse(DynamicFilterPayload payload) {
            String data = new String(payload.filterData(), java.nio.charset.StandardCharsets.UTF_8);
            String[] parts = data.split("\\|", 3);
            if (parts.length < 3) {
                return null;
            }
            String column = (payload.column() != null && !payload.column().isEmpty())
                    ? payload.column()
                    : parts[0];
            return new MinMaxPredicate(column, parts[1], parts[2]);
        }

        /**
         * Returns true if the given value falls within [min, max].
         */
        boolean matches(String value) {
            if (value == null) {
                return true;
            }
            return value.compareTo(min) >= 0 && value.compareTo(max) <= 0;
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
            if (splitMax.compareTo(min) < 0) {
                return false;
            }
            if (splitMin.compareTo(max) > 0) {
                return false;
            }
            return true;
        }
    }
}
