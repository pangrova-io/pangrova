package org.pangrova.planner.rule;

import org.pangrova.planner.stats.ColumnStatistics;
import org.pangrova.planner.stats.StatisticsCollector;

/**
 * Partition pruning rule that uses column statistics to eliminate
 * partitions whose values fall outside the predicate range.
 *
 * <p>Conservative: when no statistics are available, never prune.</p>
 *
 * <p>Validates: Requirements 2.6</p>
 */
public class PartitionPruningRule {

    /**
     * Determine whether a partition can be pruned based on column min/max statistics.
     *
     * @return true if the predicate value is outside the column's [min, max] range
     */
    public boolean shouldPrune(
            String tableName,
            String partitionColumn,
            Comparable<?> predicateValue,
            StatisticsCollector collector) {

        return collector.getStatistics(tableName)
                .map(ts -> ts.columns().get(partitionColumn))
                .map(cs -> isOutsideRange(cs, predicateValue))
                .orElse(false);
    }

    /**
     * Checks if the predicate value falls outside the column's [min, max] range.
     * Returns false (conservative: do not prune) when min/max are null or when
     * the predicate type is incompatible with the statistics type.
     */
    @SuppressWarnings("unchecked")
    private boolean isOutsideRange(ColumnStatistics cs, Comparable<?> value) {
        Comparable<?> min = cs.min();
        Comparable<?> max = cs.max();
        if (min == null || max == null) {
            return false;
        }
        if (!min.getClass().isInstance(value)) {
            return false;
        }
        Comparable<Object> typedValue = (Comparable<Object>) value;
        return typedValue.compareTo(min) < 0 || typedValue.compareTo(max) > 0;
    }
}
