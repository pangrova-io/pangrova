package org.pangrova.planner.incrementalquery;

import java.util.List;
import java.util.Objects;

/**
 * Result of analyzing a Dynamic Table query for incrementalizability.
 *
 * <p>When {@link #isIncrementalizable()} returns {@code true}, the DT refresh
 * can use incremental aggregation (merge old + delta) instead of full recompute.
 * The {@link #columnStrategies()} describe how each aggregation column should
 * be merged.</p>
 *
 * @param incrementalizable  whether the query supports incremental refresh
 * @param columnStrategies   per-column merge strategies (empty for non-aggregation queries)
 * @param reason             explanation when not incrementalizable (null when incrementalizable)
 */
public record IncrementalAnalysis(
        boolean incrementalizable,
        List<ColumnMergeStrategy> columnStrategies,
        String reason
) {

    public IncrementalAnalysis {
        Objects.requireNonNull(columnStrategies, "columnStrategies must not be null");
        columnStrategies = List.copyOf(columnStrategies);
    }

    /**
     * Whether this DT query supports incremental refresh.
     */
    public boolean isIncrementalizable() {
        return incrementalizable;
    }

    /**
     * Returns the merge strategy for each aggregation column.
     * Empty for non-aggregation queries (which are trivially incrementalizable).
     */
    public List<ColumnMergeStrategy> mergeStrategies() {
        return columnStrategies;
    }

    /**
     * Creates an incrementalizable analysis with the given merge strategies.
     */
    public static IncrementalAnalysis incrementalizable(List<ColumnMergeStrategy> strategies) {
        return new IncrementalAnalysis(true, strategies, null);
    }

    /**
     * Creates a non-incrementalizable analysis with the given reason.
     */
    public static IncrementalAnalysis nonIncrementalizable(String reason) {
        return new IncrementalAnalysis(false, List.of(), reason);
    }
}
