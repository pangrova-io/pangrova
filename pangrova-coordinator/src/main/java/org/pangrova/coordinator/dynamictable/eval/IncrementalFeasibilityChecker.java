package org.pangrova.coordinator.dynamictable.eval;

import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Performs static analysis on a defining query to determine whether
 * incremental evaluation is supported.
 *
 * <p>Accepts a set of {@link QueryFeature} flags extracted from the query AST
 * and checks against the feasibility matrix (R11 AC3).</p>
 *
 * <p>Feasibility matrix:</p>
 * <ul>
 *   <li>FILTER — supported</li>
 *   <li>PROJECTION — supported</li>
 *   <li>INNER_JOIN (equi-join only) — supported</li>
 *   <li>GROUP_BY — supported</li>
 *   <li>UNION_ALL — supported</li>
 *   <li>WINDOW — not supported</li>
 *   <li>NON_EQUI_JOIN — not supported</li>
 *   <li>DISTINCT_WITHOUT_GROUP_BY — not supported</li>
 * </ul>
 */
public final class IncrementalFeasibilityChecker {

    private static final Set<QueryFeature> UNSUPPORTED = EnumSet.of(
            QueryFeature.WINDOW,
            QueryFeature.NON_EQUI_JOIN,
            QueryFeature.DISTINCT_WITHOUT_GROUP_BY
    );

    private static final Set<String> SUPPORTED_AGGREGATIONS = Set.of("SUM", "COUNT", "AVG");

    /**
     * Check whether the given query features are all incrementally evaluable.
     *
     * <p>This overload does not consider refresh mode — it only validates
     * operator feasibility. Use {@link #check(Set, DynamicTableRefreshMode)} when the
     * refresh mode is known.</p>
     *
     * @param features set of features detected in the defining query AST
     * @return feasibility result with reason if not feasible
     */
    public FeasibilityResult check(Set<QueryFeature> features) {
        Objects.requireNonNull(features, "features must not be null");

        StringJoiner unsupportedOps = new StringJoiner(", ");
        for (QueryFeature feature : features) {
            if (UNSUPPORTED.contains(feature)) {
                unsupportedOps.add(feature.displayName());
            }
        }

        if (unsupportedOps.length() > 0) {
            String reason = "Unsupported operators for incremental evaluation: "
                    + unsupportedOps;
            return new FeasibilityResult(false, reason);
        }
        return new FeasibilityResult(true, null);
    }

    /**
     * Check whether the given query features are incrementally evaluable
     * under the specified refresh mode.
     *
     * <p>In SNAPSHOT mode, only aggregation queries (containing GROUP_BY)
     * are supported. Non-aggregation queries cannot be incrementally
     * evaluated from snapshot diffs because the diff produces data file
     * paths rather than row-level CDC events.</p>
     *
     * @param features    set of features detected in the defining query AST
     * @param refreshMode the refresh mode of the Dynamic Table
     * @return feasibility result with reason if not feasible
     */
    public FeasibilityResult check(Set<QueryFeature> features, DynamicTableRefreshMode refreshMode) {
        Objects.requireNonNull(features, "features must not be null");
        Objects.requireNonNull(refreshMode, "refreshMode must not be null");

        // Delegate operator-level check first
        FeasibilityResult operatorResult = check(features);
        if (!operatorResult.feasible()) {
            return operatorResult;
        }

        // SNAPSHOT mode requires aggregation (GROUP_BY)
        if (refreshMode == DynamicTableRefreshMode.SNAPSHOT && !features.contains(QueryFeature.GROUP_BY)) {
            return new FeasibilityResult(false,
                    "Snapshot mode only supports aggregation queries");
        }

        return new FeasibilityResult(true, null);
    }

    /**
     * Check whether the given query features and aggregation function types
     * are incrementally evaluable under the specified refresh mode.
     *
     * <p>In addition to operator and refresh-mode checks, this overload
     * validates that all aggregation functions are supported. Currently
     * supported: SUM, COUNT, AVG. MIN and MAX are rejected because their
     * retract semantics require a full scan of the source data.</p>
     *
     * @param features         set of features detected in the defining query AST
     * @param refreshMode      the refresh mode of the Dynamic Table
     * @param aggregationTypes aggregation function type names used in the query
     * @return feasibility result with reason if not feasible
     */
    public FeasibilityResult check(
            Set<QueryFeature> features,
            DynamicTableRefreshMode refreshMode,
            List<String> aggregationTypes) {
        Objects.requireNonNull(aggregationTypes, "aggregationTypes must not be null");

        FeasibilityResult modeResult = check(features, refreshMode);
        if (!modeResult.feasible()) {
            return modeResult;
        }

        return checkAggregationTypes(aggregationTypes);
    }

    /**
     * Validate that all aggregation function types are supported for
     * incremental evaluation.
     *
     * @param aggregationTypes aggregation function type names to validate
     * @return feasibility result with reason if any type is unsupported
     */
    public FeasibilityResult checkAggregationTypes(List<String> aggregationTypes) {
        Objects.requireNonNull(aggregationTypes, "aggregationTypes must not be null");

        StringJoiner unsupported = new StringJoiner(", ");
        for (String aggType : aggregationTypes) {
            if (!SUPPORTED_AGGREGATIONS.contains(aggType.toUpperCase())) {
                unsupported.add(aggType);
            }
        }

        if (unsupported.length() > 0) {
            return new FeasibilityResult(false,
                    "Unsupported aggregation functions: " + unsupported
                            + ". Supported: SUM, COUNT, AVG");
        }
        return new FeasibilityResult(true, null);
    }

    /**
     * Result of a feasibility check.
     *
     * @param feasible whether incremental evaluation is supported
     * @param reason   explanation when not feasible (null when feasible)
     */
    public record FeasibilityResult(boolean feasible, String reason) {
    }

    /** Query features detectable from the defining query AST. */
    public enum QueryFeature {

        FILTER("FILTER"),
        PROJECTION("PROJECTION"),
        INNER_JOIN("INNER JOIN"),
        GROUP_BY("GROUP BY"),
        UNION_ALL("UNION ALL"),
        WINDOW("WINDOW function"),
        NON_EQUI_JOIN("non-equi JOIN"),
        DISTINCT_WITHOUT_GROUP_BY("DISTINCT without GROUP BY");

        private final String displayName;

        QueryFeature(String displayName) {
            this.displayName = displayName;
        }

        public String displayName() {
            return displayName;
        }
    }
}
