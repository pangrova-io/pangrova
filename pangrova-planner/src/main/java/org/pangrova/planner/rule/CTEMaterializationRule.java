package org.pangrova.planner.rule;

/**
 * Cost-based CTE materialization decision engine.
 *
 * <p>Compares the cost of materializing a CTE (compute once + write to temp storage)
 * against inlining it (re-compute for each reference). Materializes when beneficial.</p>
 *
 * <p>Materialization overhead is estimated dynamically from the CTE's estimated row
 * count: serialization + temp-write cost scales with data volume rather than being
 * a fixed constant. When no row count estimate is available, falls back to a
 * conservative fixed overhead.</p>
 */
public final class CTEMaterializationRule {

    /** Per-row overhead for serializing and writing CTE results to temp storage. */
    static final double PER_ROW_WRITE_COST = 0.05;

    /** Fixed base overhead for temp-storage bookkeeping (file creation, metadata). */
    static final double BASE_OVERHEAD = 100.0;

    /** Fallback overhead when no row count estimate is available. */
    static final double FALLBACK_OVERHEAD = 1000.0;

    private CTEMaterializationRule() {
        // utility class
    }

    /**
     * Decide whether to materialize a CTE using a dynamic overhead model.
     *
     * @param cteCost        estimated cost of computing the CTE once
     * @param referenceCount number of times the CTE is referenced in the query
     * @param estimatedRows  estimated row count of the CTE result; {@code <= 0} triggers fallback
     * @return true if materialization is cheaper than or equal to inlining
     */
    public static boolean shouldMaterialize(double cteCost, int referenceCount,
                                            long estimatedRows) {
        if (referenceCount <= 1) {
            return false;
        }
        double overhead = estimateOverhead(estimatedRows);
        return (cteCost + overhead) <= estimateInlineCost(cteCost, referenceCount);
    }

    /**
     * Backward-compatible overload using fallback overhead for callers
     * that do not have a row count estimate.
     *
     * @return true if materialization is cheaper than inlining
     */
    public static boolean shouldMaterialize(double cteCost, int referenceCount) {
        return shouldMaterialize(cteCost, referenceCount, -1);
    }

    /**
     * Estimates materialization overhead based on CTE result size.
     * Scales linearly with row count: base bookkeeping + per-row serialization cost.
     *
     * @param estimatedRows estimated row count; {@code <= 0} returns fallback
     * @return estimated overhead cost
     */
    public static double estimateOverhead(long estimatedRows) {
        if (estimatedRows <= 0) {
            return FALLBACK_OVERHEAD;
        }
        return BASE_OVERHEAD + estimatedRows * PER_ROW_WRITE_COST;
    }

    /**
     * Cost of materializing: compute once + dynamic write overhead.
     */
    public static double estimateMaterializationCost(double cteCost, long estimatedRows) {
        return cteCost + estimateOverhead(estimatedRows);
    }

    /**
     * Cost of inlining: re-compute for every reference.
     */
    public static double estimateInlineCost(double cteCost, int referenceCount) {
        return cteCost * referenceCount;
    }
}
