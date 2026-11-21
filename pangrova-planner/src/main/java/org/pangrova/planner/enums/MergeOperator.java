package org.pangrova.planner.enums;

/**
 * Defines how to merge old and delta aggregation values during incremental refresh.
 *
 * <p>Each operator specifies the mathematical operation to combine the
 * materialized (old) value with the newly computed (delta) value.</p>
 */
public enum MergeOperator {

    /** {@code old_sum + delta_sum} — used for SUM and COUNT (count merges as sum of partial counts). */
    SUM,

    /** {@code min(old_min, delta_min)} — used for MIN. */
    MIN,

    /** {@code max(old_max, delta_max)} — used for MAX. */
    MAX,

    /**
     * AVG decomposition: store as (sum, count), merge as
     * {@code (old_sum + delta_sum) / (old_count + delta_count)}.
     */
    AVG_DECOMPOSE
}
