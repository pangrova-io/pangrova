package org.pangrova.streaming.core.aggregation;

import org.pangrova.connector.spi.data.value.ColumnValue;

/**
 * Accumulator for streaming aggregation with retraction support.
 *
 * <p>Supports three operations: accumulate (on INSERT), retract (on DELETE),
 * and the combination for UPDATE (retract old + accumulate new).</p>
 */
public interface StreamingAccumulator {

    /**
     * Accumulates a value (INSERT semantics).
     */
    void accumulate(ColumnValue value);

    /**
     * Retracts a previously accumulated value (DELETE semantics).
     */
    void retract(ColumnValue value);

    /**
     * Returns the current aggregation result.
     */
    ColumnValue result();

    /**
     * Resets the accumulator to its initial state.
     */
    void reset();
}
