package org.pangrova.function.api.aggregate;

import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.data.value.ColumnValue;

import java.util.List;

/**
 * A user-defined aggregate function: folds a multiset of rows into a single
 * scalar result.
 *
 * <p>The lifecycle for each aggregation group is:
 * <ol>
 *   <li>{@link #createAccumulator()} — called once per group to allocate state</li>
 *   <li>{@link #addInput(Object, List)} — called once per input row in the group</li>
 *   <li>{@link #getResult(Object)} — called once to extract the final aggregate value</li>
 * </ol>
 *
 * <p>Accumulator objects are opaque to the engine. Implementations choose their
 * own internal representation (e.g., a running sum, a list of values, a
 * sketch). Accumulator instances are not shared across groups and may assume
 * single-threaded access within a group.</p>
 *
 * <p>The factory instance itself must be thread-safe; multiple groups may be
 * processed concurrently, each with its own accumulator.</p>
 *
 * @see FunctionFactory
 * @see FunctionMetadata
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public interface AggregateFunction {

    /**
     * Returns the metadata describing this function's signature.
     *
     * @return function metadata, never null
     */
    FunctionMetadata getMetadata();

    /**
     * Creates a new accumulator for a single aggregation group.
     *
     * @return a fresh accumulator instance, never null
     */
    Object createAccumulator();

    /**
     * Incorporates one input row into the accumulator.
     *
     * @param accumulator the accumulator returned by {@link #createAccumulator()}
     * @param arguments   positional argument values for this row
     */
    void addInput(Object accumulator, List<ColumnValue> arguments);

    /**
     * Extracts the final aggregate value from the accumulator.
     *
     * @param accumulator the accumulator after all rows have been processed
     * @return the aggregate result, never null (use {@code ColumnValue.NullValue}
     *         to represent SQL NULL)
     */
    ColumnValue getResult(Object accumulator);
}
