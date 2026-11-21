package org.pangrova.function.api.scalar;

import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.value.ColumnValue;

import java.util.List;

/**
 * A user-defined scalar function: maps a fixed number of scalar inputs to a
 * single scalar output.
 *
 * <p>Implementations must be thread-safe as the engine may invoke a single
 * instance concurrently across multiple tasks.</p>
 *
 * @see FunctionFactory
 * @see FunctionMetadata
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public interface ScalarFunction {

    /**
     * Returns the metadata describing this function's signature.
     *
     * @return function metadata, never null
     */
    FunctionMetadata getMetadata();

    /**
     * Returns all supported argument signatures for this function name.
     * Implementations with overloads must return every exact signature that
     * the planner and native runtime may resolve.
     *
     * @return immutable argument signatures, never empty
     */
    default List<List<String>> getArgumentSignatures() {
        return List.of(getMetadata().argumentTypes());
    }

    /**
     * Evaluates the function for a single row.
     *
     * @param arguments positional argument values matching the declared argument types
     * @return the computed result, never null (use {@code ColumnValue.NullValue}
     *         to represent SQL NULL)
     */
    ColumnValue invoke(List<ColumnValue> arguments);
}
