package org.pangrova.spi.function;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Map;

/**
 * Factory for registering user-defined scalar and aggregate functions.
 *
 * <p>Each function plugin registers a {@code FunctionFactory} via
 * {@link java.util.ServiceLoader}. The engine discovers factories at startup
 * and collects the functions they advertise. Functions are resolved by name
 * during query planning and dispatched during execution.</p>
 *
 * <p>A single factory may provide multiple functions (e.g., a "geo" plugin
 * might register {@code st_distance}, {@code st_contains}, and
 * {@code st_area}).</p>
 *
 * @see ScalarFunction
 * @see AggregateFunction
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public interface FunctionFactory {

    /**
     * Returns the unique name of this function plugin.
     *
     * <p>Used for configuration and logging (e.g., {@code "geo"},
     * {@code "ml-predict"}).</p>
     *
     * @return the plugin name, never null
     */
    String getName();

    /**
     * Returns the scalar functions provided by this plugin.
     *
     * <p>Called once during engine startup. The returned list must be
     * immutable and consistent across calls.</p>
     *
     * @param config the plugin configuration properties
     * @return the scalar functions, never null (may be empty)
     */
    default List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
        return List.of();
    }

    /**
     * Returns the aggregate functions provided by this plugin.
     *
     * <p>Called once during engine startup. The returned list must be
     * immutable and consistent across calls.</p>
     *
     * @param config the plugin configuration properties
     * @return the aggregate functions, never null (may be empty)
     */
    default List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
        return List.of();
    }
}
