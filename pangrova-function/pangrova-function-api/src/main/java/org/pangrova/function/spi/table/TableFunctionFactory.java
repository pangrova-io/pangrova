package org.pangrova.function.spi.table;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Map;

/**
 * Factory for registering user-defined polymorphic table functions.
 *
 * <p>Each table function plugin registers a {@code TableFunctionFactory} via
 * {@link java.util.ServiceLoader}. The engine discovers factories at startup
 * and collects the table functions they provide. Table functions are resolved
 * by name during query planning and appear in the {@code FROM} clause.</p>
 *
 * <p>A single factory may provide multiple table functions (e.g., a "rest"
 * plugin might register {@code rest_get} and {@code rest_post}).</p>
 *
 * @see TableFunction
 */
@Experimental(reason = "G-SPI-08: table function (PTF) plugin SPI")
public interface TableFunctionFactory {

    /**
     * Returns the unique name of this table function plugin.
     *
     * <p>Used for configuration and logging (e.g., {@code "rest"},
     * {@code "kafka-reader"}).</p>
     *
     * @return the plugin name, never null
     */
    String getName();

    /**
     * Returns the table functions provided by this plugin.
     *
     * <p>Called once during engine startup. The returned list must be
     * immutable and consistent across calls.</p>
     *
     * @param config the plugin configuration properties
     * @return the table functions, never null (may be empty)
     */
    List<TableFunction> getTableFunctions(Map<String, String> config);
}
