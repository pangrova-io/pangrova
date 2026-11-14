package org.pangrova.connector.api.plugin;

import org.pangrova.connector.api.constants.ConnectorFactoryContract;
import org.pangrova.connector.api.enums.ConnectorRole;

import org.pangrova.connector.api.plugin.ConnectorContext;

import org.pangrova.common.api.annotation.Stable;

import java.util.Map;
import java.util.Set;

/**
 * Factory for creating {@link Connector} instances.
 *
 * <p>Each connector plugin registers a {@code ConnectorFactory} via
 * {@link java.util.ServiceLoader}. The engine discovers factories at startup
 * and uses them to create connector instances for configured catalogs.</p>
 */
@Stable(since = "1.0")
public interface ConnectorFactory {

    /**
     * Returns the unique name of this connector type.
     *
     * <p>This name is used in catalog configuration to select the connector
     * implementation (e.g., {@code "hive"}, {@code "iceberg"}, {@code "memory"}).</p>
     *
     * @return the connector name, never null
     */
    String getName();

    /** Contract version implemented by this factory; the runtime rejects unknown versions. */
    default int apiVersion() {
        return ConnectorFactoryContract.CURRENT_API_VERSION;
    }

    /** Process roles in which this connector may be loaded. */
    default Set<ConnectorRole> roles() {
        return Set.of(ConnectorRole.COORDINATOR, ConnectorRole.WORKER);
    }

    /** Validates catalog configuration before any connector resource is created. */
    default void validateConfiguration(Map<String, String> config) {
    }

    /**
     * Creates a new connector instance for the given catalog.
     *
     * @param catalogName the name of the catalog being created
     * @param config      the catalog configuration properties
     * @param context     the connector initialization context
     * @return a new connector instance, never null
     */
    Connector create(String catalogName, Map<String, String> config, ConnectorContext context);
}
