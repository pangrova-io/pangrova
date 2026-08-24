package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

/**
 * Query lifecycle hooks for a connector.
 *
 * <p>Extracted from {@link ConnectorMetadata} per Interface Segregation Principle.
 * Connectors that do not require snapshot pinning or per-query resource
 * management need not implement these methods beyond the default no-ops.</p>
 */
@Experimental
public interface ConnectorQueryLifecycle {

    /**
     * Signals the start of a query against this connector.
     *
     * <p>Connectors may use this to pin a metadata snapshot so that all
     * metadata reads within the same query see a consistent view.
     * This method is idempotent.</p>
     *
     * @param queryId the unique identifier of the query
     */
    default void beginQuery(String queryId) {
        // Intentional no-op: connectors that need snapshot pinning override this
    }

    /**
     * Signals the end of a query, releasing any resources pinned by
     * {@link #beginQuery(String)}.
     *
     * <p>The engine guarantees this method is called exactly once per query
     * regardless of outcome. This method is idempotent.</p>
     *
     * @param queryId the unique identifier of the query
     */
    default void cleanupQuery(String queryId) {
        // Intentional no-op: connectors that need resource cleanup override this
    }
}
