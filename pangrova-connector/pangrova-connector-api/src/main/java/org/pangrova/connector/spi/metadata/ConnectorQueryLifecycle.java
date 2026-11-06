package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;

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
     * @param authority the actual engine-issued lifetime of the query
     */
    default void beginQuery(ConnectorQueryAuthority authority) {
        // Intentional no-op: connectors that need snapshot pinning override this
    }

    /**
     * Signals the end of a query, releasing any resources pinned by
     * {@link #beginQuery(ConnectorQueryAuthority)}.
     *
     * <p>The engine closes admission before cleanup. Failed cleanup retains the same owner
     * for retry; successful cleanup is idempotent.</p>
     *
     * @param authority the same closed lifetime supplied to begin
     */
    default void cleanupQuery(ConnectorQueryAuthority authority) {
        // Intentional no-op: connectors that need resource cleanup override this
    }
}
