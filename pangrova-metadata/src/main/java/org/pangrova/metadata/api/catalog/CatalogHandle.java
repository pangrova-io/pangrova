package org.pangrova.metadata.api.catalog;

import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;

/**
 * Unified handle for accessing a registered catalog regardless of its type.
 *
 * <p>Implementations correspond 1:1 with {@link CatalogType} and live in the
 * catalog handle capability packages so this contract stays free of concrete
 * wiring. Callers select behavior through the descriptor type, not by
 * inspecting an implementation class.</p>
 */
public interface CatalogHandle {

    CatalogDescriptor getDescriptor();

    CatalogState getState();

    void setState(CatalogState state);

    ConnectorMetadata getMetadata(ConnectorSession session);

    /** Registers a lifecycle callback; this port grants no data or table access. */
    default void registerCommitListener(org.pangrova.connector.spi.index.CommitListener listener) {
        throw new UnsupportedOperationException("Catalog does not publish commit events");
    }

    ConnectorSplitManager getSplitManager();

    ConnectorPageSourceProvider getPageSourceProvider();

    /**
     * Returns the page sink provider for writing data.
     * Default throws UnsupportedOperationException for read-only catalogs.
     *
     * <p>Compatibility: additive default method — no migration required for existing implementations.
     */
    default ConnectorPageSinkProvider getPageSinkProvider() {
        throw new UnsupportedOperationException("Page sink not supported for this catalog");
    }

    /**
     * Returns the materialization source contract when the underlying connector
     * can pin exact immutable source snapshots.
     *
     * @return source contract, or empty if this catalog cannot serve one
     */
    default java.util.Optional<org.pangrova.connector.spi.materialization.SourceProvider>
            getMaterializationSourceProvider() {
        return java.util.Optional.empty();
    }

    CatalogHealthStatus checkHealth();

    void shutdown();
}
