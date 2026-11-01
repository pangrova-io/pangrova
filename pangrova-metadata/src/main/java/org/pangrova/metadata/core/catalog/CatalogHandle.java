package org.pangrova.metadata.core.catalog;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;

import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;

/**
 * Unified handle for accessing a registered catalog regardless of its type.
 *
 * <p>Sealed to the three permitted implementations that correspond 1:1 with
 * {@link CatalogType}: {@link ConnectorCatalogHandle}, {@link FederationCatalogHandle},
 * {@link InternalCatalogHandle}. This enables exhaustive pattern matching and
 * prevents unauthorized implementations from bypassing the catalog lifecycle.</p>
 */
public sealed interface CatalogHandle
        permits ConnectorCatalogHandle, FederationCatalogHandle, InternalCatalogHandle {

    CatalogDescriptor getDescriptor();

    CatalogState getState();

    void setState(CatalogState state);

    ConnectorMetadata getMetadata(ConnectorSession session);

    /** Registers a lifecycle callback; this port grants no data or table access. */
    default void registerCommitListener(org.pangrova.spi.connector.index.CommitListener listener) {
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
    default java.util.Optional<org.pangrova.spi.materialization.SourceProvider>
            getMaterializationSourceProvider() {
        return java.util.Optional.empty();
    }

    CatalogHealthStatus checkHealth();

    void shutdown();
}
