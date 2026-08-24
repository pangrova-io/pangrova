package org.pangrova.worker.jni.write;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;

/**
 * Resolves a catalog name to the connector resources needed for write operations.
 *
 * <p>This abstraction decouples the write bridge adapter from the metadata
 * module's {@code CatalogRegistry}, allowing the worker module to remain
 * independent of metadata internals per the module dependency rules.</p>
 *
 * <p>The implementation is provided by the bootstrap layer that has access
 * to both the worker and metadata modules.</p>
 */
@FunctionalInterface
public interface CatalogResourceResolver {

    /**
     * Resolves the given catalog name to its connector resources.
     *
     * @param catalogName the catalog to resolve
     * @return connector resources for the catalog, never null
     * @throws org.pangrova.common.exception.PangrovaException if the catalog
     *         is not found or is suspended
     */
    CatalogResources resolve(String catalogName);

    /**
     * Connector resources needed to execute write operations against a catalog.
     *
     * @param metadata         connector metadata for table handle resolution
     * @param pageSinkProvider provider for creating page sinks
     * @param session          connector session for the write operation
     */
    record CatalogResources(
            ConnectorMetadata metadata,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorSession session) {

        public CatalogResources {
            if (metadata == null) {
                throw new NullPointerException("metadata must not be null");
            }
            if (pageSinkProvider == null) {
                throw new NullPointerException("pageSinkProvider must not be null");
            }
            if (session == null) {
                throw new NullPointerException("session must not be null");
            }
        }
    }
}
