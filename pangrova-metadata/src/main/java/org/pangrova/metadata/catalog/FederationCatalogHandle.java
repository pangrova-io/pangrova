package org.pangrova.metadata.catalog;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.adapter.FederationConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;

/**
 * CatalogHandle backed by a CatalogAdapter for federated external catalogs.
 */
public final class FederationCatalogHandle implements CatalogHandle {

    private static final String NOT_SUPPORTED_SPLIT = "Federation catalogs do not support split management";
    private static final String NOT_SUPPORTED_PAGE = "Federation catalogs do not support page source";

    private final CatalogDescriptor descriptor;
    private final FederatedCatalog adapter;
    private volatile CatalogState state;
    private final ConnectorMetadata cachedMetadata;

    /** Creates a handle wrapping the given descriptor and federated catalog adapter. */
    public FederationCatalogHandle(CatalogDescriptor descriptor, FederatedCatalog adapter) {
        this.descriptor = descriptor;
        this.adapter = adapter;
        this.state = descriptor.state();
        this.cachedMetadata = new FederationConnectorMetadata(adapter);
    }

    public FederatedCatalog getFederatedCatalog() {
        return adapter;
    }

    @Override
    public CatalogDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public CatalogState getState() {
        return state;
    }

    @Override
    public void setState(CatalogState state) {
        this.state = state;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return cachedMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, NOT_SUPPORTED_SPLIT);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, NOT_SUPPORTED_PAGE);
    }

    @Override
    public CatalogHealthStatus checkHealth() {
        try {
            adapter.listSchemas();
            return CatalogHealthStatus.healthy();
        } catch (Exception e) {
            return CatalogHealthStatus.unhealthy(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        adapter.close();
    }
}
