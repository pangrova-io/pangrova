package org.pangrova.metadata.catalog.handle.internal;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogHealthStatus;
import org.pangrova.metadata.api.catalog.CatalogState;

/**
 * CatalogHandle for the internal system catalog. Always healthy, no external delegation.
 */
public final class InternalCatalogHandle implements CatalogHandle {

    private static final String NOT_SUPPORTED = "Internal catalog does not support data operations";

    private final CatalogDescriptor descriptor;
    private volatile CatalogState state;

    public InternalCatalogHandle(CatalogDescriptor descriptor) {
        this.descriptor = descriptor;
        this.state = descriptor.state();
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
        throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, NOT_SUPPORTED);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, NOT_SUPPORTED);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR, NOT_SUPPORTED);
    }

    @Override
    public CatalogHealthStatus checkHealth() {
        return CatalogHealthStatus.healthy();
    }

    @Override
    public void shutdown() {
        // no-op
    }
}
