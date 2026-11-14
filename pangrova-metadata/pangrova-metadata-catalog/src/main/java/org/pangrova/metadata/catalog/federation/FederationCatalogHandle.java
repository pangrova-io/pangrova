package org.pangrova.metadata.catalog.federation;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogHealthStatus;
import org.pangrova.metadata.api.enums.CatalogState;

import java.util.Objects;
import java.util.Optional;

/**
 * CatalogHandle backed by a {@code FederatedCatalog} for external catalogs.
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
        this(descriptor, adapter, adapter);
    }

    FederationCatalogHandle(CatalogDescriptor descriptor, FederatedCatalog source, FederatedCatalog readView) {
        this.descriptor = descriptor;
        this.adapter = Objects.requireNonNull(source, "federatedCatalog");
        this.state = descriptor.state();
        this.cachedMetadata = new FederationConnectorMetadata(readView);
    }

    @Override
    public Optional<FederatedCatalog> federationSource() {
        return Optional.of(adapter);
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
