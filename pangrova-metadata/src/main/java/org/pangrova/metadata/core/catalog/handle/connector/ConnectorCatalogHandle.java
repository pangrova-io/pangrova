package org.pangrova.metadata.core.catalog.handle.connector;

import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.HealthCheckResult;
import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogHealthStatus;
import org.pangrova.metadata.api.catalog.CatalogState;

/**
 * CatalogHandle backed by a Connector SPI implementation.
 */
public final class ConnectorCatalogHandle implements CatalogHandle {

    private final CatalogDescriptor descriptor;
    private final Connector connector;
    private volatile CatalogState state;

    public ConnectorCatalogHandle(CatalogDescriptor descriptor, Connector connector) {
        this.descriptor = descriptor;
        this.connector = connector;
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
        return connector.getMetadata(session);
    }

    @Override
    public void registerCommitListener(org.pangrova.connector.spi.index.CommitListener listener) {
        connector.registerCommitListener(listener);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return connector.getSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return connector.getPageSourceProvider();
    }
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return connector.getPageSinkProvider();
    }

    public ConnectorOutputHandleCodec getOutputHandleCodec() {
        return connector.getOutputHandleCodec();
    }

    @Override
    public java.util.Optional<org.pangrova.connector.spi.materialization.SourceProvider>
            getMaterializationSourceProvider() {
        return connector.getMaterializationSourceProvider();
    }

    @Override
    public CatalogHealthStatus checkHealth() {
        return mapHealthCheckResult(connector.healthCheck());
    }

    @Override
    public void shutdown() {
        connector.shutdown();
    }

    private CatalogHealthStatus mapHealthCheckResult(HealthCheckResult result) {
        boolean healthy = result.status() == HealthCheckResult.HealthStatus.HEALTHY;
        return new CatalogHealthStatus(healthy, result.errorMessage());
    }
}
