package org.pangrova.metadata.catalog;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.HealthCheckResult;

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
