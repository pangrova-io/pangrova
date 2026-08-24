package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.HealthCheckResult;
import org.pangrova.spi.connector.metadata.IncrementalScanCapability;
import org.apache.paimon.catalog.Catalog;

import java.util.Optional;

/**
 * Paimon connector providing metadata, split management, and page source access.
 * Wraps a real Paimon Catalog instance for all operations.
 */
public class PaimonConnector implements Connector {

    private final String catalogName;
    private final Catalog catalog;
    private final PaimonMetadata metadata;
    private final PaimonSplitManager splitManager;
    private final PaimonPageSourceProvider pageSourceProvider;
    private final PaimonPageSinkProvider pageSinkProvider;
    private final PaimonSplitRegistry splitRegistry;

    PaimonConnector(String catalogName, Catalog catalog, String tempDir) {
        this.catalogName = catalogName;
        this.catalog = catalog;
        this.splitRegistry = new PaimonSplitRegistry();
        this.metadata = new PaimonMetadata(catalog, catalogName);
        this.splitManager = new PaimonSplitManager(catalog, splitRegistry);
        this.pageSourceProvider = new PaimonPageSourceProvider(catalog, splitRegistry);
        this.pageSinkProvider = new PaimonPageSinkProvider(catalog, tempDir);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return pageSinkProvider;
    }

    @Override
    public Optional<IncrementalScanCapability> getIncrementalScanCapability() {
        return Optional.of(new PaimonIncrementalScanCapability(catalog));
    }

    @Override
    public HealthCheckResult healthCheck() {
        return HealthCheckResult.healthy();
    }

    @Override
    public void shutdown() {
        splitRegistry.clear();
        try {
            catalog.close();
        } catch (Exception e) {
            // Lenient close — filesystem catalogs may already be closed
        }
    }

    public String getCatalogName() {
        return catalogName;
    }
}
