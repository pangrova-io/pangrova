package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.lance.FormatModelRegistry;
import org.pangrova.connector.iceberg.lance.IcebergLanceFormatModel;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.iceberg.write.IcebergPageSinkProvider;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.HealthCheckResult;
import org.pangrova.spi.connector.metadata.IncrementalScanCapability;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Iceberg connector providing metadata, split management, page source and sink access.
 * Wraps a real Iceberg Catalog instance for all operations.
 */
public class IcebergConnector implements Connector {

    private final String catalogName;
    private final Catalog catalog;
    private final IcebergMetadata metadata;
    private final IcebergSplitManager splitManager;
    private final IcebergPageSourceProvider pageSourceProvider;
    private final IcebergPageSinkProvider pageSinkProvider;
    private final FormatModelRegistry formatModelRegistry;
    private final ScheduledExecutorService pinEvictionScheduler;

    public IcebergConnector(String catalogName, Catalog catalog) {
        this(catalogName, catalog, null, java.util.Set.of());
    }

    public IcebergConnector(String catalogName, Catalog catalog,
                            java.util.Set<String> accessibleEncryptionKeys) {
        this(catalogName, catalog, null, accessibleEncryptionKeys);
    }

    /**
     * Creates a connector with an explicit FileIO and encryption-key access set.
     *
     * @param catalogName the catalog name this connector serves
     * @param catalog the live Iceberg catalog
     * @param fileIO optional FileIO override, or null to use the catalog default
     * @param accessibleEncryptionKeys encryption keys this connector is allowed to read
     */
    public IcebergConnector(String catalogName, Catalog catalog,
                            FileIO fileIO,
                            java.util.Set<String> accessibleEncryptionKeys) {
        this.catalogName = catalogName;
        this.catalog = catalog;
        this.metadata = new IcebergMetadata(catalog, catalogName,
                IcebergMetadata.DEFAULT_PIN_TTL_MS, IcebergMetadata.DEFAULT_COMMIT_TIMEOUT_MS,
                accessibleEncryptionKeys, fileIO);
        this.splitManager = new IcebergSplitManager(catalogName, catalog, new IcebergExpressionToSubstrait(), metadata);
        this.pageSourceProvider = new IcebergPageSourceProvider(catalog);
        this.pageSinkProvider = new IcebergPageSinkProvider(catalog);
        this.formatModelRegistry = createFormatModelRegistry();
        this.pinEvictionScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("iceberg-pin-eviction");
        metadata.startPinEviction(pinEvictionScheduler);
        metadata.setSplitCacheInvalidator(splitManager::invalidateManifestCache);
    }

    private static FormatModelRegistry createFormatModelRegistry() {
        FormatModelRegistry registry = new FormatModelRegistry();
        registry.register(new IcebergLanceFormatModel());
        return registry;
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
        return Optional.of(new IcebergIncrementalScanCapability(catalog));
    }

    @Override
    public void shutdown() {
        pinEvictionScheduler.shutdownNow();
    }

    @Override
    public HealthCheckResult healthCheck() {
        return HealthCheckResult.healthy();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public FormatModelRegistry getFormatModelRegistry() {
        return formatModelRegistry;
    }
}
