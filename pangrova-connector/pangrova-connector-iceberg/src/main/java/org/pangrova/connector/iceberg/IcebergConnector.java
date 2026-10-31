package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.iceberg.write.IcebergPageSinkProvider;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.HealthCheckResult;
import org.pangrova.connector.spi.metadata.IncrementalScanCapability;
import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Iceberg connector providing metadata, split management, page source and sink access.
 * Wraps a real Iceberg Catalog instance for all operations.
 */
public class IcebergConnector implements Connector {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final String catalogName;
    private final Catalog catalog;
    private final IcebergMetadata metadata;
    private final IcebergSplitManager splitManager;
    private final IcebergPageSourceProvider pageSourceProvider;
    private final IcebergPageSinkProvider pageSinkProvider;
    private final IcebergMaterializationSourceProvider materializationSourceProvider;
    private final ScheduledExecutorService pinEvictionScheduler;
    private final AtomicBoolean shutdownInProgress = new AtomicBoolean();
    private volatile boolean catalogClosed;

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
        this.materializationSourceProvider = new IcebergMaterializationSourceProvider(catalogName, catalog);
        this.pinEvictionScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("iceberg-pin-eviction");
        metadata.startPinEviction(pinEvictionScheduler);
        metadata.setSplitCacheInvalidator(splitManager::invalidateManifestCache);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }

    @Override
    public Optional<org.pangrova.connector.spi.materialization.SourceProvider> getMaterializationSourceProvider() {
        return Optional.of(materializationSourceProvider);
    }

    @Override
    public void registerCommitListener(org.pangrova.connector.spi.index.CommitListener listener) {
        metadata.registerCommitListener(listener);
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
    public ConnectorOutputHandleCodec getOutputHandleCodec() {
        return new IcebergOutputHandleCodec();
    }

    @Override
    public Optional<IncrementalScanCapability> getIncrementalScanCapability() {
        return Optional.of(new IcebergIncrementalScanCapability(catalog));
    }

    ScheduledExecutorService pinEvictionExecutor() {
        return pinEvictionScheduler;
    }

    @Override
    public void shutdown() {
        if (!shutdownInProgress.compareAndSet(false, true)) {
            throw new IllegalStateException("Iceberg connector shutdown is still in progress");
        }
        try {
            if (catalogClosed) return;
            pinEvictionScheduler.shutdownNow();
            if (!pinEvictionScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Iceberg pin eviction has not terminated");
            }
            if (catalog instanceof AutoCloseable closeable) closeable.close();
            catalogClosed = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                    "Iceberg connector shutdown interrupted: " + catalogName, interrupted);
        } catch (Exception failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                    "Iceberg connector shutdown failed: " + catalogName, failure);
        } finally {
            shutdownInProgress.set(false);
        }
    }

    @Override
    public HealthCheckResult healthCheck() {
        return HealthCheckResult.healthy();
    }

    public String getCatalogName() {
        return catalogName;
    }

}
