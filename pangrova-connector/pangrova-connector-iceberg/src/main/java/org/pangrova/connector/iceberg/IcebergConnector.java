package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.connector.iceberg.write.IcebergPageSinkProvider;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.HealthCheckResult;
import org.pangrova.connector.api.metadata.IncrementalScanCapability;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
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
    private final FileIO ownedFileIO;
    private final IcebergMetadata metadata;
    private final IcebergSplitManager splitManager;
    private final IcebergPageSourceProvider pageSourceProvider;
    private final IcebergPageSinkProvider pageSinkProvider;
    private final IcebergMaterializationSourceProvider materializationSourceProvider;
    private final ScheduledExecutorService pinEvictionScheduler;
    private final AtomicBoolean shutdownInProgress = new AtomicBoolean();
    private volatile boolean catalogClosed;
    private boolean fileIOClosed;

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
     * @param fileIO separately owned FileIO transferred to this connector on success, or null for the catalog default
     * @param accessibleEncryptionKeys encryption keys this connector is allowed to read
     */
    public IcebergConnector(String catalogName, Catalog catalog,
                            FileIO fileIO,
                            java.util.Set<String> accessibleEncryptionKeys) {
        this(catalogName, catalog, fileIO, accessibleEncryptionKeys, false);
    }

    IcebergConnector(String catalogName, Catalog catalog, FileIO fileIO,
                     java.util.Set<String> accessibleEncryptionKeys, boolean exactTableNameLookup) {
        this.catalogName = catalogName;
        this.catalog = catalog;
        this.ownedFileIO = fileIO;
        this.metadata = new IcebergMetadata(catalog, catalogName,
                IcebergMetadata.DEFAULT_PIN_TTL_MS, IcebergMetadata.DEFAULT_COMMIT_TIMEOUT_MS,
                accessibleEncryptionKeys, fileIO, exactTableNameLookup);
        this.splitManager = new IcebergSplitManager(catalogName, catalog, new IcebergExpressionToSubstrait(), metadata);
        this.pageSourceProvider = new IcebergPageSourceProvider(catalog);
        this.pageSinkProvider = new IcebergPageSinkProvider(catalog);
        this.materializationSourceProvider = new IcebergMaterializationSourceProvider(catalogName, catalog);
        this.pinEvictionScheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("iceberg-pin-eviction");
        try {
            metadata.setSplitCacheInvalidator(splitManager::invalidateManifestCache);
            metadata.startPinEviction(pinEvictionScheduler);
        } catch (RuntimeException | Error failure) {
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(failure, pinEvictionScheduler::shutdownNow);
            throw failure;
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }

    @Override
    public Optional<org.pangrova.connector.api.materialization.SourceProvider> getMaterializationSourceProvider() {
        return Optional.of(materializationSourceProvider);
    }

    @Override
    public void registerCommitListener(org.pangrova.connector.api.index.CommitListener listener) {
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
            if (catalogClosed && fileIOClosed) return;
            pinEvictionScheduler.shutdownNow();
            if (!pinEvictionScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Iceberg pin eviction has not terminated");
            }
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(null, this::closeFileIO, this::closeCatalog);
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

    private void closeFileIO() {
        if (fileIOClosed) return;
        if (ownedFileIO != null) ownedFileIO.close();
        fileIOClosed = true;
    }

    private void closeCatalog() {
        if (catalogClosed) return;
        try {
            if (catalog instanceof AutoCloseable closeable) closeable.close();
            catalogClosed = true;
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to close Iceberg catalog " + catalogName, failure);
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
