package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.HealthCheckResult;
import org.pangrova.spi.connector.metadata.IncrementalScanCapability;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.apache.paimon.catalog.Catalog;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean shutdownInProgress = new AtomicBoolean();
    private volatile boolean catalogClosed;

    PaimonConnector(String catalogName, Catalog catalog, String tempDir) {
        this.catalogName = catalogName;
        this.catalog = catalog;
        this.splitRegistry = new PaimonSplitRegistry();
        this.metadata = new PaimonMetadata(catalog, catalogName, splitRegistry);
        this.splitManager = new PaimonSplitManager(catalog, splitRegistry);
        this.pageSourceProvider = new PaimonPageSourceProvider(splitRegistry);
        this.pageSinkProvider = new PaimonPageSinkProvider(catalog, tempDir);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }

    /** Immutable lifecycle counters only; this observation never issues an SDK read or admission authority. */
    public record LifecycleDiagnostics(int pendingSplitRegistrations, int activeReaders) { }

    /** Samples both counters under the actual registry's ownership lock. */
    public LifecycleDiagnostics lifecycleDiagnostics() {
        return splitRegistry.lifecycleDiagnostics();
    }

    @Override
    public void registerCommitListener(org.pangrova.spi.connector.index.CommitListener listener) {
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
        return new PaimonOutputHandleCodec();
    }

    @Override
    public Optional<org.pangrova.spi.connector.metadata.NativePhysicalWriteProvider> getNativePhysicalWriteProvider() {
        return Optional.of(metadata.physicalWriteProvider());
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
        if (!shutdownInProgress.compareAndSet(false, true)) {
            throw new IllegalStateException("Paimon connector shutdown is still in progress");
        }
        try {
            if (catalogClosed) return;
            splitRegistry.close();
            catalog.close();
            catalogClosed = true;
        } catch (Exception failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                    "Paimon connector shutdown failed: " + catalogName, failure);
        } finally {
            shutdownInProgress.set(false);
        }
    }

    public String getCatalogName() {
        return catalogName;
    }
}
