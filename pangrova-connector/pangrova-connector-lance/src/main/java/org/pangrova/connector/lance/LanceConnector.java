package org.pangrova.connector.lance;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.common.concurrent.ThreadSafe;

/** Lance plugin: durable catalog metadata in Java and all dataset rows in Native. */
@ThreadSafe
public final class LanceConnector implements Connector {
    private final Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog> authority;
    private final LanceMetadata metadata;
    private final LanceSplitManager splits;
    private final AtomicReference<CompletableFuture<LanceCatalogStore>> catalogStore = new AtomicReference<>();

    LanceConnector(String catalog, String warehouse, Map<String, String> config,
            Optional<NativeDatasetControlPlane> controlPlane,
            Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog> authority) {
        this.authority = java.util.Objects.requireNonNull(authority);
        metadata = new LanceMetadata(catalog, warehouse, this::store, controlPlane);
        splits = new LanceSplitManager(controlPlane, this::store, catalog, warehouse);
    }

    private LanceCatalogStore store() {
        CompletableFuture<LanceCatalogStore> ready = catalogStore.get();
        if (ready == null) {
            CompletableFuture<LanceCatalogStore> creating = new CompletableFuture<>();
            if (catalogStore.compareAndSet(null, creating)) {
                ready = creating;
                try {
                    creating.complete(new LanceCatalogStore(authority.orElseThrow(() ->
                            new IllegalStateException(
                                    "Lance catalog authority is available only in the owner process"))));
                } catch (RuntimeException | Error failure) {
                    creating.completeExceptionally(failure);
                }
            } else ready = catalogStore.get();
        }
        try {
            return ready.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }
    @Override public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }
    @Override public ConnectorSplitManager getSplitManager() { return splits; }
    @Override public ConnectorOutputHandleCodec getOutputHandleCodec() {
        return new LanceOutputHandleCodec();
    }
    @Override public void registerCommitListener(org.pangrova.spi.connector.index.CommitListener listener) {
        metadata.registerCommitListener(listener);
    }
    @Override public ConnectorPageSourceProvider getPageSourceProvider() {
        return (transaction, session, split, table, columns, filter) -> {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Lance scans require canonical Native dataset splits");
        };
    }
    @Override public ConnectorPageSinkProvider getPageSinkProvider() {
        throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                "Lance writes require connector-owned Native output allocation");
    }
}
