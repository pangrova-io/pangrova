package org.pangrova.connector.lance;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.NativeDatasetControlPlane;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.common.concurrency.ThreadSafe;

/** Lance plugin: durable catalog metadata in Java and all dataset rows in Native. */
@ThreadSafe
public final class LanceConnector implements Connector {
    private final Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog> authority;
    private final Optional<org.pangrova.connector.api.materialization.SourceProvider> materializationSourceProvider;
    private final LanceMetadata metadata;
    private final LanceSplitManager splits;
    private final AtomicReference<CompletableFuture<LanceCatalogStore>> catalogStore = new AtomicReference<>();

    LanceConnector(String catalog, String warehouse, Map<String, String> config,
            Optional<NativeDatasetControlPlane> controlPlane,
            Optional<org.pangrova.connector.api.metadata.ManagedSourceCatalog> authority) {
        this.authority = java.util.Objects.requireNonNull(authority);
        this.materializationSourceProvider = authority.map(sourceAuthority ->
                new LanceMaterializationSourceProvider(catalog, sourceAuthority));
        metadata = new LanceMetadata(catalog, warehouse, this::store, controlPlane);
        splits = new LanceSplitManager(controlPlane, this::store, catalog, warehouse);
    }

    @Override
    public Optional<org.pangrova.connector.api.materialization.SourceProvider> getMaterializationSourceProvider() {
        return materializationSourceProvider;
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
    @Override public void registerCommitListener(org.pangrova.connector.api.index.CommitListener listener) {
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
