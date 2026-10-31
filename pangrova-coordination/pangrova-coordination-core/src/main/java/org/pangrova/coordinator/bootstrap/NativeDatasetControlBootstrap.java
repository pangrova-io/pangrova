package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.metadata.NativeDatasetControlClient;
import org.pangrova.coordinator.metadata.WorkerNativeDatasetControl;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;

import java.util.Optional;
import java.util.function.Supplier;

/** Owns the shared authenticated metadata client and late-bound process control services. */
final class NativeDatasetControlBootstrap implements AutoCloseable {

    private final NativeDatasetControlClient client;
    private final WorkerNativeDatasetControl.Dependencies dependencies;
    private final javax.sql.DataSource metadataDataSource;
    private final CurrentModelAuthority modelAuthority;
    private boolean closed;

    NativeDatasetControlBootstrap(String secret, Supplier<QueryBootstrap> queries,
                                 Supplier<HighAvailabilityCoordinationService> coordination,
                                 javax.sql.DataSource metadataDataSource, CurrentModelAuthority modelAuthority) {
        this.metadataDataSource = java.util.Objects.requireNonNull(metadataDataSource);
        this.modelAuthority = java.util.Objects.requireNonNull(modelAuthority, "modelAuthority");
        client = new NativeDatasetControlClient(secret);
        dependencies = new WorkerNativeDatasetControl.Dependencies(
                () -> requireStarted(requireStarted(queries.get()).workerManager()).getActiveWorkers(),
                () -> requireStarted(requireStarted(queries.get()).writeCommitJournal()),
                () -> WriteCommitWiring.createFencer(requireStarted(coordination.get())),
                () -> requireStarted(coordination.get()).getCurrentEpoch());
    }

    synchronized Optional<NativeDatasetControlPlane> forCatalog(CatalogDescriptor catalog) {
        if (closed) {
            throw new IllegalStateException("Native metadata control bootstrap is closed");
        }
        return Optional.of(new WorkerNativeDatasetControl(catalog, dependencies, client, sourceCatalog(catalog)));
    }

    synchronized Optional<org.pangrova.connector.spi.metadata.ManagedSourceCatalog> sourceForCatalog(
            CatalogDescriptor catalog) {
        if (closed) throw new IllegalStateException("Native metadata control bootstrap is closed");
        return Optional.of(sourceCatalog(catalog));
    }

    private org.pangrova.connector.spi.metadata.ManagedSourceCatalog sourceCatalog(CatalogDescriptor catalog) {
        return new org.pangrova.metadata.source.jdbc.JdbcManagedSourceCatalog(metadataDataSource,
                new org.pangrova.connector.spi.metadata.ManagedSourceCatalog.Deployment(
                        catalog.name(), catalog.incarnationId(), catalog.revision()),
                executionId -> requireStarted(dependencies.fencer().get()).currentAnchor(executionId),
                new org.pangrova.query.store.jdbc.anchor.JdbcExecutionTransactionFence(), modelAuthority);
    }

    private static <T> T requireStarted(T service) {
        if (service == null) {
            throw new IllegalStateException("Native metadata control dependencies are not started");
        }
        return service;
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
        }
        client.close();
    }
}
