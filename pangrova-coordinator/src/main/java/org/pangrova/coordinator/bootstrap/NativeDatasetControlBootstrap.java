package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.metadata.NativeDatasetControlClient;
import org.pangrova.coordinator.metadata.WorkerNativeDatasetControl;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;

import java.util.Optional;
import java.util.function.Supplier;

/** Owns the shared authenticated metadata client and late-bound process control services. */
final class NativeDatasetControlBootstrap implements AutoCloseable {

    private final NativeDatasetControlClient client;
    private final WorkerNativeDatasetControl.Dependencies dependencies;
    private final javax.sql.DataSource metadataDataSource;
    private boolean closed;

    NativeDatasetControlBootstrap(String secret, Supplier<QueryBootstrap> queries,
                                 Supplier<HighAvailabilityCoordinationService> coordination, javax.sql.DataSource metadataDataSource) {
        this.metadataDataSource = java.util.Objects.requireNonNull(metadataDataSource);
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

    synchronized Optional<org.pangrova.spi.connector.metadata.ManagedSourceCatalog> sourceForCatalog(CatalogDescriptor catalog) {
        if (closed) throw new IllegalStateException("Native metadata control bootstrap is closed");
        return Optional.of(sourceCatalog(catalog));
    }

    private org.pangrova.spi.connector.metadata.ManagedSourceCatalog sourceCatalog(CatalogDescriptor catalog) {
        return new org.pangrova.metadata.source.jdbc.JdbcManagedSourceCatalog(metadataDataSource,
                new org.pangrova.spi.connector.metadata.ManagedSourceCatalog.Deployment(
                        catalog.name(), catalog.incarnationId(), catalog.revision()),
                executionId -> requireStarted(dependencies.fencer().get()).currentAnchor(executionId));
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
