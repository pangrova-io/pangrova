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
    private boolean closed;

    NativeDatasetControlBootstrap(String secret, Supplier<QueryBootstrap> queries,
                                 Supplier<HighAvailabilityCoordinationService> coordination) {
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
        return Optional.of(new WorkerNativeDatasetControl(catalog, dependencies, client));
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
