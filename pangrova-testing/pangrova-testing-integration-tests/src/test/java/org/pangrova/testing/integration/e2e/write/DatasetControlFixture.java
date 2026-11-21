package org.pangrova.testing.integration.e2e.write;

import org.pangrova.worker.dataset.NativeDatasetControlService;
import org.pangrova.worker.catalog.WorkerCatalogRuntime;
import org.pangrova.worker.task.TaskManager;

/** Real deployment transport and catalog authority; the task manager only enforces control fences. */
final class DatasetControlFixture implements AutoCloseable {
    private final CatalogDeploymentTransport transport;
    private final TaskManager tasks;
    private final WorkerCatalogRuntime catalogs;
    private final NativeDatasetControlService control;

    DatasetControlFixture(String coordinatorUrl, String secret) throws Exception {
        transport = new CatalogDeploymentTransport(coordinatorUrl);
        transport.release();
        TaskManager createdTasks = null;
        WorkerCatalogRuntime createdCatalogs = null;
        NativeDatasetControlService createdControl = null;
        try {
            createdTasks = TaskManager.forHttpWiringOnly();
            createdCatalogs = WorkerCatalogRuntime.discover(transport.url(), secret,
                    Thread.currentThread().getContextClassLoader());
            createdControl = new NativeDatasetControlService(createdTasks, createdCatalogs);
        } catch (RuntimeException | Error failure) {
            try (transport; var ownedTasks = createdTasks; var ownedCatalogs = createdCatalogs;
                    var ownedControl = createdControl) {
                throw failure;
            }
        }
        tasks = createdTasks;
        catalogs = createdCatalogs;
        control = createdControl;
    }

    TaskManager tasks() { return tasks; }
    WorkerCatalogRuntime catalogs() { return catalogs; }
    NativeDatasetControlService control() { return control; }
    int fetchCount() { return transport.requestCount(); }

    @Override
    public void close() throws Exception {
        try (transport; tasks; catalogs; control) {
            // Reverse-order close attempts every owner and retains suppressed cleanup failures.
        }
    }
}
