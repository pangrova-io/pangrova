package org.pangrova.testing.integration.e2e.write;

import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.protocol.write.NativeWriteBindings;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteTaskOptions;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.pangrova.worker.nativebridge.jni.write.catalog.WorkerCatalogRuntime;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Component lease ownership with actual connector plugins and Coordinator HTTP deployments. */
final class WorkerCatalogLifecycleFixture implements AutoCloseable {
    private final WorkerCatalogRuntime runtime;
    private final TaskRequest authenticated;
    private final Set<String> tasks = ConcurrentHashMap.newKeySet();

    WorkerCatalogLifecycleFixture(String coordinatorUrl, String internalSecret, TaskRequest authenticated) {
        runtime = WorkerCatalogRuntime.discover(coordinatorUrl, internalSecret,
                Thread.currentThread().getContextClassLoader());
        this.authenticated = authenticated;
    }

    WorkerCatalogRuntime runtime() { return runtime; }

    TaskRequest request(CatalogHandle catalog, ConnectorSession session, String table, String taskId,
            SecurityContext security) {
        var metadata = catalog.getMetadata(session);
        var target = metadata.getTableHandle(session, new SchemaTableName("default", table));
        var output = metadata.beginInsert(session, target);
        var binding = ConnectorWriteBindingFactory.create(catalog, output);
        var write = WriteTaskOptions.newBuilder().setTargetTable(catalog.getDescriptor().name() + ".default." + table)
                .setWriteMode("INSERT").setOutputBinding(binding);
        if (binding.hasLance()) write.setFileFormat(FileFormat.FILE_FORMAT_LANCE);
        return NativeWriteBindings.bindAttempt(TaskRequest.newBuilder().setProtocol(authenticated.getProtocol())
                .setSecurity(security).setTaskKind(TaskKind.TASK_KIND_WRITE)
                .setAttempt(authenticated.getAttempt().toBuilder().setTaskId(taskId).setQueryOrJobId(taskId))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setWrite(write)).build());
    }

    void bind(TaskRequest request) {
        String taskId = request.getAttempt().getTaskId();
        runtime.bindTask(taskId, request);
        tasks.add(taskId);
    }

    void release(String taskId) {
        runtime.releaseTask(taskId);
        tasks.remove(taskId);
    }

    @Override
    public void close() {
        tasks.forEach(runtime::releaseTask);
        tasks.clear();
        runtime.close();
    }
}
