package org.pangrova.worker.catalog;

import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.worker.catalog.WorkerCatalogRegistry.CatalogEntry;
import org.pangrova.worker.catalog.WorkerCatalogRegistry.CatalogRuntime;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.worker.connector.session.TaskConnectorSession;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.runtime.lifecycle.ConnectorPluginInventory;
import java.nio.file.Path;
import java.util.Optional;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;

import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Production Worker-local connector runtime. Catalog revisions are fetched from the authenticated
 * Coordinator endpoint, secret references are resolved locally, and exact Coordinator output
 * handles are leased per task.
 */
@ThreadSafe
public final class WorkerCatalogRuntime implements CatalogResourceResolver, AutoCloseable {

    private final WorkerCatalogRegistry catalogs;
    private final ConnectorPluginInventory plugins;
    private final ConcurrentHashMap<String, TaskBinding> taskBindings = new ConcurrentHashMap<>();

    /** Creates a runtime from trusted deployment and plugin inventories. */
    private WorkerCatalogRuntime(
            CatalogDeploymentClient deployments,
            ConnectorPluginInventory plugins,
            SecretResolver secrets) {
        this.plugins = plugins;
        catalogs = new WorkerCatalogRegistry(deployments, plugins, secrets);
    }

    /** Discovers connector and secret-provider plugins from the Worker class loader. */
    public static WorkerCatalogRuntime discover(
            String coordinatorUrl, String internalAuthSecret, ClassLoader classLoader) {
        return discover(coordinatorUrl, internalAuthSecret, classLoader, Optional.empty());
    }

    /** Opens a role-scoped inventory with separately deployed connector plugin JARs. */
    public static WorkerCatalogRuntime discover(String coordinatorUrl, String internalAuthSecret,
            ClassLoader classLoader, Optional<Path> pluginDirectory) {
        var client = new CatalogDeploymentClient(coordinatorUrl, internalAuthSecret);
        var secrets = SecretResolverRegistry.discover(classLoader);
        var plugins = ConnectorPluginInventory.open(classLoader, ConnectorRole.WORKER, pluginDirectory);
        try {
            return new WorkerCatalogRuntime(client, plugins, secrets);
        } catch (RuntimeException | Error failure) {
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(failure, plugins::close);
            throw failure;
        }
    }

    @Override
    public void bindTask(String taskId, TaskRequest request) {
        requireTaskIdentity(taskId, request);
        boolean cdc = request.getTaskKind() == TaskKind.TASK_KIND_CDC_INGEST;
        WriteOutputBinding output = cdc
                ? request.getCdcIngest().getWrite()
                : request.getExecutionOptions().getWrite().getOutputBinding();
        if (output.getBindingCase() == WriteOutputBinding.BindingCase.BINDING_NOT_SET) {
            throw new IllegalArgumentException(
                    "Write task requires a connector-owned output binding");
        }
        boolean nativeSink = output.hasLance() || output.hasPhysical();
        if (cdc && nativeSink) {
            throw new IllegalArgumentException("CDC write task requires a connector PageSink output binding");
        }
        CatalogExecutionBinding catalogBinding = WorkerWriteOutputs.catalog(output);
        String tenantId = request.getSecurity().getTenantId();
        // Connector instances hold locally resolved credentials. A deployment
        // shared by tenants must never share the first tenant's connector.
        CatalogEntry entry = catalogs.acquireCatalog(catalogBinding, tenantId);
        boolean bound = false;
        try {
            CatalogRuntime runtime = WorkerCatalogRegistry.awaitCatalog(entry);
            TaskConnectorSession session =
                    TaskConnectorSession.from(
                            request.getSecurity(),
                            request.getAttempt(),
                            catalogBinding.getCatalogName(),
                            request.getExecutionOptions().getSessionPropertiesMap());
            SerializedConnectorOutputHandle serialized = WorkerWriteOutputs.outputHandle(output);
            ConnectorOutputTableHandle outputHandle = decodeHandle(runtime.codec(), serialized);
            if (!runtime.codec()
                    .writeColumnNames(outputHandle)
                    .equals(requireWriteColumns(serialized))) {
                throw new IllegalArgumentException(
                        "Output handle columns differ from pinned columns");
            }
            validateNativeOutput(runtime, session, request, outputHandle);
            CatalogResources resources =
                    taskResources(runtime, session, outputHandle, serialized, nativeSink);
            catalogs.publish(() -> {
                TaskBinding previous =
                        taskBindings.putIfAbsent(
                                taskId,
                                new TaskBinding(entry, resources));
                if (previous != null) {
                    throw new IllegalStateException(
                            "Task connector binding already exists: " + taskId);
                }
            });
            bound = true;
        } finally {
            if (!bound) {
                catalogs.releaseCatalog(entry);
            }
        }
    }

    private static void validateNativeOutput(
            CatalogRuntime runtime,
            TaskConnectorSession session,
            TaskRequest request,
            ConnectorOutputTableHandle handle) {
        var binding = request.getExecutionOptions().getWrite().getOutputBinding();
        if (binding.hasLance() || binding.hasPhysical()) {
            NativeOutputAllocationValidation.validateTask(request, handle);
        }
        if (binding.hasPhysical()) {
            runtime.connector()
                    .getNativePhysicalWriteProvider()
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "Connector has no Native physical write metadata provider"))
                    .validateAllocation(
                            session, handle, handle.nativePhysicalWriteAllocation().orElseThrow());
        }
    }

    private static CatalogResources taskResources(
            CatalogRuntime runtime,
            TaskConnectorSession session,
            ConnectorOutputTableHandle handle,
            SerializedConnectorOutputHandle serialized,
            boolean nativeSink) {
        if (nativeSink) {
            return null;
        }
        return new CatalogResources(
                runtime.connector().getMetadata(session),
                runtime.connector().getPageSinkProvider(),
                session,
                handle,
                requireWriteColumns(serialized));
    }

    @Override
    public CatalogResources resolve(String taskId) {
        TaskBinding binding = taskBindings.get(taskId);
        if (binding == null) {
            throw new IllegalStateException("Task connector binding not found: " + taskId);
        }
        if (binding.resources() == null) {
            throw new IllegalStateException(
                    "Native dataset task cannot resolve a connector PageSink");
        }
        return binding.resources();
    }

    @Override
    public void releaseTask(String taskId) {
        TaskBinding binding = taskBindings.remove(taskId);
        if (binding != null) {
            catalogs.releaseCatalog(binding.entry());
        }
    }

    @Override
    public void close() {
        // A failed drain remains retryable. Active leases still need their plugin classes.
        catalogs.close();
        plugins.close();
    }

    public int cachedCatalogRevisionCount() {
        return catalogs.size();
    }

    /** Resolves and stages the exact table revision in the trusted tenant-local catalog. */
    public List<String> stageRecluster(org.pangrova.protocol.rest.maintenance.ReclusterStageRequest work)
            throws com.google.protobuf.InvalidProtocolBufferException {
        java.util.Objects.requireNonNull(work.runId(), "runId");
        java.util.Objects.requireNonNull(work.attemptId(), "attemptId");
        long now = System.currentTimeMillis();
        long deadline = Math.min(work.deadlineEpochMs(),
                now + org.pangrova.protocol.rest.maintenance.ReclusterStageRequest.TIMEOUT_MS);
        if (deadline <= now) {
            throw new IllegalArgumentException("Expired RECLUSTER staging deadline");
        }
        var catalog = CatalogExecutionBinding.parseFrom(java.util.Base64.getDecoder().decode(work.catalogBinding()));
        var attempt = org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                .setQueryOrJobId(work.runId().toString()).setTaskId(work.attemptId().toString()).build();
        var session = new TaskConnectorSession(work.tenant(), work.principal(), java.util.Set.of(), attempt,
                catalog.getCatalogName(), java.util.Map.of());
        var name = new org.pangrova.connector.api.metadata.table.SchemaTableName(work.schema(), work.table());
        var entry = catalogs.acquireCatalog(catalog, work.tenant());
        try {
            var metadata = WorkerCatalogRegistry.awaitCatalog(entry).connector().getMetadata(session);
            var table = metadata.getTableHandle(session, name);
            if (table == null) {
                throw new IllegalArgumentException("RECLUSTER table is no longer available");
            }
            return metadata.clusteringMaintenance(session, table).orElseThrow(
                    () -> new UnsupportedOperationException("Connector does not stage RECLUSTER"))
                    .stage(work.revision(), work.attemptId(), () -> {
                        if (Thread.currentThread().isInterrupted()
                                || System.currentTimeMillis() >= deadline) {
                            throw new IllegalStateException("RECLUSTER staging deadline exceeded");
                        }
                    });
        } finally {
            catalogs.releaseCatalog(entry);
        }
    }

    /** Leases an exact tenant deployment for metadata control, without acquiring a PageSink. */
    public CatalogControlLease leaseControl(
            CatalogExecutionBinding binding,
            org.pangrova.protocol.v2.SecurityContext security,
            java.util.Optional<org.pangrova.protocol.v2.WriteOutputBinding> output) {
        output.ifPresent(value -> {
            if (!WorkerWriteOutputs.catalog(value).equals(binding)) {
                throw new IllegalArgumentException("Output belongs to a different catalog deployment");
            }
        });
        String tenant = security.getTenantId();
        if (tenant.isBlank() || security.getPrincipal().isBlank()) {
            throw new IllegalArgumentException(
                    "Native dataset control requires tenant and principal");
        }
        CatalogEntry entry = catalogs.acquireCatalog(binding, tenant);
        boolean leased = false;
        try {
            CatalogRuntime runtime = WorkerCatalogRegistry.awaitCatalog(entry);
            output.ifPresent(value -> validateControlOutput(runtime, value));
            CatalogControlLease lease =
                    new CatalogControlLease(() -> catalogs.releaseCatalog(entry), runtime.storage());
            leased = true;
            return lease;
        } finally {
            if (!leased) {
                catalogs.releaseCatalog(entry);
            }
        }
    }

    private static void validateControlOutput(
            CatalogRuntime runtime, org.pangrova.protocol.v2.WriteOutputBinding binding) {
        if (!binding.hasLance() && !binding.hasPhysical()) {
            throw new IllegalArgumentException("Publication requires a Native output binding");
        }
        var serialized = WorkerWriteOutputs.outputHandle(binding);
        var handle = decodeHandle(runtime.codec(), serialized);
        if (!runtime.codec().writeColumnNames(handle).equals(requireWriteColumns(serialized))) {
            throw new IllegalArgumentException("Output handle columns differ from pinned columns");
        }
        if (binding.hasPhysical()) {
            NativeOutputAllocationValidation.validate(binding.getPhysical(), handle);
        } else {
            NativeOutputAllocationValidation.validate(binding.getLance(), handle);
        }
    }

    private static ConnectorOutputTableHandle decodeHandle(
            ConnectorOutputHandleCodec codec, SerializedConnectorOutputHandle serialized) {
        if (!codec.typeUrl().equals(serialized.getTypeUrl())
                || codec.codecVersion() != serialized.getCodecVersion()
                || !checksumMatches(serialized)) {
            throw new IllegalArgumentException("Connector output handle envelope mismatch");
        }
        return codec.deserialize(
                serialized.getCodecVersion(), serialized.getPayload().toByteArray());
    }

    private static boolean checksumMatches(SerializedConnectorOutputHandle serialized) {
        var checksum = serialized.getPayloadChecksum();
        return checksum.getAlgorithm()
                        == org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                && checksum.getDigest().size() == 32
                && MessageDigest.isEqual(
                        checksum.getDigest().toByteArray(),
                        ProtocolChecksums.sha256(serialized.getPayload().toByteArray())
                                .getDigest()
                                .toByteArray());
    }

    private static List<String> requireWriteColumns(SerializedConnectorOutputHandle handle) {
        List<String> names = List.copyOf(handle.getWriteColumnNamesList());
        if (names.isEmpty()
                || names.stream().anyMatch(name -> name == null || name.isBlank())
                || names.stream().distinct().count() != names.size()) {
            throw new IllegalArgumentException(
                    "Connector output handle requires unique write columns");
        }
        return names;
    }

    private static void requireTaskIdentity(String taskId, TaskRequest request) {
        if (taskId == null
                || taskId.isBlank()
                || !taskId.equals(request.getAttempt().getTaskId())
                || request.getAttempt().getQueryOrJobId().isBlank()
                || request.getSecurity().getTenantId().isBlank()
                || request.getSecurity().getPrincipal().isBlank()) {
            throw new IllegalArgumentException(
                    "Connector write requires exact task and security identity");
        }
    }

    private record TaskBinding(CatalogEntry entry, CatalogResources resources) {}
}
