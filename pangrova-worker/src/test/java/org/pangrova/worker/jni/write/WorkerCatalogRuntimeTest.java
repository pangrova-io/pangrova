package org.pangrova.worker.jni.write;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WriteTaskOptions;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.spi.connector.ConnectorSecretResolver;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerCatalogRuntimeTest {

    private static final byte[] HANDLE_BYTES = {1, 2, 3};
    private static final CatalogDeploymentResponse DEPLOYMENT = new CatalogDeploymentResponse(
            "lake", "test-connector", "incarnation-1", 7,
            Map.of("credential", "${TEST:credential}"), Set.of("tenant-a", "tenant-b"));
    private static final CatalogDeploymentResponse SHARED_DEPLOYMENT = new CatalogDeploymentResponse(
            "lake", "test-connector", "incarnation-1", 7,
            Map.of("credential", "${TEST:credential}"), Set.of());

    @Test
    void nativeAllocationLeasesCatalogAndRejectsAnyPageSinkResolutionOrBindingMutation() {
        var allocation = new NativeWriteAllocation("s3://warehouse/table.lance",
                NativeWriteAllocation.Operation.APPEND, 7, new byte[32],
                "write-1", "staging/write-1", "generation-1", List.of("value"));
        RecordingFactory factory = new RecordingFactory(false, allocation);
        var runtime = new WorkerCatalogRuntime((catalog, incarnation, revision) -> DEPLOYMENT,
                List.of(factory), List.of(new TenantSecretResolver()));
        var connectorTask = task("native-task", "tenant-a");
        var connectorBinding = connectorTask.getExecutionOptions().getWrite().getOutputBinding().getConnector();
        var attempt = connectorTask.getAttempt().toBuilder().setPlanEpoch(1).setFencingToken(2)
                .setControlEpoch(3).setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("owner").setOwnerIncarnation("incarnation").setFencingToken(4));
        var nativeTask = org.pangrova.protocol.NativeWriteBindings.bindAttempt(connectorTask.toBuilder()
                .setAttempt(attempt).setExecutionOptions(connectorTask.getExecutionOptions().toBuilder()
                        .setWrite(connectorTask.getExecutionOptions().getWrite().toBuilder()
                                .setFileFormat(org.pangrova.protocol.v2.FileFormat.FILE_FORMAT_LANCE)
                                .setWriteMode("INSERT").setOutputBinding(WriteOutputBinding.newBuilder()
                                        .setLance(org.pangrova.protocol.v2.LanceWriteBinding.newBuilder()
                                                .setCatalog(connectorBinding.getCatalog())
                                                .setOutputHandle(connectorBinding.getOutputHandle())
                                                .setDatasetUri(allocation.datasetUri())
                                                .setOperation(org.pangrova.protocol.v2.LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND)
                                                .setExpectedReadVersion(7).setWriteId("write-1")
                                                .setGeneration("generation-1").setAllocationStagingPrefix("staging/write-1")
                                                .setSchemaFenceSha256(org.pangrova.protocol.v2.Checksum.newBuilder()
                                                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                                                        .setDigest(ByteString.copyFrom(new byte[32])))
                                                .addWriteColumnNames("value"))))).build());
        try {
            runtime.bindTask("native-task", nativeTask);
            assertThat(runtime.cachedCatalogRevisionCount()).isEqualTo(1);
            assertThatThrownBy(() -> runtime.resolve("native-task", "lake"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("PageSink");
            runtime.releaseTask("native-task");
            var changed = nativeTask.toBuilder().setExecutionOptions(nativeTask.getExecutionOptions().toBuilder()
                    .setWrite(nativeTask.getExecutionOptions().getWrite().toBuilder()
                            .setOutputBinding(nativeTask.getExecutionOptions().getWrite().getOutputBinding().toBuilder()
                                    .setLance(nativeTask.getExecutionOptions().getWrite().getOutputBinding().getLance()
                                            .toBuilder().setDatasetUri("s3://other/table.lance"))))).build();
            assertThatThrownBy(() -> runtime.bindTask("native-task", changed))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("allocation");
        } finally { runtime.close(); }
        assertThat(factory.shutdowns).hasValue(1);
    }

    @Test
    void sharedDeploymentKeepsTenantResolvedCredentialsInSeparateConnectors() {
        RecordingFactory factory = new RecordingFactory();
        AtomicInteger fetches = new AtomicInteger();
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> {
                    fetches.incrementAndGet();
                    return DEPLOYMENT;
                }, List.of(factory), List.of(new TenantSecretResolver()));
        try {
            runtime.bindTask("task-a1", task("task-a1", "tenant-a"));
            runtime.bindTask("task-a2", task("task-a2", "tenant-a"));
            runtime.bindTask("task-b", task("task-b", "tenant-b"));

            assertThat(runtime.cachedCatalogRevisionCount()).isEqualTo(2);
            assertThat(fetches).hasValue(2);
            assertThat(factory.credentials).containsExactly("tenant-a-secret", "tenant-b-secret");
            assertThat(runtime.resolve("task-b", "lake").session().getProperties())
                    .containsEntry("tenant_id", "tenant-b");
        } finally {
            runtime.close();
        }
        assertThat(factory.shutdowns).hasValue(2);
    }

    @Test
    void rejectsUnauthorizedTenantBeforeResolvingSecretsOrCreatingConnector() {
        RecordingFactory factory = new RecordingFactory();
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> DEPLOYMENT,
                List.of(factory), List.of(new TenantSecretResolver()));
        try {
            assertThatThrownBy(() -> runtime.bindTask("task-c", task("task-c", "tenant-c")))
                    .isInstanceOf(SecurityException.class);
            assertThat(factory.credentials).isEmpty();
            assertThat(runtime.cachedCatalogRevisionCount()).isZero();
        } finally {
            runtime.close();
        }
    }

    @Test
    void failedCodecInitializationReleasesCreatedConnector() {
        RecordingFactory factory = new RecordingFactory(true);
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> DEPLOYMENT,
                List.of(factory), List.of(new TenantSecretResolver()));
        try {
            assertThatThrownBy(() -> runtime.bindTask("task-a", task("task-a", "tenant-a")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("codec");
            assertThat(factory.shutdowns).hasValue(1);
            assertThat(runtime.cachedCatalogRevisionCount()).isZero();
        } finally {
            runtime.close();
        }
    }

    @Test
    void evictsOnlyIdleTenantConnectorWhenCacheIsFull() {
        RecordingFactory factory = new RecordingFactory();
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> SHARED_DEPLOYMENT,
                List.of(factory), List.of(new TenantSecretResolver()));
        try {
            for (int i = 0; i < WorkerCatalogRuntime.MAX_CACHED_CATALOG_REVISIONS; i++) {
                runtime.bindTask("task-" + i,
                        task("task-" + i, "tenant-" + i, SHARED_DEPLOYMENT));
                if (i > 0) runtime.releaseTask("task-" + i);
            }
            runtime.bindTask("overflow-task",
                    task("overflow-task", "overflow-tenant", SHARED_DEPLOYMENT));

            assertThat(runtime.cachedCatalogRevisionCount())
                    .isEqualTo(WorkerCatalogRuntime.MAX_CACHED_CATALOG_REVISIONS);
            assertThat(factory.shutdowns).hasValue(1);
            assertThat(runtime.resolve("task-0", "lake").session().getProperties())
                    .containsEntry("tenant_id", "tenant-0");
        } finally {
            runtime.close();
        }
        assertThat(factory.shutdowns)
                .hasValue(WorkerCatalogRuntime.MAX_CACHED_CATALOG_REVISIONS + 1);
    }

    @Test
    void rejectsNewCatalogWhenEveryCachedConnectorHasActiveTaskLease() {
        RecordingFactory factory = new RecordingFactory();
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> SHARED_DEPLOYMENT,
                List.of(factory), List.of(new TenantSecretResolver()));
        try {
            for (int i = 0; i < WorkerCatalogRuntime.MAX_CACHED_CATALOG_REVISIONS; i++) {
                runtime.bindTask("task-" + i,
                        task("task-" + i, "tenant-" + i, SHARED_DEPLOYMENT));
            }
            assertThatThrownBy(() -> runtime.bindTask("overflow-task",
                    task("overflow-task", "overflow-tenant", SHARED_DEPLOYMENT)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("full of active task leases");
            assertThat(runtime.cachedCatalogRevisionCount())
                    .isEqualTo(WorkerCatalogRuntime.MAX_CACHED_CATALOG_REVISIONS);
            assertThat(factory.shutdowns).hasValue(0);
        } finally {
            runtime.close();
        }
    }

    @Test
    void concurrentTasksShareOneCatalogInitialization() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        AtomicInteger fetches = new AtomicInteger();
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch allowFetch = new CountDownLatch(1);
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> {
                    fetches.incrementAndGet();
                    fetchStarted.countDown();
                    try {
                        if (!allowFetch.await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting for catalog fetch");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Catalog fetch interrupted", interrupted);
                    }
                    return DEPLOYMENT;
                }, List.of(factory), List.of(new TenantSecretResolver()));
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> runtime.bindTask("task-a1", task("task-a1", "tenant-a")));
            assertThat(fetchStarted.await(30, TimeUnit.SECONDS)).isTrue();
            var second = executor.submit(() -> runtime.bindTask("task-a2", task("task-a2", "tenant-a")));
            allowFetch.countDown();
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
            assertThat(fetches).hasValue(1);
            assertThat(runtime.cachedCatalogRevisionCount()).isEqualTo(1);
        } finally {
            allowFetch.countDown();
            executor.shutdownNow();
            runtime.close();
        }
        assertThat(factory.shutdowns).hasValue(1);
    }

    @Test
    void closeDuringDeploymentFetchShutsDownConnectorCreatedAfterClose() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch allowFetch = new CountDownLatch(1);
        WorkerCatalogRuntime runtime = new WorkerCatalogRuntime(
                (catalog, incarnation, revision) -> {
                    fetchStarted.countDown();
                    try {
                        if (!allowFetch.await(30, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting for catalog fetch");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Catalog fetch interrupted", interrupted);
                    }
                    return DEPLOYMENT;
                }, List.of(factory), List.of(new TenantSecretResolver()));
        var executor = Executors.newSingleThreadExecutor();
        try {
            var binding = executor.submit(() -> runtime.bindTask(
                    "task-a", task("task-a", "tenant-a")));
            assertThat(fetchStarted.await(30, TimeUnit.SECONDS)).isTrue();
            runtime.close();
            allowFetch.countDown();
            assertThatThrownBy(() -> binding.get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(factory.shutdowns).hasValue(1);
            assertThat(runtime.cachedCatalogRevisionCount()).isZero();
        } finally {
            allowFetch.countDown();
            executor.shutdownNow();
            runtime.close();
        }
    }

    private static TaskRequest task(String taskId, String tenantId) {
        return task(taskId, tenantId, DEPLOYMENT);
    }

    private static TaskRequest task(
            String taskId, String tenantId, CatalogDeploymentResponse deployment) {
        CatalogExecutionBinding catalog = CatalogExecutionBinding.newBuilder()
                .setCatalogName(deployment.catalogName())
                .setConnectorProvider(deployment.connectorProvider())
                .setCatalogIncarnationId(deployment.catalogIncarnationId())
                .setCatalogRevision(deployment.catalogRevision())
                .setConfigurationFingerprint(CatalogDeploymentFingerprint.checksum(deployment))
                .build();
        SerializedConnectorOutputHandle handle = SerializedConnectorOutputHandle.newBuilder()
                .setTypeUrl("pangrova.io/test/output-handle")
                .setCodecVersion(1)
                .setPayload(ByteString.copyFrom(HANDLE_BYTES))
                .setPayloadChecksum(CatalogDeploymentFingerprint.sha256(HANDLE_BYTES))
                .addWriteColumnNames("value")
                .build();
        return TaskRequest.newBuilder()
                .setAttempt(TaskAttemptId.newBuilder().setQueryOrJobId("query-1").setTaskId(taskId))
                .setSecurity(SecurityContext.newBuilder().setTenantId(tenantId).setPrincipal("user"))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setWrite(
                        WriteTaskOptions.newBuilder().setOutputBinding(
                                WriteOutputBinding.newBuilder().setConnector(
                                        ConnectorWriteBinding.newBuilder()
                                                .setCatalog(catalog).setOutputHandle(handle)))))
                .build();
    }

    private static final class TenantSecretResolver implements ConnectorSecretResolver {
        @Override public String prefix() { return "TEST"; }
        @Override public String resolve(String tenantId, String reference) {
            return tenantId + "-secret";
        }
    }

    private static final class RecordingFactory implements ConnectorFactory {
        private final List<String> credentials = new CopyOnWriteArrayList<>();
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final boolean rejectCodec;
        private final NativeWriteAllocation nativeAllocation;

        RecordingFactory() { this(false); }
        RecordingFactory(boolean rejectCodec) { this(rejectCodec, null); }
        RecordingFactory(boolean rejectCodec, NativeWriteAllocation nativeAllocation) {
            this.rejectCodec = rejectCodec;
            this.nativeAllocation = nativeAllocation;
        }

        @Override public String getName() { return "test-connector"; }
        @Override public Connector create(
                String catalogName, Map<String, String> config, ConnectorContext context) {
            credentials.add(config.get("credential"));
            return new TestConnector(shutdowns, rejectCodec, nativeAllocation);
        }
    }

    private static final class TestConnector implements Connector {
        private final AtomicInteger shutdowns;
        private final boolean rejectCodec;
        private final NativeWriteAllocation nativeAllocation;
        TestConnector(AtomicInteger shutdowns, boolean rejectCodec, NativeWriteAllocation nativeAllocation) {
            this.shutdowns = shutdowns;
            this.rejectCodec = rejectCodec;
            this.nativeAllocation = nativeAllocation;
        }
        @Override public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new TestMetadata();
        }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() {
            if (nativeAllocation != null) throw new AssertionError("Native sink must not acquire PageSink");
            return (transaction, session, handle) -> null;
        }
        @Override public ConnectorOutputHandleCodec getOutputHandleCodec() {
            if (rejectCodec) throw new UnsupportedOperationException("Output handle codec unavailable");
            return new TestHandleCodec(nativeAllocation);
        }
        @Override public void shutdown() { shutdowns.incrementAndGet(); }
    }

    private static final class TestHandleCodec implements ConnectorOutputHandleCodec {
        private final NativeWriteAllocation allocation;
        TestHandleCodec(NativeWriteAllocation allocation) { this.allocation = allocation; }
        @Override public String typeUrl() { return "pangrova.io/test/output-handle"; }
        @Override public int codecVersion() { return 1; }
        @Override public byte[] serialize(ConnectorOutputTableHandle handle) {
            return HANDLE_BYTES.clone();
        }
        @Override public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
            if (version != 1 || !java.util.Arrays.equals(payload, HANDLE_BYTES)) {
                throw new IllegalArgumentException("Invalid test handle");
            }
            return new ConnectorOutputTableHandle() {
                @Override public Optional<NativeWriteAllocation> nativeWriteAllocation() {
                    return Optional.ofNullable(allocation);
                }
            };
        }
        @Override public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
            return List.of("value");
        }
    }

    private static final class TestMetadata implements ConnectorMetadata {
        @Override public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }
        @Override public List<SchemaTableName> listTables(
                ConnectorSession session, Optional<String> schemaName) { return List.of(); }
        @Override public ConnectorTableHandle getTableHandle(
                ConnectorSession session, SchemaTableName tableName) { return null; }
        @Override public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) { return null; }
        @Override public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) { return Map.of(); }
        @Override public TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) { return TableStatistics.UNKNOWN; }
    }
}
