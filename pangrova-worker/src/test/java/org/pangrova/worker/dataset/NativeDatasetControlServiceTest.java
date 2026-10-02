package org.pangrova.worker.dataset;

import com.google.protobuf.Any;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.DistributedWriteFragment;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.worker.jni.write.WorkerCatalogRuntime;
import org.pangrova.worker.task.ControlEpochFenceException;
import org.pangrova.worker.task.TaskManager;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeDatasetControlServiceTest {
    private static final CatalogDeploymentResponse DEPLOYMENT = new CatalogDeploymentResponse(
            "dataset", "lance", "incarnation-1", 1,
            Map.of("jdbc.url", "jdbc:postgresql://127.0.0.1:1/unopened", "warehouse", "s3://warehouse/datasets"),
            Set.of("allowed-tenant"));

    @Test
    void rejectsProtocolPayloadAndBodyBoundsBeforeFetchingCatalog() {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = catalogs(fetches);
                var control = new NativeDatasetControlService(tasks, catalogs)) {
            var valid = schemaCommand("allowed-tenant");
            assertThatThrownBy(() -> execute(control, valid.toBuilder().clearProtocol().build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical");
            assertThatThrownBy(() -> execute(control, valid.toBuilder().setPayload(Any.pack(
                    FinalizeDistributedWriteRequest.getDefaultInstance())).build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("payload type");
            assertThatThrownBy(() -> control.execute(new ByteArrayInputStream(
                    new byte[NativeDatasetControlService.MAX_REQUEST_BYTES + 1])))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limits");
            assertThatThrownBy(() -> control.execute(new ByteArrayInputStream(new byte[] {(byte) 0xff})))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Malformed");
            assertThat(fetches).hasValue(0);
        }
    }

    @Test
    void nestedSchemaUsesTopLevelColumnsAndTenantAuthorizationPrecedesNative() {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = catalogs(fetches);
                var control = new NativeDatasetControlService(tasks, catalogs)) {
            assertThatThrownBy(() -> execute(control, schemaCommand("denied-tenant")))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fetches).hasValue(1);
            assertThat(catalogs.cachedCatalogRevisionCount()).isZero();
        }
    }

    @Test
    void partitionBoundsAndClientStorageSpoofRejectBeforeNative() throws Exception {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = catalogs(fetches);
                var control = new NativeDatasetControlService(tasks, catalogs)) {
            var schema = schemaCommand("denied-tenant").getPayload().unpack(DatasetSchemaFingerprintRequest.class);
            var request = DatasetScanPartitionsRequest.newBuilder().setCatalog(schema.getCatalog())
                    .setSecurity(schema.getSecurity()).setDatasetUri("s3://warehouse/datasets/table.lance")
                    .setVersion(1).setPageSize(256).setDeadlineEpochMs(System.currentTimeMillis() + 60_000)
                    .setPhysicalSchemaSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                            .setDigest(com.google.protobuf.ByteString.copyFrom(new byte[32]))).build();
            var command = RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS)
                    .setPayload(Any.pack(request)).build();
            assertThatThrownBy(() -> execute(control, command.toBuilder()
                    .addWorkerLocalStorageBindings(ObjectStoreExecutionBinding.newBuilder()
                            .setTenantId("denied-tenant")).build()))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Worker-local");
            var oversized = command.toBuilder().setPayload(Any.pack(request.toBuilder().setPageSize(257).build()));
            assertThatThrownBy(() -> execute(control, oversized.build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bounded");
            assertThat(fetches).hasValue(0);
            assertThatThrownBy(() -> execute(control, command))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fetches).hasValue(1);
        }
    }

    @Test
    void deadlineAndControlFenceRejectBeforeCatalogOrNative() {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = catalogs(fetches);
                var control = new NativeDatasetControlService(tasks, catalogs)) {
            var request = schemaCommand("allowed-tenant").getPayload().unpack(DatasetSchemaFingerprintRequest.class);
            var expired = schemaCommand("allowed-tenant").toBuilder().setPayload(Any.pack(
                    request.toBuilder().setDeadlineEpochMs(1).build())).build();
            assertThatThrownBy(() -> execute(control, expired)).isInstanceOf(ControlDeadlineExceededException.class);
            tasks.requireControlEpochHandshake();
            tasks.observeControlEpoch(2);
            var finalize = RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE)
                    .setPayload(Any.pack(FinalizeDistributedWriteRequest.newBuilder().setControlEpoch(1).build())).build();
            assertThatThrownBy(() -> execute(control, finalize)).isInstanceOf(ControlEpochFenceException.class);
            assertThat(fetches).hasValue(0);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException(failure);
        }
    }

    @Test
    void recoveryKeepsOriginalWinnerOwnerSeparateFromCurrentPublicationAuthority() throws Exception {
        var fetches = new AtomicInteger();
        try (var tasks = TaskManager.forHttpWiringOnly(); var catalogs = catalogs(fetches);
                var control = new NativeDatasetControlService(tasks, catalogs)) {
            tasks.requireControlEpochHandshake();
            tasks.observeControlEpoch(2);
            var original = ExecutionOwnerIdentity.newBuilder().setOwnerId("original").setOwnerIncarnation("original-1")
                    .setFencingToken(10).build();
            var current = original.toBuilder().setOwnerId("current").setOwnerIncarnation("current-1")
                    .setFencingToken(11).build();
            var request = recovery(original, current);
            var forged = request.toBuilder().setFragments(0, request.getFragments(0).toBuilder()
                    .setTaskAttempt(request.getFragments(0).getTaskAttempt().toBuilder().setExecutionOwner(current)));
            assertThatThrownBy(() -> execute(control, finalizeCommand(forged.build())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("provenance");
            assertThat(fetches).hasValue(0);
            assertThatThrownBy(() -> execute(control, finalizeCommand(request)))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fetches).hasValue(1);
        }
    }

    private static FinalizeDistributedWriteRequest recovery(ExecutionOwnerIdentity original,
            ExecutionOwnerIdentity current) throws com.google.protobuf.InvalidProtocolBufferException {
        var schema = schemaCommand("denied-tenant").getPayload().unpack(DatasetSchemaFingerprintRequest.class);
        byte[] metadata = {1, 2, 3};
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("execution-1").setTaskId("task-1")
                .setControlEpoch(1).setExecutionOwner(original).setPlanEpoch(1).setFencingToken(1);
        return FinalizeDistributedWriteRequest.newBuilder().setExecutionId("execution-1")
                .setExecutionOwner(current).setIntentExecutionOwner(original).setControlEpoch(2).setRecovering(true)
                .setCommitFingerprint("a".repeat(64)).setSecurity(schema.getSecurity())
                .setDeadlineEpochMs(schema.getDeadlineEpochMs()).setOutputBinding(WriteOutputBinding.newBuilder()
                        .setLance(LanceWriteBinding.newBuilder().setCatalog(schema.getCatalog())))
                .addFragments(DistributedWriteFragment.newBuilder().setTaskAttempt(attempt)
                        .setCommitMetadata(com.google.protobuf.ByteString.copyFrom(metadata))
                        .setSha256(CanonicalTaskRequests.sha256(metadata))).build();
    }

    private static RuntimeCommand finalizeCommand(FinalizeDistributedWriteRequest request) {
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE)
                .setPayload(Any.pack(request)).build();
    }

    static WorkerCatalogRuntime catalogs(AtomicInteger fetches) {
        var factories = ServiceLoader.load(ConnectorFactory.class).stream().map(ServiceLoader.Provider::get)
                .filter(factory -> factory.getName().equals("lance")).toList();
        return new WorkerCatalogRuntime((catalog, incarnation, revision) -> {
            fetches.incrementAndGet();
            return DEPLOYMENT;
        }, factories, List.of());
    }

    static RuntimeCommand schemaCommand(String tenant) {
        Type primitive = Type.newBuilder().setI64(Type.I64.newBuilder()).build();
        Type nested = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(primitive)).build();
        var schema = NamedStruct.newBuilder().addNames("record").addNames("id")
                .setStruct(Type.Struct.newBuilder().addTypes(nested)).build();
        var catalog = CatalogExecutionBinding.newBuilder().setCatalogName(DEPLOYMENT.catalogName())
                .setConnectorProvider(DEPLOYMENT.connectorProvider()).setCatalogRevision(DEPLOYMENT.catalogRevision())
                .setCatalogIncarnationId(DEPLOYMENT.catalogIncarnationId())
                .setConfigurationFingerprint(CatalogDeploymentFingerprint.checksum(DEPLOYMENT));
        var request = DatasetSchemaFingerprintRequest.newBuilder().setCatalog(catalog).setSchema(schema)
                .addColumnTypes(LogicalType.newBuilder().setCanonicalSql("ROW(id BIGINT)").setSubstraitType(nested))
                .setSecurity(SecurityContext.newBuilder().setPrincipal("user").setTenantId(tenant))
                .setDeadlineEpochMs(System.currentTimeMillis() + 60_000).build();
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT)
                .setPayload(Any.pack(request)).build();
    }

    private static byte[] execute(NativeDatasetControlService control, RuntimeCommand command) throws Exception {
        return control.execute(new ByteArrayInputStream(command.toByteArray()));
    }
}
