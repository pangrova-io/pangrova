package org.pangrova.testing.e2e.write;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.DistributedWriteFragment;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.worker.dataset.ControlDeadlineExceededException;
import org.pangrova.worker.dataset.NativeDatasetControlService;
import org.pangrova.worker.task.ControlEpochFenceException;
import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real-deployment admission components; no successful SQL execution is inferred from rejection tests. */
class NativeDatasetControlServiceTest extends DatasetControlTestBase {

    @Test
    void rejectsProtocolPayloadAndBodyBoundsBeforeFetchingCatalog() throws Exception {
        try (var fixture = controlFixture()) {
            var control = fixture.control();
            var valid = schemaCommand(false);
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
            assertThat(fixture.fetchCount()).isZero();
        }
    }

    @Test
    void nestedSchemaUsesTopLevelColumnsAndTenantAuthorizationPrecedesNative() throws Exception {
        try (var fixture = controlFixture()) {
            assertThatThrownBy(() -> execute(fixture.control(), schemaCommand(true)))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fixture.fetchCount()).isEqualTo(1);
            assertThat(fixture.catalogs().cachedCatalogRevisionCount()).isZero();
        }
    }

    @Test
    void partitionBoundsAndClientStorageSpoofRejectBeforeNative() throws Exception {
        try (var fixture = controlFixture()) {
            var control = fixture.control();
            var schema = schemaCommand(true).getPayload().unpack(DatasetSchemaFingerprintRequest.class);
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
                            .setTenantId(schema.getSecurity().getTenantId())).build()))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Worker-local");
            var oversized = command.toBuilder().setPayload(Any.pack(request.toBuilder().setPageSize(257).build()));
            assertThatThrownBy(() -> execute(control, oversized.build()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bounded");
            assertThat(fixture.fetchCount()).isZero();
            assertThatThrownBy(() -> execute(control, command))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fixture.fetchCount()).isEqualTo(1);
        }
    }

    @Test
    void deadlineAndControlFenceRejectBeforeCatalogOrNative() throws Exception {
        try (var fixture = controlFixture()) {
            var tasks = fixture.tasks();
            var control = fixture.control();
            var request = schemaCommand(false).getPayload().unpack(DatasetSchemaFingerprintRequest.class);
            var expired = schemaCommand(false).toBuilder().setPayload(Any.pack(
                    request.toBuilder().setDeadlineEpochMs(1).build())).build();
            assertThatThrownBy(() -> execute(control, expired)).isInstanceOf(ControlDeadlineExceededException.class);
            tasks.requireControlEpochHandshake();
            tasks.observeControlEpoch(2);
            var finalize = RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE)
                    .setPayload(Any.pack(FinalizeDistributedWriteRequest.newBuilder().setControlEpoch(1).build())).build();
            assertThatThrownBy(() -> execute(control, finalize)).isInstanceOf(ControlEpochFenceException.class);
            assertThat(fixture.fetchCount()).isZero();
        }
    }

    @Test
    void recoveryKeepsOriginalWinnerOwnerSeparateFromCurrentPublicationAuthority() throws Exception {
        try (var fixture = controlFixture()) {
            var tasks = fixture.tasks();
            var control = fixture.control();
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
            assertThat(fixture.fetchCount()).isZero();
            assertThatThrownBy(() -> execute(control, finalizeCommand(request)))
                    .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
            assertThat(fixture.fetchCount()).isEqualTo(1);
        }
    }

    @Test
    void closedControlRejectsAdmissionWithoutFetchingDeployment() throws Exception {
        try (var fixture = controlFixture()) {
            fixture.control().close();
            assertThatThrownBy(() -> execute(fixture.control(), schemaCommand(false)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
            assertThat(fixture.fetchCount()).isZero();
            assertThat(fixture.catalogs().cachedCatalogRevisionCount()).isZero();
        }
    }

    private FinalizeDistributedWriteRequest recovery(ExecutionOwnerIdentity original,
            ExecutionOwnerIdentity current) throws com.google.protobuf.InvalidProtocolBufferException {
        var schema = schemaCommand(true).getPayload().unpack(DatasetSchemaFingerprintRequest.class);
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

    private static byte[] execute(NativeDatasetControlService control, RuntimeCommand command) throws Exception {
        return control.execute(new ByteArrayInputStream(command.toByteArray()));
    }
}
