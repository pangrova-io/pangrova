package org.pangrova.testing.integration.e2e.write;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
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
                    PrepareDistributedWriteRequest.getDefaultInstance())).build()))
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
            var command = RuntimeCommand.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
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
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("accepted membership");
            assertThat(fixture.fetchCount()).isZero();
            assertThatThrownBy(() -> execute(control, schemaCommand(true)))
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
            var prepare = RuntimeCommand.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
                    .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE)
                    .setPayload(Any.pack(PrepareDistributedWriteRequest.newBuilder().setControlEpoch(1).build())).build();
            assertThatThrownBy(() -> execute(control, prepare)).isInstanceOf(ControlEpochFenceException.class);
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
            var anchors = new org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore(stateStoreDataSource());
            String execution = "control-recovery-" + java.util.UUID.randomUUID();
            var previous = anchors.tryAcquire(execution, "control-original", java.util.UUID.randomUUID().toString(),
                    java.time.Duration.ofSeconds(60)).orElseThrow();
            assertThat(anchors.yield(previous)).isTrue();
            var recovered = anchors.tryAcquire(execution, "control-current", java.util.UUID.randomUUID().toString(),
                    java.time.Duration.ofSeconds(60)).orElseThrow();
            try {
                assertThat(recovered.fencingToken()).isGreaterThan(previous.fencingToken());
                var original = owner(previous); var current = owner(recovered);
                var request = recovery(execution, original, current);
                var forged = request.toBuilder().setFragments(0, request.getFragments(0).toBuilder()
                        .setTaskAttempt(request.getFragments(0).getTaskAttempt().toBuilder().setExecutionOwner(current)));
                assertThatThrownBy(() -> execute(control, prepareCommand(forged.build())))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("provenance");
                assertThat(fixture.fetchCount()).isZero();
                assertThatThrownBy(() -> execute(control, prepareCommand(request)))
                        .isInstanceOf(SecurityException.class).hasMessageContaining("Tenant");
                assertThat(fixture.fetchCount()).isEqualTo(1);
            } finally { assertThat(anchors.release(recovered)).isTrue(); }
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

    private PrepareDistributedWriteRequest recovery(String execution, ExecutionOwnerIdentity original,
            ExecutionOwnerIdentity current) throws com.google.protobuf.InvalidProtocolBufferException {
        var schema = schemaCommand(true).getPayload().unpack(DatasetSchemaFingerprintRequest.class);
        byte[] metadata = {1, 2, 3};
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId(execution).setTaskId("task-1")
                .setControlEpoch(1).setExecutionOwner(original).setPlanEpoch(1).setFencingToken(original.getFencingToken());
        return PrepareDistributedWriteRequest.newBuilder().setExecutionId(execution)
                .setInvocationId("b".repeat(32))
                .setExecutionOwner(current).setIntentExecutionOwner(original).setControlEpoch(2)
                .setCommitFingerprint("a".repeat(64)).setSecurity(schema.getSecurity())
                .setDeadlineEpochMs(schema.getDeadlineEpochMs()).setOutputBinding(WriteOutputBinding.newBuilder()
                        .setLance(LanceWriteBinding.newBuilder().setCatalog(schema.getCatalog())))
                // Untrusted request is rejected by the actual tenant allowlist before any Native preparation.
                .setReservation(org.pangrova.protocol.v2.SourcePublicationReservation.newBuilder()
                        .setTableId("untrusted-table").setTableGeneration(1).setTenantId(schema.getSecurity().getTenantId())
                        .setManifestRelativePath("_pangrova/prepared/" + "a".repeat(64) + ".manifest"))
                .setObjectPageSize(512)
                .addFragments(DistributedWriteFragment.newBuilder().setTaskAttempt(attempt)
                        .setCommitMetadata(com.google.protobuf.ByteString.copyFrom(metadata))
                        .setSha256(ProtocolChecksums.sha256(metadata))).build();
    }

    private static ExecutionOwnerIdentity owner(org.pangrova.spi.query.coordination.ExecutionAnchor anchor) {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId()).setOwnerIncarnation(anchor.ownerIncarnation())
                .setFencingToken(anchor.fencingToken()).build();
    }

    private static RuntimeCommand prepareCommand(PrepareDistributedWriteRequest request) {
        return RuntimeCommand.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
                .setCommandType(RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE)
                .setPayload(Any.pack(request)).build();
    }

    private static byte[] execute(NativeDatasetControlService control, RuntimeCommand command) throws Exception {
        return control.execute(new ByteArrayInputStream(command.toByteArray()));
    }
}
