package org.pangrova.coordinator.metadata;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.FinalizeDistributedWriteResult;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WritePublicationOutcome;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerNativeDatasetControlTest {
    @Test
    void recoveryPreservesWinnersAndCarriesNewAuthorityAndKnownManifest() {
        var intent = intent().withPublication(
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 2, new byte[32]));
        var now = Instant.now();
        var anchor = new ExecutionAnchor("query", "new-owner", "new-process", 9, now, now.plusSeconds(60));
        var request = WorkerNativeDatasetControl.finalizeRequest(SecurityContext.newBuilder()
                .setTenantId("tenant").setPrincipal("alice").build(), intent, anchor, now.plusSeconds(30).toEpochMilli(), 8);
        assertThat(request.getExecutionOwner().getFencingToken()).isEqualTo(9);
        assertThat(request.getIntentExecutionOwner().getFencingToken()).isEqualTo(3);
        assertThat(request.getFragments(0).getTaskAttempt().getExecutionOwner().getFencingToken()).isEqualTo(3);
        assertThat(request.getFragments(0).getCommitMetadata().toByteArray()).isEqualTo(intent.fragments().get(0));
        assertThat(request.getFragments(0).getSha256()).isEqualTo(CanonicalTaskRequests.sha256(intent.fragments().get(0)));
        assertThat(request.getKnownPublishedVersion()).isEqualTo(2);
        assertThat(request.getKnownManifestSha256().getDigest().size()).isEqualTo(32);
        assertThat(request.getRecovering()).isTrue();
        assertThat(request.getCommitFingerprint()).isEqualTo(intent.commitFingerprint());
    }

    @Test
    void unknownPublicationNeverContainsEvidence() {
        var unknown = FinalizeDistributedWriteResult.newBuilder().setCommitFingerprint("fp")
                .setOutcome(WritePublicationOutcome.WRITE_PUBLICATION_OUTCOME_UNKNOWN).build();
        assertThat(WorkerNativeDatasetControl.decodePublication(response(unknown), "fp"))
                .isEqualTo(new NativeWriteFinalization(NativeWriteFinalization.Outcome.UNKNOWN, 0, new byte[0]));
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePublication(response(unknown.toBuilder()
                .setPublishedVersion(2).build()), "fp")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidFingerprintOrDigestCannotBecomeCommitted() {
        var committed = FinalizeDistributedWriteResult.newBuilder().setCommitFingerprint("fp")
                .setOutcome(WritePublicationOutcome.WRITE_PUBLICATION_OUTCOME_COMMITTED).setPublishedVersion(2)
                .setManifestSha256(CanonicalTaskRequests.sha256(new byte[] {1})).build();
        assertThat(WorkerNativeDatasetControl.decodePublication(response(committed), "fp").publishedVersion())
                .isEqualTo(2);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePublication(response(committed), "another"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePublication(response(committed.toBuilder()
                .setManifestSha256(Checksum.newBuilder().setDigest(ByteString.copyFrom(new byte[32]))).build()), "fp"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void callerExecutionAndFragmentsMustMatchDurableIntent() {
        var intent = intent();
        var context = context(intent, "query");
        WorkerNativeDatasetControl.validateIntent(intent, intent.fragments(), context);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateIntent(intent, intent.fragments(),
                context(intent, "another-query"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateIntent(intent, List.of(new byte[] {1}), context))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void authorityCannotChangeIdentityAtTheSameTokenOrExceedOwnerLease() {
        var intent = intent();
        var now = Instant.now();
        var security = SecurityContext.newBuilder().setTenantId("tenant").setPrincipal("alice").build();
        var deadline = now.plusSeconds(30).toEpochMilli();
        for (ExecutionAnchor anchor : List.of(
                new ExecutionAnchor("another-query", "new-owner", "new-process", 9, now, now.plusSeconds(60)),
                new ExecutionAnchor("query", "new-owner", "new-process", 3, now, now.plusSeconds(60)),
                new ExecutionAnchor("query", "old-owner", "old-process", 2, now, now.plusSeconds(60)),
                new ExecutionAnchor("query", "new-owner", "new-process", 9, now, now.plusSeconds(10)))) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.finalizeRequest(security, intent, anchor, deadline, 8))
                    .isInstanceOf(SecurityException.class);
        }
        var anchor = new ExecutionAnchor("query", "old-owner", "old-process", 3, now, now.plusSeconds(60));
        assertThat(WorkerNativeDatasetControl.finalizeRequest(security, intent, anchor, deadline, 8)
                .getExecutionOwner().getFencingToken()).isEqualTo(3);
    }

    @Test
    void outputTransactionAndAllocationMustMatchDurableIntent() {
        var allocation = allocation();
        var output = new Output(new SchemaTableName("schema", "table"), "INSERT", 1, allocation);
        WorkerNativeDatasetControl.validateOutput(intent(), output, "catalog");
        var canonical = binding();
        List<LanceWriteBinding> mismatches = List.of(
                canonical.toBuilder().setDatasetUri("file:///other").build(),
                canonical.toBuilder().setOperation(LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE).build(),
                canonical.toBuilder().setExpectedReadVersion(2).build(),
                canonical.toBuilder().setWriteId("other-write").build(),
                canonical.toBuilder().setSchemaFenceSha256(CanonicalTaskRequests.sha256(new byte[] {1})).build(),
                canonical.toBuilder().setAllocationStagingPrefix("different/prefix").build(),
                canonical.toBuilder().setGeneration("other-generation").build(),
                canonical.toBuilder().clearWriteColumnNames().addWriteColumnNames("other-column").build());
        for (var mismatch : mismatches) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(intent(mismatch), output, "catalog"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void outputIdentityCannotBeIgnoredEvenWhenAllocationMatches() {
        for (Output output : List.of(
                new Output(new SchemaTableName("schema", "other"), "INSERT", 1, allocation()),
                new Output(new SchemaTableName("schema", "table"), "OVERWRITE", 1, allocation()),
                new Output(new SchemaTableName("schema", "table"), "INSERT", 2, allocation()))) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(intent(), output, "catalog"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(intent(),
                new Output(new SchemaTableName("schema", "table"), "INSERT", 1, allocation()), "other-catalog"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static DistributedWriteCommitContext context(WriteCommitEntry intent, String executionId) {
        return new DistributedWriteCommitContext(
                executionId,
                intent.tenantId(),
                intent.baseSnapshotId(),
                intent.writeOutputBinding(),
                intent.commitFingerprint(),
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt());
    }

    private static RuntimeCommandResponse response(FinalizeDistributedWriteResult result) {
        return RuntimeCommandResponse.newBuilder().setSuccess(true).setPayload(Any.pack(result)).build();
    }
    private static WriteCommitEntry intent() {
        return intent(binding());
    }

    private static WriteCommitEntry intent(LanceWriteBinding nativeBinding) {
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task").setPlanEpoch(1)
                .setControlEpoch(2).setFencingToken(3).setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId("old-owner").setOwnerIncarnation("old-process").setFencingToken(3)).build();
        byte[] fragment = LanceWriteFragment.newBuilder().setTaskAttempt(attempt)
                .setDatasetUri("file:///data/table.lance").build().toByteArray();
        byte[] binding = WriteOutputBinding.newBuilder().setLance(nativeBinding).build().toByteArray();
        var fragments = List.of(fragment);
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "query",
                        "tenant",
                        "catalog",
                        "schema.table",
                        "INSERT",
                        1,
                        binding,
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt()),
                fragments);
        return new WriteCommitEntry(
                "query",
                "catalog",
                "schema.table",
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                "INSERT",
                fingerprint,
                "tenant",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                1,
                binding,
                fingerprint,
                "old-owner",
                "old-process",
                3,
                null);
    }

    private static NativeWriteAllocation allocation() {
        return new NativeWriteAllocation("file:///data/table.lance", NativeWriteAllocation.Operation.APPEND,
                1, new byte[32], "write", "staging/write", "generation", List.of("id"));
    }

    private static LanceWriteBinding binding() {
        return LanceWriteBinding.newBuilder().setDatasetUri("file:///data/table.lance")
                .setOperation(LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND).setExpectedReadVersion(1)
                .setSchemaFenceSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(new byte[32])))
                .setWriteId("write").setAllocationStagingPrefix("staging/write")
                .setTransactionStagingPrefix("staging/write/attempt").setGeneration("generation")
                .addWriteColumnNames("id").build();
    }

    private record Output(SchemaTableName table, String mode, long base, NativeWriteAllocation allocation)
            implements ConnectorOutputTableHandle {

        @Override
        public Optional<SchemaTableName> targetTable() {
            return Optional.of(table);
        }

        @Override
        public String writeMode() {
            return mode;
        }

        @Override
        public OptionalLong writeBaseSnapshotId() {
            return OptionalLong.of(base);
        }

        @Override
        public Optional<NativeWriteAllocation> nativeWriteAllocation() {
            return Optional.of(allocation);
        }
    }
}
