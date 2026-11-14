package org.pangrova.coordinator.metadata;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.lance.LanceManagedSourceTestExecution;
import org.pangrova.coordinator.exception.NativeDatasetControlRpcException;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WritePreparationOutcome;
import org.pangrova.connector.api.metadata.AcceptedSourceSnapshot;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.NativeWriteAllocation;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerNativeDatasetControlTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test
    void recoveryPreservesWinnersAndCarriesNewAuthorityAndPreparedReservation() throws Exception {
        var execution = LanceManagedSourceTestExecution.append(directory);
        var intent = execution.intent();
        var previous = execution.originalAnchor();
        var current = execution.handoff();
        var original = execution.request();
        var request = WorkerNativeDatasetControl.prepareRequest(original.getSecurity(), intent, current,
                deadline(current), original.getControlEpoch(), execution.reservation());
        assertThat(request.getExecutionOwner().getFencingToken()).isEqualTo(current.fencingToken())
                .isGreaterThan(previous.fencingToken());
        assertThat(request.getIntentExecutionOwner().getFencingToken()).isEqualTo(previous.fencingToken());
        assertThat(request.getFragments(0).getTaskAttempt().getExecutionOwner().getFencingToken()).isEqualTo(previous.fencingToken());
        assertThat(request.getFragments(0).getCommitMetadata().toByteArray()).isEqualTo(intent.fragments().get(0));
        assertThat(request.getFragments(0).getSha256()).isEqualTo(original.getFragments(0).getSha256());
        assertThat(request.getReservation()).isEqualTo(original.getReservation());
        assertThat(request.getReservation().getBaseSnapshot().getVersion()).isEqualTo(1);
        assertThat(request.getReservation().getManifestRelativePath()).isEqualTo(execution.reservation().manifestRelativePath());
        assertThat(request.getCommitFingerprint()).isEqualTo(intent.commitFingerprint());
        var durable = execution.intent();
        assertThat(durable.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(durable.writeOutputBinding()).isEqualTo(intent.writeOutputBinding());
        assertThat(durable.fragments().get(0)).isEqualTo(intent.fragments().get(0));
        assertThat(durable.ownerId()).isEqualTo(previous.ownerId());
        assertThat(durable.ownerIncarnation()).isEqualTo(previous.ownerIncarnation());
        assertThat(durable.executionFencingToken()).isEqualTo(previous.fencingToken());
    }

    @Test
    void unknownPreparationNeverContainsEvidence() {
        var unknown = PrepareDistributedWriteResult.newBuilder().setCommitFingerprint("fp")
                .setOutcome(WritePreparationOutcome.WRITE_PREPARATION_OUTCOME_UNKNOWN).build();
        assertThat(WorkerNativeDatasetControl.decodePreparation(response(unknown), "fp",
                org.pangrova.protocol.v2.CatalogExecutionBinding.getDefaultInstance())).isNull();
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePreparation(response(unknown.toBuilder()
                .setPublication(org.pangrova.protocol.v2.PreparedSourcePublication.getDefaultInstance()).build()), "fp",
                org.pangrova.protocol.v2.CatalogExecutionBinding.getDefaultInstance()))
                .isInstanceOf(NativeDatasetControlRpcException.class).hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidFingerprintOrDigestCannotBecomePrepared() throws Exception {
        var execution = LanceManagedSourceTestExecution.create(directory);
        var actual = execution.pages().get(0);
        var fingerprint = execution.intent().commitFingerprint();
        var decoded = WorkerNativeDatasetControl.decodePreparation(response(actual), fingerprint, execution.catalog());
        assertThat(decoded.publication().candidate().version()).isEqualTo(1);
        assertThat(decoded.publication().candidate().manifestSha256()).isEqualTo(actual.getPublication().getCandidate()
                .getManifestSha256().getDigest().toByteArray());
        assertThat(decoded.objects().objects()).hasSameSizeAs(actual.getObjectPage().getObjectsList());
        WorkerNativeDatasetControl.requireIssuedPage(decoded.objects(), 0, "");
        assertThatThrownBy(() -> WorkerNativeDatasetControl.requireIssuedPage(decoded.objects(), 1, "!"))
                .hasMessageContaining("sequence or cursor differs");
        assertThatThrownBy(() -> WorkerNativeDatasetControl.requireIssuedPage(decoded.objects(), 0, "other"))
                .hasMessageContaining("sequence or cursor differs");
        assertThat(execution.intent().publication()).isNull();
        assertThat(execution.intent().state()).isEqualTo(WriteCommitEntry.State.PENDING);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePreparation(response(actual), "another", execution.catalog()))
                .isInstanceOf(NativeDatasetControlRpcException.class).hasCauseInstanceOf(IllegalArgumentException.class);
        var malformed = actual.toBuilder().setPublication(actual.getPublication().toBuilder().setObjectsSha256(
                Checksum.newBuilder().setDigest(ByteString.copyFrom(new byte[32])))).build();
        assertThatThrownBy(() -> WorkerNativeDatasetControl.decodePreparation(response(malformed), fingerprint, execution.catalog()))
                .isInstanceOf(NativeDatasetControlRpcException.class).hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void callerExecutionAndFragmentsMustMatchDurableIntent() throws Exception {
        var execution = LanceManagedSourceTestExecution.create(directory);
        var intent = execution.intent();
        var context = intent.commitContext();
        WorkerNativeDatasetControl.validateIntent(intent, intent.fragments(), context);
        var wrongExecution = new DistributedWriteCommitContext("another-query", context.tenantId(), context.baseSnapshotId(),
                context.writeOutputBinding(), context.commitFingerprint(), context.principal(), context.authorizationReceipt(), null);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateIntent(intent, intent.fragments(), wrongExecution))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateIntent(intent, List.of(new byte[] {1}), context))
                .isInstanceOf(IllegalArgumentException.class);
        var wrongPrincipal = new DistributedWriteCommitContext(context.executionId(), context.tenantId(), context.baseSnapshotId(),
                context.writeOutputBinding(), context.commitFingerprint(), "another-principal", context.authorizationReceipt(), null);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateIntent(intent, intent.fragments(), wrongPrincipal))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void authorityCannotChangeIdentityAtTheSameTokenOrExceedOwnerLease() throws Exception {
        var execution = LanceManagedSourceTestExecution.create(directory);
        var intent = execution.intent(); var valid = execution.originalAnchor();
        var security = execution.request().getSecurity(); var deadline = deadline(valid);
        for (var invalid : List.of(
                new ExecutionAnchor("another-query", valid.ownerId(), valid.ownerIncarnation(), valid.fencingToken(),
                        valid.acquiredAt(), valid.expiresAt()),
                new ExecutionAnchor(valid.executionId(), "another-owner", "another-process", valid.fencingToken(),
                        valid.acquiredAt(), valid.expiresAt()),
                new ExecutionAnchor(valid.executionId(), valid.ownerId(), valid.ownerIncarnation(), valid.fencingToken(),
                        valid.acquiredAt(), Instant.ofEpochMilli(deadline - 1)))) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.prepareRequest(security, intent, invalid, deadline, 1,
                    execution.reservation())).isInstanceOf(SecurityException.class);
        }
        assertThat(WorkerNativeDatasetControl.prepareRequest(security, intent, valid, deadline, 1, execution.reservation())
                .getExecutionOwner().getFencingToken()).isEqualTo(valid.fencingToken());
        var next = execution.handoff();
        var obsolete = new ExecutionAnchor(next.executionId(), next.ownerId(), next.ownerIncarnation(),
                valid.fencingToken(), next.acquiredAt(), next.expiresAt());
        assertThatThrownBy(() -> WorkerNativeDatasetControl.prepareRequest(security, intent, obsolete, deadline(next), 1,
                execution.reservation())).isInstanceOf(SecurityException.class);
        // Negative encoding input only: the actual former anchor must not authorize a higher token's intent.
        var higherIntent = new WriteCommitEntry(intent.queryId(), intent.catalogName(), intent.tableName(), intent.state(),
                intent.timestampMs(), intent.fragments(), intent.writeMode(), intent.idempotencyKey(), intent.tenantId(),
                intent.principal(), intent.authorizationReceipt(), intent.baseSnapshotId(), intent.writeOutputBinding(),
                intent.commitFingerprint(), next.ownerId(), next.ownerIncarnation(), next.fencingToken(), null, null);
        assertThatThrownBy(() -> WorkerNativeDatasetControl.prepareRequest(security, higherIntent, valid, deadline, 1,
                execution.reservation())).isInstanceOf(SecurityException.class);
    }

    @Test
    void outputTransactionAndAllocationMustMatchDurableIntent() throws Exception {
        var execution = LanceManagedSourceTestExecution.append(directory);
        var intent = execution.intent(); var output = execution.output();
        WorkerNativeDatasetControl.validateOutput(intent, output, execution.catalog().getCatalogName());
        var canonical = WriteOutputBinding.parseFrom(intent.writeOutputBinding()).getLance();
        for (var mismatch : List.of(
                canonical.toBuilder().setDatasetUri("file:///other").build(),
                canonical.toBuilder().setOperation(LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE).build(),
                canonical.toBuilder().setExpectedReadVersion(2).build(),
                canonical.toBuilder().setWriteId("other-write").build(),
                canonical.toBuilder().setSchemaFenceSha256(ProtocolChecksums.sha256(new byte[] {1})).build(),
                canonical.toBuilder().setAllocationStagingPrefix("different/prefix").build(),
                canonical.toBuilder().setGeneration("other-generation").build(),
                canonical.toBuilder().clearWriteColumnNames().addWriteColumnNames("other-column").build(),
                canonical.toBuilder().clearBaseSnapshot().build(),
                canonical.toBuilder().setBaseSnapshot(canonical.getBaseSnapshot().toBuilder().setMembershipId("0".repeat(64))).build())) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(changedBinding(intent, mismatch), output,
                    execution.catalog().getCatalogName())).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void outputIdentityCannotBeIgnoredEvenWhenAllocationMatches() throws Exception {
        var execution = LanceManagedSourceTestExecution.append(directory);
        var valid = execution.output(); var table = valid.targetTable().orElseThrow();
        var allocation = valid.nativeWriteAllocation().orElseThrow(); var base = valid.writeBaseSnapshotId().orElseThrow();
        var accepted = valid.acceptedWriteBaseSnapshot();
        for (var invalid : List.of(
                new Output(new SchemaTableName(table.schema(), "other"), valid.writeMode(), base, allocation, accepted),
                new Output(table, "OVERWRITE", base, allocation, accepted),
                new Output(table, valid.writeMode(), base + 1, allocation, accepted),
                new Output(table, valid.writeMode(), base, allocation, Optional.empty()))) {
            assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(execution.intent(), invalid,
                    execution.catalog().getCatalogName())).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> WorkerNativeDatasetControl.validateOutput(execution.intent(), valid, "another-catalog"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static long deadline(ExecutionAnchor anchor) {
        return Math.min(Instant.now().plusSeconds(15).toEpochMilli(), anchor.expiresAt().toEpochMilli());
    }
    private static RuntimeCommandResponse response(PrepareDistributedWriteResult result) {
        return RuntimeCommandResponse.newBuilder().setSuccess(true).setPayload(Any.pack(result)).build();
    }

    /** Mutation is solely a negative codec input, never an inserted PG authority or accepted reference. */
    private static WriteCommitEntry changedBinding(WriteCommitEntry original, LanceWriteBinding binding) {
        var bytes = WriteOutputBinding.newBuilder().setLance(binding).build().toByteArray();
        var fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(original.queryId(), original.tenantId(),
                original.catalogName(), original.tableName(), original.writeMode(), original.baseSnapshotId(), bytes,
                original.principal(), original.authorizationReceipt(), null), original.fragments());
        return new WriteCommitEntry(original.queryId(), original.catalogName(), original.tableName(), original.state(),
                original.timestampMs(), original.fragments(), original.writeMode(), fingerprint, original.tenantId(),
                original.principal(), original.authorizationReceipt(), original.baseSnapshotId(), bytes, fingerprint,
                original.ownerId(), original.ownerIncarnation(), original.executionFencingToken(), null, null);
    }

    private record Output(SchemaTableName table, String mode, long base, NativeWriteAllocation allocation,
                          Optional<AcceptedSourceSnapshot> accepted) implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() { return Optional.of(table); }
        @Override public String writeMode() { return mode; }
        @Override public OptionalLong writeBaseSnapshotId() { return OptionalLong.of(base); }
        @Override public Optional<NativeWriteAllocation> nativeWriteAllocation() { return Optional.of(allocation); }
        @Override public Optional<AcceptedSourceSnapshot> acceptedWriteBaseSnapshot() { return accepted; }
    }
}
