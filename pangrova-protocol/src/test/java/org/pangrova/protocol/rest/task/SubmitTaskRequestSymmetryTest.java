package org.pangrova.protocol.rest.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.http.TaskSubmission;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.Checksum;
import com.google.protobuf.ByteString;

/** Verifies that the REST layer transports one exact canonical task contract. */
class SubmitTaskRequestSymmetryTest {

    @Test
    void outboundRequestRetainsTheAlreadyBuiltCanonicalObject() {
        var canonical = fullRequest().taskRequest();
        var request = SubmitTaskRequest.from(canonical);

        assertThat(request.taskRequest()).isSameAs(canonical);
        assertThat(request.withBoundTask(canonical)).isSameAs(request);
        assertThat(request.canonicalBytes()).isEqualTo(canonical.toByteArray());
    }

    @Test
    void replacingTheCanonicalTaskDoesNotCarryItsAdmissionValidation() {
        var canonical = CanonicalTaskRequestTestFixture.builder("task", "query",
                TaskKind.TASK_KIND_READ, new byte[]{1}).build();
        var request = SubmitTaskRequest.from(canonical);
        var validated = new ValidatedTaskSubmission(request);
        assertThat(validated.submission()).isSameAs(request);

        var changed = request.withBoundTask(canonical.toBuilder().clearSubstraitPlanChecksum().build());
        assertThatThrownBy(() -> new ValidatedTaskSubmission(changed)).isInstanceOf(IllegalArgumentException.class);
        assertThat(validated.submission().taskRequest()).isSameAs(canonical);
    }

    @Test
    void malformedProtobufIsRejectedAtHttpDecoding() {
        assertThatThrownBy(() -> SubmitTaskRequest.decode(new byte[]{(byte) 0x80}))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
        assertThatThrownBy(() -> SubmitTaskRequest.decode(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("payload");
    }

    @Test
    void oversizedPlanEnvelopeIsRejectedBeforeSerializingItsRepeatedSplits() {
        var builder = fullRequest().taskRequest().toBuilder();
        var split = org.pangrova.protocol.v2.DataSplit.newBuilder()
                .setSourceId("s".repeat(1024 * 1024)).build();
        for (int index = 0; index < 129; index++) builder.addSplits(split);
        assertThatThrownBy(() -> SubmitTaskRequest.from(builder.build()).encode())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("HTTP limit");
    }

    @Test
    void binaryRoundTripCarriesOneTypedCanonicalTask() throws Exception {
        SubmitTaskRequest request = fullRequest();

        byte[] body = request.encode();
        assertThat(TaskSubmission.parseFrom(body).getTask()).isEqualTo(request.taskRequest());
        SubmitTaskRequest decoded = SubmitTaskRequest.decode(body);

        assertThat(decoded.canonicalBytes()).isEqualTo(request.canonicalBytes());
        assertThat(decoded.taskRequest()).isEqualTo(request.taskRequest());
        assertThat(decoded).isEqualTo(request);
        assertThat(SubmitTaskRequest.decode(new ByteArrayInputStream(body))).isEqualTo(decoded);
    }

    @Test
    void canonicalSubmissionCarriesThePlanSplitsPropertiesAndResourceGrant() throws Exception {
        var original = fullRequest().taskRequest();
        var split = DataSplit.newBuilder().setSplitId("split-1").build();
        var grant = StageResourceGrant.newBuilder().setGrantId("task-42:grant")
                .setNativeMemoryBytes(4096).setCpuSlots(8).setSpillBytes(8192).build();
        var request = SubmitTaskRequest.from(original.toBuilder().addSplits(split).setResourceGrant(grant)
                .setExecutionOptions(original.getExecutionOptions().toBuilder()
                        .putAllSessionProperties(Map.of("timezone", "UTC"))).build());

        var admitted = SubmitTaskRequest.decode(new ByteArrayInputStream(request.encode()));
        assertThat(admitted.taskType()).isEqualTo("OUTPUT");
        assertThat(admitted.taskRequest().getAttempt()).isEqualTo(original.getAttempt());
        assertThat(admitted.taskRequest().getSubstraitPlan()).isEqualTo(original.getSubstraitPlan());
        assertThat(admitted.taskRequest().getSplitsList()).containsExactly(split);
        assertThat(admitted.taskRequest().getExecutionOptions().getSessionPropertiesMap())
                .containsExactlyEntriesOf(Map.of("timezone", "UTC"));
        assertThat(admitted.taskRequest().getResourceGrant()).isEqualTo(grant);
    }

    @ParameterizedTest
    @CsvSource({"TASK_KIND_READ,COMPUTE", "TASK_KIND_SHUFFLE_READ,COMPUTE", "TASK_KIND_SHUFFLE_WRITE,COMPUTE",
            "TASK_KIND_WRITE,WRITE", "TASK_KIND_BACKGROUND_BUILD,BACKGROUND_BUILD",
            "TASK_KIND_BACKGROUND_VALIDATE,BACKGROUND_VALIDATE", "TASK_KIND_BACKGROUND_EXPORT,BACKGROUND_EXPORT",
            "TASK_KIND_CDC_INGEST,CDC_INGEST"})
    void canonicalTaskKindDeterminesWorkerType(TaskKind kind, String expected) throws Exception {
        byte[] plan = kind == TaskKind.TASK_KIND_CDC_INGEST ? new byte[0] : new byte[]{1};
        var request = SubmitTaskRequest.from(CanonicalTaskRequestTestFixture.builder("task", "query", kind, plan).build());
        var admitted = SubmitTaskRequest.decode(request.encode());
        assertThat(admitted.taskType()).isEqualTo(expected);
        assertThat(admitted.taskRequest().getSubstraitPlan().toByteArray()).isEqualTo(plan);
        assertThat(admitted.taskRequest().getSplitsList()).isEmpty();
        assertThat(admitted.taskRequest().getExecutionOptions().getSessionPropertiesMap()).isEmpty();
    }

    @Test
    void missingGrantIsRejectedByCanonicalAdmission() {
        var canonical = CanonicalTaskRequestTestFixture.builder("task", "query",
                TaskKind.TASK_KIND_READ, new byte[]{1}).clearResourceGrant().build();
        var submitted = SubmitTaskRequest.from(canonical);
        assertThatThrownBy(() -> new ValidatedTaskSubmission(submitted))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("resource grant is incomplete");
    }

    @Test
    void copyTicketRoundTripKeepsNonceOutsideNativeBytes() throws Exception {
        var scope = copyScope();
        var privateTicket = new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope);
        var canonical = copyRequest(scope);
        var submitted = SubmitTaskRequest.from(canonical, privateTicket);

        var decoded = SubmitTaskRequest.decode(submitted.encode());

        assertThat(decoded).isEqualTo(submitted);
        assertThat(decoded.copySourceRead()).isEqualTo(privateTicket);
        assertThat(decoded.canonicalBytes()).isEqualTo(canonical.toByteArray());
        assertThat(new String(decoded.canonicalBytes(), StandardCharsets.ISO_8859_1))
                .doesNotContain(privateTicket.nonce());
        assertThat(decoded.toString()).doesNotContain(privateTicket.nonce());
        assertThat(submitted.withBoundTask(canonical)).isEqualTo(submitted);
        assertThatThrownBy(() -> submitted.withBoundTask(canonical.toBuilder()
                .setDeadlineEpochMs(canonical.getDeadlineEpochMs() + 1).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void privateCopyRedeliveryRetainsTheActualReceivedBytes() throws Exception {
        var scope = copyScope();
        var canonical = copyRequest(scope);
        var submitted = SubmitTaskRequest.from(canonical,
                new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope));
        // An earlier value for a singular field is legal on the wire. The last
        // value determines semantics, while the lease still owns the original bytes.
        var bytes = ByteString.newOutput();
        var output = CodedOutputStream.newInstance(bytes);
        output.writeInt64(TaskRequest.DEADLINE_EPOCH_MS_FIELD_NUMBER, 1);
        canonical.writeTo(output);
        output.flush();
        var envelope = TaskSubmission.parseFrom(submitted.encode()).toBuilder();
        envelope.getCopyBuilder().setCanonicalTask(bytes.toByteString());
        var body = envelope.build().toByteArray();
        var decoded = SubmitTaskRequest.decode(new ByteArrayInputStream(body));
        assertThat(decoded).isEqualTo(SubmitTaskRequest.decode(body));
        assertThat(decoded.taskRequest()).isEqualTo(canonical);
        assertThat(decoded.canonicalBytes()).isEqualTo(bytes.toByteString().toByteArray());
        assertThat(decoded.encode()).isEqualTo(body);
        assertThat(decoded).isNotEqualTo(submitted);
    }

    @Test
    void copyTicketAndCanonicalDigestMustBePairedBeforeDispatch() {
        var scope = copyScope();
        var privateTicket = new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope);

        assertThatThrownBy(() -> SubmitTaskRequest.from(copyRequest(scope)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubmitTaskRequest.from(fullRequest().taskRequest(), privateTicket))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubmitTaskRequest.from(copyRequest(scope),
                new CopySourceReadHttp.RedeemRequest("m".repeat(43),
                        new CopySourceReadHttp.Scope(scope.tenantId(), scope.principal(),
                                scope.profileName(), scope.profileIncarnationId(),
                                scope.profileRevision(), scope.canonicalSourceUrl(),
                                scope.managedStorageRootUri(), scope.managedBackendIdentitySha256(),
                                scope.executionId(),
                                "different-task", scope.ownerId(), scope.ownerIncarnation(),
                                scope.controlEpoch(), scope.ownerFencingToken(),
                                scope.deadlineEpochMs()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reportIdentityIsProjectedFromCanonicalAttempt() {
        SubmitTaskRequest request = fullRequest();
        TaskStatusReport report = reportFrom(request.taskRequest());
        var attempt = request.taskRequest().getAttempt();

        assertThat(report.taskId()).isEqualTo(attempt.getTaskId());
        assertThat(report.queryId()).isEqualTo(attempt.getQueryOrJobId());
        assertThat(report.stageId()).isEqualTo(attempt.getStageId());
        assertThat(report.partitionId()).isEqualTo(attempt.getPartitionId());
        assertThat(report.taskType()).isEqualTo("OUTPUT");
        assertThat(report.attempt()).isEqualTo(TaskAttemptDescriptor.from(attempt));
    }

    @Test
    void taskStatusWirePreservesTypedSupplementalStatistics() throws Exception {
        var stats = org.pangrova.protocol.v2.NativeTaskStats.newBuilder()
                .setRowsRead(100).setBytesRead(2048).setRowsWritten(50).setBytesWritten(1024)
                .setElapsedTimeMs(500).setCpuTimeMs(120).setMemoryBytes(4096).setPeakMemoryBytes(8192)
                .setSpillBytes(256).setCacheHitBytes(512).setCacheMissBytes(64).setBackpressureWaitMs(12)
                .setCacheHitCount(7).setCacheMissCount(8).setCacheEvictionCount(9)
                .setCacheAdmissionRejectionCount(10).setConsumerBlockedDrivers(2)
                .setIndexSplitsEvaluated(13).setIndexSplitsSkipped(11).build();
        var progress = TaskStatusReport.fromAttempt(fullRequest().taskRequest().getAttempt(),
                "OUTPUT", "worker-1", "RUNNING", stats, null, null, 1L, 1000L, null);
        var decoded = TaskStatusWire.decode(TaskStatusWire.encode(progress));
        assertThat(decoded.supplementalStats()).isEqualTo(stats);
    }

    private static SubmitTaskRequest fullRequest() {
        TaskRequest request = CanonicalTaskRequestTestFixture.builder(
                        "task-42", "query-7", TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .setAttempt(CanonicalTaskRequestTestFixture.builder(
                                "task-42", "query-7", TaskKind.TASK_KIND_READ,
                                new byte[]{1}).getAttempt().toBuilder()
                        .setStageId(3).setPartitionId(5))
                .setExecutionOptions(TaskExecutionOptions.newBuilder()
                        .setDirectResultOutput(true))
                .build();
        return SubmitTaskRequest.from(request);
    }

    private static CopySourceReadHttp.Scope copyScope() {
        return new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", "task-42",
                "coordinator", "test", 1, 1, 4_102_444_800_000L);
    }

    private static TaskRequest copyRequest(CopySourceReadHttp.Scope scope) {
        return CanonicalTaskRequestTestFixture.builder("task-42", "query-7",
                        TaskKind.TASK_KIND_WRITE, "plan".getBytes(StandardCharsets.UTF_8))
                .setCopySourceStagingCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .build();
    }

    private static TaskStatusReport reportFrom(TaskRequest request) {
        var attempt = request.getAttempt();
        var stats = org.pangrova.protocol.v2.NativeTaskStats.newBuilder()
                .setRowsRead(10).setBytesRead(100).setRowsWritten(5).setBytesWritten(50)
                .setElapsedTimeMs(200).setCpuTimeMs(50).setMemoryBytes(1024).setPeakMemoryBytes(2048).build();
        return new TaskStatusReport(
                attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(), "OUTPUT",
                "worker-1", "FINISHED", stats, null,
                new WriteResultDescriptor(List.of(), 5, 50, new byte[0], java.util.List.of()),
                1L, System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.from(attempt));
    }
}
