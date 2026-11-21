package org.pangrova.protocol.rest.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.Checksum;
import com.google.protobuf.ByteString;

/** Verifies that the REST layer transports one exact canonical task contract. */
class SubmitTaskRequestSymmetryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void jsonRoundTripPreservesExactCanonicalBytes() throws Exception {
        SubmitTaskRequest request = fullRequest();

        SubmitTaskRequest decoded = MAPPER.readValue(
                MAPPER.writeValueAsBytes(request), SubmitTaskRequest.class);

        assertThat(decoded.canonicalBytes()).isEqualTo(request.canonicalBytes());
        assertThat(decoded.taskRequest()).isEqualTo(request.taskRequest());
        assertThat(decoded).isEqualTo(request);
    }

    @Test
    void copyTicketRoundTripKeepsNonceOutsideNativeBytes() throws Exception {
        var scope = copyScope();
        var privateTicket = new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope);
        var canonical = copyRequest(scope);
        var submitted = SubmitTaskRequest.from(canonical, privateTicket);

        var decoded = MAPPER.readValue(MAPPER.writeValueAsBytes(submitted), SubmitTaskRequest.class);

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
    void taskStatsRestDtoCoversNativeReportFields() {
        TaskStats stats = new TaskStats(
                100, 2048, 50, 1024, 500, 120, 4096, 8192, 256, 512, 64,
                32, 64, List.of(), 12, 7, 8, 9, 10);

        assertThat(stats.rowsRead()).isEqualTo(100);
        assertThat(stats.bytesRead()).isEqualTo(2048);
        assertThat(stats.rowsWritten()).isEqualTo(50);
        assertThat(stats.bytesWritten()).isEqualTo(1024);
        assertThat(stats.elapsedTimeMs()).isEqualTo(500);
        assertThat(stats.cpuTimeMs()).isEqualTo(120);
        assertThat(stats.memoryBytes()).isEqualTo(4096);
        assertThat(stats.peakMemoryBytes()).isEqualTo(8192);
        assertThat(stats.spillBytes()).isEqualTo(256);
        assertThat(stats.cacheHitBytes()).isEqualTo(512);
        assertThat(stats.cacheMissBytes()).isEqualTo(64);
        assertThat(stats.cacheHitCount()).isEqualTo(7);
        assertThat(stats.cacheMissCount()).isEqualTo(8);
        assertThat(stats.cacheEvictionCount()).isEqualTo(9);
        assertThat(stats.cacheAdmissionRejectionCount()).isEqualTo(10);
        assertThat(TaskStats.class.getRecordComponents()).hasSize(19);
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
        TaskStats stats = new TaskStats(
                10, 100, 5, 50, 200, 50, 1024, 2048, 0, 0, 0,
                0, 0, List.of(), 0, 0, 0, 0, 0);
        return new TaskStatusReport(
                attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(), "OUTPUT",
                "worker-1", "FINISHED", stats, null,
                new WriteResultDescriptor(List.of(), 5, 50, new byte[0], java.util.List.of()),
                1L, System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.from(attempt));
    }
}
