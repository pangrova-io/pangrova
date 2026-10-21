package org.pangrova.protocol;

import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Canonical control encoding/validation only, not proof that an execution owner exited. */
class CanonicalTaskAdmissionSealsTest {
    @Test
    void networkDigestEqualsActualDeterministicEncodingAndChangesWithExactAuthority() throws Exception {
        var request = request();
        var bytes = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(bytes);
        coded.useDeterministicSerialization();
        request.writeTo(coded);
        coded.flush();
        var seal = CanonicalTaskAdmissionSeals.create(request);
        assertThat(seal.getCanonicalRequestChecksum().getDigest().toByteArray())
                .isEqualTo(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        var changed = request.toBuilder().setSecurity(request.getSecurity().toBuilder().setPrincipal("another-user")).build();
        assertThat(CanonicalTaskAdmissionSeals.create(changed).getCanonicalRequestChecksum()).isNotEqualTo(seal.getCanonicalRequestChecksum());
        CanonicalTaskAdmissionSeals.validate(seal, request.getAttempt().getTaskId());
    }

    @Test
    void sealRejectsPathDigestUnknownFieldsAndIncompleteAttempt() {
        var seal = CanonicalTaskAdmissionSeals.create(request());
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.validate(seal, "other-task")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.validate(seal.toBuilder().clearCanonicalRequestChecksum().build(), "task"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.validate(seal.toBuilder().setUnknownFields(unknown()).build(), "task"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.validate(seal.toBuilder().setAttempt(seal.getAttempt().toBuilder().clearExecutionOwner()).build(), "task"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void networkAdmissionAndSealsRejectWorkerPrivateCopyBindings() {
        var privateRequest = request().toBuilder().setWorkerLocalCopyInput(
                org.pangrova.protocol.v2.WorkerLocalCopyInput.newBuilder()
                        .setLocalFilePath("/tmp/copy-scan-private/source.parquet")).build();
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.create(privateRequest))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> org.pangrova.protocol.task.CanonicalTaskRequests
                .validateForAdmission(privateRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void copySourceLeafRetainsItsReadTaskKindAndExactCommandChecksum() {
        var copy = request().toBuilder().setCopySourceStagingCommandSha256(
                org.pangrova.protocol.task.CanonicalTaskRequests.sha256(new byte[] {9})).build();
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(copy);
        assertThat(copy.getTaskKind()).isEqualTo(TaskKind.TASK_KIND_READ);
        for (var kind : TaskKind.values()) {
            if (kind == TaskKind.TASK_KIND_READ || kind == TaskKind.TASK_KIND_WRITE
                    || kind == TaskKind.UNRECOGNIZED) continue;
            var unsupported = copy.toBuilder().setTaskKind(kind).build();
            assertThatThrownBy(() -> org.pangrova.protocol.task.CanonicalTaskRequests
                    .validateForAdmission(unsupported)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void compactActualNativeReceiptPreservesExactIdentityAndStripsAllResultPayload() {
        var actual = report().setNativeTaskStats(NativeTaskStats.newBuilder().setRowsWritten(13))
                .setUsage(ResourceUsage.newBuilder().setPeakNativeMemoryBytes(101))
                .setWinningAttemptId("actual-winner").build();
        var compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(actual);
        assertThat(compact.getAttempt()).isEqualTo(actual.getAttempt());
        assertThat(compact.getExecutionDrainState()).isEqualTo(actual.getExecutionDrainState());
        assertThat(compact.getState()).isEqualTo(actual.getState());
        assertThat(compact.hasNativeTaskStats()).isFalse();
        assertThat(compact.hasUsage()).isFalse();
        assertThat(compact.getWinningAttemptId()).isEmpty();
        assertThat(compact.hasError()).isFalse();
    }

    @Test
    void reconciledFailurePreservesActualNativeDrainAndBoundedUnicodeDiagnostic() {
        var error = ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL)
                .setCode("SDK_DRAIN_FAILED").setFailedComponent("native-writer").setRetryable(true)
                .addFailedPartitionIds("large-publication-partition-payload")
                .setMessage("\uD83D\uDE00".repeat(2048)).build();
        var actual = report().setState(TaskState.TASK_STATE_CANCELLED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED).setError(error).build();
        var compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(actual);
        assertThat(compact.getError().getMessage().codePointCount(0, compact.getError().getMessage().length())).isEqualTo(1024);
        assertThat(compact.getError().getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSize(4096);
        assertThat(compact.getError().getCode()).isEqualTo(error.getCode());
        assertThat(compact.getError().getRetryable()).isTrue();
        assertThat(compact.getError().getFailedPartitionIdsList()).isEmpty();
        assertThat(compact.getExecutionDrainState()).isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
    }

    @Test
    void noOwnerBuilderAndProjectionNeverInferEvidenceFromMissingStatistics() {
        var closed = CanonicalTaskAdmissionSeals.closedWithoutOwner(CanonicalTaskAdmissionSeals.create(request()));
        assertThat(CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(closed)).isEqualTo(closed);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report().clearExecutionDrainState().build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(closed.toBuilder()
                .setNativeTaskStats(NativeTaskStats.newBuilder()).build())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report()
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void projectionRejectsUnknownAndContradictoryTerminalErrorAuthority() {
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report().setState(TaskState.TASK_STATE_FAILED).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report()
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED).build())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report().setUnknownFields(unknown()).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static org.pangrova.protocol.v2.TaskRequest request() {
        return CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ, new byte[]{1}).build();
    }
    private static TaskReport.Builder report() {
        return TaskReport.newBuilder().setAttempt(request().getAttempt()).setState(TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
    }
    private static UnknownFieldSet unknown() {
        return UnknownFieldSet.newBuilder().addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }
}
