package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.write.NativeWriteBindings;
import org.pangrova.protocol.write.PhysicalFileWriteReceipts;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.protocol.v2.PhysicalFileWriteFragment;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WriteTaskOptions;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises metadata admission, not execution or physical object publication. */
class PhysicalWriteTaskReportValidatorTest {

    @Test
    void refusesCompletionWithoutTheBoundPhysicalReceipt() {
        var request = request();
        var status = TaskReport.newBuilder().setState(TaskState.TASK_STATE_FINISHED).build();
        assertThatThrownBy(() -> PhysicalWriteTaskReportValidator.validate(status, report(request, null), request))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing");
        assertThatThrownBy(() -> PhysicalWriteTaskReportValidator.validate(status,
                report(request, new byte[0]), request))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("size limit");
    }

    @Test
    void refusesFragmentFromAnotherAttemptUnderTheCorrectOuterReport() {
        var request = request();
        var status = TaskReport.newBuilder().setState(TaskState.TASK_STATE_FINISHED).build();
        var foreign = request.taskRequest().getAttempt().toBuilder().setAttemptId(9).build();
        assertThatThrownBy(() -> PhysicalWriteTaskReportValidator.validate(status,
                report(request, fragment(request, foreign)), request))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched task attempt");
    }

    @Test
    void acceptsOnlyTheExactEmptyFragmentCodecAndRejectsDifferentWriteBinding() {
        var request = request();
        var status = TaskReport.newBuilder().setState(TaskState.TASK_STATE_FINISHED).build();
        var bytes = fragment(request, request.taskRequest().getAttempt());
        assertThatCode(() -> PhysicalWriteTaskReportValidator.validate(status, report(request, bytes), request))
                .doesNotThrowAnyException();
        var unbound = SubmitTaskRequest.from(request.taskRequest().toBuilder()
                .setExecutionOptions(request.taskRequest().getExecutionOptions().toBuilder()
                        .setWrite(request.taskRequest().getExecutionOptions().getWrite().toBuilder()
                                .setOutputBinding(WriteOutputBinding.newBuilder().setPhysical(
                                        PhysicalFileWriteBinding.newBuilder().setWriteId("other"))))).build());
        assertThatThrownBy(() -> PhysicalWriteTaskReportValidator.validate(status, report(request, bytes), unbound))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("durable write/schema identity");
    }

    private static SubmitTaskRequest request() {
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId("task")
                .setPlanEpoch(1).setFencingToken(2).setControlEpoch(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("owner")
                        .setOwnerIncarnation("process").setFencingToken(4)).build();
        var template = PhysicalFileWriteBinding.newBuilder().setWriteId("write").setGeneration("generation")
                .setAllocationPrefix("objects/write")
                .setPhysicalSchemaChecksum(ProtocolChecksums.sha256(new byte[] {1})).build();
        return SubmitTaskRequest.from(TaskRequest.newBuilder().setAttempt(attempt).setTaskKind(TaskKind.TASK_KIND_WRITE)
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setWrite(WriteTaskOptions.newBuilder()
                        .setOutputBinding(WriteOutputBinding.newBuilder().setPhysical(
                                NativeWriteBindings.bindPhysical(template, attempt))))).build());
    }

    private static byte[] fragment(SubmitTaskRequest request, TaskAttemptId attempt) {
        var physical = request.taskRequest().getExecutionOptions().getWrite().getOutputBinding().getPhysical();
        return PhysicalFileWriteFragment.newBuilder().setContractVersion(PhysicalFileWriteReceipts.CONTRACT_VERSION)
                .setWriteId(physical.getWriteId()).setGeneration(physical.getGeneration())
                .setPhysicalSchemaChecksum(physical.getPhysicalSchemaChecksum()).setTaskAttempt(attempt).build()
                .toByteArray();
    }

    private static TaskStatusReport report(SubmitTaskRequest request, byte[] fragment) {
        var attempt = request.taskRequest().getAttempt();
        var write = fragment == null ? null : new WriteResultDescriptor(List.of(), 0, 0, fragment);
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(), 0, 0, "WRITE", "worker",
                "FINISHED", null, null, write, 1, 1, null, null, TaskAttemptDescriptor.from(attempt), null);
    }
}
