package org.pangrova.coordinator.query.task;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.query.api.error.QueryFailure;
import static org.assertj.core.api.Assertions.assertThat;

class TaskReportCompletionTest {
    private static final TaskAttemptDescriptor ATTEMPT = TaskAttemptDescriptor.of(1, 0);
    private static final TaskReport TERMINAL_REPORT = TaskReport.newBuilder()
            .setAttempt(TaskAttemptId.newBuilder().setTaskId("task").setQueryOrJobId("q")
                    .setPlanEpoch(ATTEMPT.planEpoch()).setAttemptId(ATTEMPT.attemptId())
                    .setFencingToken(ATTEMPT.fencingToken()))
            .setState(TaskState.TASK_STATE_FAILED)
            .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
            .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
            .build();

    @Test
    void retainsDiagnosticsWithoutChangingFrozenReportIdentityOrPayload() {
        var wire = new ErrorDetail("EXCHANGE-0001", "WorkerLost", "节点失败", "stack\nat worker");
        var report = report(wire);
        var result = TaskReportCompletion.failure(report);
        assertThat(result).isEqualTo(new QueryFailure(wire.errorCode(), wire.errorType(), wire.message(), wire.stackTrace()));
        assertThat(report.error()).isSameAs(wire);
        assertThat(report.reportSequence()).isEqualTo(17);
        assertThat(report.timestampMs()).isEqualTo(123);
        assertThat(report.canonicalTaskReport()).containsExactly(TERMINAL_REPORT.toByteArray());
        assertThat(TaskReportCompletion.failure(report)).isEqualTo(result);
    }

    @Test
    void distinguishesAbsentFailureFromIncompleteDiagnostics() {
        assertThat(TaskReportCompletion.failure(report(null))).isNull();
        assertThat(TaskReportCompletion.failure(report(new ErrorDetail(null, null, null, null))))
                .isEqualTo(new QueryFailure(null, null, null, null));
    }

    private static TaskStatusReport report(ErrorDetail error) {
        return new TaskStatusReport("task", "q", 0, 0, "COMPUTE", "worker", "FAILED",
                null, error, null, 17, 123, null, Map.of(), ATTEMPT, TERMINAL_REPORT.toByteArray());
    }
}
