package org.pangrova.coordinator.query.task;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.error.QueryFailure;
import static org.assertj.core.api.Assertions.assertThat;

class TaskReportQueryAdapterTest {
    @Test
    void retainsDiagnosticsWithoutChangingFrozenReportIdentityOrPayload() {
        var wire = new ErrorDetail("EXCHANGE-0001", "WorkerLost", "节点失败", "stack\nat worker");
        var report = report(wire);
        var result = TaskReportQueryAdapter.failure(report);
        assertThat(result).isEqualTo(new QueryFailure(wire.errorCode(), wire.errorType(), wire.message(), wire.stackTrace()));
        assertThat(report.error()).isSameAs(wire);
        assertThat(report.reportSequence()).isEqualTo(17);
        assertThat(report.timestampMs()).isEqualTo(123);
        assertThat(report.canonicalTaskReport()).containsExactly(1, 2, 3);
        assertThat(TaskReportQueryAdapter.failure(report)).isEqualTo(result);
    }

    @Test
    void distinguishesAbsentFailureFromIncompleteDiagnostics() {
        assertThat(TaskReportQueryAdapter.failure(report(null))).isNull();
        assertThat(TaskReportQueryAdapter.failure(report(new ErrorDetail(null, null, null, null))))
                .isEqualTo(new QueryFailure(null, null, null, null));
    }

    private static TaskStatusReport report(ErrorDetail error) {
        return new TaskStatusReport("task", "q", 0, 0, "COMPUTE", "worker", "FAILED",
                null, error, null, 17, 123, null, Map.of(), TaskAttemptDescriptor.of(1, 0), new byte[]{1, 2, 3});
    }
}
