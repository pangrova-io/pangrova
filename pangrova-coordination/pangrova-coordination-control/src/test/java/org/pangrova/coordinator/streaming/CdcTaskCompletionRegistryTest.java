package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskReport;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CdcTaskCompletionRegistryTest {

    @Test
    void terminalReportWakesTheExactWaiterAndUpdatesTaskState() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var expected = request();
        String queryId = expected.taskRequest().getAttempt().getQueryOrJobId();
        String taskId = expected.taskRequest().getAttempt().getTaskId();
        tracker.registerTask(queryId, taskId);
        tracker.markScheduled(queryId, taskId);
        var registry = new CdcTaskCompletionRegistry(tracker);
        var report = report(expected);

        assertThat(registry.accept(expected, report, canonical(expected))).isTrue();

        assertThat(registry.await(taskId).get(1, TimeUnit.SECONDS)).isSameAs(report);
        assertThat(tracker.getTaskState(queryId, taskId).name()).isEqualTo("FINISHED");
        assertThat(registry.remove(taskId)).contains(report);
    }

    private static SubmitTaskRequest request() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task", "query", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST, new byte[]{1});
    }

    private static TaskReport canonical(SubmitTaskRequest expected) {
        return TaskReport.newBuilder().setAttempt(expected.taskRequest().getAttempt()).build();
    }

    private static TaskStatusReport report(SubmitTaskRequest expected) {
        var attempt = expected.taskRequest().getAttempt();
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(), "CDC_INGEST", "worker-1",
                "FINISHED", null, null, null, 1, 1, null, null,
                TaskAttemptDescriptor.from(attempt), canonical(expected).toByteArray());
    }
}
