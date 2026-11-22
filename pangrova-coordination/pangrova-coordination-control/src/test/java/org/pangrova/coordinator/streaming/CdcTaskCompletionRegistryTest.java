package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskReport;

import java.util.Optional;
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
        tracker.registerTaskRequest(taskId, expected);
        tracker.bindDispatchTarget(expected, org.pangrova.coordinator.worker.TestWorkerInfoBuilder
                .active("worker-1", "worker-host", 8080, 9090));
        tracker.enterTaskDispatch(expected);
        tracker.markScheduled(queryId, taskId);
        var registry = new CdcTaskCompletionRegistry(tracker);
        var report = report(expected);
        var running = new TaskStatusReport(taskId, queryId, report.stageId(), report.partitionId(),
                report.taskType(), report.workerId(), "RUNNING", null, null, null, 1, 1, null, null,
                report.attempt(), null);

        assertThat(registry.accept(expected, running, Optional.empty())).isTrue();
        assertThat(registry.accept(expected, report, Optional.empty())).isFalse();
        assertThat(registry.await(taskId)).isNotDone();
        assertThat(registry.accept(expected, report, canonical(expected))).isTrue();

        assertThat(registry.await(taskId).get(1, TimeUnit.SECONDS)).isSameAs(report);
        assertThat(tracker.getTaskState(queryId, taskId).name()).isEqualTo("FINISHED");
        assertThat(registry.remove(taskId)).contains(report);
        assertThat(tracker.isRetiredDrainedAttempt(queryId, taskId, report.attempt())).isTrue();
        assertThat(tracker.acknowledgesExecutionDrain(report)).isTrue();
    }

    private static SubmitTaskRequest request() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task", "query", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST, new byte[]{1});
    }

    private static TaskReport canonical(SubmitTaskRequest expected) {
        return TaskReport.newBuilder().setAttempt(expected.taskRequest().getAttempt())
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED)
                .setExecutionDrainState(org.pangrova.protocol.v2.ExecutionDrainState
                        .EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED).build();
    }

    private static TaskStatusReport report(SubmitTaskRequest expected) {
        var attempt = expected.taskRequest().getAttempt();
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(),
                attempt.getStageId(), attempt.getPartitionId(), "CDC_INGEST", "worker-1",
                "FINISHED", null, null, null, 2, 1, null, null,
                TaskAttemptDescriptor.from(attempt), canonical(expected).toByteArray());
    }
}
