package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import static org.assertj.core.api.Assertions.assertThat;

/** Real lifecycle paths retain canonical reports; the unwired path tests registration only. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerStructuredLoggingTest {
    @Test
    void taskSubmitDoesNotThrow() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "submit-log")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            assertThat(tasks.terminalReport(info).taskId()).isEqualTo(info.request().taskRequest().getAttempt().getTaskId());
        }
    }

    @Test
    void taskCompletesSuccessfully() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "completion-log")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            assertThat(tasks.manager().getTaskState(info.taskId())).hasValue(TaskManager.TaskState.FINISHED);
        }
    }

    @Test
    void taskFailureTransitionsToFailed() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "failure-log")) {
            var info = tasks.submit(ActualNativeTaskFixture.planTaskRequest(false, ActualNativeTaskPlans.failingCast()));
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            assertThat(tasks.manager().getTaskState(info.taskId())).hasValue(TaskManager.TaskState.FAILED);
            assertThat(tasks.terminalReport(info).error().message()).contains("not-an-integer");
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("FINISHED"));
        }
    }

    @Test
    void submitWithoutExecutionWiringDoesNotThrow() {
        var request = SubmitTaskRequest.from(ActualNativeTaskFixture.valuesTaskRequest(false));
        try (var manager = TaskManager.forHttpWiringOnly()) {
            String id = manager.submitTask(TaskRequestDtoConverter.toTaskDescriptor(request));
            assertThat(manager.getTaskState(id)).hasValue(TaskManager.TaskState.INITIALIZING);
            assertThat(manager.getRunningTaskCount()).isZero();
        }
    }
}
