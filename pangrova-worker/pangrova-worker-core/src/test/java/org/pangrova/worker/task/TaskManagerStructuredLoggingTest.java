package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

/** Real lifecycle paths retain canonical reports and actual Native registration. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerStructuredLoggingTest {
    @Test
    void taskSubmitDoesNotThrow() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "submit-log")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(tasks.terminalReport(info).taskId())
                    .isEqualTo(info.request().taskRequest().getAttempt().getTaskId());
        }
    }

    @Test
    void taskCompletesSuccessfully() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "completion-log")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(tasks.manager().getTaskState(info.taskId()))
                    .hasValue(TaskManager.TaskState.FINISHED);
        }
    }

    @Test
    void taskFailureTransitionsToFailed() {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "failure-log")) {
            var info =
                    tasks.submit(
                            runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(
                                    false, ActualNativeTaskPlans.failingCast())));
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            assertThat(tasks.manager().getTaskState(info.taskId()))
                    .hasValue(TaskManager.TaskState.FAILED);
            assertThat(tasks.terminalReport(info).error().message()).contains("not-an-integer");
            assertThat(tasks.reports().reports())
                    .noneMatch(report -> report.state().equals("FINISHED"));
        }
    }

    @Test
    void submitWithActualExecutionWiringDoesNotThrow() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "registration-log")) {
            var admitted = new java.util.concurrent.atomic.AtomicReference<TaskInfo>();
            assertThatNoException()
                    .isThrownBy(() -> admitted.set(tasks.submit(runtime.valuesRequest(true))));
            var info = admitted.get();
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(tasks.manager().getRunningTaskCount()).isEqualTo(1);
            assertThat(info.executorHandle()).isPositive();
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.executorHandle()).isZero();
        }
    }
}
