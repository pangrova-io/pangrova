package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.scheduler.SpeculationAdapters;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.scheduler.speculation.DuplicateTaskResolver;
import org.pangrova.scheduler.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import static org.assertj.core.api.Assertions.assertThat;

/** Control-plane election inputs only: no Native execution or physical drain is simulated. */
public final class SpeculationReceiverWiringFixture {
    private SpeculationReceiverWiringFixture() {}

    public static void assertCompletionBehavior() {
        try (var control = new QueryControlPlaneFixture()) {
            var launcher = new SpeculativeTaskLauncher();
            var resolver = new DuplicateTaskResolver();
            var dispatcher = new ControlDispatch();
            var cancellations = new TaskCancelRetrier(dispatcher, control.tracker());
            control.workers().registerWorker(TestWorkerInfoBuilder.active("original-worker", "first", 8001, 9001));
            control.workers().registerWorker(TestWorkerInfoBuilder.active("copy-worker", "second", 8002, 9002));
            launcher.setDispatchDependencies(
                    SpeculationAdapters.dispatcher(dispatcher),
                    SpeculationAdapters.workerInventory(control.workers()),
                    SpeculationAdapters.taskRegistry(control.tracker()));
            launcher.setCancelRetrier(SpeculationAdapters.canceller(cancellations));
            var logicalSlot = new java.util.concurrent.atomic.AtomicReference<String>();
            var receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(
                    control.queries(), control.tracker(), control.workers(), launcher, resolver,
                    dispatcher, null, null, null, new org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback() {
                        @Override
                        public void onTaskTerminal(String query, String task, String state,
                                org.pangrova.client.error.ErrorDetail error) {
                            logicalSlot.set(task);
                        }

                        @Override
                        public void onQueryTerminal(String query, java.util.function.Consumer<Boolean> listener) {
                            throw new UnsupportedOperationException("This task-receiver fixture does not own a query DAG");
                        }
                    }, null, null));
            var original = CanonicalTaskRequestTestFixture.request("original", "query", 1, 0,
                    TaskKind.TASK_KIND_READ, new byte[]{1});
            control.tracker().registerTask("query", "original");
            control.tracker().registerTaskRequest("original", original);
            control.tracker().assignWorker("original", "original-worker");
            control.tracker().markScheduled("query", "original");
            control.tracker().updateTaskState("query", "original", "RUNNING");
            String copyId = launcher.launchSpeculative("original", "original-worker", "query");
            var copy = control.tracker().getTaskRequest(copyId);

            assertThat(receiver.onTaskStatus(completion(copy))).isTrue();
            assertThat(resolver.getWinner("query", "1", 0)).contains(copyId);
            assertThat(logicalSlot.get()).isEqualTo("original");
            assertThat(dispatcher.cancelled).isSameAs(original);
            assertThat(cancellations.pendingTaskIds()).containsExactly("original");
            assertThat(control.tracker().getTaskRequest("original")).isSameAs(original);
            assertThat(control.tracker().getTaskRequest(copyId)).isSameAs(copy);
            assertThat(control.tracker().executionDrainReceipt("original")).isEmpty();
            assertThat(control.tracker().executionDrainReceipt(copyId)).isEmpty();
            assertThat(launcher.getSpeculativeTaskId("original")).contains(copyId);
        }
    }

    private static TaskStatusReport completion(SubmitTaskRequest request) {
        var attempt = request.taskRequest().getAttempt();
        var status = TaskReport.newBuilder().setAttempt(attempt)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).setState(TaskState.TASK_STATE_FINISHED)
                .setCoverage(CoverageReport.newBuilder().setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE)
                        .setExpectedWorkUnits(1).setCompletedWorkUnits(1).setCoverageRatio(1.0)).build();
        return new TaskStatusReport(attempt.getTaskId(), "query", 1, 0, "COMPUTE", "copy-worker", "FINISHED",
                null, null, null, 1L, 1000L, null, null, TaskAttemptDescriptor.from(attempt), status.toByteArray());
    }

    private static final class ControlDispatch implements TaskDispatcher {
        private SubmitTaskRequest cancelled;
        @Override public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            return new DispatchResult(worker.host(), request.taskRequest().getAttempt().getTaskId(), null, worker.workerId());
        }
        @Override public SchedulerCancelResult cancelTask(SubmitTaskRequest request, String host, int port) {
            cancelled = request;
            return SchedulerCancelResult.failed("Component transport provides no physical drain evidence", false);
        }
    }
}
