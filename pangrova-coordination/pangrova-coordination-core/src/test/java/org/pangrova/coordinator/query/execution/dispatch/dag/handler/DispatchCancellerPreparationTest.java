package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.protocol.v2.TaskKind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatchCancellerPreparationTest {

    private static final String QUERY_ID = "query-early-report";
    private static final String TASK_ID = "task-early-report";
    private static final String WORKER_ID = "worker-1";

    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final org.pangrova.coordinator.scheduler.RemoteTaskDispatcher remote =
            new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                    org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(), org.pangrova.testing.testkit.TestObservability.TRACING);
    private final DispatchCanceller canceller = new DispatchCanceller(remote, tracker);

    @org.junit.jupiter.api.AfterEach void close() { remote.close(); }

    @Test
    void workerReportCanArriveBeforeSubmitResponse() {
        var request = request();
        canceller.prepareTaskDispatch(request, org.pangrova.coordinator.worker.TestWorkerInfoBuilder
                .active(WORKER_ID, "localhost", 8080, 8081));

        assertThat(tracker.getTaskState(QUERY_ID, TASK_ID))
                .isEqualTo(TaskStateMachine.State.SCHEDULED);
        assertThat(tracker.getTaskRequest(TASK_ID)).isEqualTo(request);
        assertThat(tracker.getWorkerForTask(TASK_ID)).isEqualTo(WORKER_ID);

        // Worker reports before dispatchAsync's HTTP response completes.
        tracker.updateTaskState(QUERY_ID, TASK_ID, "RUNNING");
        canceller.registerDispatchedTask(QUERY_ID,
                new DispatchResult("localhost", TASK_ID,
                        new ResultEndpoint("localhost", 9091, "ticket", "grpc")),
                WORKER_ID);
        assertThat(tracker.getTaskState(QUERY_ID, TASK_ID))
                .isEqualTo(TaskStateMachine.State.RUNNING);
    }

    @Test
    void preparationCanBeRolledBackBeforeAnyDispatchStarts() {
        var request = request();
        canceller.prepareTaskDispatch(request, org.pangrova.coordinator.worker.TestWorkerInfoBuilder
                .active(WORKER_ID, "localhost", 8080, 8081));
        canceller.discardPreparedTask(request);

        assertThat(tracker.getTaskAttempt(TASK_ID)).isNull();
        assertThat(tracker.getWorkerForTask(TASK_ID)).isNull();
        assertThatThrownBy(() -> tracker.getTaskState(QUERY_ID, TASK_ID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static org.pangrova.protocol.rest.task.SubmitTaskRequest request() {
        return CanonicalTaskRequestTestFixture.request(
                TASK_ID, QUERY_ID, 0, 0, TaskKind.TASK_KIND_READ, new byte[]{1});
    }
}
