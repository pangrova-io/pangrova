package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.v2.TaskKind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatchCancellerPreparationTest {

    private static final String QUERY_ID = "query-early-report";
    private static final String TASK_ID = "task-early-report";
    private static final String WORKER_ID = "worker-1";

    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final DispatchCanceller canceller = new DispatchCanceller(
            (TaskDispatcher) (request, worker) -> {
                throw new UnsupportedOperationException("not dispatched in this test");
            }, tracker);

    @Test
    void workerReportCanArriveBeforeSubmitResponse() {
        var request = request();
        canceller.prepareTaskDispatch(request, WORKER_ID);

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
        canceller.prepareTaskDispatch(request, WORKER_ID);
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
