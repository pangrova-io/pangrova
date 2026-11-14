package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskKind;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit boundaries for delayed HTTP acknowledgement; these transport values are not Native result evidence. */
class StagePartitionAcknowledgementTest {
    private static final String QUERY = "early-http-query";
    private static final String TASK = "actual-issued-task";
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final ControlledAcknowledgement transport = new ControlledAcknowledgement();
    private final WorkerInfo worker = TestWorkerInfoBuilder.active("worker", "localhost", 8080, 8081);
    private final StagePartitionDispatcher partitions = new StagePartitionDispatcher(
            transport, new DispatchCanceller(transport, tracker), java.util.function.UnaryOperator.identity());

    @Test
    void lateAcknowledgementPreservesTheAcceptedTerminalAttemptBeforePublishingItsFuture() {
        var issued = partitions.dispatch(request(5_000), worker);
        assertThat(tracker.updateTaskState(QUERY, TASK, "FINISHED")).isTrue();
        var observed = issued.acknowledgement().thenApply(result -> tracker.getTaskState(QUERY, result.taskId()));
        transport.response.complete(result(TASK));
        assertThat(observed.join()).isEqualTo(TaskStateMachine.State.FINISHED);
        assertThat(tracker.getWorkerForTask(TASK)).isEqualTo(worker.workerId());
        assertThatThrownBy(() -> tracker.unregisterSettledTask(QUERY, TASK))
                .hasMessageContaining("actual drain evidence");
        assertThat(tracker.getTaskRequest(TASK)).isNotNull();
    }

    @Test
    void missingHttpAcknowledgementExpiresAtTheActualCanonicalTaskDeadline() {
        var issued = partitions.dispatch(request(300), worker);
        assertThat(tracker.updateTaskState(QUERY, TASK, "FINISHED")).isTrue();
        assertThatThrownBy(() -> issued.acknowledgement().get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(transport.response).isCompletedExceptionally();
        assertThatThrownBy(() -> tracker.unregisterSettledTask(QUERY, TASK))
                .hasMessageContaining("actual drain evidence");
        assertThat(tracker.getTaskAttempt(TASK)).isNotNull();
        assertThat(tracker.getTaskRequest(TASK)).isNotNull();
    }

    @Test
    void changedAcknowledgementCannotReplaceTheIssuedTaskIdentity() {
        var issued = partitions.dispatch(request(5_000), worker);
        transport.response.complete(result("different-task"));
        assertThatThrownBy(() -> issued.acknowledgement().join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(tracker.getTaskState(QUERY, TASK)).isEqualTo(TaskStateMachine.State.SCHEDULED);
    }

    @Test
    void privateSourceBindingFollowsAccessBindingAndPrecedesTrackedAdmission() {
        var accessComplete = new java.util.concurrent.atomic.AtomicBoolean();
        var bound = new java.util.concurrent.atomic.AtomicReference<SubmitTaskRequest>();
        TaskDispatcher binding = new TaskDispatcher() {
            @Override public SubmitTaskRequest bindSourceInput(SubmitTaskRequest request) {
                assertThat(accessComplete).isTrue();
                assertThat(request.taskRequest().getAttempt().getWorkerExecutionIdentity())
                        .isEqualTo(worker.workerExecutionIdentity().toProto());
                var result = request.withBoundTask(request.taskRequest().toBuilder()
                        .setDeadlineEpochMs(request.taskRequest().getDeadlineEpochMs() - 1).build());
                bound.set(result);
                return result;
            }
            @Override public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo target) {
                throw new AssertionError("Expected asynchronous dispatch");
            }
            @Override public CompletableFuture<DispatchResult> dispatchAsync(
                    SubmitTaskRequest request, WorkerInfo target) {
                assertThat(request).isSameAs(bound.get());
                assertThat(tracker.getTaskRequest(TASK)).isEqualTo(request);
                return transport.response;
            }
        };
        var dispatcher = new StagePartitionDispatcher(binding, new DispatchCanceller(binding, tracker), request -> {
            assertThat(request.taskRequest().getAttempt().getWorkerExecutionIdentity())
                    .isEqualTo(worker.workerExecutionIdentity().toProto());
            accessComplete.set(true);
            return request;
        });
        assertThat(dispatcher.dispatch(request(5_000), worker).request()).isSameAs(bound.get());
    }

    @Test
    void aPreviouslyBoundPhysicalWorkerCannotBeReplacedBeforeDispatch() {
        var bound = transport.bindWorkerIdentity(request(5_000), worker);
        var replacement = TestWorkerInfoBuilder.active("replacement", "localhost", 8080, 8081);

        assertThatThrownBy(() -> partitions.dispatch(bound, replacement))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("registered Worker incarnation");
        assertThat(tracker.getTaskRequest(TASK)).isNull();
        assertThat(transport.response).isNotDone();
    }

    private static SubmitTaskRequest request(long durationMillis) {
        return SubmitTaskRequest.from(CanonicalTaskRequestTestFixture.builder(
                TASK, QUERY, 0, 0, TaskKind.TASK_KIND_READ, new byte[] {1})
                .setDeadlineEpochMs(System.currentTimeMillis() + durationMillis).build());
    }

    private static DispatchResult result(String task) {
        return new DispatchResult("localhost", task, new ResultEndpoint("localhost", 8081, "ticket", "grpc"));
    }

    private static final class ControlledAcknowledgement implements TaskDispatcher {
        private final CompletableFuture<DispatchResult> response = new CompletableFuture<>();

        @Override public CompletableFuture<DispatchResult> dispatchAsync(SubmitTaskRequest request, WorkerInfo worker) {
            return response;
        }

        @Override public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            throw new AssertionError("The shared boundary must use its asynchronous dispatch port");
        }
    }
}
