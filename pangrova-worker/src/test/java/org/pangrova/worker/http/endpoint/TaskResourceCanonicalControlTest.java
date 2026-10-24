package org.pangrova.worker.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.task.ActualTaskManagerFixture;
import org.pangrova.worker.task.TaskManager;

/**
 * Canonical HTTP control of actual admitted JNI owners, including the real not-started boundary.
 */
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskResourceCanonicalControlTest {
    @Test
    void cancelsCanonicalIdentityReturnedBySubmitRatherThanWorkerLocalKey() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "control-http");
                var resource = resource(fixture.manager(), runtime)) {
            var manager = fixture.manager();
            String canonical = "canonical-task";
            var original = request(runtime, canonical);
            var owner = fixture.preparePending(original.taskRequest());
            try (Response submitted = resource.submitTask(original)) {
                assertThat(submitted.getStatus()).isEqualTo(200);
                assertThat(((SubmitTaskResponse) submitted.getEntity()).taskId())
                        .isEqualTo(canonical);
            }
            String local = manager.resolveWorkerTaskId(canonical);
            assertThat(local).isNotNull().isNotEqualTo(canonical);
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            assertThat(owner.executorHandle()).isPositive();
            try (Response cancelled = resource.cancelTask(canonical, seal(original))) {
                assertThat(cancelled.getStatus()).isEqualTo(202);
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLING);
            manager.releaseTaskExecutor(local);
            assertThat(owner.executorHandle()).isZero();
            assertThat(owner.allocatedBufferMemory()).isZero();
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
            assertThat(manager.resolveWorkerTaskId(canonical)).isEqualTo(local);
            try (Response delayed = resource.submitTask(original)) {
                assertThat(delayed.getStatus()).isEqualTo(409);
            }
            assertThat(manager.resolveWorkerTaskId(canonical)).isEqualTo(local);
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
            try (Response joined = resource.cancelTask(canonical, seal(original))) {
                assertThat(joined.getStatus()).isEqualTo(200);
                var receipt =
                        org.pangrova.protocol.v2.TaskReport.parseFrom((byte[]) joined.getEntity());
                assertThat(receipt.getAttempt()).isEqualTo(original.taskRequest().getAttempt());
                assertThat(receipt.getExecutionDrainState())
                        .isEqualTo(
                                org.pangrova.protocol.v2.ExecutionDrainState
                                        .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
            }
        }
    }

    @Test
    void internalIdentityIsNotAnAlternateHttpControlContract() {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "control-http");
                var resource = resource(fixture.manager(), runtime)) {
            var manager = fixture.manager();
            var original = request(runtime, "canonical-task");
            var owner = fixture.preparePending(original.taskRequest());
            try (Response submitted = resource.submitTask(original)) {
                assertThat(submitted.getStatus()).isEqualTo(200);
            }
            String local = manager.resolveWorkerTaskId("canonical-task");
            try (Response cancelled = resource.cancelTask(local, seal(original))) {
                assertThat(cancelled.getStatus()).isEqualTo(400);
                assertThat(((ErrorDetail) cancelled.getEntity()).errorCode())
                        .isEqualTo("BAD_REQUEST");
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            assertThat(owner.executorHandle()).isPositive();
            try (Response cancelled = resource.cancelTask("canonical-task", seal(original))) {
                assertThat(cancelled.getStatus()).isEqualTo(202);
            }
            manager.releaseTaskExecutor(local);
            assertThat(owner.executorHandle()).isZero();
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
        }
    }

    @Test
    void exactAbsentAdmissionIsClosedAndLateSubmissionIsRejected() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "control-http");
                var resource = resource(fixture.manager(), runtime)) {
            var original = request(runtime, "absent");
            try (Response cancelled = resource.cancelTask("absent", seal(original))) {
                var manager = fixture.manager();
                assertThat(cancelled.getStatus()).isEqualTo(200);
                var receipt =
                        org.pangrova.protocol.v2.TaskReport.parseFrom((byte[]) cancelled.getEntity());
                assertThat(receipt.getAttempt().getTaskId()).isEqualTo("absent");
                assertThat(receipt.getExecutionDrainState())
                        .isEqualTo(
                                org.pangrova.protocol.v2.ExecutionDrainState
                                        .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
                assertThat(manager.resolveWorkerTaskId("absent")).isNull();
                try (Response late = resource.submitTask(original)) {
                    assertThat(late.getStatus()).isEqualTo(409);
                }
                assertThat(runtime.nodePool().getUsedBytes()).isZero();
                assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            }
        }
    }

    @Test
    void rawTaskIdWithoutExactSealDoesNotAcknowledgeCancellation() {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "control-http");
                var resource = resource(fixture.manager(), runtime);
                Response rejected = resource.cancelTask("absent", null)) {
            assertThat(rejected.getStatus()).isEqualTo(400);
            assertThat(fixture.manager().resolveWorkerTaskId("absent")).isNull();
        }
    }

    @Test
    void freshHttpSubmissionExecutesItsActualValuesPlanAndDrainsTheRealFlightConsumer()
            throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "control-http");
                var resource = resource(fixture.manager(), runtime)) {
            TaskRequest request = runtime.valuesRequest(true);
            try (Response submitted = resource.submitTask(SubmitTaskRequest.from(request))) {
                assertThat(submitted.getStatus()).isEqualTo(200);
                var response = (SubmitTaskResponse) submitted.getEntity();
                assertThat(response.accepted()).isTrue();
                assertThat(response.taskId()).isEqualTo(request.getAttempt().getTaskId());
                assertThat(response.resultEndpoint()).isNotNull();
            }
            var owner = fixture.admittedTask(request);
            var actual = fixture.consume(owner);
            assertThat(actual.values()).containsExactly(42L);
            assertThat(actual.rows()).isEqualTo(1);
            assertThat(actual.batches()).isPositive();
            assertThat(owner.executorHandle()).isZero();
            assertThat(owner.allocatedBufferMemory()).isZero();
            assertThat(owner.getState()).isEqualTo(TaskManager.TaskState.FINISHED);
        }
    }

    private static TaskResource resource(TaskManager manager, ActualNativeTaskFixture runtime) {
        return new TaskResource(
                manager,
                runtime.transports().exchangeTransport(),
                new org.pangrova.worker.exchange.source.ExchangeSourceRegistry(),
                "127.0.0.1",
                runtime.resultPort(),
                null,
                new TaskResource.ResultBinding("grpc", runtime.resultTickets()));
    }

    private static byte[] seal(SubmitTaskRequest original) {
        return org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(original.taskRequest())
                .toByteArray();
    }

    private static SubmitTaskRequest request(ActualNativeTaskFixture runtime, String taskId) {
        var canonical = runtime.valuesRequest(false).toBuilder();
        canonical.setAttempt(
                canonical.getAttempt().toBuilder()
                        .setTaskId(taskId)
                        .setQueryOrJobId("query-control"));
        return SubmitTaskRequest.from(canonical.build());
    }
}
