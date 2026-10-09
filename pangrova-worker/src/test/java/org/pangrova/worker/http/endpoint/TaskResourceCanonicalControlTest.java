package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture;
import org.pangrova.worker.exchange.NativeExchangeTransport;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.task.TaskManager;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests control-plane identity translation only; this does not execute the submitted plan. */
class TaskResourceCanonicalControlTest {
    @Test
    void cancelsCanonicalIdentityReturnedBySubmitRatherThanWorkerLocalKey() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1)) {
            String canonical = "canonical-task";
            var original = request(canonical);
            try (Response submitted = resource.submitTask(original)) {
                assertThat(submitted.getStatus()).isEqualTo(200);
                assertThat(((SubmitTaskResponse) submitted.getEntity()).taskId()).isEqualTo(canonical);
            }
            String local = manager.resolveWorkerTaskId(canonical);
            assertThat(local).isNotNull().isNotEqualTo(canonical);
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            try (Response cancelled = resource.cancelTask(canonical, seal(original))) {
                // State-machine-only wiring cannot provide actual resource exit evidence.
                assertThat(cancelled.getStatus()).isEqualTo(202);
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
            assertThat(manager.resolveWorkerTaskId(canonical)).isEqualTo(local);
            try (Response delayed = resource.submitTask(original)) {
                assertThat(delayed.getStatus()).isEqualTo(409);
            }
            assertThat(manager.resolveWorkerTaskId(canonical)).isEqualTo(local);
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
        }
    }

    @Test
    void internalIdentityIsNotAnAlternateHttpControlContract() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1)) {
            var original = request("canonical-task");
            try (Response submitted = resource.submitTask(original)) {
                assertThat(submitted.getStatus()).isEqualTo(200);
            }
            String local = manager.resolveWorkerTaskId("canonical-task");
            try (Response cancelled = resource.cancelTask(local, seal(original))) {
                assertThat(cancelled.getStatus()).isEqualTo(400);
                assertThat(((ErrorDetail) cancelled.getEntity()).errorCode()).isEqualTo("BAD_REQUEST");
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            try (Response cancelled = resource.cancelTask("canonical-task", seal(original))) {
                assertThat(cancelled.getStatus()).isEqualTo(202);
            }
        }
    }

    @Test
    void exactAbsentAdmissionIsClosedAndLateSubmissionIsRejected() throws Exception {
        var original = request("absent");
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1);
                Response cancelled = resource.cancelTask("absent", seal(original))) {
            assertThat(cancelled.getStatus()).isEqualTo(200);
            var receipt = org.pangrova.protocol.v2.TaskReport.parseFrom((byte[]) cancelled.getEntity());
            assertThat(receipt.getAttempt().getTaskId()).isEqualTo("absent");
            assertThat(receipt.getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                    .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            assertThat(manager.resolveWorkerTaskId("absent")).isNull();
            try (Response late = resource.submitTask(original)) {
                assertThat(late.getStatus()).isEqualTo(409);
            }
        }
    }

    @Test
    void rawTaskIdWithoutExactSealDoesNotAcknowledgeCancellation() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1);
                Response rejected = resource.cancelTask("absent", null)) {
            assertThat(rejected.getStatus()).isEqualTo(400);
        }
    }

    private static byte[] seal(org.pangrova.protocol.rest.task.SubmitTaskRequest original) {
        return org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(original.taskRequest()).toByteArray();
    }

    private static org.pangrova.protocol.rest.task.SubmitTaskRequest request(String taskId) {
        var canonical = CanonicalTaskRequestTestFixture.builder(taskId, "query-control", 0, 0,
                TaskKind.TASK_KIND_READ, "plan".getBytes(StandardCharsets.UTF_8))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(1).setNativeMemoryBytes(1_048_576))
                .build();
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(canonical);
        return org.pangrova.protocol.rest.task.SubmitTaskRequest.from(canonical);
    }
}
