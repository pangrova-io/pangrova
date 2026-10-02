package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.worker.CanonicalTaskRequestTestFixture;
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
            try (Response submitted = resource.submitTask(request(canonical))) {
                assertThat(submitted.getStatus()).isEqualTo(200);
                assertThat(((SubmitTaskResponse) submitted.getEntity()).taskId()).isEqualTo(canonical);
            }
            String local = manager.resolveWorkerTaskId(canonical);
            assertThat(local).isNotNull().isNotEqualTo(canonical);
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            try (Response cancelled = resource.cancelTask(canonical)) {
                assertThat(cancelled.getStatus()).isEqualTo(200);
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.CANCELLED);
            assertThat(manager.resolveWorkerTaskId(canonical)).isNull();
        }
    }

    @Test
    void internalIdentityIsNotAnAlternateHttpControlContract() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1)) {
            try (Response submitted = resource.submitTask(request("canonical-task"))) {
                assertThat(submitted.getStatus()).isEqualTo(200);
            }
            String local = manager.resolveWorkerTaskId("canonical-task");
            try (Response cancelled = resource.cancelTask(local)) {
                assertThat(cancelled.getStatus()).isEqualTo(404);
                assertThat(((ErrorDetail) cancelled.getEntity()).errorCode()).isEqualTo("NOT_FOUND");
            }
            assertThat(manager.getTaskState(local)).contains(TaskManager.TaskState.INITIALIZING);
            try (Response cancelled = resource.cancelTask("canonical-task")) {
                assertThat(cancelled.getStatus()).isEqualTo(200);
            }
        }
    }

    @Test
    void absentCanonicalIdentityReturnsStructuredNotFound() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var executor = new NativeTaskExecutor();
                var transport = new NativeExchangeTransport(executor);
                var resource = new TaskResource(manager, transport, "worker.test", 1);
                Response cancelled = resource.cancelTask("absent")) {
            assertThat(cancelled.getStatus()).isEqualTo(404);
            assertThat(((ErrorDetail) cancelled.getEntity()).message()).contains("absent");
        }
    }

    private static org.pangrova.protocol.rest.task.SubmitTaskRequest request(String taskId) {
        return CanonicalTaskRequestTestFixture.request(taskId, "query-control", 0, 0,
                TaskKind.TASK_KIND_READ, "plan".getBytes(StandardCharsets.UTF_8));
    }
}
