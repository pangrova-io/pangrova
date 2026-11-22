package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R10.2: TaskResource returns structured SubmitTaskResponse on error
 * instead of propagating raw exceptions.
 */
class TaskResourceSubmitErrorTest {

    private TaskResource resource;
    private TaskManager taskManager;

    @BeforeEach
    void setUp() {
        taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport = TestExchangeTransport.INSTANCE;
        ExchangeSourceRegistry sourceRegistry = new ExchangeSourceRegistry();

        resource = new TaskResource(
                taskManager, exchangeTransport, sourceRegistry,
                "test-host", 9090, null);
    }

    @AfterEach
    void tearDown() {
        resource.close();
        taskManager.close();
    }

    @Test
    void submitTask_invalidChecksumReturnsBadRequestBeforeAdmission() {
        var canonical =
                org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 1,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .clearSubstraitPlanChecksum()
                        .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);

        Response response = resource.submitTask(request);

        assertThat(response.getStatus()).isEqualTo(400);
        Object entity = response.getEntity();
        assertThat(entity).isInstanceOf(SubmitTaskResponse.class);

        SubmitTaskResponse errorResponse = (SubmitTaskResponse) entity;
        assertThat(errorResponse.accepted()).isFalse();
        assertThat(errorResponse.rejectReason()).contains("checksum is invalid");
        assertThat(errorResponse.taskId()).isNull();
        assertThat(errorResponse.resultEndpoint()).isNull();
        assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
    }

    @Test
    void submitTask_missingGrantReturnsBadRequestBeforeAdmission() {
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 1, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8)).clearResourceGrant().build();
        var request = SubmitTaskRequest.from(canonical);
        try (Response response = resource.submitTask(request)) {
            assertThat(response.getStatus()).isEqualTo(400);
            var error = (SubmitTaskResponse) response.getEntity();
            assertThat(error.accepted()).isFalse();
            assertThat(error.rejectReason()).contains("resource grant is incomplete");
            assertThat(error.taskId()).isNull();
            assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
        }
    }

    @Test
    void submitTask_internalSigningFailureRemainsServerErrorBeforeAdmission() {
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                "task-1", "query-1", 0, 1, org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("task-1:grant").setCpuSlots(1).setNativeMemoryBytes(1_048_576)
                        .setShuffleBufferBytes(1_048_576))
                .setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions.newBuilder()
                        .setDirectResultOutput(true))
                .build();
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(canonical);
        try (Response response = resource.submitTask(SubmitTaskRequest.from(canonical))) {
            assertThat(response.getStatus()).isEqualTo(500);
            var error = (SubmitTaskResponse) response.getEntity();
            assertThat(error.accepted()).isFalse();
            assertThat(error.rejectReason()).contains("signing authority");
            assertThat(taskManager.resolveWorkerTaskId("task-1")).isNull();
        }
    }

    @Test
    void submitTask_nullRequest_returnsBadRequest() {
        Response response = resource.submitTask(null);
        assertThat(response.getStatus()).isEqualTo(400);
    }
}
