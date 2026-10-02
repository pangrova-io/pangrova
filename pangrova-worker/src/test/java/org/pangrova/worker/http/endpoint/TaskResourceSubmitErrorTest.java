package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.TestExchangeTransport;
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

    @BeforeEach
    void setUp() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport exchangeTransport = TestExchangeTransport.INSTANCE;
        ExchangeSourceRegistry sourceRegistry = new ExchangeSourceRegistry();

        resource = new TaskResource(
                taskManager, exchangeTransport, sourceRegistry,
                "test-host", 9090, null);
    }

    @AfterEach
    void tearDown() {
        resource.close();
    }

    @Test
    void submitTask_internalError_returnsStructuredSubmitTaskResponse() {
        var canonical =
                org.pangrova.worker.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 1,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .clearSubstraitPlanChecksum()
                        .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);

        Response response = resource.submitTask(request);

        assertThat(response.getStatus()).isEqualTo(500);
        Object entity = response.getEntity();
        assertThat(entity).isInstanceOf(SubmitTaskResponse.class);

        SubmitTaskResponse errorResponse = (SubmitTaskResponse) entity;
        assertThat(errorResponse.accepted()).isFalse();
        assertThat(errorResponse.rejectReason()).isNotNull();
        assertThat(errorResponse.taskId()).isNull();
        assertThat(errorResponse.resultEndpoint()).isNull();
    }

    @Test
    void submitTask_nullRequest_returnsBadRequest() {
        Response response = resource.submitTask(null);
        assertThat(response.getStatus()).isEqualTo(400);
    }
}
