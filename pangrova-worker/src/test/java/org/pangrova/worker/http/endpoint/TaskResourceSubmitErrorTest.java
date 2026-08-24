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

import java.util.List;
import java.util.Map;

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
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(1)
                .taskType("SCAN")
                .planFragment("plan-fragment")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();

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
