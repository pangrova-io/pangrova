package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for BUG-D43 fix: {@link TaskResource#submitTask} must return
 * a structured {@link ErrorDetail} on exception instead of an unhandled 500
 * with a raw stack trace.
 */
class TaskResourceStructuredErrorTest {

    @Test
    @DisplayName("submitTask returns BAD_REQUEST for null request body")
    void submitTaskReturnsBadRequestForNull() {
        TaskResource resource = buildResource();

        Response response = resource.submitTask(null);

        assertThat(response.getStatus()).isEqualTo(400);
        Object entity = response.getEntity();
        assertThat(entity).isInstanceOf(ErrorDetail.class);
        ErrorDetail error = (ErrorDetail) entity;
        assertThat(error.errorCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    @DisplayName("submitTask returns structured ErrorDetail on conversion failure (BUG-D43)")
    void submitTaskReturnsStructuredErrorOnConversionFailure() {
        TaskResource resource = buildResource();

        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("LEAF")
                .planFragment("not-valid-base64-plan")
                .splits(null)
                .exchangeSources(null)
                .resourceLimit(null)
                .sessionProperties(null)
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
    }

    private static TaskResource buildResource() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport exchangeTransport = TestExchangeTransport.INSTANCE;
        return new TaskResource(
                taskManager, exchangeTransport,
                new ExchangeSourceRegistry(), "localhost", 8816, null);
    }
}
