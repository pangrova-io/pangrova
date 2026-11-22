package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.testkit.TestExchangeTransport;
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

        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
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
    }

    private static TaskResource buildResource() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport = TestExchangeTransport.INSTANCE;
        return new TaskResource(
                taskManager, exchangeTransport,
                new ExchangeSourceRegistry(), "localhost", 8816, null);
    }
}
