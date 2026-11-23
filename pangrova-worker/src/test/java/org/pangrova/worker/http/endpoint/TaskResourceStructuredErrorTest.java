package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskSubmitAck;
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
 * Tests structured task admission errors.
 */
class TaskResourceStructuredErrorTest {

    @Test
    @DisplayName("submitTask returns BAD_REQUEST for null request body")
    void submitTaskReturnsBadRequestForNull() throws Exception {
        TaskResource resource = buildResource();

        Response response = resource.submitTask(null, null).toCompletableFuture().join();

        assertThat(response.getStatus()).isEqualTo(400);
        var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    @DisplayName("submitTask returns a rejected acknowledgement on conversion failure")
    void submitTaskReturnsStructuredErrorOnConversionFailure() throws Exception {
        TaskResource resource = buildResource();

        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .clearSubstraitPlanChecksum()
                .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);

        Response response = resource.submitTask(request.encode(), null).toCompletableFuture().join();

        assertThat(response.getStatus()).isEqualTo(400);
        var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getReason()).contains("checksum is invalid");
    }

    private static TaskResource buildResource() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport = TestExchangeTransport.INSTANCE;
        return new TaskResource(
                taskManager, exchangeTransport,
                new ExchangeSourceRegistry(), "localhost", 8816, null);
    }
}
