package org.pangrova.worker.http.endpoint;

import org.pangrova.protocol.rest.task.UpstreamCommitNotification;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import jakarta.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the upstream-commit endpoint on TaskResource.
 * Covers Task 21: Worker upstream-commit endpoint delegates to ExchangeSourceRegistry.
 */
class UpstreamCommitEndpointTest {

    @Test
    @DisplayName("POST /v1/task/upstream-commit returns 200")
    void upstreamCommitReturns200() {
        TaskResource resource = createResource();

        UpstreamCommitNotification notification = new UpstreamCommitNotification("q-1", 0);
        Response response = resource.onUpstreamCommit(notification);

        assertEquals(200, response.getStatus());
    }

    @Test
    @DisplayName("POST /v1/task/upstream-commit with null body returns 400")
    void nullBodyReturns400() {
        TaskResource resource = createResource();

        Response response = resource.onUpstreamCommit(null);
        assertEquals(400, response.getStatus());
    }

    @Test
    @DisplayName("Commit notification for unknown query returns 200 (idempotent)")
    void unknownQueryReturns200() {
        TaskResource resource = createResource();

        UpstreamCommitNotification notification = new UpstreamCommitNotification("unknown-q", 99);
        Response response = resource.onUpstreamCommit(notification);

        assertEquals(200, response.getStatus());
    }

    private TaskResource createResource() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport exchangeTransport = TestExchangeTransport.INSTANCE;
        ExchangeSourceRegistry registry = new ExchangeSourceRegistry();
        return new TaskResource(taskManager, exchangeTransport, registry, "localhost", 8816, null);
    }
}
