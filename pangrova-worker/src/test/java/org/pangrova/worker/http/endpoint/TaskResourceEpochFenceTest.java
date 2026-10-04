package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;

import static org.assertj.core.api.Assertions.assertThat;

class TaskResourceEpochFenceTest {

    @Test
    void rejectsBeforeControlHandshakeWithRetryableServiceUnavailable() {
        TaskManager manager = TaskManager.forHttpWiringOnly();
        manager.requireControlEpochHandshake();
        TaskResource resource = resource(manager);

        Response response = resource.submitTask(request(1L));

        assertThat(response.getStatus()).isEqualTo(503);
        assertRejected(response, "handshake");
    }

    @Test
    void distinguishesFutureEpochWaitingFromPermanentlyStaleTaskBeforePlanDecode() {
        TaskManager manager = TaskManager.forHttpWiringOnly();
        manager.requireControlEpochHandshake();
        manager.observeControlEpoch(7);
        TaskResource resource = resource(manager);

        Response missing = resource.submitTask(request(null));
        Response stale = resource.submitTask(request(6L));
        Response future = resource.submitTask(request(8L));

        assertThat(missing.getStatus()).isEqualTo(409);
        assertThat(stale.getStatus()).isEqualTo(409);
        assertThat(future.getStatus()).isEqualTo(425);
        assertRejected(stale, "does not match");
        assertRejected(future, "does not match");
    }

    @Test
    void exactSealWaitsForActualHandshakeRatherThanClaimingPermanentConflict() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var resource = resource(manager)) {
            manager.requireControlEpochHandshake();
            try (Response response = seal(resource, 1L)) {
                assertThat(response.getStatus()).isEqualTo(503);
                assertThat(((org.pangrova.protocol.rest.error.ErrorDetail) response.getEntity()).errorCode())
                        .isEqualTo("CONTROL_PLANE_NOT_READY");
            }
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    @Test
    void exactSealDistinguishesUnobservedFutureEpochWithoutSealingIt() {
        try (var manager = TaskManager.forHttpWiringOnly();
                var resource = resource(manager)) {
            manager.requireControlEpochHandshake();
            manager.observeControlEpoch(7);
            try (Response response = seal(resource, 8L)) {
                assertThat(response.getStatus()).isEqualTo(425);
                assertThat(((org.pangrova.protocol.rest.error.ErrorDetail) response.getEntity()).errorCode())
                        .isEqualTo("EPOCH_AHEAD");
            }
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    private static Response seal(TaskResource resource, long epoch) {
        var exact = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(request(epoch).taskRequest());
        return resource.cancelTask(exact.getAttempt().getTaskId(), exact.toByteArray());
    }

    private static TaskResource resource(TaskManager manager) {
        return new TaskResource(manager, TestExchangeTransport.INSTANCE,
                new ExchangeSourceRegistry(), "worker.test", 8816, null);
    }

    private static SubmitTaskRequest request(Long epoch) {
        var canonical = org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture.builder(
                "task-epoch", "query-epoch", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        canonical.setAttempt(canonical.getAttempt().toBuilder()
                .setControlEpoch(epoch == null ? 0L : epoch));
        return SubmitTaskRequest.from(canonical.build());
    }

    private static void assertRejected(Response response, String messageFragment) {
        assertThat(response.getEntity()).isInstanceOf(SubmitTaskResponse.class);
        SubmitTaskResponse body = (SubmitTaskResponse) response.getEntity();
        assertThat(body.accepted()).isFalse();
        assertThat(body.rejectReason()).contains(messageFragment);
    }
}
