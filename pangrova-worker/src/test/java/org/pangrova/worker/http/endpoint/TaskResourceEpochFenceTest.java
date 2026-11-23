package org.pangrova.worker.http.endpoint;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.ActualTaskManagerFixture;
import org.pangrova.worker.task.TaskManager;

@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskResourceEpochFenceTest {

    @Test
    void rejectsBeforeControlHandshakeWithRetryableServiceUnavailable() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "epoch-http");
                var resource = resource(fixture.manager(), runtime)) {
            TaskManager manager = fixture.manager();
            manager.requireControlEpochHandshake();

            Response response = resource.submitTask(new ByteArrayInputStream(request(1L).encode()), null).toCompletableFuture().join();

            assertThat(response.getStatus()).isEqualTo(503);
            assertRejected(response, "handshake");
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    @Test
    void distinguishesFutureEpochWaitingFromPermanentlyStaleTaskBeforePlanDecode() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "epoch-http");
                var resource = resource(fixture.manager(), runtime)) {
            TaskManager manager = fixture.manager();
            manager.requireControlEpochHandshake();
            manager.observeControlEpoch(7);

            Response missing = resource.submitTask(new ByteArrayInputStream(request(null).encode()), null).toCompletableFuture().join();
            Response stale = resource.submitTask(new ByteArrayInputStream(request(6L).encode()), null).toCompletableFuture().join();
            Response future = resource.submitTask(new ByteArrayInputStream(request(8L).encode()), null).toCompletableFuture().join();

            assertThat(missing.getStatus()).isEqualTo(409);
            assertThat(stale.getStatus()).isEqualTo(409);
            assertThat(future.getStatus()).isEqualTo(425);
            assertRejected(stale, "does not match");
            assertRejected(future, "does not match");
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    @Test
    void exactSealWaitsForActualHandshakeRatherThanClaimingPermanentConflict() {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "epoch-http");
                var resource = resource(fixture.manager(), runtime)) {
            TaskManager manager = fixture.manager();
            manager.requireControlEpochHandshake();
            try (Response response = seal(resource, runtime, 1L)) {
                assertThat(response.getStatus()).isEqualTo(503);
                assertThat(
                                ((org.pangrova.common.client.error.ErrorDetail)
                                                response.getEntity())
                                        .errorCode())
                        .isEqualTo("CONTROL_PLANE_NOT_READY");
            }
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    @Test
    void exactSealDistinguishesUnobservedFutureEpochWithoutSealingIt() {
        try (var runtime = new ActualNativeTaskFixture();
                var fixture = new ActualTaskManagerFixture(runtime, "epoch-http");
                var resource = resource(fixture.manager(), runtime)) {
            TaskManager manager = fixture.manager();
            manager.requireControlEpochHandshake();
            manager.observeControlEpoch(7);
            try (Response response = seal(resource, runtime, 8L)) {
                assertThat(response.getStatus()).isEqualTo(425);
                assertThat(
                                ((org.pangrova.common.client.error.ErrorDetail)
                                                response.getEntity())
                                        .errorCode())
                        .isEqualTo("EPOCH_AHEAD");
            }
            assertThat(manager.resolveWorkerTaskId("task-epoch")).isNull();
        }
    }

    private static Response seal(TaskResource resource, ActualNativeTaskFixture runtime, long epoch) {
        var exact =
                org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(
                        runtime.bindWorker(request(epoch).taskRequest()));
        return resource.cancelTask(exact.getAttempt().getTaskId(), exact.toByteArray());
    }

    private static TaskResource resource(TaskManager manager, ActualNativeTaskFixture runtime) {
        return new TaskResource(
                manager,
                runtime.transports().exchangeTransport(),
                new ExchangeSourceRegistry(),
                "127.0.0.1",
                runtime.resultPort(),
                null);
    }

    private static SubmitTaskRequest request(Long epoch) {
        var canonical = ActualNativeTaskFixture.valuesTaskRequest(false).toBuilder();
        canonical.setAttempt(
                canonical.getAttempt().toBuilder()
                        .setTaskId("task-epoch")
                        .setQueryOrJobId("query-epoch"));
        canonical.setAttempt(
                canonical.getAttempt().toBuilder().setControlEpoch(epoch == null ? 0L : epoch));
        return SubmitTaskRequest.from(canonical.build());
    }

    private static void assertRejected(Response response, String messageFragment) throws Exception {
        var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getReason()).contains(messageFragment);
    }
}
