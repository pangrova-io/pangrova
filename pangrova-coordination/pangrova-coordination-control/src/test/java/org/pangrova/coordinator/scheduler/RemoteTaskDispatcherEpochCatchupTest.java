package org.pangrova.coordinator.scheduler;

import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.scheduler.api.exception.TaskDispatchException;
import org.pangrova.scheduler.api.enums.ErrorCategory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskAdmitted;
import org.pangrova.protocol.http.TaskRejected;
import org.pangrova.protocol.http.TaskResultEndpoint;
import org.pangrova.protocol.http.TaskShuffleEndpoint;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.common.enums.MemoryPressure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real HTTP pre-admission epoch catch-up protocol on both dispatch paths. */
class RemoteTaskDispatcherEpochCatchupTest {

    private static final ExecutionOwnerIdentity OWNER =
            new ExecutionOwnerIdentity("coordinator-1", "incarnation-1", 7);

    @Test
    void syncRetriesIdenticalAttemptAfterExplicitEpochAhead() throws Exception {
        verifyCatchup(false);
    }

    @Test
    void asyncRetriesIdenticalAttemptAfterExplicitEpochAhead() throws Exception {
        verifyCatchup(true);
    }

    @Test
    void asyncDoesNotRetryWhenLeadershipChangesDuringCatchup() throws Exception {
        AtomicLong epoch = new AtomicLong(2);
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startServer(body -> {
            requests.incrementAndGet();
            epoch.set(3);
            return epochAhead();
        });
        try (RemoteTaskDispatcher dispatcher = dispatcher(epoch)) {
            var future = dispatcher.dispatchAsync(request(), worker(server.getAddress().getPort()));
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TaskDispatchException.class)
                    .satisfies(error -> assertThat(((TaskDispatchException) error.getCause())
                            .getCategory()).isEqualTo(ErrorCategory.CONTROL_FENCED));
            assertThat(requests).hasValue(1);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void persistentEpochAheadIsControlFencedRatherThanWorkerBusy() throws Exception {
        HttpServer server = startServer(body -> epochAhead());
        try (RemoteTaskDispatcher dispatcher = dispatcher(new AtomicLong(2))) {
            var future = dispatcher.dispatchAsync(request(), worker(server.getAddress().getPort()));
            assertThatThrownBy(() -> future.get(20, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TaskDispatchException.class)
                    .satisfies(error -> assertThat(((TaskDispatchException) error.getCause())
                            .getCategory()).isEqualTo(ErrorCategory.CONTROL_FENCED));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void catchupRetryRequiresTimeForAnHttpExchange() {
        var worker = worker(1);
        Duration interval = Duration.ofMillis(250);
        assertThatThrownBy(() -> RemoteTaskDispatcher.dispatchRequestTimeout(interval.toNanos(), true, worker))
                .isInstanceOfSatisfying(TaskDispatchException.class, failure ->
                        assertThat(failure.getCategory()).isEqualTo(ErrorCategory.CONTROL_FENCED));
        assertThat(RemoteTaskDispatcher.dispatchRequestTimeout(interval.toNanos(), false, worker))
                .isEqualTo(interval);
        Duration remaining = interval.plusNanos(1);
        assertThat(RemoteTaskDispatcher.dispatchRequestTimeout(remaining.toNanos(), true, worker))
                .isEqualTo(remaining);
    }

    @Test
    void rejectsAcknowledgementForADifferentTask() throws Exception {
        assertRejectedAcknowledgement(admitted("other-task").build(),
                "Worker acknowledgement does not match the submitted task identity");
    }

    @Test
    void rejectsShuffleTicketsOutsideTheirPartitionPositions() throws Exception {
        var shuffle = TaskShuffleEndpoint.newBuilder().setHost("127.0.0.1").setPort(1)
                .addPartitionTickets(org.pangrova.protocol.v2.ShufflePartitionTicket.newBuilder()
                        .setPartitionId(1).setSignedTicket(com.google.protobuf.ByteString.copyFromUtf8("opaque"))
                        .setExpiresAtEpochMs(System.currentTimeMillis() + 60_000));
        assertRejectedAcknowledgement(admitted("task-epoch").setShuffleEndpoint(shuffle).build(),
                "Invalid shuffle ticket for partition 0");
    }

    private static void assertRejectedAcknowledgement(TaskAdmitted ack, String reason) throws Exception {
        HttpServer server = startServer(body -> new Reply(200, TaskSubmitAck.newBuilder().setAdmitted(ack).build()));
        try (RemoteTaskDispatcher dispatcher = dispatcher(new AtomicLong(2))) {
            assertThatThrownBy(() -> dispatcher.dispatch(request(), worker(server.getAddress().getPort())))
                    .isInstanceOf(TaskDispatchException.class).hasRootCauseMessage(reason);
        } finally {
            server.stop(0);
        }
    }

    private static Reply epochAhead() {
        return new Reply(425, TaskSubmitAck.newBuilder().setRejected(
                TaskRejected.newBuilder().setCode("CONTROL_EPOCH_AHEAD").setReason("epoch ahead")).build());
    }

    private static TaskAdmitted.Builder admitted(String taskId) {
        var endpoint = TaskResultEndpoint.newBuilder().setHost("127.0.0.1").setPort(1)
                .setTicket("ticket").setProtocol(org.pangrova.common.client.enums.ResultProtocol.GRPC.value());
        return TaskAdmitted.newBuilder().setTaskId(taskId).setResultEndpoint(endpoint);
    }

    private static void verifyCatchup(boolean async) throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<byte[]> first = new AtomicReference<>();
        AtomicReference<byte[]> second = new AtomicReference<>();
        HttpServer server = startServer(body -> {
            if (requests.incrementAndGet() == 1) {
                first.set(body);
                return epochAhead();
            }
            second.set(body);
            return new Reply(200, TaskSubmitAck.newBuilder().setAdmitted(
                    admitted("task-epoch")).build());
        });
        try (RemoteTaskDispatcher dispatcher = dispatcher(new AtomicLong(2))) {
            WorkerInfo worker = worker(server.getAddress().getPort());
            var result = async
                    ? dispatcher.dispatchAsync(request(), worker).get(5, TimeUnit.SECONDS)
                    : dispatcher.dispatch(request(), worker);
            assertThat(result.taskId()).isEqualTo("task-epoch");
            assertThat(result.resultEndpoint()).isEqualTo(
                    new ResultEndpoint("127.0.0.1", 1, "ticket",
                            org.pangrova.common.client.enums.ResultProtocol.GRPC.value()));
            assertThat(requests).hasValue(2);
            assertThat(Arrays.equals(first.get(), second.get())).isTrue();
            SubmitTaskRequest sent = SubmitTaskRequest.decode(first.get());
            assertThat(sent.taskRequest().getAttempt().getControlEpoch()).isEqualTo(2);
            assertThat(ExecutionOwnerIdentity.from(
                    sent.taskRequest().getAttempt().getExecutionOwner()))
                    .isEqualTo(OWNER);
        } finally {
            server.stop(0);
        }
    }

    private static RemoteTaskDispatcher dispatcher(AtomicLong epoch) {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        dispatcher.setControlEpochSupplier(epoch::get);
        dispatcher.setExecutionOwnerResolver(ignored -> OWNER);
        return dispatcher;
    }

    private static SubmitTaskRequest request() {
        SubmitTaskRequest fixture = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-epoch", "q-epoch", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var canonical = fixture.taskRequest();
        return SubmitTaskRequest.from(canonical.toBuilder()
                .setAttempt(canonical.getAttempt().toBuilder()
                        .setControlEpoch(0).clearExecutionOwner())
                .setExecutionOptions(canonical.getExecutionOptions().toBuilder().setDirectResultOutput(true))
                .build());
    }

    private static WorkerInfo worker(int port) {
        return new WorkerInfo("w-epoch", "127.0.0.1", port, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-epoch"));
    }

    private static HttpServer startServer(Responder responder) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/task", exchange -> {
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();
                assertThat(exchange.getRequestHeaders().getFirst("Content-Type")).isEqualTo(SubmitTaskRequest.MEDIA_TYPE);
                assertThat(exchange.getRequestHeaders().getFirst("Accept")).isEqualTo(SubmitTaskRequest.MEDIA_TYPE);
                Reply reply = responder.respond(body);
                byte[] bytes = reply.body().toByteArray();
                exchange.getResponseHeaders().set("Content-Type", SubmitTaskRequest.MEDIA_TYPE);
                exchange.sendResponseHeaders(reply.status(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
        return server;
    }

    @FunctionalInterface
    private interface Responder {
        Reply respond(byte[] body) throws IOException;
    }

    private record Reply(int status, TaskSubmitAck body) {}
}
