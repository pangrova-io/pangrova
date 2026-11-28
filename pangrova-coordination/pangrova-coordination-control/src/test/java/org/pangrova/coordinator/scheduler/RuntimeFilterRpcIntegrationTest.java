package org.pangrova.coordinator.scheduler;

import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.task.TaskRuntimeFilterWire;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Worker HTTP handling for the exact runtime-filter control message. */
class RuntimeFilterRpcIntegrationTest {
    private static final String AUTH = "runtime-filter-test-auth";

    @Test
    void binaryDeliveryDeclinesAMissingAttemptAndReplacesTheJsonRoute() throws Exception {
        try (var manager = TaskManager.forHttpWiringOnly();
                var dispatcher = new RemoteTaskDispatcher(
                        CircuitBreakerPolicy.noOp(), new NoOpTracingProvider(), AUTH)) {
            var server = new WorkerHttpServer(new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0, AUTH),
                    manager, TestExchangeTransport.INSTANCE);
            server.start();
            try {
                var worker = TestWorkerInfoBuilder.active("worker", "localhost", server.getPort(), 8082,
                        manager.workerExecutionIdentity());
                var attempt = CanonicalTaskRequestTestFixture.builder("task", "query", 1, 0,
                        TaskKind.TASK_KIND_READ, new byte[] {1}).getAttempt().toBuilder()
                        .setWorkerExecutionIdentity(worker.workerExecutionIdentity().toProto()).build();
                var update = update();
                assertThat(dispatcher.injectDynamicFilter(attempt, update, worker).get(5, TimeUnit.SECONDS)).isFalse();
                var wrongWorker = attempt.getWorkerExecutionIdentity().toBuilder()
                        .setIncarnationId("00000000-0000-0000-0000-000000000099").build();
                assertThatThrownBy(() -> dispatcher.injectDynamicFilter(
                        attempt.toBuilder().setWorkerExecutionIdentity(wrongWorker).build(), update, worker).join())
                        .hasCauseInstanceOf(IllegalArgumentException.class);

                var uri = URI.create("http://localhost:" + server.getPort() + "/v1/task/task/dynamic-filter");
                var client = HttpClient.newHttpClient();
                var unauthenticated = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                        .header("Content-Type", TaskRuntimeFilterWire.MEDIA_TYPE)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(TaskRuntimeFilterWire.encode(attempt, update)))
                        .build();
                assertThat(client.send(unauthenticated, HttpResponse.BodyHandlers.discarding()).statusCode())
                        .isEqualTo(401);
                assertThat(send(client, uri, "application/json", new byte[] {'{', '}'})).isEqualTo(415);
                assertThat(send(client, uri, TaskRuntimeFilterWire.MEDIA_TYPE, new byte[] {1})).isEqualTo(400);
                assertThat(send(client, uri, TaskRuntimeFilterWire.MEDIA_TYPE,
                        new byte[TaskRuntimeFilterWire.MAX_BODY_BYTES + 1])).isEqualTo(413);
                assertThat(send(client, uri, TaskRuntimeFilterWire.MEDIA_TYPE, TaskRuntimeFilterWire.encode(
                        attempt.toBuilder().setTaskId("other").build(), update))).isEqualTo(409);
            } finally {
                server.stop();
            }
        }
    }

    private static int send(HttpClient client, URI uri, String type, byte[] bytes) throws Exception {
        var request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                .header("Content-Type", type).header("X-Internal-Auth", AUTH)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
        return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static RuntimeFilterBinding update() {
        return RuntimeFilterBinding.newBuilder().setSoleTableScan(true)
                .addFilters(RuntimeFilter.newBuilder().setColumnName("id")
                        .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER)
                        .setRange(RangeFilter.newBuilder()
                                .setLower(TypedValue.newBuilder().setInt64Value(1))
                                .setUpper(TypedValue.newBuilder().setInt64Value(2))
                                .setLowerInclusive(true).setUpperInclusive(true))).build();
    }
}
