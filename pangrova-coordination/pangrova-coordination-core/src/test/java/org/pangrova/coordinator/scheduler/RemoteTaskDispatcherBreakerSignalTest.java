package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.failure.TaskDispatchException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.observability.api.TracingProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for F-RESIL-02: the dispatcher must feed worker-down signals
 * (NETWORK / RPC_TIMEOUT) to the circuit breaker on BOTH the synchronous and the
 * asynchronous dispatch paths, while NOT counting WORKER_BUSY (503 backpressure).
 */
class RemoteTaskDispatcherBreakerSignalTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    /** Records every recordFailure/recordSuccess call. allowRequest always permits. */
    private static final class RecordingPolicy implements CircuitBreakerPolicy {
        final List<String> failures = new CopyOnWriteArrayList<>();
        final List<String> successes = new CopyOnWriteArrayList<>();

        @Override
        public boolean allowRequest(String target) {
            return true;
        }

        @Override
        public void recordSuccess(String target) {
            successes.add(target);
        }

        @Override
        public void recordFailure(String target, Throwable cause) {
            failures.add(target);
        }
    }

    @Test
    void asyncDispatch_networkFailure_recordsBreakerFailure() throws Exception {
        RecordingPolicy policy = new RecordingPolicy();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(policy, TRACING);

        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(dummyRequest(), closedPortWorker());

        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TaskDispatchException.class);

        assertThat(policy.failures)
                .as("async NETWORK failure must be recorded as a circuit-breaker failure")
                .isNotEmpty();
    }

    @Test
    void syncDispatch_networkFailure_recordsBreakerFailure() {
        RecordingPolicy policy = new RecordingPolicy();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(policy, TRACING);

        assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), closedPortWorker()))
                .isInstanceOf(TaskDispatchException.class);

        assertThat(policy.failures)
                .as("sync NETWORK failure must be recorded as a circuit-breaker failure")
                .isNotEmpty();
    }

    @Test
    void asyncDispatch_workerBusy503_doesNotRecordBreakerFailure() throws Exception {
        RecordingPolicy policy = new RecordingPolicy();
        HttpServer server = start503Server();
        try {
            int port = server.getAddress().getPort();
            RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(policy, TRACING);

            CompletableFuture<DispatchResult> future =
                    dispatcher.dispatchAsync(dummyRequest(), workerAt("127.0.0.1", port));

            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TaskDispatchException.class);

            assertThat(policy.failures)
                    .as("WORKER_BUSY (503 backpressure) must NOT trip the circuit breaker")
                    .isEmpty();
            // F-RESIL-05: WORKER_BUSY must still settle the permit (as success)
            // to prevent half-open permit leak
            assertThat(policy.successes)
                    .as("WORKER_BUSY must settle the permit as success (worker is alive)")
                    .isNotEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void syncDispatch_workerBusy503_doesNotRecordBreakerFailure() {
        RecordingPolicy policy = new RecordingPolicy();
        HttpServer server = start503Server();
        try {
            int port = server.getAddress().getPort();
            RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(policy, TRACING);

            assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), workerAt("127.0.0.1", port)))
                    .isInstanceOf(TaskDispatchException.class);

            assertThat(policy.failures)
                    .as("WORKER_BUSY (503 backpressure) must NOT trip the circuit breaker")
                    .isEmpty();
            // F-RESIL-05: WORKER_BUSY must still settle the permit (as success)
            // to prevent half-open permit leak
            assertThat(policy.successes)
                    .as("WORKER_BUSY must settle the permit as success (worker is alive)")
                    .isNotEmpty();
        } finally {
            server.stop(0);
        }
    }

    // ── Helpers ──

    private static HttpServer start503Server() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/task", exchange -> {
                byte[] resp = "worker busy".getBytes();
                exchange.sendResponseHeaders(503, resp.length);
                exchange.getResponseBody().write(resp);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns a worker pointing at a port that nothing is listening on (connect refused). */
    private static WorkerInfo closedPortWorker() {
        return workerAt("127.0.0.1", closedPort());
    }

    private static int closedPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SubmitTaskRequest dummyRequest() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-sig-1", "q-sig", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static WorkerInfo workerAt(String host, int httpPort) {
        return new WorkerInfo(
                "w-sig", host, httpPort, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-sig"));
    }
}
