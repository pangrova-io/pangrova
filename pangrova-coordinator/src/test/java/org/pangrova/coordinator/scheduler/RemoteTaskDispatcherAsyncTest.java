package org.pangrova.coordinator.scheduler;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.resilience.breaker.CircuitBreaker;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.worker.http.WorkerHttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests async dispatch in RemoteTaskDispatcher using real HTTP.
 * Validates: Req 2.2 — async HTTP dispatch via sendAsync.
 * No mocks — real WorkerHttpServer, real HttpClient.
 */
@org.junit.jupiter.api.Tag("native-required")
class RemoteTaskDispatcherAsyncTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    private WorkerHttpServer workerServer;
    private RemoteTaskDispatcher dispatcher;
    private SourceFreeNativeWorkerFixture worker;
    private int closedPort;
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory;

    @BeforeEach
    void setUp() throws Exception {
        try (var endpoint = new java.net.ServerSocket(0)) {
            closedPort = endpoint.getLocalPort();
        }
        worker = new SourceFreeNativeWorkerFixture("w-1", directory);
        // This dispatcher component owns no Coordinator journal. Observe the real
        // collector reports without manufacturing task or query completion.
        worker.deliverReportsTo(report -> {
            assertThat(report.workerId()).isEqualTo("w-1");
            assertThat(report.attempt()).isNotNull();
            if (!"RUNNING".equals(report.state())) {
                assertThat(report.canonicalTaskReport()).isNotEmpty();
            }
        });
        workerServer = worker.httpServer();
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
    }

    @AfterEach
    void tearDown() throws Exception {
        try { if (worker != null) worker.close(); }
        finally { if (dispatcher != null) dispatcher.close(); }
    }

    @Test
    void dispatchAsync_returnsCompletableFutureWithResult() throws Exception {
        WorkerInfo worker = activeWorker(workerServer.getPort());
        var request = actualRequest();
        CompletableFuture<DispatchResult> future = dispatcher.dispatchAsync(request, worker);

        DispatchResult result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("localhost");
        assertThat(result.taskId()).isNotBlank();
        assertThat(result.resultEndpoint().ticket()).isEqualTo(this.worker.resultTicket(request.taskRequest()));
        consumeActualResult(request, result);
    }

    @Test
    void dispatchAsync_nonBlocking_returnsImmediately() throws Exception {
        WorkerInfo worker = activeWorker(workerServer.getPort());
        var request = actualRequest();
        long start = System.nanoTime();

        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(request, worker);

        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(future).isNotNull();
        assertThat(elapsed).isLessThan(500);
        consumeActualResult(request, future.get(5, TimeUnit.SECONDS));
    }

    @Test
    void dispatchAsync_circuitOpen_returnsFailedFuture() {
        CircuitBreaker breaker = forceOpenBreaker();
        try (var cbDispatcher = new RemoteTaskDispatcher(breaker, TRACING)) {
            var future = cbDispatcher.dispatchAsync(actualRequest(), unreachableWorker());
            assertThat(future).isCompletedExceptionally();
            assertThat(worker.reports()).isEmpty();
        }
    }

    @Test
    void dispatchAsync_unreachableWorker_completesExceptionally() {
        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(actualRequest(), unreachableWorker());

        assertThat(future).isNotNull();
        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        assertThat(worker.reports()).isEmpty();
    }

    // ── Helpers ──

    private org.pangrova.protocol.rest.task.SubmitTaskRequest actualRequest() {
        return org.pangrova.protocol.rest.task.SubmitTaskRequest.from(
                ActualNativeTaskFixture.valuesTaskRequest(true));
    }

    private void consumeActualResult(org.pangrova.protocol.rest.task.SubmitTaskRequest request,
                                     DispatchResult result) throws Exception {
        worker.consume(result.resultEndpoint());
        assertThat(worker.rows(result)).containsExactly(List.of(42L));
        assertThat(result.taskId()).isEqualTo(request.taskRequest().getAttempt().getTaskId());
        String localTaskId = worker.taskManager().resolveWorkerTaskId(result.taskId());
        assertThat(localTaskId).isNotNull();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(worker.taskManager().getTaskState(localTaskId))
                        .contains(org.pangrova.worker.task.TaskManager.TaskState.FINISHED));
        worker.taskManager().releaseTaskExecutor(localTaskId);
        assertThat(worker.taskManager().getExecutorHandle(localTaskId)).isEmpty();
        assertThat(worker.reports()).anySatisfy(report -> {
            assertThat(report.taskId()).isEqualTo(request.taskRequest().getAttempt().getTaskId());
            assertThat(report.canonicalTaskReport()).isNotEmpty();
            assertThat(org.pangrova.protocol.v2.TaskReport.parseFrom(report.canonicalTaskReport())
                    .getExecutionDrainState()).isEqualTo(org.pangrova.protocol.v2.ExecutionDrainState
                            .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
        });
    }

    private static WorkerInfo activeWorker(int httpPort) {
        return new WorkerInfo(
                "w-1", "localhost", httpPort, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory());
    }

    private WorkerInfo unreachableWorker() {
        return new WorkerInfo(
                "w-bad", "127.0.0.1", closedPort, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory());
    }

    private static CircuitBreaker forceOpenBreaker() {
        CircuitBreaker breaker = new CircuitBreaker(0.01, 1, 60_000, 3, java.time.Clock.systemUTC());
        breaker.onFailure();
        return breaker;
    }
}
