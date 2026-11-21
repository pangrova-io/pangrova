package org.pangrova.coordinator.scheduler;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.coordinator.resilience.breaker.CircuitBreaker;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests async dispatch in RemoteTaskDispatcher using real HTTP.
 * Validates: Req 2.2 — async HTTP dispatch via sendAsync.
 * No mocks — real WorkerHttpServer, real HttpClient.
 */
class RemoteTaskDispatcherAsyncTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    private WorkerHttpServer workerServer;
    private TaskManager taskManager;
    private RemoteTaskDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
        workerServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerServer.start();
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerServer != null) {
            workerServer.stop();
        }
        if (taskManager != null) {
            taskManager.close();
        }
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    @Test
    void dispatchAsync_returnsCompletableFutureWithResult() throws Exception {
        WorkerInfo worker = activeWorker(workerServer.getPort());
        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(dummyRequest(), worker);

        DispatchResult result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("localhost");
        assertThat(result.taskId()).isNotBlank();
    }

    @Test
    void dispatchAsync_nonBlocking_returnsImmediately() {
        WorkerInfo worker = activeWorker(workerServer.getPort());
        long start = System.nanoTime();

        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(dummyRequest(), worker);

        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(future).isNotNull();
        assertThat(elapsed).isLessThan(500);
    }

    @Test
    void dispatchAsync_circuitOpen_returnsFailedFuture() {
        CircuitBreaker breaker = forceOpenBreaker();
        RemoteTaskDispatcher cbDispatcher = new RemoteTaskDispatcher(breaker, TRACING);

        CompletableFuture<DispatchResult> future =
                cbDispatcher.dispatchAsync(dummyRequest(), unreachableWorker());

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    void dispatchAsync_unreachableWorker_completesExceptionally() {
        CompletableFuture<DispatchResult> future =
                dispatcher.dispatchAsync(dummyRequest(), unreachableWorker());

        assertThat(future).isNotNull();
        try {
            future.get(10, TimeUnit.SECONDS);
            assertThat(false).as("Expected exception").isTrue();
        } catch (Exception e) {
            assertThat(e).isInstanceOf(ExecutionException.class);
        }
    }

    // ── Helpers ──

    private static org.pangrova.protocol.rest.task.SubmitTaskRequest dummyRequest() {
        return org.pangrova.protocol.rest.task.SubmitTaskRequest.from(
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                "task-async-1", "q-async", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                                .setGrantId("task-async-1:grant")
                                .setCpuSlots(1).setNativeMemoryBytes(64L * 1024 * 1024))
                        .build());
    }

    private WorkerInfo activeWorker(int httpPort) {
        return new WorkerInfo(
                "w-1", "localhost", httpPort, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                taskManager.workerExecutionIdentity());
    }

    private static WorkerInfo unreachableWorker() {
        return new WorkerInfo(
                "w-bad", "unreachable-host", 99999, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-bad"));
    }

    private static CircuitBreaker forceOpenBreaker() {
        CircuitBreaker breaker = new CircuitBreaker(0.01, 1, 60_000, 3, java.time.Clock.systemUTC());
        breaker.onFailure();
        return breaker;
    }
}
