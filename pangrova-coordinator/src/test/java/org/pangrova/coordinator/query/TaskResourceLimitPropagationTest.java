package org.pangrova.coordinator.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.testing.TestObservability;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.worker.task.convert.TaskRequestConverter;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that QueryDispatcher propagates non-null, non-zero
 * TaskResourceLimit through the full dispatch path (REQ-M3).
 * Uses real HTTP dispatch — WorkerHttpServer on random port.
 */
class TaskResourceLimitPropagationTest {

    private static final long WORKER_MEMORY_BYTES = 4L * 1024 * 1024 * 1024; // 4 GB
    private static final TracingProvider TRACING = TestObservability.TRACING;

    private WorkerHttpServer workerServer;
    private QueryDispatcher orchestrator;
    private RecordingDispatcher recordingDispatcher;

    @BeforeEach
    void setUp() throws Exception {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
        workerServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerServer.start();

        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerWithMemory("w1", workerServer.getPort()));

        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(
                new org.pangrova.resilience.breaker.CircuitBreaker(), TRACING);
        recordingDispatcher = new RecordingDispatcher(httpDispatcher);

        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, recordingDispatcher,
                new TaskScheduler(new WorkerPerformanceScorer()),
                workerManager, new CoordinatorTaskTracker(), null, null, null, null, 0L);
        orchestrator = new QueryDispatcher(deps, null, null);

        var dagEngine = orchestrator.dagEngine();
        taskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerServer != null) {
            workerServer.stop();
        }
    }

    @Test
    void singleStageDispatch_resourceLimitIsNonNull() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("rl-q1", Map.of(), null, null);
        QueryStateMachine sm = new QueryStateMachine();

        orchestrator.execute(sm, "SELECT 1", session);

        assertThat(recordingDispatcher.capturedRequests()).hasSize(1);
        ResourceLimitPayload limit = recordingDispatcher.capturedRequests().get(0).resourceLimit();
        assertThat(limit).isNotNull();
        assertThat(limit.maxMemoryBytes()).isGreaterThan(0);
        assertThat(limit.maxThreads()).isGreaterThan(0);
    }

    @Test
    void singleStageDispatch_memoryDerivedFromClusterCapacity() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("rl-q2", Map.of(), null, null);
        QueryStateMachine sm = new QueryStateMachine();

        orchestrator.execute(sm, "SELECT 1 + 2", session);

        ResourceLimitPayload limit = recordingDispatcher.capturedRequests().get(0).resourceLimit();
        // Single stage, parallelism=1, concurrentStages=1 → per-task = cluster total
        assertThat(limit.maxMemoryBytes()).isEqualTo(WORKER_MEMORY_BYTES);
    }

    @Test
    void resourceLimitConversion_protocolToSpi_preservesValues() {
        ResourceLimitPayload protocol = new ResourceLimitPayload(2048L, 8, 4096L, "/tmp/spill");

        TaskResourceLimit spi = TaskRequestConverter.toResourceLimit(protocol);

        assertThat(spi.maxMemoryBytes()).isEqualTo(2048L);
        assertThat(spi.maxThreads()).isEqualTo(8);
        assertThat(spi.maxSpillBytes()).isEqualTo(4096L);
        assertThat(spi.spillDirectory()).isEqualTo("/tmp/spill");
    }

    @Test
    void resourceLimitConversion_nullProtocol_returnsNone() {
        TaskResourceLimit spi = TaskRequestConverter.toResourceLimit(null);

        assertThat(spi).isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void fullPipeline_workerReceivesNonZeroResourceLimit() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("rl-q3", Map.of(), null, null);
        QueryStateMachine sm = new QueryStateMachine();

        orchestrator.execute(sm, "SELECT 42", session);

        // Verify the request that was dispatched has resource limits
        SubmitTaskRequest request = recordingDispatcher.capturedRequests().get(0);
        TaskDescriptor descriptor = TaskRequestConverter.toTaskDescriptor(request);
        assertThat(descriptor.resourceLimit()).isNotEqualTo(TaskResourceLimit.NONE);
        assertThat(descriptor.resourceLimit().maxMemoryBytes()).isGreaterThan(0);
    }

    // ── Helpers ──

    private static WorkerInfo workerWithMemory(String workerId, int httpPort) {
        return TestWorkerInfoBuilder.withMetrics(
                workerId, "localhost", httpPort, 0,
                0.1, 0, WORKER_MEMORY_BYTES, 0.9);
    }

    /**
     * Wraps a real TaskDispatcher, recording all dispatched requests for assertion.
     * Not a mock — delegates to the real HTTP dispatcher.
     */
    static final class RecordingDispatcher implements TaskDispatcher {

        private final TaskDispatcher delegate;
        private final CopyOnWriteArrayList<SubmitTaskRequest> captured = new CopyOnWriteArrayList<>();

        RecordingDispatcher(TaskDispatcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            captured.add(request);
            return delegate.dispatch(request, worker);
        }

        List<SubmitTaskRequest> capturedRequests() {
            return List.copyOf(captured);
        }
    }
}
