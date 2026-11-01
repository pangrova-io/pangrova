package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.protocol.resource.StageResourceGrantCodec;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.planner.SqlCompiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Verifies that QueryDispatcher propagates non-null, non-zero
 * TaskResourceLimit through the full dispatch path (REQ-M3).
 * Uses real HTTP dispatch — WorkerHttpServer on random port.
 */
@org.junit.jupiter.api.Tag("native-required")
class TaskResourceLimitPropagationTest {

    private static final long WORKER_MEMORY_BYTES =
            org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture.MEMORY_BYTES;
    private static final TracingProvider TRACING = TestObservability.TRACING;
    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);

    private RemoteTaskDispatcher httpDispatcher;
    private QueryDispatcher orchestrator;
    private RecordingDispatcher recordingDispatcher;
    private org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture worker;
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        worker = new org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture("w1", nativeDirectory);

        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(worker.workerInfo());

        httpDispatcher = new RemoteTaskDispatcher(
                new org.pangrova.coordinator.resilience.breaker.CircuitBreaker(), TRACING);
        httpDispatcher.setControlEpochSupplier(() -> 1L);
        httpDispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        recordingDispatcher = new RecordingDispatcher(httpDispatcher);

        var tracker = new CoordinatorTaskTracker();
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, recordingDispatcher,
                new TaskScheduler(new WorkerPerformanceScorer()),
                workerManager, tracker, null, null, null, 30L);
        orchestrator = new QueryDispatcher(withPolicies(deps), null, null);
        worker.connect(orchestrator, tracker, workerManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        try { if (worker != null) worker.close(); }
        finally { if (httpDispatcher != null) httpDispatcher.close(); }
    }

    @Test
    void singleStageDispatch_resourceLimitIsNonNull() {
        CoordinatorSessionContext session = testSession("rl-q1");
        QueryStateMachine sm = new QueryStateMachine();

        worker.execute(orchestrator, sm, "SELECT 1", session);

        assertThat(recordingDispatcher.capturedRequests()).hasSize(1);
        StageResourceGrant limit = grant(recordingDispatcher.capturedRequests().get(0));
        assertThat(limit).isNotNull();
        assertThat(limit.getNativeMemoryBytes()).isGreaterThan(0);
        assertThat(limit.getCpuSlots()).isGreaterThan(0);
    }

    @Test
    void singleStageDispatch_memoryDerivedFromClusterCapacity() {
        CoordinatorSessionContext session = testSession("rl-q2");
        QueryStateMachine sm = new QueryStateMachine();

        worker.execute(orchestrator, sm, "SELECT 1 + 2", session);

        StageResourceGrant limit = grant(recordingDispatcher.capturedRequests().get(0));
        // Single stage receives the worker budget, split between Velox and
        // its independently owned serialized shuffle queue.
        assertThat(limit.getNativeMemoryBytes() + limit.getShuffleBufferBytes())
                .isEqualTo(WORKER_MEMORY_BYTES);
    }

    @Test
    void resourceLimitConversion_protocolToSpi_preservesValues() {
        StageResourceGrant protocol = StageResourceGrant.newBuilder()
                .setGrantId("conversion-test")
                .setNativeMemoryBytes(2048L)
                .setCpuSlots(8)
                .setSpillBytes(4096L)
                .build();

        TaskResourceLimit spi = org.pangrova.protocol.rest.task.convert
                .TaskRequestDtoConverter.toResourceLimit(protocol);

        assertThat(spi.maxMemoryBytes()).isEqualTo(2048L);
        assertThat(spi.maxThreads()).isEqualTo(8);
        assertThat(spi.maxSpillBytes()).isEqualTo(4096L);
        assertThat(spi.spillDirectory()).isEmpty();
    }

    @Test
    void resourceLimitConversion_nullProtocol_returnsNone() {
        TaskResourceLimit spi = org.pangrova.protocol.rest.task.convert
                .TaskRequestDtoConverter.toResourceLimit(null);

        assertThat(spi).isEqualTo(TaskResourceLimit.NONE);
    }

    @Test
    void fullPipeline_workerReceivesNonZeroResourceLimit() {
        CoordinatorSessionContext session = testSession("rl-q3");
        QueryStateMachine sm = new QueryStateMachine();

        worker.execute(orchestrator, sm, "SELECT 42", session);

        // Verify the request that was dispatched has resource limits
        SubmitTaskRequest request = recordingDispatcher.capturedRequests().get(0);
        TaskDescriptor descriptor = TaskRequestDtoConverter.toTaskDescriptor(request);
        assertThat(descriptor.resourceLimit()).isNotEqualTo(TaskResourceLimit.NONE);
        assertThat(descriptor.resourceLimit().maxMemoryBytes()).isGreaterThan(0);
    }

    // ── Helpers ──

    private static CoordinatorSessionContext testSession(String queryId) {
        return new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "test-tenant");
    }

    private static StageResourceGrant grant(SubmitTaskRequest request) {
        return request.taskRequest().getResourceGrant();
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
        public SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
            return delegate.bindControlIdentity(request);
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
