package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGCompletionEvaluator;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.execution.task.DispatchResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShufflePublication;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies logical dispatch failures leave Shuffle with the physical execution owner.
 */
class DAGDispatchEngineShuffleCleanupTest {

    @Test
    @DisplayName("dispatch failure leaves physical Shuffle cleanup with the query owner")
    void dispatchFailureDoesNotDeleteShuffle() {
        RecordingShuffleManager recorder = new RecordingShuffleManager();
        DAGDispatchEngine engine = buildEngine(new FailingTaskDispatcher(), recorder,
                List.of(new ShuffleStageHandler(recorder)));

        QuerySession session = SourceFreeQueryFixture.session("q-d29");
        StageDAG dag = buildSingleStageDAG("q-d29");
        SqlCompiler compiler = new SqlCompiler();

        try (engine) {
            assertThrows(Exception.class, () -> await(engine, session, dag, compiler));
            assertTrue(recorder.cleanupCalls.isEmpty());
        }
    }

    @Test
    void stageLinkageFailureRetainsItsCauseWithoutDeletingShuffle() {
        var recorder = new RecordingShuffleManager();
        var session = SourceFreeQueryFixture.session("q-linkage");
        var dag = buildSingleStageDAG(session.queryId());
        var missingClass = new NoClassDefFoundError("org/pangrova/planner/exchange/ExchangeInputLeafDetail");
        StageLifecycleHandler failPreparation = event -> { throw missingClass; };
        try (var engine = buildEngine(new NoOpTaskDispatcher(), recorder,
                List.of(new ShuffleStageHandler(recorder), failPreparation))) {
            var failure = assertThrows(PangrovaException.class, () ->
                    await(engine, session, dag, new SqlCompiler()));
            assertEquals(PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED, failure.getErrorCode());
            assertSame(missingClass, failure.getCause());
            assertEquals(StageState.FAILED, dag.findStage(1).stateMachine().getState());
            assertEquals(0, dag.findStage(1).retryCount());
            assertTrue(recorder.cleanupCalls.isEmpty());
        }
    }

    private static DispatchResult await(DAGDispatchEngine engine, QuerySession session, StageDAG dag,
            SqlCompiler compiler) {
        return DAGCompletionEvaluator.awaitResult(engine.start(session, dag, compiler, testWorkerSelector(),
                5_000L, QueryExecutionScope.readOnly(session), ignored -> { }),
                session.queryId(), () -> engine.cancelLiveDispatch(session.queryId()));
    }

    @Test
    void dispatchDeadlineSettlesTheOwnedFutureWithoutAWaitingThread() {
        var completion = new CompletableFuture<DispatchResult>();
        var timeout = new PangrovaException(PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT, "dispatch timed out");
        try (var engine = buildEngine(new NoOpTaskDispatcher(), new RecordingShuffleManager(), List.of())) {
            engine.armDeadline(completion, 0, () -> timeout);
            var failed = assertThrows(ExecutionException.class, () -> completion.get(5, TimeUnit.SECONDS));
            assertSame(timeout, failed.getCause());
            assertTrue(completion.isCompletedExceptionally());
        }
    }

    @Test
    @DisplayName("D20+D29: cleanup is idempotent — multiple calls do not throw")
    void cleanupIsIdempotent() {
        RecordingShuffleManager recorder = new RecordingShuffleManager();
        recorder.cleanupShuffle("q-idem");
        recorder.cleanupShuffle("q-idem");

        assertEquals(2, recorder.cleanupCalls.stream()
                .filter("q-idem"::equals).count());
    }

    // --- test doubles (no mock frameworks) ---

    /** Records all shuffle lifecycle calls for verification. */
    static final class RecordingShuffleManager implements ShuffleLifecycleManager {
        final List<String> cleanupCalls = new CopyOnWriteArrayList<>();

        @Override
        public ShuffleCapabilityDescriptor capabilities() {
            return ShuffleCapabilityDescriptor.none();
        }

        @Override
        public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
            throw new AssertionError("This Direct-path fixture must not register an external shuffle");
        }

        @Override
        public ShuffleCommitReceipt commitShuffle(ShufflePublication publication) {
            throw new UnsupportedOperationException("Test lifecycle only exercises cancellation and cleanup");
        }

        @Override
        public void cleanupShuffle(String queryId) {
            cleanupCalls.add(queryId);
        }
    }

    /** Dispatcher that always throws to trigger the dispatch() catch block. */
    static final class FailingTaskDispatcher implements TaskDispatcher {
        @Override
        public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            throw new RuntimeException("Simulated dispatch failure");
        }

        @Override
        public CompletableFuture<DispatchResult> dispatchAsync(
                SubmitTaskRequest request, WorkerInfo worker) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("Simulated async dispatch failure"));
        }
    }

    /** Dispatcher that returns a successful no-op result. */
    static final class NoOpTaskDispatcher implements TaskDispatcher {
        @Override
        public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            return new DispatchResult("localhost", "task-noop",
                    new ResultEndpoint("localhost", 8080, "ticket-noop", "grpc"));
        }
    }

    // --- helpers ---

    private static DAGDispatchEngine buildEngine(TaskDispatcher dispatcher, ShuffleLifecycleManager shuffleManager,
            List<StageLifecycleHandler> handlers) {
        DAGDispatchContext deps = new DAGDispatchContext(
                dispatcher,
                new CoordinatorTaskTracker(),
                null,  // lineageEmitter
                null,  // catalogRegistry
                30L,   // taskTimeoutSeconds
                null,  // adaptiveQueryConfig
                null,  // speculativeStageHandler
                PipelineConfig.DISABLED,
                shuffleManager,
                null,  // resilienceRegistry
                null); // stateJournal
        return new DAGDispatchEngine(
                deps, handlers, null, null, null);
    }

    private static StageDAG buildSingleStageDAG(String queryId) {
        StageNode output = new StageNode(1, dummyPlan(), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(List.of(output), Map.of(), List.of()).withStages(queryId);
    }

    private static WorkerSelector testWorkerSelector() {
        return new WorkerSelector() {
            @Override
            public WorkerInfo select(String tenantId) {
                return TestWorkerInfoBuilder.active("w-test", "localhost", 8080, 9090);
            }

            @Override
            public StageResourceGrant computeResourceLimit(
                    int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
                return StageResourceGrant.newBuilder()
                        .setGrantId("unbound-test-grant")
                        .setNativeMemoryBytes(1024)
                        .setCpuSlots(1)
                        .build();
            }
        };
    }

    private static org.pangrova.planner.analysis.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
