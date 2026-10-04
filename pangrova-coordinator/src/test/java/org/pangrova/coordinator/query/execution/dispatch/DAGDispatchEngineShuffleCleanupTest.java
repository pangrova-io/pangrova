package org.pangrova.coordinator.query.execution.dispatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleStageHandler;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests shuffle resource cleanup on dispatch failure paths.
 */
class DAGDispatchEngineShuffleCleanupTest {

    @Test
    @DisplayName("dispatch failure notifies shuffle cleanup exactly once")
    void dispatchCatchBlockCleansUpShuffle() {
        RecordingShuffleManager recorder = new RecordingShuffleManager();
        DAGDispatchEngine engine = buildEngine(new FailingTaskDispatcher(), recorder);

        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-d29", Map.of(), null, null);
        StageDAG dag = buildSingleStageDAG("q-d29");
        SqlCompiler compiler = new SqlCompiler();

        assertThrows(Exception.class, () ->
                engine.dispatch(session, dag, compiler, testWorkerSelector(), 5000L));

        assertEquals(List.of("q-d29"), recorder.cleanupCalls);
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
        public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
            return ShuffleHandle.LOCAL;
        }

        @Override
        public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
            return ShuffleCommitResult.EMPTY;
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
                    new ResultEndpoint("localhost", 8080, "ticket-noop", "http"));
        }
    }

    // --- helpers ---

    private static DAGDispatchEngine buildEngine(TaskDispatcher dispatcher,
                                                  ShuffleLifecycleManager shuffleManager) {
        DAGDispatchContext deps = new DAGDispatchContext(
                dispatcher,
                new CoordinatorTaskTracker(),
                null,  // lineageEmitter
                null,  // catalogRegistry
                30L,   // taskTimeoutSeconds
                null,  // adaptiveQueryExecutor
                null,  // runtimeFilterRegistry
                null,  // adaptiveQueryConfig
                null,  // speculativeStageHandler
                PipelineConfig.DISABLED,
                shuffleManager,
                new SplitAssignmentManager(),
                null,  // resilienceRegistry
                null); // stateJournal
        return new DAGDispatchEngine(
                deps, List.of(new ShuffleStageHandler(shuffleManager)), null);
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

    private static org.pangrova.planner.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
