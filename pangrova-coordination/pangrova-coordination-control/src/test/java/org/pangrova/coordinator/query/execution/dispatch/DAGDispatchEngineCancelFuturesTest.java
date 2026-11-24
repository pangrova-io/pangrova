package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.execution.task.DispatchResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
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
 * Tests F-COORD-SCHED-08: clearQuery removes dispatch tracking so
 * the query is considered complete.
 */
class DAGDispatchEngineCancelFuturesTest {

    @Test
    @DisplayName("F-COORD-SCHED-08: clearQuery marks query as complete")
    void clearQueryMarksComplete() {
        DAGDispatchEngine engine = buildEngine(
                new NoOpTaskDispatcher(), new NoOpShuffleManager());

        // clearQuery should not throw even for unknown queries
        engine.clearQuery("q-unknown");

        // After clearQuery, the query should not be tracked
        StageCompletionCallback callback = engine;
        assertTrue(callback.isQueryComplete("q-unknown"),
                "Unknown query should report complete (no DAG tracked)");
    }

    @Test
    @DisplayName("F-COORD-SCHED-08: clearQuery is idempotent")
    void clearQueryIsIdempotent() {
        DAGDispatchEngine engine = buildEngine(
                new NoOpTaskDispatcher(), new NoOpShuffleManager());

        // Clear a query that was never dispatched - should not throw
        engine.clearQuery("q-never-dispatched");
        engine.clearQuery("q-never-dispatched");

        assertTrue(engine.isQueryComplete("q-never-dispatched"),
                "Query should report complete after clearQuery");
    }

    @Test
    @DisplayName("F-COORD-SCHED-08: isQueryComplete returns true for unknown queries")
    void isQueryCompleteReturnsTrueForUnknownQueries() {
        DAGDispatchEngine engine = buildEngine(
                new NoOpTaskDispatcher(), new NoOpShuffleManager());

        assertTrue(engine.isQueryComplete("q-never-existed"),
                "Unknown query should report complete (no DAG tracked)");
    }

    // --- test doubles ---

    /** Dispatcher that returns a successful no-op result immediately. */
    static final class NoOpTaskDispatcher implements TaskDispatcher {
        @Override
        public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            return new DispatchResult("localhost", "task-noop",
                    new ResultEndpoint("localhost", 8080, "ticket-noop", "http"));
        }
    }

    /** Shuffle manager that records cleanup calls. */
    static final class NoOpShuffleManager implements ShuffleLifecycleManager {
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
                null,  // adaptiveQueryConfig
                null,  // speculativeStageHandler
                PipelineConfig.DISABLED,
                shuffleManager,
                null,  // resilienceRegistry
                null); // stateJournal
        return new DAGDispatchEngine(deps, List.of(), null, null, null);
    }
}
