package org.pangrova.coordinator.query;

import java.util.Map;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.testing.TestObservability;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.apache.calcite.rel.RelNode;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link QueryDispatcher}.
 * Uses real HTTP communication — WorkerHttpServer on random port.
 * Uses real SqlCompiler (Calcite + Substrait) — no stubs.
 * Validates: R1.1-R1.6, AC2, Requirement 17.5.
 */
class QueryDispatcherTest {

    private static final int TRANSPORT_PORT = 0;

    private SqlCompiler compiler;
    private WorkerManager workerManager;
    private TaskScheduler taskScheduler;
    private TaskManager taskManager;
    private WorkerHttpServer workerHttpServer;
    private QueryDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        compiler = new SqlCompiler();
        taskManager = TaskManager.forHttpWiringOnly();

        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        workerHttpServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerHttpServer.start();

        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(activeWorker("w1", workerHttpServer.getPort()));
        var deps = QueryDispatcherContext.withDefaults(compiler, null, null, null, httpDispatcher, taskScheduler, workerManager,
                new CoordinatorTaskTracker(), null, null, null, null, 0L);
        dispatcher = new QueryDispatcher(deps, null, null);

        var dagEngine = dispatcher.dagEngine();
        taskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerHttpServer != null) {
            workerHttpServer.stop();
        }
    }

    @Test
    void happyPath_compileScheduleDispatch_reachesRunning() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-1", Map.of(), null, null);

        DispatchResult result = dispatcher.execute(sm, "SELECT 1", session);

        assertNotNull(result);
        assertEquals("localhost", result.workerHost());
        assertEquals(QueryStateMachine.State.RUNNING, sm.getState());
    }

    @Test
    void compileFailure_reachesFailed() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-2", Map.of(), null, null);

        assertThrows(RuntimeException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));
        assertEquals(QueryStateMachine.State.FAILED, sm.getState());
    }

    @Test
    void noAvailableWorker_reachesFailedWithException() {
        WorkerManager emptyWm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        var emptyDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, httpDispatcher, taskScheduler, emptyWm,
                new CoordinatorTaskTracker(), null, null, null, null, 0L);
        QueryDispatcher emptyDispatcher = new QueryDispatcher(emptyDeps, null, null);

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-3", Map.of(), null, null);

        // Constant queries (SELECT 1) complete locally when no workers and no resultService.
        // Use a non-constant query to trigger worker selection path.
        assertThrows(RuntimeException.class,
                () -> emptyDispatcher.execute(sm, "SELECT * FROM nonexistent_table", session));
        assertEquals(QueryStateMachine.State.FAILED, sm.getState());
    }

    private static WorkerInfo activeWorker(String workerId, int httpPort) {
        return TestWorkerInfoBuilder.active(workerId, "localhost", httpPort, TRANSPORT_PORT);
    }

    // --- Constant folding detection tests (via PangrovaPlanner.isFoldedConstant) ---

    @Test
    void isFoldedConstant_afterPlannerFold_returnsTrue() {
        // PangrovaPlanner.tryFoldConstant converts constant expressions to LogicalValues
        RelNode relNode = compiler.compileToRelNode("SELECT 1");
        // Without planner fold, raw Calcite output may or may not be LogicalValues
        // This test validates the static utility on a known LogicalValues node
        // The planner integration is tested in PangrovaPlannerConstantFoldTest
        boolean result = PangrovaPlanner.isFoldedConstant(relNode);
        // Raw Calcite compile of "SELECT 1" produces LogicalValues, so this should be true
        assertTrue(result);
    }

    @Test
    void execute_constantQuery_dispatchesToWorker_whenNoResultService() {
        // Without CoordinatorResultService, constant queries still dispatch to worker
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("const-1", Map.of(), null, null);

        DispatchResult result = dispatcher.execute(sm, "SELECT 1", session);

        assertNotNull(result);
        assertEquals(QueryStateMachine.State.RUNNING, sm.getState());
    }
}
