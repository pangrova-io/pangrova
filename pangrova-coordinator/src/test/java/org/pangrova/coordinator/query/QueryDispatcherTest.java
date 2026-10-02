package org.pangrova.coordinator.query;

import java.util.Map;
import java.util.Set;
import java.time.Instant;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.apache.calcite.rel.RelNode;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Unit tests for {@link QueryDispatcher}.
 * Uses real HTTP communication — WorkerHttpServer on random port.
 * Uses real SqlCompiler (Calcite + Substrait) — no stubs.
 * Validates: R1.1-R1.6, AC2, Requirement 17.5.
 */
@org.junit.jupiter.api.Tag("native-required")
class QueryDispatcherTest {

    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);

    private SqlCompiler compiler;
    private WorkerManager workerManager;
    private TaskScheduler taskScheduler;
    private RemoteTaskDispatcher httpDispatcher;
    private QueryDispatcher dispatcher;
    private org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture worker;
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        compiler = new SqlCompiler();
        worker = new org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture("w1", nativeDirectory);
        httpDispatcher = testDispatcher();
        taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(worker.workerInfo());
        var tracker = new CoordinatorTaskTracker();
        var deps = QueryDispatcherContext.withDefaults(compiler, null, null, null, httpDispatcher, taskScheduler, workerManager,
                tracker, null, null, null, 30L);
        dispatcher = new QueryDispatcher(withPolicies(deps), null, null);
        worker.connect(dispatcher, tracker, workerManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        try { if (worker != null) worker.close(); }
        finally { if (httpDispatcher != null) httpDispatcher.close(); }
    }

    @Test
    void happyPath_compileScheduleDispatch_reachesRunning() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = testSession("q-1");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1", session);

        assertNotNull(result);
        assertEquals("localhost", result.workerHost());
        assertEquals(QueryStateMachine.State.RUNNING, sm.getState());
    }

    @Test
    void compileFailure_propagatesWithoutOwningTerminalTransition() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = testSession("q-2");

        assertThrows(RuntimeException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));
        // QueryManager owns the terminal transition after dispatch errors.
        assertEquals(QueryStateMachine.State.PLANNING, sm.getState());
    }

    @Test
    void noAvailableWorker_preservesErrorClassification() {
        WorkerManager emptyWm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        try (RemoteTaskDispatcher httpDispatcher = testDispatcher()) {
        var emptyDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, httpDispatcher, taskScheduler, emptyWm,
                new CoordinatorTaskTracker(), null, null, null, 0L);
        QueryDispatcher emptyDispatcher = new QueryDispatcher(withPolicies(emptyDeps), null, null);

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = testSession("q-3");

        // A valid constant query still requires Worker dispatch without a result service.
        NoAvailableWorkerException failure = assertThrows(NoAvailableWorkerException.class,
                () -> emptyDispatcher.execute(sm, "SELECT 1", session));
        assertTrue(emptyWm.getActiveWorkers().isEmpty());
        assertTrue(failure.getMessage().contains("No available worker"));
        // Direct dispatcher does not own the QueryManager terminal transition.
        assertEquals(QueryStateMachine.State.SCHEDULING, sm.getState());
        }
    }

    private static RemoteTaskDispatcher testDispatcher() {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        return dispatcher;
    }

    private static CoordinatorSessionContext testSession(String queryId) {
        return new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "test-tenant");
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
        CoordinatorSessionContext session = testSession("const-1");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1", session);

        assertNotNull(result);
        assertEquals(QueryStateMachine.State.RUNNING, sm.getState());
        assertEquals(1, worker.rows(result).size());
        assertEquals(1, worker.rows(result).get(0).size());
        assertEquals(1, ((Number) worker.rows(result).get(0).get(0)).intValue());
    }
}
