package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestObservability;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies BUG-D09 fix: {@link QueryDispatcher#cleanupQuery(String)} releases
 * HA query ownership via {@link HighAvailabilityCoordinationService#releaseQueryOwnership}.
 *
 * <p>Uses real {@link HighAvailabilityCoordinationService} (in-memory lease)
 * and real HTTP dispatch to validate the full cleanup path.</p>
 */
class QueryDispatcherOwnershipReleaseTest {

    private static final String COORDINATOR_ID = "coord-1";
    private static final int TRANSPORT_PORT = 0;

    private HighAvailabilityCoordinationService haService;
    private QueryDispatcher dispatcher;
    private WorkerHttpServer workerHttpServer;

    @BeforeEach
    void setUp() throws Exception {
        haService = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());

        SqlCompiler compiler = new SqlCompiler();
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var httpConfig = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        workerHttpServer = new WorkerHttpServer(httpConfig, taskManager, TestExchangeTransport.INSTANCE);
        workerHttpServer.start();

        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(activeWorker("w1", workerHttpServer.getPort()));

        var deps = new QueryDispatcherContext(
                compiler,
                new org.pangrova.coordinator.query.execution.SecurityContextBundle(null, null, null),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        httpDispatcher, taskScheduler, workerManager, new CoordinatorTaskTracker(),
                        null, null, null, null),
                org.pangrova.coordinator.query.execution.CacheContextBundle.none(),
                null, null, null, 0L,
                null, null, null, null, haService, COORDINATOR_ID, null, null);
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
        if (haService != null) {
            haService.close();
        }
    }

    @Test
    void cleanupQuery_releasesOwnership() {
        String queryId = "q-ownership-1";

        // Acquire ownership
        boolean acquired = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(acquired, "ownership should be acquired");

        // cleanupQuery should release it
        dispatcher.cleanupQuery(queryId);

        // Verify ownership was released — re-acquiring should succeed
        boolean reacquired = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(reacquired, "ownership should be re-acquirable after cleanup released it");
    }

    @Test
    void cleanupQuery_toleratesNoOwnership() {
        // cleanupQuery on a query that was never owned should not throw
        dispatcher.cleanupQuery("q-never-owned");
    }

    @Test
    void cleanupQuery_withoutHaService_doesNotThrow() {
        // Dispatcher without HA wiring should still clean up without error
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()), new CoordinatorTaskTracker(),
                null, null, null, null, 0L);
        QueryDispatcher noHaDispatcher = new QueryDispatcher(deps, null, null);
        // No enableOwnershipTracking called — haService is null
        noHaDispatcher.cleanupQuery("q-no-ha");
    }

    @Test
    void execute_thenCleanup_releasesOwnership() {
        String queryId = "q-exec-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        // Acquire ownership before dispatch (simulating QueryManager behavior)
        haService.acquireQueryOwnership(queryId, COORDINATOR_ID);

        // Execute a constant query (SELECT 1) — completes locally
        QueryStateMachine sm = new QueryStateMachine();
        var result = dispatcher.execute(sm, "SELECT 1", session);
        assertNotNull(result);

        // Simulate query completion cleanup
        dispatcher.cleanupQuery(queryId);

        // Ownership should be released
        boolean canReacquire = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(canReacquire, "ownership should be released after query completion cleanup");
    }

    @Test
    void failedQuery_cleanupInCatchBlock_releasesOwnership() {
        String queryId = "q-fail-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        // Acquire ownership
        haService.acquireQueryOwnership(queryId, COORDINATOR_ID);

        // Execute invalid SQL — triggers the catch block in execute() which calls cleanupQuery
        QueryStateMachine sm = new QueryStateMachine();
        try {
            dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session);
        } catch (RuntimeException expected) {
            // expected
        }

        // The catch block in execute() calls cleanupQuery, which should release ownership
        boolean canReacquire = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(canReacquire, "ownership should be released after failed query cleanup");
    }

    private static WorkerInfo activeWorker(String workerId, int httpPort) {
        return TestWorkerInfoBuilder.active(workerId, "localhost", httpPort, TRANSPORT_PORT);
    }
}
