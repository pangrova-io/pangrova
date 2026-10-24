package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.TestObservability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Verifies BUG-D09 fix: {@link QueryDispatcher#cleanupQuery(String)} releases
 * HA query ownership via {@link HighAvailabilityCoordinationService#releaseQueryOwnership}.
 *
 * <p>Uses real {@link HighAvailabilityCoordinationService} (in-memory lease)
 * and real HTTP dispatch to validate the full cleanup path.</p>
 */
@org.junit.jupiter.api.Tag("native-required")
class QueryDispatcherOwnershipReleaseTest {

    private static final String COORDINATOR_ID = "coord-1";
    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);

    private HighAvailabilityCoordinationService haService;
    private QueryDispatcher dispatcher;
    private CoordinatorTaskTracker taskTracker;
    private RemoteTaskDispatcher httpDispatcher;
    private org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture worker;
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        haService = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());

        SqlCompiler compiler = new SqlCompiler();
        worker = new org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture("w1", nativeDirectory);
        httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        httpDispatcher.setControlEpochSupplier(() -> 1L);
        httpDispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        COORDINATOR_ID, "test-incarnation", 1L));
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(worker.workerInfo());
        taskTracker = new CoordinatorTaskTracker();

        var deps = new QueryDispatcherContext(
                compiler,
                new org.pangrova.coordinator.query.execution.SecurityContextBundle(null, null, null),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        httpDispatcher, taskScheduler, workerManager, taskTracker,
                        null, null, null, null),
                null, null, 0L,
                null, null, null, null, haService, COORDINATOR_ID, null, null, null);
        dispatcher = new QueryDispatcher(withPolicies(deps), null, null);
        worker.connect(dispatcher, taskTracker, workerManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (worker != null) worker.close();
        } finally {
            if (httpDispatcher != null) httpDispatcher.close();
            if (haService != null) haService.close();
        }
    }

    @Test
    void cleanupQuery_releasesOwnership() {
        String queryId = "q-ownership-1";

        // Acquire ownership
        boolean acquired = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(acquired, "ownership should be acquired");
        taskTracker.openQueryIssuance(queryId);

        // cleanupQuery should release it
        dispatcher.cleanupQuery(queryId);

        // Verify ownership was released — re-acquiring should succeed
        boolean reacquired = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(reacquired, "ownership should be re-acquirable after cleanup released it");
    }

    @Test
    void cleanupQuery_toleratesNoOwnership() {
        // A locally admitted query need not have acquired HA execution ownership.
        taskTracker.openQueryIssuance("q-never-owned");
        dispatcher.cleanupQuery("q-never-owned");
    }

    @Test
    void cleanupQuery_withoutHaService_doesNotThrow() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
        // Dispatcher without HA wiring should still clean up without error
        var noHaTracker = new CoordinatorTaskTracker();
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                remote,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()), noHaTracker,
                null, null, null, 0L);
        QueryDispatcher noHaDispatcher = new QueryDispatcher(deps, null, null);
        // No enableOwnershipTracking called — haService is null
        noHaTracker.openQueryIssuance("q-no-ha");
        noHaDispatcher.cleanupQuery("q-no-ha");
        }
    }

    @Test
    void execute_thenCleanup_releasesOwnership() {
        String queryId = "q-exec-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "default");

        // Acquire ownership before dispatch (simulating QueryManager behavior)
        haService.acquireQueryOwnership(queryId, COORDINATOR_ID);

        // Execute a constant query through the real Worker HTTP admission path.
        QueryStateMachine sm = new QueryStateMachine();
        var result = worker.execute(dispatcher, sm, "SELECT 1", session);
        assertNotNull(result);
        worker.taskManager().cleanupTerminalTasks(java.time.Duration.ofHours(1));
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                org.junit.jupiter.api.Assertions.assertTrue(
                        taskTracker.executionDrainReceipt(result.taskId()).isPresent()));

        // Simulate query completion cleanup
        dispatcher.cleanupQuery(queryId);

        // Ownership should be released
        boolean canReacquire = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(canReacquire, "ownership should be released after query completion cleanup");
    }

    @Test
    void failedQuery_preservesMetadataAdmissionUntilTerminalCleanup() {
        String queryId = "q-fail-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "default");

        // Acquire ownership
        haService.acquireQueryOwnership(queryId, COORDINATOR_ID);

        // Dispatch propagates the failure without closing the terminal metadata scope.
        QueryStateMachine sm = new QueryStateMachine();
        assertThrows(org.pangrova.parser.ParseException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));

        try (var terminalOperation = dispatcher.enterQueryMetadataOperation(queryId)) {
            assertTrue(sm.tryTransition(QueryStateMachine.State.FAILED));
        }
        dispatcher.cleanupQuery(queryId);

        boolean canReacquire = haService.acquireQueryOwnership(queryId, COORDINATOR_ID);
        assertTrue(canReacquire, "ownership should be released after failed query cleanup");
    }

}
