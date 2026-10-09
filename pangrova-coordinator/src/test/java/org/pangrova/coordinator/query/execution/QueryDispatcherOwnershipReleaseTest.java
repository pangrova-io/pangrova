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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    private CoordinatorTaskTracker tracker;
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
        tracker = new CoordinatorTaskTracker();

        var deps = new QueryDispatcherContext(
                compiler,
                new org.pangrova.coordinator.query.execution.SecurityContextBundle(null, null, null),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        httpDispatcher, taskScheduler, workerManager, tracker,
                        null, null, null, null),
                null, null, 0L,
                null, null, null, null, haService, COORDINATOR_ID, null, null, null);
        dispatcher = new QueryDispatcher(withPolicies(deps), null, null);
        worker.connect(dispatcher, tracker, workerManager);
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
        assertFalse(haService.acquireQueryOwnership(queryId, "other-coordinator"));
        tracker.openQueryIssuance(queryId);

        // cleanupQuery should release it
        dispatcher.cleanupQuery(queryId);

        // Verify ownership was released — re-acquiring should succeed
        boolean reacquired = haService.acquireQueryOwnership(queryId, "other-coordinator");
        assertTrue(reacquired, "ownership should be re-acquirable after cleanup released it");
    }

    @Test
    void cleanupQuery_toleratesNoOwnership() {
        // Local issuance exists, but no distributed ownership was acquired.
        tracker.openQueryIssuance("q-never-owned");
        dispatcher.cleanupQuery("q-never-owned");
    }

    @Test
    void cleanupQuery_withoutHaService_doesNotThrow() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
        // Dispatcher without HA wiring should still clean up without error
        var localTracker = new CoordinatorTaskTracker();
        localTracker.openQueryIssuance("q-no-ha");
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                remote,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()), localTracker,
                null, null, null, 0L);
        QueryDispatcher noHaDispatcher = new QueryDispatcher(deps, null, null);
        // No enableOwnershipTracking called — haService is null
        noHaDispatcher.cleanupQuery("q-no-ha");
        }
    }

    @Test
    void execute_thenCleanup_releasesOwnership() {
        String queryId = "q-exec-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "default");

        // Acquire ownership before dispatch (simulating QueryManager behavior)
        assertTrue(haService.acquireQueryOwnership(queryId, COORDINATOR_ID));
        assertFalse(haService.acquireQueryOwnership(queryId, "other-coordinator"));

        // Execute a constant query through the real Worker HTTP admission path.
        QueryStateMachine sm = new QueryStateMachine();
        var result = worker.execute(dispatcher, sm, "SELECT 1", session);
        assertNotNull(result);

        String taskId = result.taskId();
        String workerTaskId = worker.taskManager().resolveWorkerTaskId(taskId);
        assertNotNull(workerTaskId);
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                org.assertj.core.api.Assertions.assertThat(worker.taskManager().getTaskState(workerTaskId))
                        .contains(org.pangrova.worker.task.TaskManager.TaskState.FINISHED));
        assertThrows(IllegalStateException.class, () -> dispatcher.cleanupQuery(queryId));
        assertFalse(haService.acquireQueryOwnership(queryId, "other-coordinator"));
        worker.taskManager().releaseTaskExecutor(workerTaskId);
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                org.assertj.core.api.Assertions.assertThat(tracker.executionDrainReceipt(taskId))
                        .isPresent());

        // Cleanup requires the real Native owner release, not merely logical FINISHED.
        dispatcher.cleanupQuery(queryId);

        // Ownership should be released
        boolean canReacquire = haService.acquireQueryOwnership(queryId, "other-coordinator");
        assertTrue(canReacquire, "ownership should be released after query completion cleanup");
    }

    @Test
    void failedQuery_cleanupInCatchBlock_releasesOwnership() {
        String queryId = "q-fail-cleanup";
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);

        // Acquire ownership
        assertTrue(haService.acquireQueryOwnership(queryId, COORDINATOR_ID));
        assertFalse(haService.acquireQueryOwnership(queryId, "other-coordinator"));

        // Execute invalid SQL — triggers the catch block in execute() which calls cleanupQuery
        QueryStateMachine sm = new QueryStateMachine();
        assertThrows(org.pangrova.parser.ParseException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));

        // The catch block in execute() calls cleanupQuery, which should release ownership
        boolean canReacquire = haService.acquireQueryOwnership(queryId, "other-coordinator");
        assertTrue(canReacquire, "ownership should be released after failed query cleanup");
    }

}
