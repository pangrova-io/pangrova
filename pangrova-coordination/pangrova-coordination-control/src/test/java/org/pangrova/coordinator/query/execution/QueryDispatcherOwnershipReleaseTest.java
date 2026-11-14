package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.TestObservability;
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
    private QueryExecutionCleanup executionCleanup;
    private CoordinatorTaskTracker taskTracker;
    private RemoteTaskDispatcher httpDispatcher;
    private org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture worker;
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        haService = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());

        SqlCompiler compiler = new SqlCompiler();
        worker = new org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture("w1", nativeDirectory);
        httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        httpDispatcher.setControlEpochSupplier(() -> 1L);
        httpDispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        COORDINATOR_ID, "test-incarnation", 1L));
        TaskScheduler taskScheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry());
        workerManager.registerWorker(worker.workerInfo());
        taskTracker = new CoordinatorTaskTracker();

        var deps = new QueryDispatcherContext(new org.pangrova.shuffle.skew.MedianSkewDetector(),
                compiler,
                new org.pangrova.coordinator.query.execution.SecurityContextBundle(null, null, null),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        httpDispatcher, taskScheduler, workerManager, taskTracker,
                        null, null, null, null),
                null, null, 0L,
                null, null, null, null, haService, COORDINATOR_ID, null, null, null, null, null);
        dispatcher = new QueryDispatcher(withPolicies(deps), null, null);
        executionCleanup = dispatcher;
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
        executionCleanup.cleanupQuery(queryId);

        // Verify ownership was released — re-acquiring should succeed
        boolean reacquired = haService.acquireQueryOwnership(queryId, "successor");
        assertTrue(reacquired, "ownership should be re-acquirable after cleanup released it");
    }

    @Test
    void cleanupQuery_toleratesNoOwnership() {
        // A locally admitted query need not have acquired HA execution ownership.
        taskTracker.openQueryIssuance("q-never-owned");
        executionCleanup.cleanupQuery("q-never-owned");
    }

    @Test
    void cleanupQuery_withoutHaService_doesNotThrow() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
        // Dispatcher without HA wiring should still clean up without error
        var noHaTracker = new CoordinatorTaskTracker();
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                remote,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry()), noHaTracker,
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
        QuerySession session = new QuerySession(queryId, Map.of(), TEST_IDENTITY, "default", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

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
        executionCleanup.cleanupQuery(queryId);

        // Ownership should be released
        boolean canReacquire = haService.acquireQueryOwnership(queryId, "successor");
        assertTrue(canReacquire, "ownership should be released after query completion cleanup");
    }

    @Test
    void failedQuery_preservesMetadataAdmissionUntilTerminalCleanup() {
        String queryId = "q-fail-cleanup";
        QuerySession session = new QuerySession(queryId, Map.of(), TEST_IDENTITY, "default", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        // Acquire ownership
        haService.acquireQueryOwnership(queryId, COORDINATOR_ID);

        // Dispatch propagates the failure without closing the terminal metadata scope.
        QueryStateMachine sm = new QueryStateMachine();
        assertThrows(org.pangrova.parser.api.exception.ParseException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));

        try (var terminalOperation = dispatcher.enterQueryMetadataOperation(queryId)) {
            assertTrue(sm.tryTransition(QueryState.FAILED));
        }
        executionCleanup.cleanupQuery(queryId);

        boolean canReacquire = haService.acquireQueryOwnership(queryId, "successor");
        assertTrue(canReacquire, "ownership should be released after failed query cleanup");
    }

    @Test
    void cancellationDoesNotReleaseLiveOperationOrExecutionOwnership() {
        String queryId = "q-port-operation-drain";
        assertTrue(haService.acquireQueryOwnership(queryId, COORDINATOR_ID));
        taskTracker.openQueryIssuance(queryId);
        try (var operation = dispatcher.enterQueryOperation(queryId)) {
            executionCleanup.cancelDispatchedTasks(queryId);
            assertThrows(IllegalStateException.class, () -> executionCleanup.cleanupQuery(queryId));
            org.junit.jupiter.api.Assertions.assertFalse(
                    haService.acquireQueryOwnership(queryId, "successor"));
        }
        executionCleanup.cleanupQuery(queryId);
        assertTrue(haService.acquireQueryOwnership(queryId, "successor"));
    }

    @Test
    void unadmittedCleanupCannotReleaseForeignExecutionOwner() {
        String queryId = "q-port-foreign-owner";
        assertTrue(haService.acquireQueryOwnership(queryId, "foreign"));
        taskTracker.openQueryIssuance(queryId);
        executionCleanup.cleanupUnadmittedQuery(queryId);
        org.junit.jupiter.api.Assertions.assertFalse(
                haService.acquireQueryOwnership(queryId, COORDINATOR_ID));
        assertTrue(haService.releaseQueryOwnership(queryId, "foreign"));
    }

}
