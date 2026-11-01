package org.pangrova.coordinator.query.state;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;

import java.util.List;
import java.util.Set;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.query.state.TaskStateEvent;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Task 51 / SI-6: owner-scoped journal recovery and worker-liveness
 * grace window after failover.
 */
class JournalRecoveryOwnerScopeTest {
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final WorkerManager workers = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());
    private final java.util.List<org.pangrova.coordinator.scheduler.RemoteTaskDispatcher> transports =
            new java.util.ArrayList<>();

    @org.junit.jupiter.api.AfterEach
    void closeActualControlTransports() {
        transports.forEach(org.pangrova.coordinator.scheduler.RemoteTaskDispatcher::close);
    }

    @Test
    void ownerScopedRecoveryOnlyReplaysOwnedQueries() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        // Query owned by coord-1 (the failed leader)
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-1"), "q-owned", "SELECT 1", "{}", "RUNNING"));
        // Query owned by coord-2 (a different coordinator, should NOT be replayed)
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-2"), "q-other", "SELECT 2", "{}", "RUNNING"));
        // Query with no owner (legacy, should NOT be replayed when scoped)
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-legacy", "SELECT 3", "{}", "RUNNING"));

        QueryManager qm = buildQueryManager(journal);
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, null, "coord-1", 0);

        int recovered = recovery.recover();

        assertEquals(1, recovered);
        assertTrue(qm.getQueryState("q-owned").isPresent());
        assertFalse(qm.getQueryState("q-other").isPresent(),
                "Queries owned by other coordinators must not be replayed");
        assertFalse(qm.getQueryState("q-legacy").isPresent(),
                "Unowned legacy queries must not be replayed in scoped mode");
    }

    @Test
    void explicitQueryFenceDoesNotReplayLiveQueryFromReusedOwnerId() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-1"), "q-orphan", "SELECT 1", "{}", "RUNNING"));
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-1"), "q-live", "SELECT 2", "{}", "RUNNING"));

        QueryManager queryManager = buildQueryManager(journal);
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, queryManager, tracker,
                null, "coord-1", 0);

        int recovered = recovery.recover(Set.of("q-orphan"));

        assertEquals(1, recovered);
        assertTrue(queryManager.getQueryState("q-orphan").isPresent());
        assertFalse(queryManager.getQueryState("q-live").isPresent());
    }

    @Test
    void unscopedRecoveryReplaysAllQueries() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-1"), "q-1", "SELECT 1", "{}", "RUNNING"));
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), "coord-2"), "q-2", "SELECT 2", "{}", "SCHEDULING"));

        QueryManager qm = buildQueryManager(journal);
        // previousLeaderId = null → unscoped
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, null, null, 0);

        int recovered = recovery.recover();

        assertEquals(2, recovered);
        assertTrue(qm.getQueryState("q-1").isPresent());
        assertTrue(qm.getQueryState("q-2").isPresent());
    }

    @Test
    void graceWindowDoesNotFailUnknownWorkers() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "SELECT 1", "{}", "RUNNING"));
        journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "t-1", "w-unknown", "RUNNING", 0, 1L));

        QueryManager qm = buildQueryManager(journal);
        // WorkerManager with no registered workers — w-unknown evaluates to UNKNOWN

        // With grace window > 0, UNKNOWN workers should NOT cause query failure
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, workers, null, 60_000);

        int recovered = recovery.recover();

        assertEquals(1, recovered, "Query should survive when worker is UNKNOWN during grace window");
        assertTrue(qm.getQueryState("q-1").isPresent());
        assertEquals(QueryStateMachine.State.RUNNING, qm.getQueryState("q-1").get());
    }

    @Test
    void noGraceWindowFailsUnknownWorkers() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "SELECT 1", "{}", "RUNNING"));
        journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "t-1", "w-unknown", "RUNNING", 0, 1L));

        QueryManager qm = buildQueryManager(journal);

        // With grace window = 0, UNKNOWN workers cause query failure
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, workers, null, 0);

        int recovered = recovery.recover();

        assertEquals(0, recovered, "Query should fail when worker is UNKNOWN without grace window");
    }

    @Test
    void graceWindowStillFailsTrulyDeadWorkers() {
        var journal = new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal();
        journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "SELECT 1", "{}", "RUNNING"));
        journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), "q-1", "t-1", "w-dead", "RUNNING", 0, 1L));

        QueryManager qm = buildQueryManager(journal);

        // Register the worker with an ancient heartbeat (dead)
        WorkerInfo deadWorker = new WorkerInfo(
                "w-dead", "host1", 8080, 9090, 0,
                org.pangrova.scheduler.api.placement.HealthState.ACTIVE,
                MemoryPressure.NONE, 0.0, 0, 1_000_000L,
                Instant.now().minusSeconds(120), // 2 minutes ago → DEAD
                0.0, null, 2, null, java.util.List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-dead"));
        workers.registerWorker(deadWorker);

        // Even with grace window, truly DEAD workers (registered but stale) fail the query
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, workers, null, 60_000);

        int recovered = recovery.recover();

        assertEquals(0, recovered, "Query should fail when worker is truly DEAD even with grace window");
    }

    @Test
    void coordinatorIdFieldOnQueryStateEvent() {
        QueryStateEvent withOwner = new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(12345L, "coord-1"), "q-1", "sql", "{}", "RUNNING");
        assertEquals("coord-1", withOwner.context().coordinatorId());

        QueryStateEvent withoutOwner = new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(12345L, null), "q-2", "sql", "{}", "RUNNING");
        assertNull(withoutOwner.context().coordinatorId());
    }

    @Test
    void coordinatorIdFieldOnTaskStateEvent() {
        TaskStateEvent withOwner = new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(12345L, "coord-1"), "q-1", "t-1", "w-1", "RUNNING", 0, 1L);
        assertEquals("coord-1", withOwner.context().coordinatorId());

        TaskStateEvent withoutOwner = new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(12345L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 1L);
        assertNull(withoutOwner.context().coordinatorId());
    }

    // ── Helpers ──

    private QueryManager buildQueryManager(QueryStateJournal journal) {
        var transport = new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                org.pangrova.testing.testkit.TestObservability.TRACING);
        transports.add(transport);
        var dispatcherContext = new QueryDispatcherContext(new SqlCompiler(),
                org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.policies(),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(transport,
                        new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                        null, null, null, null),
                null, null, 0, null, null, null, null, null, null, journal, null, null);
        var dispatcher = new QueryDispatcher(dispatcherContext, null, null);
        return new QueryManager(new QueryManagerContext(dispatcher,
                org.pangrova.testing.testkit.TestObservability.TRACING, null, null, null, null, null,
                null, null, null, journal, null));
    }
}
