package org.pangrova.coordinator.query.state;

import java.util.List;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Task 51 / SI-6: owner-scoped journal recovery and worker-liveness
 * grace window after failover.
 */
class JournalRecoveryOwnerScopeTest {

    @Test
    void ownerScopedRecoveryOnlyReplaysOwnedQueries() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        // Query owned by coord-1 (the failed leader)
        journal.queryEvents.add(new QueryStateEvent(
                "q-owned", "SELECT 1", "{}", "RUNNING",
                System.currentTimeMillis(), "coord-1"));
        // Query owned by coord-2 (a different coordinator, should NOT be replayed)
        journal.queryEvents.add(new QueryStateEvent(
                "q-other", "SELECT 2", "{}", "RUNNING",
                System.currentTimeMillis(), "coord-2"));
        // Query with no owner (legacy, should NOT be replayed when scoped)
        journal.queryEvents.add(new QueryStateEvent(
                "q-legacy", "SELECT 3", "{}", "RUNNING",
                System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
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
    void unscopedRecoveryReplaysAllQueries() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING",
                System.currentTimeMillis(), "coord-1"));
        journal.queryEvents.add(new QueryStateEvent(
                "q-2", "SELECT 2", "{}", "SCHEDULING",
                System.currentTimeMillis(), "coord-2"));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
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
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING",
                System.currentTimeMillis()));
        journal.taskEvents.add(new TaskStateEvent(
                "q-1", "t-1", "w-unknown", "RUNNING", 0, 1L,
                System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        // WorkerManager with no registered workers — w-unknown evaluates to UNKNOWN
        WorkerManager wm = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());

        // With grace window > 0, UNKNOWN workers should NOT cause query failure
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, wm, null, 60_000);

        int recovered = recovery.recover();

        assertEquals(1, recovered, "Query should survive when worker is UNKNOWN during grace window");
        assertTrue(qm.getQueryState("q-1").isPresent());
        assertEquals(QueryStateMachine.State.RUNNING, qm.getQueryState("q-1").get());
    }

    @Test
    void noGraceWindowFailsUnknownWorkers() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING",
                System.currentTimeMillis()));
        journal.taskEvents.add(new TaskStateEvent(
                "q-1", "t-1", "w-unknown", "RUNNING", 0, 1L,
                System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());

        // With grace window = 0, UNKNOWN workers cause query failure
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, wm, null, 0);

        int recovered = recovery.recover();

        assertEquals(0, recovered, "Query should fail when worker is UNKNOWN without grace window");
    }

    @Test
    void graceWindowStillFailsTrulyDeadWorkers() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING",
                System.currentTimeMillis()));
        journal.taskEvents.add(new TaskStateEvent(
                "q-1", "t-1", "w-dead", "RUNNING", 0, 1L,
                System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());

        // Register the worker with an ancient heartbeat (dead)
        WorkerInfo deadWorker = new WorkerInfo(
                "w-dead", "host1", 8080, 9090, 0,
                org.pangrova.coordinator.worker.HealthState.ACTIVE,
                MemoryPressure.NONE, 0.0, 0, 1_000_000L,
                Instant.now().minusSeconds(120), // 2 minutes ago → DEAD
                0.0, null, 1, null, null, null);
        wm.registerWorker(deadWorker);

        // Even with grace window, truly DEAD workers (registered but stale) fail the query
        JournalRecoveryService recovery = new JournalRecoveryService(
                journal, qm, tracker, wm, null, 60_000);

        int recovered = recovery.recover();

        assertEquals(0, recovered, "Query should fail when worker is truly DEAD even with grace window");
    }

    @Test
    void coordinatorIdFieldOnQueryStateEvent() {
        QueryStateEvent withOwner = new QueryStateEvent(
                "q-1", "sql", "{}", "RUNNING", 12345L, "coord-1");
        assertEquals("coord-1", withOwner.coordinatorId());

        QueryStateEvent withoutOwner = new QueryStateEvent(
                "q-2", "sql", "{}", "RUNNING", 12345L);
        assertNull(withoutOwner.coordinatorId());
    }

    @Test
    void coordinatorIdFieldOnTaskStateEvent() {
        TaskStateEvent withOwner = new TaskStateEvent(
                "q-1", "t-1", "w-1", "RUNNING", 0, 1L, 12345L, "coord-1");
        assertEquals("coord-1", withOwner.coordinatorId());

        TaskStateEvent withoutOwner = new TaskStateEvent(
                "q-1", "t-1", "w-1", "RUNNING", 0, 1L, 12345L);
        assertNull(withoutOwner.coordinatorId());
    }

    // ── Helpers ──

    private static QueryManager buildQueryManager(QueryStateJournal journal) {
        QueryDispatcher dispatcher = buildMinimalDispatcher();
        QueryManagerContext deps = new QueryManagerContext(
                dispatcher, new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, journal, null);
        return new QueryManager(deps);
    }

    private static QueryDispatcher buildMinimalDispatcher() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager wm = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        QueryDispatcherContext deps = new QueryDispatcherContext(
                new SqlCompiler(),
                org.pangrova.coordinator.query.execution.SecurityContextBundle.none(),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        (req, worker) -> null,
                        scheduler, wm, tracker,
                        null, null, null, null),
                org.pangrova.coordinator.query.execution.CacheContextBundle.none(),
                null, null, null, 0, null, null, null, null, null, null, null, null);
        return new QueryDispatcher(deps, null, null);
    }

    /** Journal that returns pre-configured events; supports owner-scoped replay. */
    private static class InMemoryReplayJournal implements QueryStateJournal {
        final java.util.ArrayList<QueryStateEvent> queryEvents = new java.util.ArrayList<>();
        final java.util.ArrayList<TaskStateEvent> taskEvents = new java.util.ArrayList<>();

        @Override public void appendQueryEvent(QueryStateEvent event) {}
        @Override public void appendQueryEventSync(QueryStateEvent event) {}
        @Override public void appendTaskEvent(TaskStateEvent event) {}
        @Override public void appendTaskEventSync(TaskStateEvent event) {}

        @Override public List<QueryStateEvent> replayActiveQueries() {
            return List.copyOf(queryEvents);
        }

        @Override public List<TaskStateEvent> replayActiveTasks() {
            return List.copyOf(taskEvents);
        }

        @Override public List<QueryStateEvent> replayActiveQueriesForOwner(String coordinatorId) {
            return queryEvents.stream()
                    .filter(e -> coordinatorId.equals(e.coordinatorId()))
                    .toList();
        }

        @Override public List<TaskStateEvent> replayActiveTasksForOwner(String coordinatorId) {
            return taskEvents.stream()
                    .filter(e -> coordinatorId.equals(e.coordinatorId()))
                    .toList();
        }

        @Override public void purgeQuery(String queryId) {}
    }
}
