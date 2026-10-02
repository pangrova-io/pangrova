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
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying JournalRecoveryService rebuilds state from journal events.
 */
class JournalRecoveryServiceTest {

    @Test
    void recoverQueriesRebuildsStateMachines() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING", System.currentTimeMillis()));
        journal.queryEvents.add(new QueryStateEvent(
                "q-2", "SELECT 2", "{}", "SCHEDULING", System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        JournalRecoveryService recovery = new JournalRecoveryService(journal, qm, tracker);

        int recovered = recovery.recover();

        assertEquals(2, recovered);
        assertTrue(qm.getQueryState("q-1").isPresent());
        assertEquals(QueryStateMachine.State.RUNNING, qm.getQueryState("q-1").get());
        assertTrue(qm.getQueryState("q-2").isPresent());
        assertEquals(QueryStateMachine.State.SCHEDULING, qm.getQueryState("q-2").get());
    }

    @Test
    void recoverTasksRebuildsTracking() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-1", "SELECT 1", "{}", "RUNNING", System.currentTimeMillis()));
        journal.taskEvents.add(new TaskStateEvent(
                "q-1", "t-1", "w-1", "RUNNING", 0, 5L, System.currentTimeMillis()));
        journal.taskEvents.add(new TaskStateEvent(
                "q-1", "t-2", "w-2", "SCHEDULED", 0, 1L, System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        JournalRecoveryService recovery = new JournalRecoveryService(journal, qm, tracker);

        recovery.recover();

        assertEquals("w-1", tracker.getWorkerForTask("t-1"));
        assertEquals("w-2", tracker.getWorkerForTask("t-2"));
    }

    @Test
    void recoverEmptyJournalReturnsZero() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        JournalRecoveryService recovery = new JournalRecoveryService(journal, qm, tracker);

        assertEquals(0, recovery.recover());
    }

    @Test
    void recoverFailedQueryState() {
        InMemoryReplayJournal journal = new InMemoryReplayJournal();
        journal.queryEvents.add(new QueryStateEvent(
                "q-fail", "SELECT err", "{}", "FAILED", System.currentTimeMillis()));

        QueryManager qm = buildQueryManager(journal);
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        JournalRecoveryService recovery = new JournalRecoveryService(journal, qm, tracker);

        recovery.recover();

        assertTrue(qm.getQueryState("q-fail").isPresent());
        assertEquals(QueryStateMachine.State.FAILED, qm.getQueryState("q-fail").get());
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
                null, null, 0, null, null, null, null, null, null, null, null, null);
        return new QueryDispatcher(deps, null, null);
    }

    /** Journal that returns pre-configured events for replay. */
    private static class InMemoryReplayJournal implements QueryStateJournal {
        final java.util.ArrayList<QueryStateEvent> queryEvents = new java.util.ArrayList<>();
        final java.util.ArrayList<TaskStateEvent> taskEvents = new java.util.ArrayList<>();

        @Override public void appendQueryEvent(QueryStateEvent event) {}
        @Override public void appendQueryEventSync(QueryStateEvent event) {}
        @Override public void appendTaskEvent(TaskStateEvent event) {}
        @Override public void appendTaskEventSync(TaskStateEvent event) {}
        @Override public List<QueryStateEvent> replayActiveQueries() { return List.copyOf(queryEvents); }
        @Override public List<TaskStateEvent> replayActiveTasks() { return List.copyOf(taskEvents); }
        @Override public void purgeQuery(String queryId) {}
    }
}
