package org.pangrova.coordinator.query.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests verifying QueryManager journals state transitions.
 *
 * <p>Uses a real QueryDispatcher with minimal dependencies. The dispatcher
 * will fail on SQL compilation, but journal writes happen before dispatch,
 * allowing us to verify QUEUED journaling. FINISHED and FAILED are tested
 * via onAllTasksFinished/onTaskFailed after pre-registering a state machine
 * through submitQuery.
 */
class QueryManagerJournalTest {

    private RecordingJournal journal;
    private QueryManager queryManager;

    @BeforeEach
    void setUp() {
        journal = new RecordingJournal();
        QueryDispatcher dispatcher = buildMinimalDispatcher();
        QueryManagerContext deps = new QueryManagerContext(
                dispatcher, new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, journal, null);
        queryManager = new QueryManager(deps);
    }

    @Test
    void submitQueryJournalsQueuedSync() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-1", java.util.Map.of(), null, null);
        try {
            queryManager.submitQuery("q-1", "SELECT 1", session);
        } catch (Exception ignored) {
            // Dispatcher may fail on SQL compilation — irrelevant for this test.
            // QUEUED journal write happens before dispatch.
        }

        assertFalse(journal.syncQueryEvents.isEmpty(), "QUEUED event should be journaled");
        QueryStateEvent event = journal.syncQueryEvents.get(0);
        assertEquals("QUEUED", event.state());
        assertEquals("q-1", event.queryId());
        assertEquals("SELECT 1", event.sql());
    }

    @Test
    void onAllTasksFinishedJournalsFinishedSync() {
        // Register a state machine in RUNNING state (the prerequisite for FINISHED).
        // Cannot use submitQuery because the dispatcher fails and transitions SM to FAILED.
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("q-2", sm);

        queryManager.onAllTasksFinished("q-2");

        assertEquals(1, journal.syncQueryEvents.size());
        assertEquals("FINISHED", journal.syncQueryEvents.get(0).state());
        assertEquals("q-2", journal.syncQueryEvents.get(0).queryId());
    }

    @Test
    void onTaskFailedJournalsFailedSync() {
        // Register a state machine in RUNNING state (non-terminal, can transition to FAILED).
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("q-3", sm);

        queryManager.onTaskFailed("q-3", new ErrorDetail("ERR", "test error", null));

        assertEquals(1, journal.syncQueryEvents.size());
        assertEquals("FAILED", journal.syncQueryEvents.get(0).state());
        assertEquals("q-3", journal.syncQueryEvents.get(0).queryId());
    }

    // ── Helpers ──

    private static QueryDispatcher buildMinimalDispatcher() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        QueryDispatcherContext deps = new QueryDispatcherContext(
                new SqlCompiler(),
                org.pangrova.coordinator.query.execution.SecurityContextBundle.none(),
                new org.pangrova.coordinator.query.execution.SchedulingContextBundle(
                        (req, worker) -> null,
                        scheduler, workerManager, tracker,
                        null, null, null, null),
                org.pangrova.coordinator.query.execution.CacheContextBundle.none(),
                null, null, null, 0, null, null, null, null, null, null, null, null);
        return new QueryDispatcher(deps, null, null);
    }

    // ── Test doubles ──

    /** Records all journal calls for assertion. */
    static class RecordingJournal implements QueryStateJournal {
        final List<QueryStateEvent> syncQueryEvents =
                Collections.synchronizedList(new ArrayList<>());
        final List<QueryStateEvent> asyncQueryEvents =
                Collections.synchronizedList(new ArrayList<>());
        final List<TaskStateEvent> syncTaskEvents =
                Collections.synchronizedList(new ArrayList<>());
        final List<TaskStateEvent> asyncTaskEvents =
                Collections.synchronizedList(new ArrayList<>());

        @Override public void appendQueryEvent(QueryStateEvent event) { asyncQueryEvents.add(event); }
        @Override public void appendQueryEventSync(QueryStateEvent event) { syncQueryEvents.add(event); }
        @Override public void appendTaskEvent(TaskStateEvent event) { asyncTaskEvents.add(event); }
        @Override public void appendTaskEventSync(TaskStateEvent event) { syncTaskEvents.add(event); }
        @Override public List<QueryStateEvent> replayActiveQueries() { return List.of(); }
        @Override public List<TaskStateEvent> replayActiveTasks() { return List.of(); }
        @Override public void purgeQuery(String queryId) {}
    }
}
