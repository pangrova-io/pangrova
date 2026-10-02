package org.pangrova.coordinator.query.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.pangrova.spi.resource.admission.AdmissionDecision;
import org.pangrova.spi.resource.admission.AdmissionRequest;
import org.pangrova.spi.resource.admission.ClusterAdmissionController;
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
    void failedQueuedJournalWriteRemovesUnownedQuery() {
        RecordingJournal failingJournal = new RecordingJournal() {
            @Override
            public void appendQueryEventSync(QueryStateEvent event) {
                throw new IllegalStateException("journal unavailable");
            }
        };
        queryManager = new QueryManager(new QueryManagerContext(
                buildMinimalDispatcher(), new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, failingJournal, null));
        TrackingAdmissionController admission = new TrackingAdmissionController();
        queryManager.setClusterAdmissionController(admission);

        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "journal-failure", java.util.Map.of(), null, null);
        assertThrows(IllegalStateException.class,
                () -> queryManager.submitQuery("journal-failure", "SELECT 1", session));
        assertEquals(0, queryManager.getActiveQueryCount());
        assertTrue(queryManager.getQueryState("journal-failure").isEmpty());
        assertFalse(queryManager.cancelQuery("journal-failure"));
        assertEquals(1, admission.releases);
        assertEquals(0, admission.admittedCount("default"));
    }

    @Test
    void deniedOwnershipNeverChangesAnotherCoordinatorsJournal() {
        QueryStateEvent foreignEvent = new QueryStateEvent(
                "shared-id", "SELECT 1", null, "RUNNING", 1L, "other-coordinator");
        journal.syncQueryEvents.add(foreignEvent);
        queryManager = new QueryManager(new QueryManagerContext(
                buildMinimalDispatcher(false), new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, journal, null));
        TrackingAdmissionController admission = new TrackingAdmissionController();
        queryManager.setClusterAdmissionController(admission);

        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "shared-id", java.util.Map.of(), null, null);
        assertThrows(org.pangrova.coordinator.query.QueryRejectedException.class,
                () -> queryManager.submitQuery("shared-id", "SELECT 2", session));

        assertEquals(List.of(foreignEvent), journal.syncQueryEvents);
        assertTrue(journal.purgedQueryIds.isEmpty());
        assertEquals(0, queryManager.getActiveQueryCount());
        assertEquals(1, admission.releases);
        assertEquals(0, admission.admittedCount("default"));
    }

    @Test
    void duplicateTerminalQueryIdPreservesRegisteredStateAndJournal() {
        QueryStateMachine registered = new QueryStateMachine();
        registered.tryTransition(QueryStateMachine.State.FAILED);
        queryManager.registerStateMachine("shared-id", registered);
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "shared-id", java.util.Map.of(), null, null);

        assertThrows(org.pangrova.coordinator.query.QueryRejectedException.class,
                () -> queryManager.submitQuery("shared-id", "SELECT 2", session));

        assertSame(registered, queryManager.getStateMachine("shared-id"));
        assertTrue(journal.syncQueryEvents.isEmpty());
        assertTrue(journal.purgedQueryIds.isEmpty());
        assertEquals(0, queryManager.getActiveQueryCount());
    }

    @Test
    void deniedClusterAdmissionRemovesLocalReservationExactlyOnce() {
        TrackingAdmissionController admission = new TrackingAdmissionController(false);
        queryManager.setClusterAdmissionController(admission);
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "denied-admission", java.util.Map.of(), null, null);

        assertThrows(org.pangrova.coordinator.query.QueryRejectedException.class,
                () -> queryManager.submitQuery("denied-admission", "SELECT 1", session));

        assertEquals(0, queryManager.getActiveQueryCount());
        assertTrue(queryManager.getQueryState("denied-admission").isEmpty());
        assertEquals(0, admission.releases);
        assertTrue(journal.syncQueryEvents.isEmpty());
        assertTrue(journal.purgedQueryIds.isEmpty());
    }

    @Test
    void terminalJournalWriteDoesNotHoldStateMachineMonitor() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        CountDownLatch appendEntered = new CountDownLatch(1);
        CountDownLatch releaseAppend = new CountDownLatch(1);
        AtomicBoolean monitorHeld = new AtomicBoolean();
        RecordingJournal blockingJournal = new RecordingJournal() {
            @Override
            public void appendQueryEventSync(QueryStateEvent event) {
                monitorHeld.set(Thread.holdsLock(sm));
                appendEntered.countDown();
                try {
                    if (!releaseAppend.await(3, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("journal append timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                super.appendQueryEventSync(event);
            }
        };
        queryManager = new QueryManager(new QueryManagerContext(
                buildMinimalDispatcher(), new NoOpTracingProvider(),
                null, null, null, null, null, null, null, null, blockingJournal, null));
        queryManager.registerStateMachine("slow-journal", sm);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var completion = executor.submit(() -> queryManager.onAllTasksFinished("slow-journal"));
            assertTrue(appendEntered.await(3, TimeUnit.SECONDS));
            executor.submit(() -> sm.setBeforeTerminalTransition(() -> { }))
                    .get(1, TimeUnit.SECONDS);
            assertFalse(monitorHeld.get());
            releaseAppend.countDown();
            completion.get(3, TimeUnit.SECONDS);
        } finally {
            releaseAppend.countDown();
            executor.shutdownNow();
        }
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
        return buildMinimalDispatcher(true);
    }

    private static QueryDispatcher buildMinimalDispatcher(boolean ownershipGranted) {
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
                null, null, 0, null, null, null, null, null, null, null, null, null);
        return new QueryDispatcher(deps, null, null) {
            @Override
            public boolean acquireQueryOwnership(String queryId) {
                return ownershipGranted;
            }
        };
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
        final List<String> purgedQueryIds =
                Collections.synchronizedList(new ArrayList<>());

        @Override public void appendQueryEvent(QueryStateEvent event) { asyncQueryEvents.add(event); }
        @Override public void appendQueryEventSync(QueryStateEvent event) { syncQueryEvents.add(event); }
        @Override public void appendTaskEvent(TaskStateEvent event) { asyncTaskEvents.add(event); }
        @Override public void appendTaskEventSync(TaskStateEvent event) { syncTaskEvents.add(event); }
        @Override public List<QueryStateEvent> replayActiveQueries() { return List.of(); }
        @Override public List<TaskStateEvent> replayActiveTasks() { return List.of(); }
        @Override public void purgeQuery(String queryId) { purgedQueryIds.add(queryId); }
    }

    private static final class TrackingAdmissionController implements ClusterAdmissionController {
        private final boolean grant;
        private int admitted;
        private int releases;

        private TrackingAdmissionController() {
            this(true);
        }

        private TrackingAdmissionController(boolean grant) {
            this.grant = grant;
        }

        @Override public AdmissionDecision admit(AdmissionRequest request) {
            if (!grant) {
                return AdmissionDecision.reject("at capacity");
            }
            admitted++;
            return AdmissionDecision.admit();
        }

        @Override public void release(String queryId, String tenantId) {
            releases++;
            admitted--;
        }

        @Override public int admittedCount(String tenantId) {
            return admitted;
        }
    }
}
