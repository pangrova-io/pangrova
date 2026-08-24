package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.governance.lineage.LineageBackend;
import org.pangrova.governance.lineage.LineageEvent;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.testing.TestObservability;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests LineageEventEmitter integration with QueryManager.
 * Verifies lineage events are emitted on query FINISHED.
 */
class QueryManagerLineageTest {

    private LineageEventEmitter emitter;

    @AfterEach
    void cleanup() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void emitsLineageEventOnConstantQueryFinished() throws Exception {
        var capturedEvents = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new CapturingLineageBackend(capturedEvents);
        emitter = new LineageEventEmitter(backend);

        QueryManager qm = buildQueryManagerWithLineage(emitter);
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-1", Map.of(), null, null);

        qm.submitQuery("q-1", "SELECT 1 + 2", session);

        // LineageEventEmitter is async — wait for events to be processed
        awaitEvents(capturedEvents, 2, 5);

        assertThat(capturedEvents).hasSize(2);
        assertThat(capturedEvents.get(0).eventType()).isEqualTo(LineageEvent.EventType.START);
        assertThat(capturedEvents.get(0).sql()).isEqualTo("SELECT 1 + 2");
        assertThat(capturedEvents.get(0).runId()).isEqualTo("q-1");
        assertThat(capturedEvents.get(1).eventType()).isEqualTo(LineageEvent.EventType.COMPLETE);
    }

    @Test
    void noLineageEmittedWhenEmitterIsNull() {
        QueryManager qm = buildQueryManagerWithoutLineage();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-2", Map.of(), null, null);

        // Should not throw even without lineage emitter
        qm.submitQuery("q-2", "SELECT 42", session);
    }

    @Test
    void constantQueryHasEmptySourceAndTargetTables() throws Exception {
        var capturedEvents = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new CapturingLineageBackend(capturedEvents);
        emitter = new LineageEventEmitter(backend);

        QueryManager qm = buildQueryManagerWithLineage(emitter);
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-3", Map.of(), null, null);

        qm.submitQuery("q-3", "SELECT 1", session);
        awaitEvents(capturedEvents, 2, 5);

        LineageEvent startEvent = capturedEvents.get(0);
        // Constant query has no table references
        assertThat(startEvent.inputDatasets()).isEmpty();
        assertThat(startEvent.outputDatasets()).isEmpty();
    }

    private QueryManager buildQueryManagerWithLineage(LineageEventEmitter lineageEmitter) {
        QueryDispatcher orchestrator = buildOrchestrator();
        return new QueryManager(new QueryManagerContext(orchestrator, TestObservability.TRACING,
                null, null, null, lineageEmitter, null, null, null, null,
                org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE, null));
    }

    private QueryManager buildQueryManagerWithoutLineage() {
        QueryDispatcher orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerContext.minimal(orchestrator, TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING), scheduler, wm,
                        new CoordinatorTaskTracker(), null, null, null, null, 0L),
                null, null);
    }

    private static void awaitEvents(List<LineageEvent> events, int expected, int timeoutSeconds)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    static class CapturingLineageBackend implements LineageBackend {
        private final List<LineageEvent> captured;

        CapturingLineageBackend(List<LineageEvent> captured) {
            this.captured = captured;
        }

        @Override
        public void send(LineageEvent event) {
            captured.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
