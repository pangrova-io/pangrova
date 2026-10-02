package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
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
import org.pangrova.coordinator.query.audit.QueryLineageInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Tests LineageEventEmitter integration with QueryManager.
 * Verifies lineage events are emitted on query FINISHED.
 */
class QueryManagerLineageTest {

    private LineageEventEmitter emitter;
    private QueryDispatcher orchestrator;

    @AfterEach
    void cleanup() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void noWorkerDoesNotEmitSuccessfulLineage() throws Exception {
        var capturedEvents = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new CapturingLineageBackend(capturedEvents);
        emitter = new LineageEventEmitter(backend);

        QueryManager qm = buildQueryManagerWithLineage(emitter);
        CoordinatorSessionContext session = session("q-1");

        assertThatThrownBy(() -> qm.submitQuery("q-1", "SELECT 1 + 2", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        emitter.close();

        assertThat(qm.getQueryState("q-1")).contains(QueryStateMachine.State.FAILED);
        assertThat(capturedEvents).isEmpty();
    }

    @Test
    void nullLineageEmitterDoesNotChangeDispatchFailure() {
        QueryManager qm = buildQueryManagerWithoutLineage();
        CoordinatorSessionContext session = session("q-2");

        assertThatThrownBy(() -> qm.submitQuery("q-2", "SELECT 42", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-2")).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void constantQueryHasEmptySourceAndTargetTables() throws Exception {
        var capturedEvents = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new CapturingLineageBackend(capturedEvents);
        emitter = new LineageEventEmitter(backend);

        QueryManager qm = buildQueryManagerWithLineage(emitter);
        CoordinatorSessionContext session = session("q-3");

        assertThatThrownBy(() -> qm.submitQuery("q-3", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        QueryLineageInfo compiled = orchestrator.removeLineageInfo("q-3");

        // Compilation captures lineage before dispatch, even when admission
        // later fails because no Worker is available.
        assertThat(compiled).isNotNull();
        assertThat(compiled.sourceTables()).isEmpty();
        assertThat(compiled.targetTables()).isEmpty();
        emitter.close();
        assertThat(capturedEvents).isEmpty();
    }

    private QueryManager buildQueryManagerWithLineage(LineageEventEmitter lineageEmitter) {
        orchestrator = buildOrchestrator();
        return new QueryManager(new QueryManagerContext(orchestrator, TestObservability.TRACING,
                null, null, null, lineageEmitter, null, null, null, null,
                org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE, null));
    }

    private QueryManager buildQueryManagerWithoutLineage() {
        orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerContext.minimal(orchestrator, TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        return new QueryDispatcher(
                withPolicies(QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING), scheduler, wm,
                        new CoordinatorTaskTracker(), null, null, null, 0L)),
                null, null);
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
