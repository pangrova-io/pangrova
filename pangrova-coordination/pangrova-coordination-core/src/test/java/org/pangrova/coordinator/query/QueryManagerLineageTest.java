package org.pangrova.coordinator.query;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
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
        QuerySession session = session("q-1");

        assertThatThrownBy(() -> qm.submitQuery("q-1", "SELECT 1 + 2", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        emitter.close();

        assertThat(qm.getQueryState("q-1")).contains(QueryState.FAILED);
        assertThat(capturedEvents).extracting(LineageEvent::eventType)
                .containsExactly(LineageEvent.EventType.START, LineageEvent.EventType.FAIL);
    }

    @Test
    void nullLineageEmitterDoesNotChangeDispatchFailure() {
        QueryManager qm = buildQueryManagerWithoutLineage();
        QuerySession session = session("q-2");

        assertThatThrownBy(() -> qm.submitQuery("q-2", "SELECT 42", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-2")).contains(QueryState.FAILED);
    }

    @Test
    void constantQueryHasEmptySourceAndTargetTables() throws Exception {
        var capturedEvents = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new CapturingLineageBackend(capturedEvents);
        emitter = new LineageEventEmitter(backend);

        QueryManager qm = buildQueryManagerWithLineage(emitter);
        QuerySession session = session("q-3");

        assertThatThrownBy(() -> qm.submitQuery("q-3", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        emitter.close();
        assertThat(capturedEvents).extracting(LineageEvent::eventType)
                .containsExactly(LineageEvent.EventType.START, LineageEvent.EventType.FAIL);
        assertThat(capturedEvents).allSatisfy(event -> {
            assertThat(event.inputDatasets()).isEmpty();
            assertThat(event.outputDatasets()).isEmpty();
        });
    }

    private QueryManager buildQueryManagerWithLineage(LineageEventEmitter lineageEmitter) {
        orchestrator = buildOrchestrator();
        return new QueryManager(new QueryManagerContext(orchestrator, TestObservability.TRACING,
                null, null, null, lineageEmitter, null, null, null, null,
                new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), null, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()));
    }

    private QueryManager buildQueryManagerWithoutLineage() {
        orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerTestContexts.minimal(orchestrator, TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry());
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
