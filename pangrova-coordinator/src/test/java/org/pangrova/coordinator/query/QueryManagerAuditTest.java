package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.observability.audit.LogAuditStore;
import org.pangrova.observability.audit.QueryAuditService;
import org.pangrova.testing.TestObservability;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.security.audit.AuditEventWriter;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests AuditEventEmitter integration with QueryManager lifecycle.
 * Verifies audit events are emitted on submit, complete, and fail.
 */
class QueryManagerAuditTest {

    private AuditEventEmitter emitter;

    @AfterEach
    void cleanup() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void emitsQuerySubmittedAndCompletedOnConstantQuery() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-audit-1", Map.of(), null, null);

        qm.submitQuery("q-audit-1", "SELECT 1 + 2", session);
        awaitEvents(captured, 2, 5);

        assertThat(captured).hasSizeGreaterThanOrEqualTo(2);
        assertThat(captured.get(0).eventType()).isEqualTo(AuditEventType.QUERY_SUBMITTED);
        assertThat(captured.get(0).queryId()).isEqualTo("q-audit-1");
        assertThat(captured.get(0).sql()).isEqualTo("SELECT 1 + 2");
        assertThat(captured.get(1).eventType()).isEqualTo(AuditEventType.QUERY_COMPLETED);
        assertThat(captured.get(1).queryId()).isEqualTo("q-audit-1");
        assertThat(captured.get(1).success()).isTrue();
    }

    @Test
    void emitsQueryFailedOnTaskFailure() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);

        // Register a state machine at RUNNING directly (submitQuery with constant
        // queries completes synchronously to FINISHED, blocking FAILED transition).
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-audit-2", sm);
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        captured.clear();

        // Simulate task failure
        qm.onTaskFailed("q-audit-2", new ErrorDetail("INTERNAL", "INTERNAL", "simulated failure"));
        awaitEvents(captured, 1, 5);

        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).eventType()).isEqualTo(AuditEventType.QUERY_FAILED);
        assertThat(captured.get(0).queryId()).isEqualTo("q-audit-2");
        assertThat(captured.get(0).success()).isFalse();
        assertThat(captured.get(0).errorMessage()).contains("simulated failure");
    }

    @Test
    void noAuditEmittedWhenEmitterIsNull() {
        QueryManager qm = buildQueryManagerWithoutAudit();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-audit-3", Map.of(), null, null);

        // Should not throw even without audit emitter
        qm.submitQuery("q-audit-3", "SELECT 1", session);
    }

    @Test
    void emitsTableAccessedAuditEvent() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-audit-4", Map.of(), null, null);

        qm.emitTableAccessedAudit("q-audit-4", "iceberg_catalog", "db", "orders", session);
        awaitEvents(captured, 1, 5);

        assertThat(captured).hasSize(1);
        AuditEvent event = captured.get(0);
        assertThat(event.eventType()).isEqualTo(AuditEventType.TABLE_ACCESSED);
        assertThat(event.queryId()).isEqualTo("q-audit-4");
        assertThat(event.accessedObjects()).hasSize(1);
        assertThat(event.accessedObjects().get(0).table()).isEqualTo("orders");
        assertThat(event.accessedObjects().get(0).catalog()).isEqualTo("iceberg_catalog");
    }

    @Test
    void auditPrincipalIsAnonymousWhenNoIdentity() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-audit-5", Map.of(), null, null);

        qm.submitQuery("q-audit-5", "SELECT 1", session);
        awaitEvents(captured, 1, 5);

        assertThat(captured.get(0).principal()).isEqualTo("anonymous");
    }

    private QueryManager buildAuditQueryManager(List<AuditEvent> captured) {
        emitter = new AuditEventEmitter(new CapturingAuditEventWriter(captured));
        QueryAuditService auditService = new QueryAuditService(new LogAuditStore());
        QueryDispatcher orchestrator = buildOrchestrator();
        QueryManagerContext deps = new QueryManagerContext(
                orchestrator, TestObservability.TRACING,
                null, null, null, null, emitter, auditService, null, null,
                org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal.INSTANCE, null);
        return new QueryManager(deps);
    }

    private QueryManager buildQueryManagerWithoutAudit() {
        return new QueryManager(QueryManagerContext.minimal(buildOrchestrator(), TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING), scheduler, wm,
                        new CoordinatorTaskTracker(), null, null, null, null, 0L),
                null, null);
    }

    private static void awaitEvents(List<AuditEvent> events, int expected, int timeoutSeconds)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    /** Real AuditEventWriter that captures events for assertion. */
    static class CapturingAuditEventWriter implements AuditEventWriter {
        private final List<AuditEvent> captured;

        CapturingAuditEventWriter(List<AuditEvent> captured) {
            this.captured = captured;
        }

        @Override
        public void write(AuditEvent event) {
            captured.add(event);
        }
    }
}
