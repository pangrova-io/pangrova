package org.pangrova.coordinator.query;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.persistence.memory.query.audit.InMemoryQueryAuditStore;
import org.pangrova.query.core.audit.QueryAuditService;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.security.core.audit.AuditEventEmitter;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

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
    void emitsSubmittedAndFailedWhenConstantQueryHasNoWorker() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        CoordinatorSessionContext session = session("q-audit-1");

        assertThatThrownBy(() -> qm.submitQuery("q-audit-1", "SELECT 1 + 2", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        awaitEvents(captured, 2, 5);

        assertThat(captured).hasSize(2);
        assertThat(captured.get(0).eventType()).isEqualTo(AuditEventType.QUERY_SUBMITTED);
        assertThat(captured.get(0).queryId()).isEqualTo("q-audit-1");
        assertThat(captured.get(0).sql()).isEqualTo("SELECT 1 + 2");
        assertThat(captured.get(1).eventType()).isEqualTo(AuditEventType.QUERY_FAILED);
        assertThat(captured.get(1).queryId()).isEqualTo("q-audit-1");
        assertThat(captured.get(1).success()).isFalse();
        assertThat(captured.get(1).errorMessage()).contains("No available worker");
        assertThat(captured.get(0).principal()).isEqualTo("fixture-subject");
        assertThat(captured.get(1).principal()).isEqualTo("fixture-subject");
        assertThat(captured.get(1).properties()).containsEntry("tenant_id", "fixture-tenant");
        assertThat(qm.getQueryState("q-audit-1")).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void emitsQueryCompletedWhenRunningQueryFinishes() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-audit-complete", sm);
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);

        qm.onAllTasksFinished("q-audit-complete");
        awaitEvents(captured, 1, 5);

        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).eventType()).isEqualTo(AuditEventType.QUERY_COMPLETED);
        assertThat(captured.get(0).success()).isTrue();
        assertThat(qm.getQueryState("q-audit-complete"))
                .contains(QueryStateMachine.State.FINISHED);
    }

    @Test
    void emitsQueryFailedOnTaskFailure() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);

        // This component fixture exercises task-failure audit delivery, not Native SQL execution.
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
        CoordinatorSessionContext session = session("q-audit-3");

        assertThatThrownBy(() -> qm.submitQuery("q-audit-3", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-audit-3")).contains(QueryStateMachine.State.FAILED);
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

        assertThatThrownBy(() -> qm.submitQuery("q-audit-5", "SELECT 1", session))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class);
        awaitEvents(captured, 2, 5);

        assertThat(captured.get(0).principal()).isEqualTo("anonymous");
        assertThat(captured.get(1).principal()).isEqualTo("anonymous");
        assertThat(captured.get(1).eventType()).isEqualTo(AuditEventType.QUERY_FAILED);
        assertThat(captured.get(1).success()).isFalse();
    }

    private QueryManager buildAuditQueryManager(List<AuditEvent> captured) {
        emitter = new AuditEventEmitter(new CapturingAuditEventWriter(captured));
        QueryAuditService auditService = new QueryAuditService(new InMemoryQueryAuditStore());
        QueryDispatcher orchestrator = buildOrchestrator();
        QueryManagerContext deps = new QueryManagerContext(
                orchestrator, TestObservability.TRACING,
                null, null, null, null, emitter, auditService, null, null,
                new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal(), null);
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
                withPolicies(QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING), scheduler, wm,
                        new CoordinatorTaskTracker(), null, null, null, 0L)),
                null, null);
    }

    private static void awaitEvents(List<AuditEvent> events, int expected, int timeoutSeconds)
            throws InterruptedException {
        org.awaitility.Awaitility.await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(events).hasSizeGreaterThanOrEqualTo(expected));
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
