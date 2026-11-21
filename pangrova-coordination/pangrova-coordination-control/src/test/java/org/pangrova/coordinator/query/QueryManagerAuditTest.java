package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.persistence.query.memory.audit.InMemoryQueryAuditStore;
import org.pangrova.query.audit.QueryAuditService;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
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
        QuerySession session = session("q-audit-1");

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
        assertThat(qm.getQueryState("q-audit-1")).contains(QueryState.FAILED);
    }

    @Test
    void emitsQueryCompletedWhenRunningQueryFinishes() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-audit-complete", sm);
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);

        qm.onAllTasksFinished("q-audit-complete");
        awaitEvents(captured, 1, 5);

        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).eventType()).isEqualTo(AuditEventType.QUERY_COMPLETED);
        assertThat(captured.get(0).success()).isTrue();
        assertThat(qm.getQueryState("q-audit-complete"))
                .contains(QueryState.FINISHED);
    }

    @Test
    void emitsQueryFailedOnTaskFailure() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);

        // This component fixture exercises task-failure audit delivery, not Native SQL execution.
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-audit-2", sm);
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        captured.clear();

        // Simulate task failure
        qm.onTaskFailed("q-audit-2", new QueryFailure("INTERNAL", "INTERNAL", "simulated failure"));
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
        QuerySession session = session("q-audit-3");

        assertThatThrownBy(() -> qm.submitQuery("q-audit-3", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-audit-3")).contains(QueryState.FAILED);
    }

    @Test
    void emitsTableAccessedAuditEvent() throws Exception {
        var captured = new CopyOnWriteArrayList<AuditEvent>();
        QueryManager qm = buildAuditQueryManager(captured);
        QuerySession session = new QuerySession("q-audit-4", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

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
        QuerySession session = new QuerySession("q-audit-5", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        assertThatThrownBy(() -> qm.submitQuery("q-audit-5", "SELECT 1", session))
                .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class);
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
        var audit = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                emitter, auditService, null, orchestrator, null);
        QueryManagerContext deps = new QueryManagerContext(
                orchestrator, TestObservability.TRACING,
                null, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                audit,
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), audit),
                null,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
        return new QueryManager(deps);
    }

    private QueryManager buildQueryManagerWithoutAudit() {
        return new QueryManager(QueryManagerTestContexts.minimal(buildOrchestrator(), TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
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
