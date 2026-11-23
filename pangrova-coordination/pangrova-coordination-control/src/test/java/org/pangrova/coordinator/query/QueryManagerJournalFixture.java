package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.scheduler.api.resource.AdmissionRequest;
import org.pangrova.scheduler.api.resource.AdmissionRequest.TenantLimits;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.tenant.ResolvedTenantContext;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.testing.testkit.coordination.admission.InMemoryClusterConcurrencySlots;
import org.pangrova.scheduler.admission.TenantQuotaAdmissionController;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.constants.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Real compiler/policy/control owners and PostgreSQL metadata; no task or Native completion is simulated. */
final class QueryManagerJournalFixture implements AutoCloseable {
    final QueryTerminalJournalFixture store = new QueryTerminalJournalFixture();
    final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    final AtomicLong clock = new AtomicLong();
    final InMemoryClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
    final TenantQuotaAdmissionController admission;
    private final TenantLimits tenantLimits;
    final QueryManager manager;
    final QueryDispatcher dispatcher;
    final HighAvailabilityCoordinationService authority;
    final HighAvailabilityCoordinationService foreignAuthority;
    private final RemoteTaskDispatcher transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);

    QueryManagerJournalFixture(int capacity) throws java.sql.SQLException {
        tenantLimits = new TenantLimits(Long.MAX_VALUE, capacity, 0);
        var tenant = new Tenant("fixture-tenant", "journal-test", TenantStatus.ACTIVE,
                new ResourceQuota(Long.MAX_VALUE, capacity, 0, Long.MAX_VALUE, Long.MAX_VALUE,
                        Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
                List.of(), List.of(), "default", Map.of(), Instant.EPOCH, "test");
        var resolved = new ResolvedTenantContext(tenant, TenantRole.OWNER);
        admission = new TenantQuotaAdmissionController(null, slots);
        var backend = new JdbcExecutionAnchorStore(store.administrator());
        authority = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), backend);

        foreignAuthority = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), backend);

        var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, transport,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()), tracker, null, null, null, 0L);
        dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
        dispatcher.setOwnershipTracking(authority, "journal-local");
        manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                null, (user, active) -> resolved,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                        null, null, null, dispatcher, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        store.journal(), null),
                null,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()), clock::get);
        manager.setClusterAdmissionController(admission);
        manager.setLocalCoordinatorId("journal-local");
    }

    AdmissionRequest admissionRequest(String queryId) {
        return AdmissionRequest.of("fixture-tenant", "default", queryId, tenantLimits);
    }

    void retry() { clock.addAndGet(TimeUnit.SECONDS.toNanos(31)); manager.retryPendingTerminalCompletions(); }

    QueryStateMachine running(String queryId) {
        store.journal().appendQueryEventSync(new org.pangrova.query.api.journal.QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(System.currentTimeMillis(), "journal-local"),
                queryId, null, null, "RUNNING"));
        var state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        state.tryTransition(QueryState.RUNNING);
        manager.registerStateMachine(queryId, state);
        return state;
    }

    @Override public void close() throws java.sql.SQLException {
        store.allowQueryInsert();
        store.allowQueryDelete();
        manager.getActiveQueryIds().forEach(manager::cancelQuery);
        retry();
        assertThat(manager.awaitAllQueries(5000)).isTrue();
        transport.close();
        authority.close();
        foreignAuthority.close();
        store.close();
    }
}
