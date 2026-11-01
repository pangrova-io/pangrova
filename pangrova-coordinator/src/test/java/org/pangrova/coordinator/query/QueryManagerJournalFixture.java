package org.pangrova.coordinator.query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.query.transaction.JdbcExecutionAnchorStore;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.quota.InMemoryClusterConcurrencySlots;
import org.pangrova.security.tenant.quota.TenantQuotaAdmissionController;
import org.pangrova.security.tenant.store.InMemoryTenantStore;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Real compiler/policy/control owners and PostgreSQL metadata; no task or Native completion is simulated. */
final class QueryManagerJournalFixture implements AutoCloseable {
    final QueryTerminalJournalFixture store = new QueryTerminalJournalFixture();
    final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    final AtomicLong clock = new AtomicLong();
    final InMemoryClusterConcurrencySlots slots = new InMemoryClusterConcurrencySlots();
    final TenantQuotaAdmissionController admission;
    final QueryManager manager;
    final QueryDispatcher dispatcher;
    final HighAvailabilityCoordinationService authority = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
    final HighAvailabilityCoordinationService foreignAuthority = new HighAvailabilityCoordinationService(new InMemoryDistributedLease());
    private final RemoteTaskDispatcher transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);

    QueryManagerJournalFixture(int capacity) throws java.sql.SQLException {
        var tenants = new InMemoryTenantStore();
        tenants.createTenant(new Tenant("fixture-tenant", "journal-test", TenantStatus.ACTIVE,
                new ResourceQuota(Long.MAX_VALUE, capacity, 0, Long.MAX_VALUE, Long.MAX_VALUE,
                        Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
                List.of(), List.of(), "default", Map.of(), Instant.EPOCH, "test"));
        admission = new TenantQuotaAdmissionController(tenants, null, slots);
        var backend = new JdbcExecutionAnchorStore(store.administrator());
        authority.setExecutionAnchorStore(backend);
        foreignAuthority.setExecutionAnchorStore(backend);
        var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, transport,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()), tracker, null, null, null, 0L);
        dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
        dispatcher.setOwnershipTracking(authority, "journal-local");
        manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null, null, null, store.journal(), null), clock::get);
        manager.setClusterAdmissionController(admission);
        manager.setLocalCoordinatorId("journal-local");
    }

    void retry() { clock.addAndGet(TimeUnit.SECONDS.toNanos(31)); manager.retryPendingTerminalCompletions(); }

    QueryStateMachine running(String queryId) {
        store.journal().appendQueryEventSync(new org.pangrova.spi.coordination.QueryStateEvent(
                org.pangrova.spi.coordination.JournalEventContext.create(System.currentTimeMillis(), "journal-local"),
                queryId, null, null, "RUNNING"));
        var state = new QueryStateMachine();
        state.tryTransition(QueryStateMachine.State.PLANNING);
        state.tryTransition(QueryStateMachine.State.SCHEDULING);
        state.tryTransition(QueryStateMachine.State.RUNNING);
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
