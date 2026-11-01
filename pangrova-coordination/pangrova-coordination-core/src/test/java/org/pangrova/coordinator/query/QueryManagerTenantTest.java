package org.pangrova.coordinator.query;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.client.core.protocol.SubmitQueryResponse;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.spi.security.tenant.ResourceQuota;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.security.core.tenant.QuotaChecker;
import org.pangrova.security.core.tenant.TenantQuotaExceededException;
import org.pangrova.security.core.tenant.TenantResolver;
import org.pangrova.security.core.tenant.InMemoryTenantStore;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests tenant resolution and quota checking wired into QueryManager.
 * Uses real TenantResolver, QuotaChecker, and InMemoryTenantStore.
 */
class QueryManagerTenantTest {

    private InMemoryTenantStore tenantStore;
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private WorkerHttpServer workerHttpServer;

    @BeforeEach
    void setUp() throws Exception {
        tenantStore = new InMemoryTenantStore();
        tenantResolver = new TenantResolver(tenantStore);
        quotaChecker = new QuotaChecker();

        TaskManager workerTaskManager = TaskManager.forHttpWiringOnly();
        workerHttpServer = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0),
                workerTaskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(new byte[32]));
        workerHttpServer.start();

        orchestrator = buildOrchestrator(workerHttpServer.getPort(),
                workerTaskManager.workerExecutionIdentity());

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));

        queryManager = new QueryManager(new QueryManagerContext(
                orchestrator, TestObservability.TRACING, null, tenantResolver, quotaChecker, null, null, null, null, null,
                new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal(), null));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerHttpServer != null) {
            workerHttpServer.stop();
        }
    }

    @Test
    void submitQueryResolveTenantAndSetTenantId() {
        Tenant tenant = createTenant("t1", 10);
        tenantStore.createTenant(tenant);
        tenantStore.addUserToTenant("t1", "alice", TenantRole.MEMBER);

        UserIdentity identity = createIdentity("alice");
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                UUID.randomUUID().toString(), Map.of(), identity, null);

        SubmitQueryResponse response = queryManager.submitQuery(
                session.queryId(), "SELECT 1", session);

        assertThat(response).isNotNull();
        // Unified path: constant queries dispatch to worker, reaching RUNNING
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(response.state()).isEqualTo("RUNNING");
    }

    @Test
    void submitQueryWithoutTenantMembershipDenied() {
        UserIdentity identity = createIdentity("bob");
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                UUID.randomUUID().toString(), Map.of(), identity, null);

        assertThatThrownBy(() ->
                queryManager.submitQuery(session.queryId(), "SELECT 1", session))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no tenant mapping")
                .hasMessageContaining("fail-close");
    }

    @Test
    void submitQueryWithNullIdentityIsDeniedWhenTenancyEnabled() {
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                UUID.randomUUID().toString(), Map.of(), null, null);

        assertThatThrownBy(() -> queryManager.submitQuery(
                session.queryId(), "SELECT 1", session))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no user identity provided");
    }

    @Test
    void submitQueryExceedingQuotaThrowsException() {
        // Tenant with maxConcurrentQueries=0 — any query exceeds quota
        Tenant tenant = createTenant("t-limited", 0);
        tenantStore.createTenant(tenant);
        tenantStore.addUserToTenant("t-limited", "charlie", TenantRole.MEMBER);

        UserIdentity identity = createIdentity("charlie");
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                UUID.randomUUID().toString(), Map.of(), identity, null);

        assertThatThrownBy(() ->
                queryManager.submitQuery(session.queryId(), "SELECT 1", session))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("t-limited");
    }

    @Test
    void throttleUsesResolvedTenantInsteadOfSharedDefaultBucket() {
        for (String tenantId : List.of("tenant-a", "tenant-b")) {
            tenantStore.createTenant(createTenant(tenantId, 10));
            tenantStore.addUserToTenant(tenantId, tenantId + "-user", TenantRole.MEMBER);
        }
        QueryManager throttled = new QueryManager(new QueryManagerContext(
                orchestrator, TestObservability.TRACING, null, tenantResolver, quotaChecker,
                null, null, null, null,
                new QueryThrottlingService(2, 0.001, 100.0),
                new org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal(),
                null));

        for (String tenantId : List.of("tenant-a", "tenant-b")) {
            CoordinatorSessionContext session = new CoordinatorSessionContext(
                    UUID.randomUUID().toString(), Map.of(),
                    createIdentity(tenantId + "-user"), null);
            assertThat(throttled.submitQuery(session.queryId(), "SELECT 1", session))
                    .isNotNull();
        }
    }

    @Test
    void submitQueryWithoutTenantResolverSkipsTenantChecks() throws Exception {
        TaskManager localTaskManager = TaskManager.forHttpWiringOnly();
        WorkerHttpServer localServer = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0),
                localTaskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(new byte[32]));
        localServer.start();
        try {
            QueryDispatcher orchestrator = buildOrchestrator(localServer.getPort(),
                    localTaskManager.workerExecutionIdentity());
            var dagEngine = orchestrator.dagEngine();
            localTaskManager.setAutoFinishReporter(report ->
                    dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                            report.state(), report.error()));

            QueryManager noTenantManager = new QueryManager(
                    QueryManagerContext.minimal(orchestrator, TestObservability.TRACING));

            UserIdentity identity = createIdentity("dave");
            CoordinatorSessionContext session = new CoordinatorSessionContext(
                    UUID.randomUUID().toString(), Map.of(), identity, null);

            SubmitQueryResponse response = noTenantManager.submitQuery(
                    session.queryId(), "SELECT 1", session);

            assertThat(response).isNotNull();
            assertThat(response.state()).isEqualTo("RUNNING");
        } finally {
            localServer.stop();
        }
    }

    private static QueryDispatcher buildOrchestrator(int workerPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerIdentity) {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                "test-w", "localhost", workerPort, 0, workerIdentity));
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        return new QueryDispatcher(
                org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, dispatcher, scheduler,
                        wm, new CoordinatorTaskTracker(), null, null, null, 0L)),
                null, null);
    }

    private static Tenant createTenant(String tenantId, int maxConcurrentQueries) {
        ResourceQuota quota = new ResourceQuota(
                1024L * 1024 * 1024, maxConcurrentQueries, 100,
                60_000L, 1024L * 1024 * 1024, 10_000L,
                1024L * 1024 * 1024 * 1024L, 1024L * 1024 * 1024 * 1024L);
        return new Tenant(
                tenantId, tenantId + "-name", TenantStatus.ACTIVE, quota,
                List.of(), List.of(), null, Map.of(),
                Instant.now(), "system");
    }

    private static UserIdentity createIdentity(String principal) {
        return new UserIdentity(principal, Set.of(), Map.of(), Instant.now());
    }
}
