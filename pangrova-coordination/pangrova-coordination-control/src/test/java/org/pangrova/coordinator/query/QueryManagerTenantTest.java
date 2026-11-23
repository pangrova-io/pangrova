package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.exception.TenantQuotaExceededException;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.persistence.security.memory.tenant.InMemoryTenantStore;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
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
    private static final int RESULT_PORT = 19090;

    private InMemoryTenantStore tenantStore;
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private final java.util.concurrent.atomic.AtomicReference<QuerySession> dispatchedSession =
            new java.util.concurrent.atomic.AtomicReference<>();
    private WorkerHttpServer workerHttpServer;

    @BeforeEach
    void setUp() throws Exception {
        tenantStore = new InMemoryTenantStore();
        tenantResolver = new TenantResolver(tenantStore);
        quotaChecker = new QuotaChecker();

        TaskManager workerTaskManager = TaskManager.forHttpWiringOnly();
        workerHttpServer = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", RESULT_PORT),
                workerTaskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(new byte[32]));
        workerHttpServer.start();

        orchestrator = buildOrchestrator(workerHttpServer.getPort(),
                workerTaskManager.workerExecutionIdentity(), dispatchedSession::set);

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), org.pangrova.coordinator.query.task.TaskReportCompletion.failure(report)));

        queryManager = new QueryManager(new QueryManagerContext(
                orchestrator, TestObservability.TRACING, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.tenant(
                        tenantResolver, quotaChecker),
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                        null, null, null, orchestrator, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), null),
                null,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()));
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
        QuerySession session = new QuerySession(
                UUID.randomUUID().toString(), Map.of(), identity, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        QuerySubmissionResult response = queryManager.submitQuery(
                session.queryId(), "SELECT 1", session);

        assertThat(response).isNotNull();
        // Unified path: constant queries dispatch to worker, reaching RUNNING
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.RUNNING);
    }

    @Test
    void tenantAdmissionRetainsTrustedInputsAndTheActualIssuedLifetimeAtDispatch() {
        tenantStore.createTenant(createTenant("tenant-selected", 10));
        tenantStore.addUserToTenant("tenant-selected", "pinned-reader", TenantRole.MEMBER);
        var supplied = new QuerySession(UUID.randomUUID().toString(), Map.of(),
                createIdentity("pinned-reader"), null, java.util.Optional.empty(),
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())
                .withSourceSnapshots(Map.of("catalog.schema.exact", 31L))
                .withSourceIncrementalRanges(Map.of("catalog.schema.delta",
                        new org.pangrova.connector.api.metadata.table.SnapshotRange(19, 7)))
                .withInternalMaterializationExecution();
        try {
            queryManager.submitQuery(supplied.queryId(), "SELECT 1", supplied);
            var dispatched = dispatchedSession.get();
            assertThat(dispatched).isNotNull();
            assertThat(dispatched.tenantId()).isEqualTo("tenant-selected");
            assertThat(dispatched.sourceSnapshots()).isEqualTo(supplied.sourceSnapshots());
            assertThat(dispatched.sourceIncrementalRanges()).isEqualTo(supplied.sourceIncrementalRanges());
            assertThat(dispatched.internalMaterializationExecution()).isTrue();
            assertThat(dispatched.userIdentity()).isSameAs(supplied.userIdentity());
            assertThat(dispatched.queryAuthority()).containsSame(
                    queryManager.getStateMachine(supplied.queryId()).connectorAuthority());
            assertThat(dispatched.traceContext().traceId()).isEqualTo(supplied.traceContext().traceId());
            assertThat(supplied.queryAuthority()).isEmpty();
        } finally {
            queryManager.cancelQuery(supplied.queryId());
        }
    }

    @Test
    void submitQueryWithoutTenantMembershipDenied() {
        UserIdentity identity = createIdentity("bob");
        QuerySession session = new QuerySession(
                UUID.randomUUID().toString(), Map.of(), identity, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        assertThatThrownBy(() ->
                queryManager.submitQuery(session.queryId(), "SELECT 1", session))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no tenant mapping")
                .hasMessageContaining("fail-close");
    }

    @Test
    void submitQueryWithNullIdentityIsDeniedWhenTenancyEnabled() {
        QuerySession session = new QuerySession(
                UUID.randomUUID().toString(), Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

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
        QuerySession session = new QuerySession(
                UUID.randomUUID().toString(), Map.of(), identity, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

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
                orchestrator, TestObservability.TRACING, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.tenant(
                        tenantResolver, quotaChecker),
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(
                        null, new QueryThrottlingService(2, 0.001, 100.0)),
                org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                        null, null, null, orchestrator, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), null),
                null, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()));

        for (String tenantId : List.of("tenant-a", "tenant-b")) {
            QuerySession session = new QuerySession(
                    UUID.randomUUID().toString(), Map.of(),
                    createIdentity(tenantId + "-user"), null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
            assertThat(throttled.submitQuery(session.queryId(), "SELECT 1", session))
                    .isNotNull();
        }
    }

    @Test
    void submitQueryWithoutTenantResolverSkipsTenantChecks() throws Exception {
        TaskManager localTaskManager = TaskManager.forHttpWiringOnly();
        WorkerHttpServer localServer = new WorkerHttpServer(
                new WorkerHttpServer.WorkerHttpConfig(0, "localhost", RESULT_PORT),
                localTaskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(new byte[32]));
        localServer.start();
        try {
            QueryDispatcher orchestrator = buildOrchestrator(localServer.getPort(),
                    localTaskManager.workerExecutionIdentity());
            var dagEngine = orchestrator.dagEngine();
            localTaskManager.setAutoFinishReporter(report ->
                    dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                            report.state(), org.pangrova.coordinator.query.task.TaskReportCompletion.failure(report)));

            QueryManager noTenantManager = new QueryManager(
                    QueryManagerTestContexts.minimal(orchestrator, TestObservability.TRACING));

            UserIdentity identity = createIdentity("dave");
            QuerySession session = new QuerySession(
                    UUID.randomUUID().toString(), Map.of(), identity, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

            QuerySubmissionResult response = noTenantManager.submitQuery(
                    session.queryId(), "SELECT 1", session);

            assertThat(response).isNotNull();
            assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.RUNNING);
        } finally {
            localServer.stop();
        }
    }

    private static QueryDispatcher buildOrchestrator(int workerPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerIdentity) {
        return buildOrchestrator(workerPort, workerIdentity, ignored -> { });
    }

    private static QueryDispatcher buildOrchestrator(int workerPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerIdentity,
            java.util.function.Consumer<QuerySession> observer) {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(TestWorkerInfoBuilder.active(
                "test-w", "localhost", workerPort, RESULT_PORT, workerIdentity));
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
                null, null) {
            @Override
            public org.pangrova.execution.task.DispatchResult execute(
                    QueryStateMachine state, String sql, QuerySession session) {
                observer.accept(session);
                return super.execute(state, sql, session);
            }
        };
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
