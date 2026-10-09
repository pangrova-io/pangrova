package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.testing.TestObservability;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.quota.QuotaChecker;
import org.pangrova.security.tenant.quota.TenantQuotaExceededException;
import org.pangrova.security.tenant.resolver.TenantResolver;
import org.pangrova.security.tenant.store.InMemoryTenantStore;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests tenant resolution and quota checking wired into QueryManager.
 * Uses real TenantResolver, QuotaChecker, and InMemoryTenantStore.
 */
@Tag("native-required")
class QueryManagerTenantTest {

    private InMemoryTenantStore tenantStore;
    private TenantResolver tenantResolver;
    private QuotaChecker quotaChecker;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private SourceFreeNativeWorkerFixture worker;
    private WorkerManager workers;
    private CoordinatorTaskTracker tracker;
    private RemoteTaskDispatcher remote;
    private ExecutorService submissions;
    @TempDir Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        tenantStore = new InMemoryTenantStore();
        tenantResolver = new TenantResolver(tenantStore);
        quotaChecker = new QuotaChecker();

        worker = new SourceFreeNativeWorkerFixture("test-w", nativeDirectory);
        submissions = Executors.newSingleThreadExecutor(runnable ->
                new Thread(runnable, "tenant-query-dispatch"));
        workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workers.registerWorker(worker.workerInfo());
        tracker = new CoordinatorTaskTracker();
        orchestrator = buildOrchestrator();
        queryManager = createManager(tenantResolver, quotaChecker, null);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (worker != null) worker.close();
        } finally {
            if (remote != null) remote.close();
            if (submissions != null) {
                submissions.shutdownNow();
                assertThat(submissions.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
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
        finish(queryManager, response);
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
        QueryManager throttled = createManager(tenantResolver, quotaChecker,
                new QueryThrottlingService(2, 0.001, 100.0));

        for (String tenantId : List.of("tenant-a", "tenant-b")) {
            CoordinatorSessionContext session = new CoordinatorSessionContext(
                    UUID.randomUUID().toString(), Map.of(),
                    createIdentity(tenantId + "-user"), null);
            var response = throttled.submitQuery(session.queryId(), "SELECT 1", session);
            assertThat(response).isNotNull();
            finish(throttled, response);
        }
    }

    @Test
    void submitQueryWithoutTenantResolverSkipsTenantChecks() {
        QueryManager noTenantManager = createManager(null, null, null);
        UserIdentity identity = createIdentity("dave");
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                UUID.randomUUID().toString(), Map.of(), identity, null);
        SubmitQueryResponse response = noTenantManager.submitQuery(session.queryId(), "SELECT 1", session);
        assertThat(response).isNotNull();
        assertThat(response.state()).isEqualTo("RUNNING");
        finish(noTenantManager, response);
    }

    private QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        remote = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        remote.setControlEpochSupplier(() -> 1L);
        remote.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        return new QueryDispatcher(
                org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null, remote, scheduler,
                        workers, tracker, null, null, null, 30L)),
                null, null);
    }

    private QueryManager createManager(TenantResolver resolver, QuotaChecker quota,
                                       QueryThrottlingService throttling) {
        var manager = new QueryManager(new QueryManagerContext(orchestrator, TestObservability.TRACING,
                null, resolver, quota, null, null, null, null, throttling,
                new org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal(), null, null,
                AsyncSubmitConfig.defaultEnabled(), submissions));
        worker.connect(orchestrator, tracker, workers, manager);
        return manager;
    }

    private void finish(QueryManager manager, SubmitQueryResponse response) {
        String queryId = response.queryId();
        var taskIds = tracker.getTaskIds(queryId);
        assertThat(taskIds).hasSize(1);
        String taskId = taskIds.iterator().next();
        assertThat(response.resultEndpoint().ticket()).isEqualTo(
                worker.resultTicket(tracker.getTaskRequest(taskId).taskRequest()));
        worker.consume(response.resultEndpoint());
        assertThat(worker.rows(response.resultEndpoint())).containsExactly(List.of(1));
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(manager.getQueryState(queryId)).contains(QueryStateMachine.State.FINISHED));
        String localTaskId = worker.taskManager().resolveWorkerTaskId(taskId);
        assertThat(localTaskId).isNotNull();
        worker.taskManager().releaseTaskExecutor(localTaskId);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(worker.taskManager().getExecutorHandle(localTaskId)).isEmpty());
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
