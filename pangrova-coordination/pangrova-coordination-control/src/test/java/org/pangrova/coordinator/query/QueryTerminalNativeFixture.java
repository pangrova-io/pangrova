package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.submission.AsyncSubmitConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.constants.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Owns actual SQL/HTTP/JNI execution and one real frontend operation delaying physical cleanup. */
final class QueryTerminalNativeFixture implements AutoCloseable {
    final String queryId = "terminal-" + UUID.randomUUID();
    final AtomicLong clock = new AtomicLong();
    final QueryThrottlingService throttle = new QueryThrottlingService(1, 1000, 1000);
    final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    final QueryTerminalJournalFixture store;
    private final ExecutorService submissions = Executors.newSingleThreadExecutor();
    private final AuditEventEmitter audit = new AuditEventEmitter(events::add);
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private SourceFreeNativeWorkerFixture worker;
    private RemoteTaskDispatcher transport;
    private org.pangrova.query.api.lifecycle.QueryOperation operation;
    QueryManager manager;
    ResultEndpoint endpoint;

    QueryTerminalNativeFixture(Path directory) throws Exception {
        store = new QueryTerminalJournalFixture();
        try { initialize(directory); }
        catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void initialize(Path directory) throws Exception {
        worker = new SourceFreeNativeWorkerFixture("terminal-worker", directory);
        var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workers.registerWorker(worker.workerInfo());
        transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        transport.setControlEpochSupplier(() -> 1L);
        transport.setExecutionOwnerResolver(ignored -> new ExecutionOwnerIdentity("terminal-coordinator",
                "terminal-coordinator-incarnation", 1L));
        var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, transport,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()), workers, tracker, null, null, null, 30L);
        var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
        var auditPort = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                audit, null, null, dispatcher, null);
        manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                null, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, throttle),
                auditPort,
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        store.journal(), auditPort),
                null, AsyncSubmitConfig.defaultEnabled(), submissions,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()), clock::get);
        worker.connect(dispatcher, tracker, workers, manager);
        tracker.openQueryIssuance(queryId);
        operation = tracker.enterQueryOperation(queryId);
    }

    void submit(String sql) {
        endpoint = manager.submitQuery(queryId, sql, SourceFreeQueryFixture.session(queryId)).resultEndpoint();
        assertThat(throttle.availableGlobalPermits()).isZero();
    }

    void submitNativeFailure(String sql) {
        try { submit(sql); }
        catch (RuntimeException failure) {
            assertThat(manager.getQueryState(queryId)).contains(QueryState.FAILED);
            endpoint = manager.getStateMachine(queryId).getResultEndpoint().orElse(null);
        }
    }

    void consume() { worker.consume(java.util.Objects.requireNonNull(endpoint)); }

    void awaitLogicalTerminal(QueryState state) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(manager.getQueryState(queryId)).contains(state));
    }

    void releaseActualWorkerOwners() {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            cleanupActualWorkerOwners();
            requireActualNativeOwnersReleased(ignored -> { });
        });
    }

    void cleanupActualWorkerOwners() {
        worker.taskManager().cleanupTerminalTasks(Duration.ZERO);
    }

    void requireUnacknowledgedExecutionTerminalRetries() {
        var actual = worker.reports().stream().filter(report -> report.queryId().equals(queryId)
                        && report.canonicalTaskReport() != null
                        && canonical(report).getExecutionDrainState()
                                == ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED)
                .toList();
        assertThat(actual).isNotEmpty();
        var original = actual.get(0);
        assertThat(actual).allSatisfy(report -> {
            assertThat(report.state()).isEqualTo("FINISHED");
            assertThat(canonical(report).getState())
                    .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
            assertThat(report).usingRecursiveComparison().isEqualTo(original);
            tracker.executionDrainReceipt(report.taskId()).ifPresent(receipt ->
                    assertThat(receipt.getExecutionDrainState())
                            .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED));
            assertThat(tracker.acknowledgesReportPublication(report)).isFalse();
        });
        // The release phase publishes its own notice once the physical owners are free;
        // it must also stay unacknowledged until the retained terminal publication clears.
        assertThat(worker.reports()).filteredOn(report -> report.queryId().equals(queryId)
                        && report.canonicalTaskReport() != null
                        && canonical(report).getExecutionDrainState()
                                == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .allSatisfy(report -> assertThat(tracker.acknowledgesReportPublication(report)).isFalse());
    }

    org.pangrova.protocol.rest.task.TaskStatusReport awaitActualNativeOwnersReleased() {
        var actual = new java.util.concurrent.atomic.AtomicReference<org.pangrova.protocol.rest.task.TaskStatusReport>();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> requireActualNativeOwnersReleased(actual::set));
        return java.util.Objects.requireNonNull(actual.get());
    }

    private void requireActualNativeOwnersReleased(
            java.util.function.Consumer<org.pangrova.protocol.rest.task.TaskStatusReport> observed) {
        assertThat(worker.reports()).filteredOn(report -> report.queryId().equals(queryId)
                && report.canonicalTaskReport() != null).anySatisfy(report -> {
            assertThat(report.queryId()).isEqualTo(queryId);
            var canonical = canonical(report);
            assertThat(canonical.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
            assertThat(tracker.executionDrainReceipt(report.taskId())).isPresent();
            assertThat(org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(
                    tracker.executionDrainReceipt(report.taskId()).orElseThrow()))
                    .isEqualTo(org.pangrova.protocol.task.CanonicalTaskAdmissionSeals
                            .compactExecutionDrainReceipt(canonical));
            observed.accept(report);
        });
    }

    void requireActualNativeFailure(String message) {
        assertThat(worker.reports()).filteredOn(report -> report.queryId().equals(queryId)
                && report.canonicalTaskReport() != null).anySatisfy(report -> {
            var canonical = canonical(report);
            assertThat(canonical.getState()).isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED);
            assertThat(canonical.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
            assertThat(canonical.getError().getMessage().toLowerCase(java.util.Locale.ROOT)).contains(message);
        });
    }

    private static TaskReport canonical(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        try { return TaskReport.parseFrom(report.canonicalTaskReport()); }
        catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual Worker emitted malformed task evidence", failure);
        }
    }

    void retryPhysicalCleanup() {
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        manager.retryPendingTerminalCompletions();
    }

    void exitFrontend() { operation.close(); }

    long events(AuditEventType type) {
        return events.stream().filter(event -> event.eventType() == type).count();
    }

    List<List<Object>> rows() { return worker.rows(endpoint); }

    @Override
    public void close() throws Exception {
        Throwable failure = null;
        try { drainOwners(); }
        catch (Exception | Error cleanup) { failure = cleanup; }
        failure = closeOwners(failure);
        try { joinSubmissions(); }
        catch (Exception | Error cleanup) { failure = combine(failure, cleanup); }
        if (failure instanceof Error error) throw error;
        if (failure instanceof Exception exception) throw exception;
    }

    private void drainOwners() throws Exception {
        store.allowFinishedQueryInsert();
        store.allowQueryInsert();
        store.allowQueryDelete();
        if (operation != null) operation.close();
        if (manager != null) {
            manager.getActiveQueryIds().forEach(manager::cancelQuery);
            if (worker != null) worker.taskManager().releaseTerminalTaskExecutors();
            retryPhysicalCleanup();
            assertThat(manager.awaitAllQueries(30_000)).isTrue();
        }
    }

    private Throwable closeOwners(Throwable failure) {
        for (AutoCloseable owner : new AutoCloseable[]{worker, transport, audit::close, store}) {
            if (owner == null) continue;
            try { owner.close(); }
            catch (Exception | Error cleanup) { failure = combine(failure, cleanup); }
        }
        return failure;
    }

    private void joinSubmissions() throws InterruptedException {
        submissions.shutdownNow();
        if (!submissions.awaitTermination(35, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Actual query submission executor did not exit");
        }
    }

    private static Throwable combine(Throwable failure, Throwable cleanup) {
        if (failure == null) return cleanup;
        if (cleanup != failure) failure.addSuppressed(cleanup);
        return failure;
    }
}
