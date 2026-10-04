package org.pangrova.coordinator.query.task;

import org.pangrova.common.http.HttpClientFactory;
import org.pangrova.common.http.ManagedHttpClient;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.AsyncSubmitConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.task.RemoteTaskStatusReporter;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

/** Actual SQL/JNI and authenticated internal HTTP in one JVM; not process-isolated E2E. */
final class NativeReportDeliveryFixture implements AutoCloseable {
    static final String SECRET = "actual-status-report-internal-service-secret";
    private final String queryId = "report-" + UUID.randomUUID();
    private final String workerId = "report-worker";
    private final WorkerManager workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    private final StatusReportMetricExport metrics = new StatusReportMetricExport();
    private final CompletableFuture<Throwable> deliveryFailure = new CompletableFuture<>();
    private SourceFreeNativeWorkerFixture worker;
    private RemoteTaskDispatcher dispatcher;
    private QueryManager queries;
    private AuthenticationManager authentication;
    private CoordinatorHttpServer coordinator;
    private StatusReportHttpRelay relay;
    private ManagedHttpClient http;
    private RemoteTaskStatusReporter reporter;
    private ScheduledExecutorService cleanupExecutor;
    private ExecutorService queryExecutor;
    private ResultEndpoint endpoint;
    private Throwable resultFailure;

    NativeReportDeliveryFixture(Path directory, String selectedState, boolean drained, int before, int after)
            throws Exception {
        this(directory, selectedState, drained, before, after, null);
    }

    NativeReportDeliveryFixture(Path directory, String selectedState, boolean drained, int before, int after,
            QueryStateJournal journal) throws Exception {
        try {
            initialize(directory, selectedState, drained, before, after, journal);
        } catch (Exception | Error failure) {
            try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void initialize(Path directory, String state, boolean drained, int before, int after, QueryStateJournal journal)
            throws Exception {
        worker = new SourceFreeNativeWorkerFixture(workerId, directory);
        workers.registerWorker(worker.workerInfo());
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored -> new ExecutionOwnerIdentity(
                "report-coordinator", "report-coordinator-incarnation", 1L));
        var tracker = new CoordinatorTaskTracker();
        var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, dispatcher,
                new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker, null, null, null, 30L);
        var execution = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
        connectQueries(execution, tracker, journal);
        authentication = new AuthenticationManager(List.of());
        coordinator = new CoordinatorHttpServer(0, new CoordinatorHttpServerContext(worker.statusReceiver(),
                queries, null, authentication, null, workers, null, null, null, null, null, SECRET, () -> 1L));
        coordinator.start();
        relay = new StatusReportHttpRelay(coordinatorUri());
        relay.select(state, drained, before, after);
        http = HttpClientFactory.createDefault();
        reporter = new RemoteTaskStatusReporter(relay.uri().toString(), http.httpClient(),
                TestObservability.TRACING, metrics.provider(), SECRET);
        worker.deliverReportsTo(report -> {
            try { reporter.report(report); }
            catch (RuntimeException | Error failure) {
                deliveryFailure.complete(failure);
                // Observe one actual failed delivery round before explicitly recovering its retained owner.
                cleanupExecutor.shutdown();
                throw failure;
            }
        });
        cleanupExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("report-fixture-task-cleanup");
        ((java.util.concurrent.ScheduledThreadPoolExecutor) cleanupExecutor).setRemoveOnCancelPolicy(true);
        worker.taskManager().startPeriodicCleanup(cleanupExecutor, Duration.ofMinutes(15), Duration.ofSeconds(1));
    }

    private void connectQueries(QueryDispatcher execution, CoordinatorTaskTracker tracker, QueryStateJournal journal) {
        if (journal == null) {
            queries = worker.connectAsync(execution, tracker, workers);
        } else {
            queryExecutor = org.pangrova.common.concurrent.ManagedExecutorFactory
                    .newSingleThreadExecutor("report-fixture-query-admission");
            queries = new QueryManager(new QueryManagerContext(execution, TestObservability.TRACING,
                    null, null, null, null, null, null, null, null, journal, null, null,
                    AsyncSubmitConfig.defaultEnabled(), queryExecutor));
            queries.setLocalCoordinatorId("report-coordinator");
            worker.connect(execution, tracker, workers, queries);
        }
    }

    void finish() {
        begin("SELECT 42");
        worker.consume(java.util.Objects.requireNonNull(endpoint));
    }

    void failDuringExecution() {
        begin("SELECT 10 / n FROM (VALUES (0), (1)) AS t(n)");
        if (endpoint != null) {
            try { worker.consume(endpoint); }
            catch (RuntimeException failure) { resultFailure = failure; }
        }
    }

    void cancel() {
        begin("SELECT 42");
        if (!queries.cancelQuery(queryId)) throw new IllegalStateException("Actual query cancellation was rejected");
    }

    void startWithoutConsumingResult() { begin("SELECT 42"); }
    void consumeResult() { worker.consume(java.util.Objects.requireNonNull(endpoint)); }

    private void begin(String sql) {
        endpoint = queries.submitQuery(queryId, sql, SourceFreeQueryFixture.session(queryId)).resultEndpoint();
    }

    TaskStatusReport actualReleasedReport(String state) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                worker.reports().stream().anyMatch(report -> report.state().equals(state) && isDrained(report)));
        return worker.reports().stream().filter(report -> report.state().equals(state) && isDrained(report))
                .findFirst().orElseThrow();
    }

    TaskStatusReport actualRunningReport() {
        return worker.reports().stream().filter(report -> report.state().equals("RUNNING")).findFirst().orElseThrow();
    }

    private static boolean isDrained(TaskStatusReport report) {
        byte[] body = report.canonicalTaskReport();
        if (body == null || body.length == 0) return false;
        try {
            ExecutionDrainState state = TaskReport.parseFrom(body).getExecutionDrainState();
            return state == ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED
                    || state == ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual task emitted malformed drain evidence", failure);
        }
    }

    Throwable awaitDeliveryFailure() throws Exception { return deliveryFailure.get(40, TimeUnit.SECONDS); }
    long failedReports() { return metrics.failedReports(); }
    QueryManager queries() { return queries; }
    String queryId() { return queryId; }
    StatusReportHttpRelay relay() { return relay; }
    RemoteTaskStatusReporter reporter() { return reporter; }
    ManagedHttpClient http() { return http; }
    URI coordinatorUri() { return URI.create("http://127.0.0.1:" + coordinator.getPort()); }
    List<List<Object>> rows() { return worker.rows(endpoint); }
    Throwable resultFailure() { return resultFailure; }

    void unregisterBeforeDrain() { relay.beforeFirstSelected(() -> workers.removeWorker(workerId)); }

    void recoverAndDrain() {
        if (worker == null || reporter == null) return;
        workers.registerWorker(worker.workerInfo());
        reporter.updateTarget(coordinatorUri().toString());
        worker.taskManager().releaseTerminalTaskExecutors();
        if (!queries.awaitAllQueries(30_000)) throw new IllegalStateException("Actual query owners did not drain");
    }

    @Override
    public void close() throws Exception {
        // A live cleanup callback still owns the task and its HTTP reporter; retain both on failed join.
        stopCleanup();
        if (queries != null) queries.setAcceptingQueries(false);
        recoverAndDrain();
        stopQueryExecutor();
        if (worker != null) worker.close();
        var failures = new java.util.ArrayList<Exception>();
        cleanup(failures, () -> { if (reporter != null) reporter.close(); });
        cleanup(failures, () -> { if (relay != null) relay.close(); });
        cleanup(failures, () -> { if (coordinator != null) coordinator.stop(); });
        cleanup(failures, () -> { if (authentication != null) authentication.close(); });
        cleanup(failures, () -> { if (dispatcher != null) dispatcher.close(); });
        cleanup(failures, () -> { if (http != null) http.close(); });
        cleanup(failures, metrics::close);
        if (!failures.isEmpty()) {
            Exception first = failures.get(0);
            failures.stream().skip(1).forEach(first::addSuppressed);
            throw first;
        }
    }

    private void stopQueryExecutor() throws InterruptedException {
        if (queryExecutor == null) return;
        queryExecutor.shutdown();
        try {
            if (!queryExecutor.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual journal query admission has not exited");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private void stopCleanup() throws InterruptedException {
        if (cleanupExecutor == null) return;
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(40, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Actual terminal-task cleanup has not exited");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    private static void cleanup(List<Exception> failures, Cleanup action) {
        try { action.run(); }
        catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            failures.add(failure);
        }
    }

    @FunctionalInterface
    private interface Cleanup { void run() throws Exception; }
}
