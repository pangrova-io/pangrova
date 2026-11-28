package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.runtime.QueryManagerContext;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.query.lifecycle.TenantAdmissionPort;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.exception.TenantQuotaExceededException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression for F-OBS-01 (task 179): the coordinator KPI instruments
 * (submitted/running/completed/failed counters) are recorded through
 * the {@link MetricsProvider} wired into {@link QueryManagerContext}.
 *
 * <p>This test drives the lifecycle terminal paths and asserts the corresponding
 * KPI values move off zero, and that a null metrics provider degrades to a no-op.</p>
 */
class QueryManagerMetricsRecordingTest {
    private static final String REJECTIONS = "pangrova.coordinator.queries.admission.rejections";
    private static final String DISPATCH_QUEUE = "pangrova.coordinator.queries.dispatch.queue_ms";

    @Test
    @DisplayName("completing a query records completed counter")
    void completionRecordsKpis() {
        RecordingMetricsProvider provider = new RecordingMetricsProvider();
        QueryManager queryManager = newQueryManager(provider);

        String queryId = registerRunningQuery(queryManager);
        queryManager.onAllTasksFinished(queryId);

        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryState.FINISHED);
        assertThat(provider.counter("pangrova.coordinator.queries.completed")).isEqualTo(1L);
    }

    @Test
    @DisplayName("failing a query records failed counter")
    void failureRecordsKpis() {
        RecordingMetricsProvider provider = new RecordingMetricsProvider();
        QueryManager queryManager = newQueryManager(provider);

        String queryId = registerRunningQuery(queryManager);
        queryManager.onTaskFailed(queryId, new QueryFailure("RUNTIME_INTERNAL", "boom", null));

        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryState.FAILED);
        assertThat(provider.counter("pangrova.coordinator.queries.failed")).isEqualTo(1L);
    }

    @Test
    @DisplayName("cancelling a query records failed counter")
    void cancellationRecordsKpis() {
        RecordingMetricsProvider provider = new RecordingMetricsProvider();
        QueryManager queryManager = newQueryManager(provider);

        String queryId = registerRunningQuery(queryManager);
        assertThat(queryManager.cancelQuery(queryId)).isTrue();

        assertThat(provider.counter("pangrova.coordinator.queries.failed")).isEqualTo(1L);
    }

    @Test
    @DisplayName("null MetricsProvider degrades to a no-op (no NPE)")
    void nullMetricsIsNoOp() {
        QueryManager queryManager = newQueryManager(null);
        String queryId = registerRunningQuery(queryManager);

        assertThatCode(() -> queryManager.onAllTasksFinished(queryId))
                .doesNotThrowAnyException();
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryState.FINISHED);
    }

    @Test
    void onlyWinningTimeoutRecordsOnceAndLogicalCompletionStillReleasesLocalOwnership() {
        var provider = new RecordingMetricsProvider();
        var manager = newQueryManager(provider);
        String finished = registerRunningQuery(manager);
        manager.onAllTasksFinished(finished);
        String timedOut = registerRunningQuery(manager);
        manager.timeoutQueriesCreatedBy(Instant.MAX, Duration.ofSeconds(30));
        manager.timeoutQueriesCreatedBy(Instant.MAX, Duration.ofSeconds(30));
        manager.retryPendingTerminalCompletions();

        assertThat(manager.getQueryState(finished)).hasValue(QueryState.FINISHED);
        assertThat(manager.getQueryState(timedOut)).hasValue(QueryState.FAILED);
        assertThat(provider.counter("pangrova.coordinator.queries.timed_out")).isOne();
        assertThat(provider.histogramCount("pangrova.coordinator.queries.terminal_cleanup.local_release_ms"))
                .isEqualTo(2);
    }

    @Test
    void queueWaitAndAdmissionRejectionsUseDistinctBoundaries() throws Exception {
        var provider = new RecordingMetricsProvider();
        var clock = new AtomicLong();
        var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));
        var timer = Executors.newSingleThreadScheduledExecutor();
        var occupied = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try {
            executor.submit(() -> {
                occupied.countDown();
                return release.await(5, TimeUnit.SECONDS);
            });
            assertThat(occupied.await(5, TimeUnit.SECONDS)).isTrue();
            var manager = new QueryManager(context(provider, null, executor, timer), clock::get);
            var queued = submit(manager, "queued", "a");
            var rejected = submit(manager, "saturated", "b");

            assertThat(manager.getQueryError(rejected.queryId()).orElseThrow().errorType())
                    .isEqualTo("ASYNC_SUBMIT_QUEUE_FULL");
            assertThat(provider.rejections("QUEUE_FULL")).isOne();
            assertThat(provider.dispatchWaits).isEmpty();

            assertThat(manager.cancelQuery(queued.queryId())).isTrue();
            clock.set(TimeUnit.MILLISECONDS.toNanos(17));
            executor.shutdown();
            var stopping = submit(manager, "stopping", "c");
            assertThat(manager.getQueryError(stopping.queryId()).orElseThrow().errorType())
                    .isEqualTo("ASYNC_SUBMIT_STOPPING");
            assertThat(provider.rejections("EXECUTOR_STOPPING")).isOne();

            manager.setAcceptingQueries(false);
            assertThatThrownBy(() -> submit(manager, "closed", "d")).isInstanceOf(QueryRejectedException.class);
            assertThat(provider.rejections("ADMISSION_CLOSED")).isOne();
            assertThat(provider.rejections("TENANT_LIMIT")).isZero();

            release.countDown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(provider.dispatchWaits).containsExactly(17.0);
        } finally {
            release.countDown();
            executor.shutdownNow();
            timer.shutdownNow();
        }
    }

    @Test
    void tenantLimitDoesNotCountAsDispatchSaturation() {
        var provider = new RecordingMetricsProvider();
        TenantAdmissionPort limited = (user, active) -> {
            throw new TenantQuotaExceededException("fixture tenant limit");
        };
        var manager = new QueryManager(context(provider, limited, null, null));

        assertThatThrownBy(() -> submit(manager, "limited", "a")).isInstanceOf(TenantQuotaExceededException.class);
        assertThat(provider.rejections("TENANT_LIMIT")).isOne();
        assertThat(provider.rejections("QUEUE_FULL")).isZero();
        assertThat(provider.dispatchWaits).isEmpty();
    }

    private static QuerySubmissionResult submit(QueryManager manager, String id, String key) {
        var identity = new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
        var session = new QuerySession(id, Map.of(), identity, null, Optional.empty(),
                new OtelExecutionTraceContextFactory().capture());
        var acceptance = new QueryAcceptanceRequest("alice", key.repeat(64), "f".repeat(64));
        return manager.submitQueryAsync(id, "SELECT 1", session, acceptance).toCompletableFuture().join();
    }

    @Test
    void metricBackendFailureCannotPreventTimeoutOrLocalRelease() {
        var provider = new RecordingMetricsProvider();
        provider.failLifecycleRecording = true;
        var manager = newQueryManager(provider);
        String timedOut = registerRunningQuery(manager);
        manager.timeoutQueriesCreatedBy(Instant.MAX, Duration.ofSeconds(30));
        assertThat(manager.getQueryState(timedOut)).hasValue(QueryState.FAILED);
        assertThat(provider.counter("pangrova.coordinator.queries.failed")).isOne();
        assertThat(provider.counter("pangrova.coordinator.queries.timed_out")).isZero();
    }

    private static QueryManager newQueryManager(MetricsProvider metricsProvider) {
        return new QueryManager(context(metricsProvider, null, null, null));
    }

    private static QueryManagerContext context(MetricsProvider metrics, TenantAdmissionPort tenant,
            ExecutorService executor, ScheduledExecutorService timer) {
        var dispatcher = buildDispatcher();
        return new QueryManagerContext(dispatcher, TestObservability.TRACING, metrics, tenant,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                        null, null, null, dispatcher, null),
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), null),
                null, executor == null ? null : AsyncSubmitConfig.defaultEnabled(), executor, timer,
                new OtelExecutionTraceContextFactory());
    }

    private static String registerRunningQuery(QueryManager queryManager) {
        String queryId = "test-" + UUID.randomUUID();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine(queryId, sm);
        return queryId;
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                        scheduler, wm, new CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null, null);
    }

    /** In-memory MetricsProvider that records instrument values for assertions. */
    private static final class RecordingMetricsProvider implements MetricsProvider {
        private boolean failLifecycleRecording;
        private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> upDownCounters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> histogramCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<MetricAttributes, AtomicLong> rejectionCounts = new ConcurrentHashMap<>();
        private final ConcurrentLinkedQueue<Double> dispatchWaits = new ConcurrentLinkedQueue<>();

        long rejections(String reason) {
            var value = rejectionCounts.get(MetricAttributes.of("reason", reason));
            return value == null ? 0 : value.get();
        }

        long counter(String name) {
            AtomicLong v = counters.get(name);
            return v == null ? 0L : v.get();
        }

        long upDown(String name) {
            AtomicLong v = upDownCounters.get(name);
            return v == null ? 0L : v.get();
        }

        long histogramCount(String name) {
            AtomicLong v = histogramCounts.get(name);
            return v == null ? 0L : v.get();
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            if (REJECTIONS.equals(name)) {
                return new LongCounter() {
                    @Override
                    public void add(long value) {
                        throw new AssertionError("Admission rejection requires a reason");
                    }

                    @Override
                    public void add(long value, MetricAttributes attributes) {
                        rejectionCounts.computeIfAbsent(attributes, ignored -> new AtomicLong()).addAndGet(value);
                    }
                };
            }
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return value -> {
                if (failLifecycleRecording && name.endsWith(".timed_out")) {
                    throw new IllegalStateException("Metrics unavailable");
                }
                v.addAndGet(value);
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            AtomicLong v = upDownCounters.computeIfAbsent(name, k -> new AtomicLong());
            return v::addAndGet;
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                               String unit, List<Double> boundaries) {
            AtomicLong c = histogramCounts.computeIfAbsent(name, k -> new AtomicLong());
            return value -> {
                if (failLifecycleRecording && name.endsWith(".local_release_ms")) {
                    throw new IllegalStateException("Metrics unavailable");
                }
                if (name.equals(DISPATCH_QUEUE)) dispatchWaits.add(value);
                c.incrementAndGet();
            };
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                 String unit, Supplier<Double> supplier) {
            return () -> { /* This recording fixture retains no callback. */ };
        }
    }
}
