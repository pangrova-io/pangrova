package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.ObservableDoubleGauge;
import org.pangrova.testing.testkit.TestObservability;
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

    @Test
    @DisplayName("completing a query records completed counter")
    void completionRecordsKpis() {
        RecordingMetricsProvider provider = new RecordingMetricsProvider();
        QueryManager queryManager = newQueryManager(provider);

        String queryId = registerRunningQuery(queryManager);
        queryManager.onAllTasksFinished(queryId);

        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FINISHED);
        assertThat(provider.counter("pangrova.coordinator.queries.completed")).isEqualTo(1L);
    }

    @Test
    @DisplayName("failing a query records failed counter")
    void failureRecordsKpis() {
        RecordingMetricsProvider provider = new RecordingMetricsProvider();
        QueryManager queryManager = newQueryManager(provider);

        String queryId = registerRunningQuery(queryManager);
        queryManager.onTaskFailed(queryId, new ErrorDetail("RUNTIME_INTERNAL", "boom", null));

        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FAILED);
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
                .hasValue(QueryStateMachine.State.FINISHED);
    }

    private static QueryManager newQueryManager(MetricsProvider metricsProvider) {
        QueryManagerContext ctx = new QueryManagerContext(
                buildDispatcher(), TestObservability.TRACING,
                metricsProvider, null, null, null, null, null, null, null,
                null, null);
        return new QueryManager(ctx);
    }

    private static String registerRunningQuery(QueryManager queryManager) {
        String queryId = "test-" + UUID.randomUUID();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine(queryId, sm);
        return queryId;
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
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
        private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> upDownCounters = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> histogramCounts = new ConcurrentHashMap<>();

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
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return v::addAndGet;
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
            return value -> c.incrementAndGet();
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                 String unit, Supplier<Double> supplier) {
            return new ObservableDoubleGauge() {};
        }
    }
}
