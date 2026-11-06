package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

import java.util.List;
import java.util.function.Supplier;

/**
 * Pre-defined coordinator-level metrics: query counters, duration histograms,
 * scheduling metrics, and active worker gauge.
 *
 * <p>Metric naming follows OTel conventions:
 * <ul>
 *   <li>Component segment: {@code pangrova.coordinator.*}</li>
 *   <li>Histograms use "duration" (not "latency")</li>
 *   <li>Duration unit is seconds ("s")</li>
 * </ul>
 */
public final class CoordinatorMetrics {

    private static final List<Double> DURATION_BOUNDARIES_SECONDS =
            List.of(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0);

    private final LongCounter queriesSubmitted;
    private final LongCounter queriesCompleted;
    private final LongCounter queriesFailed;
    private final LongUpDownCounter queriesRunning;
    private final DoubleHistogram queryDuration;
    private final LongCounter tasksScheduled;
    private final DoubleHistogram schedulingDuration;
    private final ObservableDoubleGauge workersActive;

    /**
     * Registers the full coordinator metric set, wiring the active-worker gauge.
     */
    public CoordinatorMetrics(MetricsProvider provider, Supplier<Double> activeWorkerSupplier) {
        this.queriesSubmitted = provider.createCounter(
                "pangrova.coordinator.queries.submitted", "Total queries submitted", "queries");
        this.queriesCompleted = provider.createCounter(
                "pangrova.coordinator.queries.completed", "Total queries completed", "queries");
        this.queriesFailed = provider.createCounter(
                "pangrova.coordinator.queries.failed", "Total queries failed", "queries");
        this.queriesRunning = provider.createUpDownCounter(
                "pangrova.coordinator.queries.running", "Currently running queries", "queries");
        this.queryDuration = provider.createHistogram(
                "pangrova.coordinator.query.duration", "Query duration", "s", DURATION_BOUNDARIES_SECONDS);
        this.tasksScheduled = provider.createCounter(
                "pangrova.coordinator.tasks.scheduled", "Total tasks scheduled", "tasks");
        this.schedulingDuration = provider.createHistogram(
                "pangrova.coordinator.scheduling.duration", "Scheduling duration", "s", DURATION_BOUNDARIES_SECONDS);
        this.workersActive = provider.createGauge(
                "pangrova.coordinator.workers.active", "Active workers", "workers", activeWorkerSupplier);
    }

    public LongCounter getQueriesSubmitted() { return queriesSubmitted; }

    public LongCounter getQueriesCompleted() { return queriesCompleted; }

    public LongCounter getQueriesFailed() { return queriesFailed; }

    public LongUpDownCounter getQueriesRunning() { return queriesRunning; }

    public DoubleHistogram getQueryDuration() { return queryDuration; }

    public LongCounter getTasksScheduled() { return tasksScheduled; }

    public DoubleHistogram getSchedulingDuration() { return schedulingDuration; }

    public ObservableDoubleGauge getWorkersActive() { return workersActive; }

    // --- Per-tenant/user dimensional recording methods (F-OBS-03) ---

    /**
     * Records a query submission with tenant and user dimensions.
     */
    public void recordQuerySubmitted(String tenantId, String user) {
        queriesSubmitted.add(1, tenantUserAttrs(tenantId, user));
    }

    /**
     * Records a query completion with tenant, user dimensions and duration.
     */
    public void recordQueryCompleted(String tenantId, String user, double durationSeconds) {
        MetricAttributes attrs = tenantUserAttrs(tenantId, user);
        queriesCompleted.add(1, attrs);
        queryDuration.record(durationSeconds, attrs);
    }

    /**
     * Records a query failure with tenant, user dimensions and duration.
     */
    public void recordQueryFailed(String tenantId, String user, double durationSeconds) {
        MetricAttributes attrs = tenantUserAttrs(tenantId, user);
        queriesFailed.add(1, attrs);
        queryDuration.record(durationSeconds, attrs);
    }

    private static MetricAttributes tenantUserAttrs(String tenantId, String user) {
        return MetricAttributes.of(
                "tenant_id", tenantId != null ? tenantId : "unknown",
                "user", user != null ? user : "unknown");
    }
}
