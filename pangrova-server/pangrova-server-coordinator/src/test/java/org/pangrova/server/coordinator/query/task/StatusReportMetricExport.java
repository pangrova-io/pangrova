package org.pangrova.server.coordinator.query.task;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import org.pangrova.observability.sdk.metrics.otel.OtelMetricsProvider;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.observability.api.metrics.MetricsProvider;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Observes the real production metrics provider through the real SDK export boundary. */
@ThreadSafe
final class StatusReportMetricExport implements MetricExporter {
    private final AtomicReference<List<MetricData>> observed = new AtomicReference<>(List.of());
    private final SdkMeterProvider meter = SdkMeterProvider.builder().registerMetricReader(
            PeriodicMetricReader.builder(this).setInterval(Duration.ofHours(1)).build()).build();

    MetricsProvider provider() { return new OtelMetricsProvider(meter.get("status-report-delivery-test")); }

    long failedReports() {
        var flushed = meter.forceFlush().join(5, TimeUnit.SECONDS);
        if (!flushed.isSuccess()) throw new IllegalStateException("Actual metrics export did not complete");
        return observed.get().stream().filter(metric -> metric.getName().equals("task_status_report_failed"))
                .flatMap(metric -> metric.getLongSumData().getPoints().stream())
                .mapToLong(point -> point.getValue()).sum();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType type) {
        return AggregationTemporality.CUMULATIVE;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        observed.set(List.copyOf(metrics));
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() { return CompletableResultCode.ofSuccess(); }

    @Override
    public CompletableResultCode shutdown() { return CompletableResultCode.ofSuccess(); }

    @Override
    public void close() { meter.close(); }
}
