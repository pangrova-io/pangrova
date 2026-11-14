package org.pangrova.observability.sdk.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.observability.sdk.metrics.otel.OtelMetricsProvider;

/**
 * Verifies that metric recording methods with {@link MetricAttributes}
 * correctly propagate dimensional attributes to the OTel export pipeline.
 */
class MetricAttributesOverloadTest {

    private InMemoryMetricReader reader;
    private SdkMeterProvider meterProvider;
    private MetricsProvider metricsProvider;

    @Test
    void streamGaugeExportsKnownOffsetsWithDimensionsAndUnregisters() {
        var sample = new java.util.concurrent.atomic.AtomicReference<>(java.util.OptionalDouble.empty());
        String name = "pangrova.streaming.cdc.consumer.lag";
        try (var scope = new org.pangrova.observability.metrics.MetricsScope(metricsProvider)) {
            try (var registration = scope.createObservedGauge(name, "Offset distance", "{offset}",
                    MetricAttributes.of("stream", "orders"), sample::get)) {
                assertThat(findMetric(reader.collectAllMetrics(), name)).isNull();
                sample.set(java.util.OptionalDouble.of(17));
                var point = findMetric(reader.collectAllMetrics(), name).getDoubleGaugeData().getPoints()
                        .iterator().next();
                assertThat(point.getValue()).isEqualTo(17);
                assertThat(point.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("stream")))
                        .isEqualTo("orders");
            }
            assertThat(findMetric(reader.collectAllMetrics(), name)).isNull();
        }
    }

    @Test
    void coordinatorGettersRetainRegisteredInstrumentIdentityAndExportUpdates() {
        var metrics = new CoordinatorMetrics(metricsProvider, () -> 1.0);
        assertThat(metrics.getQueriesSubmitted()).isSameAs(metrics.getQueriesSubmitted());
        assertThat(metrics.getQueriesCompleted()).isSameAs(metrics.getQueriesCompleted());
        assertThat(metrics.getQueriesFailed()).isSameAs(metrics.getQueriesFailed());
        assertThat(metrics.getTasksScheduled()).isSameAs(metrics.getTasksScheduled());
        assertThat(metrics.getQueriesRunning()).isSameAs(metrics.getQueriesRunning());
        metrics.getQueriesSubmitted().add(2);
        metrics.getQueriesCompleted().add(2);
        metrics.getQueriesFailed().add(2);
        metrics.getTasksScheduled().add(2);
        metrics.getQueriesRunning().add(3);
        metrics.getQueriesRunning().add(-1);
        for (String name : List.of("queries.submitted", "queries.completed", "queries.failed",
                "tasks.scheduled", "queries.running")) {
            assertExportedSum("pangrova.coordinator." + name, 2);
        }
    }

    @Test
    void workerGettersRetainRegisteredInstrumentIdentityAndExportUpdates() {
        var metrics = new WorkerMetrics(metricsProvider, () -> 1.0, () -> 2.0);
        assertThat(metrics.getTasksReceived()).isSameAs(metrics.getTasksReceived());
        assertThat(metrics.getTasksCompleted()).isSameAs(metrics.getTasksCompleted());
        assertThat(metrics.getTasksFailed()).isSameAs(metrics.getTasksFailed());
        assertThat(metrics.getTasksRunning()).isSameAs(metrics.getTasksRunning());
        assertThat(metrics.getIoBytesRead()).isSameAs(metrics.getIoBytesRead());
        assertThat(metrics.getIoBytesWritten()).isSameAs(metrics.getIoBytesWritten());
        assertThat(metrics.getIoRowsRead()).isSameAs(metrics.getIoRowsRead());
        assertThat(metrics.getIoRowsWritten()).isSameAs(metrics.getIoRowsWritten());
        metrics.getTasksReceived().add(2);
        metrics.getTasksCompleted().add(2);
        metrics.getTasksFailed().add(2);
        metrics.getTasksRunning().add(3);
        metrics.getTasksRunning().add(-1);
        metrics.getIoBytesRead().add(2);
        metrics.getIoBytesWritten().add(2);
        metrics.getIoRowsRead().add(2);
        metrics.getIoRowsWritten().add(2);
        for (String name : List.of("tasks.received", "tasks.completed", "tasks.failed", "tasks.running",
                "io.bytes.read", "io.bytes.written", "io.rows.read", "io.rows.written")) {
            assertExportedSum("pangrova.worker." + name, 2);
        }
    }

    private void assertExportedSum(String name, long value) {
        MetricData metric = findMetric(reader.collectAllMetrics(), name);
        assertThat(metric.getLongSumData().getPoints()).hasSize(1);
        assertThat(metric.getLongSumData().getPoints().iterator().next().getValue()).isEqualTo(value);
    }

    @BeforeEach
    void setUp() {
        reader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .build();
        Meter meter = meterProvider.get("test");
        metricsProvider = new OtelMetricsProvider(meter);
    }

    @AfterEach
    void tearDown() {
        meterProvider.close();
    }

    @Test
    void counterWithAttributesExportsAttributes() {
        LongCounter counter = metricsProvider.createCounter(
                "test.requests", "Total requests", "1");
        MetricAttributes attrs = MetricAttributes.of("tenant", "acme", "region", "us-east-1");

        counter.add(5, attrs);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        MetricData requestMetric = findMetric(metrics, "test.requests");
        assertThat(requestMetric).isNotNull();

        PointData point = requestMetric.getLongSumData().getPoints().iterator().next();
        assertThat(point.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("tenant"))).isEqualTo("acme");
        assertThat(point.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("region"))).isEqualTo("us-east-1");
    }

    @Test
    void histogramWithAttributesExportsAttributes() {
        DoubleHistogram histogram = metricsProvider.createHistogram(
                "test.latency", "Request latency", "ms",
                List.of(1.0, 5.0, 10.0, 50.0, 100.0));
        MetricAttributes attrs = MetricAttributes.of("endpoint", "/api/query");

        histogram.record(42.5, attrs);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        MetricData latencyMetric = findMetric(metrics, "test.latency");
        assertThat(latencyMetric).isNotNull();

        PointData point = latencyMetric.getHistogramData().getPoints().iterator().next();
        assertThat(point.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("endpoint"))).isEqualTo("/api/query");
    }

    @Test
    void upDownCounterWithAttributesExportsAttributes() {
        LongUpDownCounter upDown = metricsProvider.createUpDownCounter(
                "test.active_connections", "Active connections", "1");
        MetricAttributes attrs = MetricAttributes.of("pool", "main");

        upDown.add(3, attrs);
        upDown.add(-1, attrs);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        MetricData connectionMetric = findMetric(metrics, "test.active_connections");
        assertThat(connectionMetric).isNotNull();

        PointData point = connectionMetric.getLongSumData().getPoints().iterator().next();
        assertThat(point.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("pool"))).isEqualTo("main");
    }

    @Test
    void counterWithoutAttributesStillWorks() {
        LongCounter counter = metricsProvider.createCounter(
                "test.simple", "Simple counter", "1");

        counter.add(10);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        MetricData simpleMetric = findMetric(metrics, "test.simple");
        assertThat(simpleMetric).isNotNull();

        PointData point = simpleMetric.getLongSumData().getPoints().iterator().next();
        assertThat(point.getAttributes().isEmpty()).isTrue();
    }

    @Test
    void counterWithEmptyAttributesBehavesLikeNoAttributes() {
        LongCounter counter = metricsProvider.createCounter(
                "test.empty_attrs", "Counter with empty attrs", "1");

        counter.add(7, MetricAttributes.empty());

        Collection<MetricData> metrics = reader.collectAllMetrics();
        MetricData metric = findMetric(metrics, "test.empty_attrs");
        assertThat(metric).isNotNull();

        PointData point = metric.getLongSumData().getPoints().iterator().next();
        assertThat(point.getAttributes().isEmpty()).isTrue();
    }

    private static MetricData findMetric(Collection<MetricData> metrics, String name) {
        return metrics.stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
