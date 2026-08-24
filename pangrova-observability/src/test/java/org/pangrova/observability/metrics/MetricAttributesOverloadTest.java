package org.pangrova.observability.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that metric recording methods with {@link MetricAttributes}
 * correctly propagate dimensional attributes to the OTel export pipeline.
 */
class MetricAttributesOverloadTest {

    private InMemoryMetricReader reader;
    private SdkMeterProvider meterProvider;
    private MetricsProvider metricsProvider;

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
