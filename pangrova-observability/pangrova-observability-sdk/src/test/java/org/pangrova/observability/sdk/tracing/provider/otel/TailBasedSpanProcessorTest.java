package org.pangrova.observability.sdk.tracing.provider.otel;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tests for {@link TailBasedSpanProcessor} — verifies that error traces and
 * slow queries are always exported regardless of head-based sampling decisions.
 */
class TailBasedSpanProcessorTest {

    private final List<SpanData> tailExported = new CopyOnWriteArrayList<>();
    private final List<SpanData> headExported = new CopyOnWriteArrayList<>();
    private SdkTracerProvider tracerProvider;

    @AfterEach
    void tearDown() {
        if (tracerProvider != null) {
            tracerProvider.shutdown();
        }
    }

    @Test
    void errorSpan_alwaysExported_evenWhenNotHeadSampled() {
        Tracer tracer = buildTracer(Sampler.alwaysOff(), true, Duration.ZERO);

        Span span = tracer.spanBuilder("failing-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setStatus(StatusCode.ERROR, "query execution failed");
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).hasSize(1);
        assertThat(tailExported.get(0).getName()).isEqualTo("failing-query");
        assertThat(tailExported.get(0).getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        // Head processor should NOT have exported (not sampled)
        assertThat(headExported).isEmpty();
    }

    @Test
    void okSpan_notExportedByTailProcessor_whenNotHeadSampled() {
        Tracer tracer = buildTracer(Sampler.alwaysOff(), true, Duration.ZERO);

        Span span = tracer.spanBuilder("normal-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setStatus(StatusCode.OK);
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).isEmpty();
        assertThat(headExported).isEmpty();
    }

    @Test
    void headSampledSpan_notDuplicatedByTailProcessor() {
        Tracer tracer = buildTracer(Sampler.alwaysOn(), true, Duration.ZERO);

        Span span = tracer.spanBuilder("sampled-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setStatus(StatusCode.ERROR, "error in sampled span");
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        // Head processor exports; tail processor skips (already sampled)
        assertThat(headExported).hasSize(1);
        assertThat(tailExported).isEmpty();
    }

    @Test
    void slowSpan_exported_whenExceedsThreshold() {
        Duration threshold = Duration.ofMillis(1);
        Tracer tracer = buildTracer(Sampler.alwaysOff(), false, threshold);

        Span span = tracer.spanBuilder("slow-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        // Sleep to exceed the 1ms threshold
        sleepUninterruptibly(Duration.ofMillis(10));
        span.setStatus(StatusCode.OK);
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).hasSize(1);
        assertThat(tailExported.get(0).getName()).isEqualTo("slow-query");
    }

    @Test
    void fastSpan_notExported_belowThreshold() {
        Duration threshold = Duration.ofSeconds(60);
        Tracer tracer = buildTracer(Sampler.alwaysOff(), false, threshold);

        Span span = tracer.spanBuilder("fast-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setStatus(StatusCode.OK);
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).isEmpty();
    }

    @Test
    void errorSamplingDisabled_errorsNotTailSampled() {
        Tracer tracer = buildTracer(Sampler.alwaysOff(), false, Duration.ZERO);

        Span span = tracer.spanBuilder("error-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.setStatus(StatusCode.ERROR, "this error should be ignored");
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).isEmpty();
    }

    @Test
    void bothCriteriaTrigger_errorAndSlowSpan_exportedOnce() {
        Duration threshold = Duration.ofMillis(1);
        Tracer tracer = buildTracer(Sampler.alwaysOff(), true, threshold);

        Span span = tracer.spanBuilder("error-and-slow")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        sleepUninterruptibly(Duration.ofMillis(10));
        span.setStatus(StatusCode.ERROR, "slow and errored");
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        // Should export exactly once (error criterion triggers first)
        assertThat(tailExported).hasSize(1);
    }

    @Test
    void alwaysRecordSampler_convertsDrop_toRecordOnly() {
        Sampler neverSample = Sampler.alwaysOff();
        Sampler wrapped = TailBasedSpanProcessor.alwaysRecordSampler(neverSample);

        SamplingResult result = wrapped.shouldSample(
                io.opentelemetry.context.Context.root(),
                "0000000000000001",
                "test-span",
                SpanKind.INTERNAL,
                Attributes.empty(),
                List.of());

        assertThat(result.getDecision()).isEqualTo(SamplingDecision.RECORD_ONLY);
    }

    @Test
    void alwaysRecordSampler_preservesRecordAndSample() {
        Sampler alwaysSample = Sampler.alwaysOn();
        Sampler wrapped = TailBasedSpanProcessor.alwaysRecordSampler(alwaysSample);

        SamplingResult result = wrapped.shouldSample(
                io.opentelemetry.context.Context.root(),
                "0000000000000001",
                "test-span",
                SpanKind.INTERNAL,
                Attributes.empty(),
                List.of());

        assertThat(result.getDecision()).isEqualTo(SamplingDecision.RECORD_AND_SAMPLE);
    }

    @Test
    void alwaysRecordSampler_description_includesDelegate() {
        Sampler delegate = Sampler.traceIdRatioBased(0.5);
        Sampler wrapped = TailBasedSpanProcessor.alwaysRecordSampler(delegate);

        assertThat(wrapped.getDescription()).contains("AlwaysRecordSampler");
        assertThat(wrapped.getDescription()).contains(delegate.getDescription());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ERROR"})
    void exceptionRecordedSpan_treatedAsError(String statusCodeName) {
        Tracer tracer = buildTracer(Sampler.alwaysOff(), true, Duration.ZERO);

        Span span = tracer.spanBuilder("exception-query")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        span.recordException(new RuntimeException("boom"));
        span.setStatus(StatusCode.valueOf(statusCodeName), "exception occurred");
        span.end();

        tracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(tailExported).hasSize(1);
        assertThat(tailExported.get(0).getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    }

    @Test
    void processorReports_endRequired_true() {
        SpanExporter exporter = new CollectingExporter(tailExported);
        TailBasedSpanProcessor processor = new TailBasedSpanProcessor(exporter, true, Duration.ZERO);

        assertThat(processor.isEndRequired()).isTrue();
        assertThat(processor.isStartRequired()).isFalse();

        processor.shutdown();
    }

    // ---------- helpers ----------

    /**
     * Builds a tracer with:
     * - Head sampler wrapped in AlwaysRecordSampler (so non-sampled spans are RECORD_ONLY)
     * - BatchSpanProcessor for head-sampled spans -> headExported list
     * - TailBasedSpanProcessor for tail criteria -> tailExported list
     */
    private Tracer buildTracer(Sampler headSampler,
                               boolean errorSamplingEnabled,
                               Duration slowThreshold) {
        SpanExporter headCollector = new CollectingExporter(headExported);
        SpanExporter tailCollector = new CollectingExporter(tailExported);

        Sampler sampler = TailBasedSpanProcessor.alwaysRecordSampler(headSampler);

        tracerProvider = SdkTracerProvider.builder()
                .setResource(Resource.getDefault())
                .setSampler(sampler)
                .addSpanProcessor(BatchSpanProcessor.builder(headCollector).build())
                .addSpanProcessor(new TailBasedSpanProcessor(
                        tailCollector, errorSamplingEnabled, slowThreshold))
                .build();

        return tracerProvider.get("test-instrumentation");
    }

    /** In-memory exporter that collects all exported spans into a list. */
    private static final class CollectingExporter implements SpanExporter {

        private final List<SpanData> collected;

        CollectingExporter(List<SpanData> target) {
            this.collected = target;
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            collected.addAll(spans);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    private static void sleepUninterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
