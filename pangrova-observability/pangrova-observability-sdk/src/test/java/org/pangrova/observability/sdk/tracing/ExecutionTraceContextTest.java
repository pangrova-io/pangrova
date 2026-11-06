package org.pangrova.observability.sdk.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.observability.sdk.tracing.nativebridge.NativeTraceExporter;

class ExecutionTraceContextTest {
    @Test
    void executionWithoutExporterHasDistinctValidUnsampledRoots() {
        var first = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
        var second = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
        assertThat(first.traceId()).matches("[0-9a-f]{32}").doesNotContainOnlyWhitespaces();
        assertThat(first.traceId()).isNotEqualTo("00000000000000000000000000000000");
        assertThat(first.spanId()).matches("[0-9a-f]{16}").isNotEqualTo("0000000000000000");
        assertThat(second.traceId()).isNotEqualTo(first.traceId());
        assertThat(second.spanId()).isNotEqualTo(first.spanId());
        assertThat(first.traceFlags()).isZero();
        assertThat(first.traceState()).isEmpty();
        assertThat(Span.current().getSpanContext().isValid()).isFalse();
    }

    @Test
    void capturesActualSampledAndUnsampledSdkSpans() {
        for (var sampler : new Sampler[] {Sampler.alwaysOn(), Sampler.alwaysOff()}) {
            try (var provider = SdkTracerProvider.builder().setSampler(sampler).build()) {
                var parent = provider.get("actual-execution-trace").spanBuilder("query").startSpan();
                try (var scope = parent.makeCurrent()) {
                    var captured = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
                    assertThat(captured.traceId()).isEqualTo(parent.getSpanContext().getTraceId());
                    assertThat(captured.spanId()).isEqualTo(parent.getSpanContext().getSpanId());
                    assertThat(captured.traceFlags())
                            .isEqualTo(Byte.toUnsignedInt(parent.getSpanContext().getTraceFlags().asByte()));
                } finally {
                    parent.end();
                }
            }
        }
    }

    @Test
    void snapshotSurvivesAsyncThreadHandoffAndRestoresItsPreviousContext() throws Exception {
        var captured = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
        var executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                var previous = Span.current().getSpanContext();
                try (var scope = captured.makeCurrent()) {
                    assertThat(NativeTraceExporter.currentTraceId()).isEqualTo(captured.traceId());
                    assertThat(NativeTraceExporter.currentSpanId()).isEqualTo(captured.spanId());
                    assertThat(Span.current().isRecording()).isFalse();
                }
                assertThat(Span.current().getSpanContext()).isEqualTo(previous);
            }).get(5, TimeUnit.SECONDS);
            assertThat(Span.current().getSpanContext().isValid()).isFalse();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void preservesOfficialW3CStateEncodingAndFlagsWithoutStartingAnExporter() {
        var root = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
        var context = SpanContext.createFromRemoteParent(root.traceId(), root.spanId(), TraceFlags.getSampled(),
                TraceState.builder().put("first", "one").put("second", "two").build());
        try (var scope = Span.wrap(context).makeCurrent()) {
            var captured = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
            assertThat(captured.traceId()).isEqualTo(context.getTraceId());
            assertThat(captured.spanId()).isEqualTo(context.getSpanId());
            assertThat(captured.traceFlags()).isEqualTo(1);
            assertThat(captured.traceState()).isEqualTo("second=two,first=one");
            try (var active = captured.makeCurrent()) {
                assertThat(Span.current().getSpanContext().getTraceState()).isEqualTo(context.getTraceState());
            }
        }
    }
}
