package org.pangrova.worker.execution;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.worker.jni.NativeRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-CHAIN-OBS-04 fix: JNI trace context propagation passes BOTH
 * traceId AND parentSpanId to the native layer, preventing orphaned native spans.
 *
 * <p>Without this fix, native spans share the same trace ID but have no parent
 * linkage, making them appear as disconnected roots in distributed trace visualization.</p>
 */
class NativeTaskExecutorTraceContextTest {

    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_SPAN_ID = "0000000000000000";

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private Tracer tracer;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        tracer = tracerProvider.get("pangrova-trace-context-test");
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void activeSpanPropagatesBothTraceIdAndSpanId() {
        Span span = tracer.spanBuilder("native-task-create").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            String traceId = NativeTraceExporter.currentTraceId();
            String spanId = NativeTraceExporter.currentSpanId();

            assertThat(traceId)
                    .as("traceId must be valid (non-zero) when span is active")
                    .isNotEqualTo(INVALID_TRACE_ID)
                    .hasSize(32)
                    .matches("[0-9a-f]{32}");

            assertThat(spanId)
                    .as("spanId must be valid (non-zero) when span is active — "
                            + "this is the parent span ID passed to native")
                    .isNotEqualTo(INVALID_SPAN_ID)
                    .hasSize(16)
                    .matches("[0-9a-f]{16}");
        } finally {
            span.end();
        }
    }

    @Test
    void nativeRuntimeExposesBothTraceIdAndSpanIdInCreateMethod() {
        // Verify the JNI bridge method that accepts full trace context exists
        // with the correct signature: (byte[], String, String) -> long
        Method method = findMethod(NativeRuntime.class,
                "nativeCreateTaskExecutorWithTraceContext",
                byte[].class, String.class, String.class);

        assertThat(method)
                .as("NativeRuntime must expose nativeCreateTaskExecutorWithTraceContext"
                        + "(byte[], String traceId, String spanId)")
                .isNotNull();
        assertThat(method.getReturnType())
                .as("Must return a long executor handle")
                .isEqualTo(long.class);
    }

    @Test
    void spanIdMatchesCurrentSpanForParentLinkage() {
        // The spanId passed to native becomes the parent_span_id in the native trace.
        // Verify it matches the current span's ID (not some other value).
        Span parentSpan = tracer.spanBuilder("task-dispatch").startSpan();
        try (Scope ignored = parentSpan.makeCurrent()) {
            String exportedSpanId = NativeTraceExporter.currentSpanId();
            String actualSpanId = parentSpan.getSpanContext().getSpanId();

            assertThat(exportedSpanId)
                    .as("Exported spanId must equal the active span's ID — "
                            + "native will use this as parent_span_id")
                    .isEqualTo(actualSpanId);
        } finally {
            parentSpan.end();
        }
    }

    @Test
    void noActiveSpanReturnsSentinelValues() {
        // When no span is active, both must return well-formed sentinels (all zeros).
        // Native side should treat these as "no parent context" rather than crash.
        String traceId = NativeTraceExporter.currentTraceId();
        String spanId = NativeTraceExporter.currentSpanId();

        assertThat(traceId).isEqualTo(INVALID_TRACE_ID);
        assertThat(spanId).isEqualTo(INVALID_SPAN_ID);
    }

    private static Method findMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
