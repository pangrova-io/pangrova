package org.pangrova.observability.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.pangrova.spi.observability.Scope;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the span <em>activation</em> contract (F-CHAIN-OBS-01): an entry point that calls
 * {@link Span#makeCurrent()} causes subsequent {@code startSpan} calls to attach as children
 * (same trace, parent linkage) and {@code currentTraceId()} to be non-zero.
 *
 * <p>The regression case demonstrates the bug being fixed: when the entry span is NOT made current,
 * a nested span detaches into its own root trace — exactly the silent breakage where nested spans
 * "脱挂" and trace ids come back zero.
 */
class SpanActivationTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private TracingProvider provider;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        provider = new OtelTracingProvider(tracerProvider.get("pangrova-activation-test"));
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void nestedSpanAttachesToActivatedParent() {
        Span parent = provider.startSpan("parent", SpanKind.SERVER);
        Span child;
        try (Scope ignored = parent.makeCurrent()) {
            // No explicit parent passed: must inherit the activated context.
            child = provider.startSpan("child", SpanKind.INTERNAL);
            child.end();
        }
        parent.end();

        SpanData parentData = findSpan("parent");
        SpanData childData = findSpan("child");

        assertThat(childData.getSpanContext().getTraceId())
                .as("child must share the parent's trace id")
                .isEqualTo(parentData.getSpanContext().getTraceId());
        assertThat(childData.getParentSpanContext().getSpanId())
                .as("child's parent span id must be the activated parent's span id")
                .isEqualTo(parentData.getSpanContext().getSpanId());
    }

    @Test
    void currentSpanReflectsActivatedSpan() {
        Span parent = provider.startSpan("parent", SpanKind.SERVER);
        String activeTraceId;
        try (Scope ignored = parent.makeCurrent()) {
            activeTraceId = ((OtelSpanWrapper) provider.currentSpan())
                    .unwrap().getSpanContext().getTraceId();
        }
        parent.end();

        assertThat(activeTraceId)
                .as("currentSpan() must report the activated span's (non-zero) trace id")
                .isEqualTo(findSpan("parent").getSpanContext().getTraceId())
                .isNotEqualTo("00000000000000000000000000000000");
    }

    @Test
    void withoutActivationNestedSpanDetachesIntoOwnRoot() {
        // Regression: this is the pre-fix behavior — no makeCurrent at the entry, so the nested span
        // does NOT attach to "parent" and instead starts its own root trace.
        Span parent = provider.startSpan("parent", SpanKind.SERVER);
        Span child = provider.startSpan("child", SpanKind.INTERNAL); // no makeCurrent around this
        child.end();
        parent.end();

        SpanData parentData = findSpan("parent");
        SpanData childData = findSpan("child");

        assertThat(childData.getSpanContext().getTraceId())
                .as("without activation, child is a detached root with a different trace id")
                .isNotEqualTo(parentData.getSpanContext().getTraceId());
        assertThat(childData.getParentSpanContext().isValid())
                .as("without activation, child has no valid parent")
                .isFalse();
    }

    private SpanData findSpan(String name) {
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        return spans.stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no span named " + name + " in " + spans));
    }
}
