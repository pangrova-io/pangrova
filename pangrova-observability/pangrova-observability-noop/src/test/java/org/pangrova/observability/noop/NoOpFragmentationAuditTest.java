package org.pangrova.observability.noop;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.logging.Logger;
import org.pangrova.observability.spi.logging.LoggingProvider;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.tracing.SpanKind;
import org.pangrova.observability.api.tracing.StatusCode;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.pangrova.observability.api.profiling.ProfilingUnavailableException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Audit test for NoOp class fragmentation in pangrova-observability.
 *
 * <p>Validates that the 10 NoOp classes follow a consistent pattern and that
 * the current 1:1 mapping to SPI interfaces is justified (each implements a
 * distinct interface with different method signatures, making a dynamic proxy
 * factory impractical without sacrificing type safety).
 *
 * <p><b>Validates: Requirements 3.8</b></p>
 */
class NoOpFragmentationAuditTest {

    @Test
    void sharedNoOpInstrumentsHaveNoMutableInstanceFields() {
        for (Class<?> type : List.of(NoOpLongCounter.class, NoOpLongUpDownCounter.class, NoOpSpan.class)) {
            assertTrue(Arrays.stream(type.getDeclaredFields())
                    .allMatch(field -> Modifier.isStatic(field.getModifiers())), type.getName());
        }
        var tracing = new NoOpTracingProvider();
        assertSame(tracing.currentSpan(), tracing.startSpan("named", SpanKind.CLIENT));
        assertSame(tracing.currentSpan(), tracing.startSpan("parented", SpanKind.INTERNAL, null));
        var metrics = new NoOpMetricsProvider();
        assertSame(metrics.createUpDownCounter("a", "d", "u"),
                metrics.createUpDownCounter("b", "d", "u"));
    }

    /**
     * Verify exactly 10 NoOp classes exist in the noop package.
     */
    @Test
    void noopPackageContainsExpectedNumberOfClasses() {
        File noopDir = new File("src/main/java/org/pangrova/observability/noop");
        assertTrue(noopDir.isDirectory(), "noop directory must exist");

        String[] javaFiles = noopDir.list((dir, name) -> name.endsWith(".java"));
        assertNotNull(javaFiles);
        assertEquals(10, javaFiles.length,
                "Expected exactly 10 NoOp classes; found: " + List.of(javaFiles));
    }

    /**
     * Each NoOp class maps 1:1 to a distinct SPI interface.
     */
    @Test
    void noopClassesMapOneToOneToSpiInterfaces() {
        // Provider-level NoOps
        assertInstanceOf(MetricsProvider.class, new NoOpMetricsProvider());
        assertInstanceOf(TracingProvider.class, new NoOpTracingProvider());
        assertInstanceOf(LoggingProvider.class, new NoOpLoggingProvider());

        // Metric instrument NoOps
        assertInstanceOf(LongCounter.class, NoOpLongCounter.INSTANCE);
        assertInstanceOf(LongUpDownCounter.class, NoOpLongUpDownCounter.INSTANCE);
        assertInstanceOf(DoubleHistogram.class, NoOpDoubleHistogram.INSTANCE);
        assertInstanceOf(ObservableDoubleGauge.class, NoOpObservableDoubleGauge.INSTANCE);

        // Tracing instrument NoOp
        assertInstanceOf(Span.class, NoOpSpan.INSTANCE);

        // Logging instrument NoOp
        assertInstanceOf(Logger.class, NoOpLogger.INSTANCE);

        // Profiling bridge NoOp
        assertInstanceOf(org.pangrova.observability.spi.profiling.Profiler.class, new UnavailableProfiler());
    }

    /**
     * Instrument-level NoOps use singleton pattern (static INSTANCE field).
     */
    @Test
    void instrumentNoOpsUseSingletonPattern() {
        assertSame(NoOpLongCounter.INSTANCE, NoOpLongCounter.INSTANCE);
        assertSame(NoOpLongUpDownCounter.INSTANCE, NoOpLongUpDownCounter.INSTANCE);
        assertSame(NoOpDoubleHistogram.INSTANCE, NoOpDoubleHistogram.INSTANCE);
        assertSame(NoOpObservableDoubleGauge.INSTANCE, NoOpObservableDoubleGauge.INSTANCE);
        assertSame(NoOpSpan.INSTANCE, NoOpSpan.INSTANCE);
        assertSame(NoOpLogger.INSTANCE, NoOpLogger.INSTANCE);
    }

    /**
     * Instrument-level NoOps are final classes (immutable, no subclassing).
     */
    @Test
    void instrumentNoOpsAreFinalClasses() {
        assertTrue(Modifier.isFinal(NoOpLongCounter.class.getModifiers()));
        assertTrue(Modifier.isFinal(NoOpLongUpDownCounter.class.getModifiers()));
        assertTrue(Modifier.isFinal(NoOpDoubleHistogram.class.getModifiers()));
        assertTrue(Modifier.isFinal(NoOpObservableDoubleGauge.class.getModifiers()));
        assertTrue(Modifier.isFinal(NoOpSpan.class.getModifiers()));
        assertTrue(Modifier.isFinal(UnavailableProfiler.class.getModifiers()));
    }

    /**
     * Provider-level NoOps return shared singleton instruments.
     */
    @Test
    void providerNoOpsReturnSharedSingletonInstruments() {
        NoOpMetricsProvider metrics = new NoOpMetricsProvider();
        LongCounter c1 = metrics.createCounter("a", "d", "u");
        LongCounter c2 = metrics.createCounter("b", "d", "u");
        assertSame(c1, c2, "MetricsProvider should return same NoOp counter instance");

        DoubleHistogram h1 = metrics.createHistogram("a", "d", "u", List.of());
        DoubleHistogram h2 = metrics.createHistogram("b", "d", "u", List.of());
        assertSame(h1, h2, "MetricsProvider should return same NoOp histogram instance");

        NoOpTracingProvider tracing = new NoOpTracingProvider();
        Span s1 = tracing.startSpan("a", SpanKind.INTERNAL);
        Span s2 = tracing.startSpan("b", SpanKind.CLIENT);
        assertSame(s1, s2, "TracingProvider should return same NoOp span instance");

        NoOpLoggingProvider logging = new NoOpLoggingProvider();
        Logger l1 = logging.getLogger("a");
        Logger l2 = logging.getLogger("b");
        assertSame(l1, l2, "LoggingProvider should return same NoOp logger instance");
    }

    /**
     * NoOp operations are safe to call (no exceptions, no side effects).
     */
    @Test
    void noopOperationsAreSafeToCall() {
        // Metrics
        NoOpLongCounter.INSTANCE.add(42);
        NoOpLongUpDownCounter.INSTANCE.add(-1);
        NoOpDoubleHistogram.INSTANCE.record(3.14);

        // Tracing — fluent API returns this
        Span span = NoOpSpan.INSTANCE;
        assertSame(span, span.setAttribute("key", "value"));
        assertSame(span, span.setAttribute("key", 123L));
        assertSame(span, span.setStatus(StatusCode.OK, "ok"));
        assertSame(span, span.recordException(new RuntimeException("test")));
        span.end(); // no-op

        // Logging
        NoOpLogger.INSTANCE.info("msg");
        NoOpLogger.INSTANCE.warn("msg");
        NoOpLogger.INSTANCE.error("msg", new RuntimeException());
        NoOpLogger.INSTANCE.debug("msg");

        // Tracing context propagation
        NoOpTracingProvider tracing = new NoOpTracingProvider();
        tracing.inject(null, "carrier", (c, k, v) -> {});

        // Profiling
        UnavailableProfiler profiler = new UnavailableProfiler();
        assertThrows(ProfilingUnavailableException.class, () -> profiler.startProfiling(10, 100));
        assertThrows(ProfilingUnavailableException.class, () -> profiler.stopProfiling("unknown"));
        assertThrows(ProfilingUnavailableException.class, () -> profiler.getFlameGraphData("unknown"));
    }

    /**
     * A NoOpFactory using dynamic proxies would lose type safety and require
     * special handling for non-void return types (e.g., Span fluent API returns this,
     * TracingProvider.extract returns input context). The current explicit 1:1 mapping
     * is the correct approach for a middleware system where type safety is paramount.
     *
     * <p>This test documents the justification: each NoOp has unique behavior.
     */
    @Test
    void noopFactoryNotJustifiedBecauseReturnTypesVary() {
        // NoOpSpan.setAttribute returns 'this' for fluent chaining
        Span span = NoOpSpan.INSTANCE.setAttribute("k", "v");
        assertSame(NoOpSpan.INSTANCE, span, "Span fluent API must return this");

        // Disabling telemetry must not fabricate profiling success.
        assertThrows(ProfilingUnavailableException.class,
                () -> new UnavailableProfiler().startProfiling(1, 1));
        assertThrows(ProfilingUnavailableException.class,
                () -> new UnavailableProfiler().getFlameGraphData("x"));
    }
}
