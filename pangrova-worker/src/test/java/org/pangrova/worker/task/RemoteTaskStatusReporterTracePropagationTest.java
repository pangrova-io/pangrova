package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.spi.observability.Context;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.Test;

/**
 * Validates that RemoteTaskStatusReporter injects trace context into outgoing HTTP headers.
 * Uses a recording TracingProvider — no mocks.
 */
class RemoteTaskStatusReporterTracePropagationTest {

    @Test
    void report_injectsTraceContextHeaders() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        // Port 0 means unreachable — report is best-effort, errors are logged
        RemoteTaskStatusReporter reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", recorder);

        reporter.report(dummyReport());

        assertThat(recorder.injectCalled).isTrue();
    }

    @Test
    void report_usesHttpRequestBuilderAsCarrier() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        RemoteTaskStatusReporter reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", recorder);

        reporter.report(dummyReport());

        assertThat(recorder.lastSetter).isNotNull();
    }

    @Test
    void noOpTracingProvider_doesNotThrow() {
        RemoteTaskStatusReporter reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", TestObservability.TRACING);

        // Should not throw — best-effort reporting with NoOp tracing
        reporter.report(dummyReport());
    }

    private static TaskStatusReport dummyReport() {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "RUNNING",
                null, null, null, 1, System.currentTimeMillis(), null, null);
    }

    /**
     * Real TracingProvider that records whether inject() was called.
     * Delegates span operations to NoOp — only tracks propagation calls.
     */
    private static final class RecordingTracingProvider implements TracingProvider {

        private final NoOpTracingProvider delegate = new NoOpTracingProvider();
        volatile boolean injectCalled;
        volatile TextMapSetter<?> lastSetter;

        @Override
        public Span startSpan(String name, SpanKind kind) {
            return delegate.startSpan(name, kind);
        }

        @Override
        public Span startSpan(String name, SpanKind kind, Context parent) {
            return delegate.startSpan(name, kind, parent);
        }

        @Override
        public Span currentSpan() {
            return delegate.currentSpan();
        }

        @Override
        public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
            injectCalled = true;
            lastSetter = setter;
        }

        @Override
        public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
            return delegate.extract(context, carrier, getter);
        }
    }
}
