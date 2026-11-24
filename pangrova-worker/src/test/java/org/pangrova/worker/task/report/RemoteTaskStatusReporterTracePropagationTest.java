package org.pangrova.worker.task.report;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.observability.api.tracing.Context;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.enums.SpanKind;
import org.pangrova.observability.api.tracing.TextMapGetter;
import org.pangrova.observability.api.tracing.TextMapSetter;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.constants.TestObservability;

/**
 * Validates that RemoteTaskStatusReporter injects trace context into outgoing HTTP headers.
 * Uses a recording TracingProvider — no mocks.
 */
class RemoteTaskStatusReporterTracePropagationTest {

    @Test
    void report_injectsTraceContextHeaders() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        // Port 0 means unreachable — report is best-effort, errors are logged
        try (var reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", recorder, TestObservability.METRICS, null)) {
            reporter.report(dummyReport());

            assertThat(recorder.injectCalled).isTrue();
        }
    }

    @Test
    void report_usesHttpRequestBuilderAsCarrier() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        try (var reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", recorder, TestObservability.METRICS, null)) {
            reporter.report(dummyReport());

            assertThat(recorder.lastSetter).isNotNull();
        }
    }

    @Test
    void noOpTracingProvider_doesNotThrow() {
        try (var reporter = new RemoteTaskStatusReporter(
                "http://localhost:0", TestObservability.TRACING, TestObservability.METRICS, null)) {
            // Should not throw — best-effort reporting with NoOp tracing
            reporter.report(dummyReport());
        }
    }

    private static TaskStatusReport dummyReport() {
        var attempt = org.pangrova.protocol.v2.TaskAttemptId.newBuilder().setTaskId("task-1")
                .setQueryOrJobId("q-1").setPlanEpoch(1).setFencingToken(1).build();
        return TaskStatusReport.fromAttempt(attempt, "COMPUTE", "w-1", "RUNNING",
                null, null, null, 1, System.currentTimeMillis(), null);
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
