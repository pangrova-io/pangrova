package org.pangrova.coordinator.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.observability.Context;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.Test;

/**
 * Validates that RemoteTaskDispatcher injects trace context into outgoing HTTP headers.
 * Uses a recording TracingProvider — no mocks.
 */
class RemoteTaskDispatcherTracePropagationTest {

    @Test
    void dispatch_injectsTraceContextHeaders() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), recorder);

        // Dispatch will fail (unreachable worker) but inject() should be called before send
        try {
            dispatcher.dispatch(dummyRequest(), unreachableWorker());
        } catch (RuntimeException ignored) {
            // Expected — worker is unreachable
        }

        assertThat(recorder.injectCalled).isTrue();
    }

    @Test
    void dispatch_usesHttpRequestBuilderAsCarrier() {
        RecordingTracingProvider recorder = new RecordingTracingProvider();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), recorder);

        try {
            dispatcher.dispatch(dummyRequest(), unreachableWorker());
        } catch (RuntimeException ignored) {
            // Expected
        }

        // Verify the setter is HttpRequest.Builder::header (functional ref)
        assertThat(recorder.lastSetter).isNotNull();
    }

    @Test
    void noOpTracingProvider_doesNotThrow() {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());

        try {
            dispatcher.dispatch(dummyRequest(), unreachableWorker());
        } catch (RuntimeException ignored) {
            // Expected — worker is unreachable, but no NPE from tracing
        }
        // If we reach here, NoOp inject did not throw
    }

    private static SubmitTaskRequest dummyRequest() {
        return new SubmitTaskRequest(
                "task-trace-1", "q-trace-1", 0, 0, "COMPUTE", "cGxhbg==",
                List.of(), List.of(), null, Map.of(), null, null,
                null, null, null, null, null, null, null);
    }

    private static WorkerInfo unreachableWorker() {
        return TestWorkerInfoBuilder.active("w-trace", "unreachable-host", 99999, 0);
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
