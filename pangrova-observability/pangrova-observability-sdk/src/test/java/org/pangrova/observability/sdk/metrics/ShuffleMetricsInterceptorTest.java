package org.pangrova.observability.sdk.metrics;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ShuffleMetricsInterceptor} — verifies that the gRPC
 * interceptor correctly records per-request shuffle metrics (F-SHUFFLE-07).
 */
class ShuffleMetricsInterceptorTest {

    private RecordingProvider recorder;
    private ShuffleMetrics metrics;
    private ShuffleMetricsInterceptor interceptor;

    @BeforeEach
    void setUp() {
        recorder = new RecordingProvider();
        metrics = new ShuffleMetrics(recorder);
        interceptor = new ShuffleMetricsInterceptor(metrics);
    }

    @Test
    void rejectsNullMetrics() {
        assertThatThrownBy(() -> new ShuffleMetricsInterceptor(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void interceptCallRecordsRequestAndDuration() {
        var call = new StubServerCall<String, String>("ShuffleService/readPartition");
        var handler = new PassThroughHandler<String, String>();

        var listener = interceptor.interceptCall(call, new Metadata(), handler);

        // Connection opened
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isEqualTo(1);

        // Simulate RPC completion
        handler.capturedCall.close(Status.OK, new Metadata());

        // Request recorded
        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(1);
        // Duration recorded (should be > 0 since we have non-zero processing time)
        assertThat(recorder.histogramObservations("pangrova.shuffle.request.duration")).hasSize(1);
        // Connection closed
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isZero();
    }

    @Test
    void interceptCallRecordsBackpressureOnResourceExhausted() {
        var call = new StubServerCall<String, String>("ShuffleService/writeBatch");
        var handler = new PassThroughHandler<String, String>();

        interceptor.interceptCall(call, new Metadata(), handler);
        handler.capturedCall.close(Status.RESOURCE_EXHAUSTED, new Metadata());

        assertThat(recorder.counterTotal("pangrova.shuffle.backpressure.events")).isEqualTo(1);
        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(1);
    }

    @Test
    void interceptCallDoesNotRecordBackpressureOnOk() {
        var call = new StubServerCall<String, String>("ShuffleService/readPartition");
        var handler = new PassThroughHandler<String, String>();

        interceptor.interceptCall(call, new Metadata(), handler);
        handler.capturedCall.close(Status.OK, new Metadata());

        assertThat(recorder.counterTotal("pangrova.shuffle.backpressure.events")).isZero();
    }

    @Test
    void multipleCallsAccumulateMetrics() {
        for (int i = 0; i < 3; i++) {
            var call = new StubServerCall<String, String>("ShuffleService/readPartition");
            var handler = new PassThroughHandler<String, String>();
            interceptor.interceptCall(call, new Metadata(), handler);
            handler.capturedCall.close(Status.OK, new Metadata());
        }

        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(3);
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isZero();
    }

    @Test
    void connectionClosedEvenOnError() {
        var call = new StubServerCall<String, String>("ShuffleService/writeBatch");
        var handler = new PassThroughHandler<String, String>();

        interceptor.interceptCall(call, new Metadata(), handler);
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isEqualTo(1);

        handler.capturedCall.close(Status.INTERNAL, new Metadata());
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isZero();
    }

    @Test
    void extractsMethodNameFromFullMethodName() {
        var call = new StubServerCall<String, String>("org.pangrova.shuffle.ShuffleService/commitPartition");
        var handler = new PassThroughHandler<String, String>();

        interceptor.interceptCall(call, new Metadata(), handler);
        handler.capturedCall.close(Status.OK, new Metadata());

        MetricAttributes attrs = recorder.lastAttributesFor("pangrova.shuffle.requests");
        assertThat(attrs).isNotNull();
        assertThat(attrs.asMap()).containsEntry("rpc.method", "commitPartition");
    }

    // -- Test infrastructure --

    /**
     * Minimal ServerCall stub for testing interceptors.
     */
    static class StubServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

        private final String fullMethodName;
        private boolean closed;

        StubServerCall(String fullMethodName) {
            this.fullMethodName = fullMethodName;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RespT message) {}

        @Override
        public void close(Status status, Metadata trailers) {
            this.closed = true;
        }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            @SuppressWarnings("unchecked")
            MethodDescriptor<ReqT, RespT> md = (MethodDescriptor<ReqT, RespT>)
                    MethodDescriptor.newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(fullMethodName)
                            .setRequestMarshaller(new NoOpMarshaller())
                            .setResponseMarshaller(new NoOpMarshaller())
                            .build();
            return md;
        }
    }

    /**
     * Handler that captures the instrumented call for later close() invocation.
     */
    static class PassThroughHandler<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
        ServerCall<ReqT, RespT> capturedCall;

        @Override
        public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
            this.capturedCall = call;
            return new ServerCall.Listener<>() {};
        }
    }

    static class NoOpMarshaller implements MethodDescriptor.Marshaller<Object> {
        @Override
        public InputStream stream(Object value) {
            return InputStream.nullInputStream();
        }

        @Override
        public Object parse(InputStream stream) {
            return null;
        }
    }

    // Reuse the recording infrastructure from ShuffleMetricsTest
    static final class RecordingProvider implements MetricsProvider {
        final List<String> counterNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final List<String> upDownCounterNames = new ArrayList<>();
        private final Map<String, RecordingCounter> counters = new LinkedHashMap<>();
        private final Map<String, RecordingHistogram> histograms = new LinkedHashMap<>();
        private final Map<String, RecordingUpDownCounter> upDownCounters = new LinkedHashMap<>();
        private final Map<String, String> units = new LinkedHashMap<>();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            units.put(name, unit);
            var counter = new RecordingCounter();
            counters.put(name, counter);
            return counter;
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            units.put(name, unit);
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            histogramNames.add(name);
            units.put(name, unit);
            var histogram = new RecordingHistogram();
            histograms.put(name, histogram);
            return histogram;
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            upDownCounterNames.add(name);
            units.put(name, unit);
            var udc = new RecordingUpDownCounter();
            upDownCounters.put(name, udc);
            return udc;
        }

        long counterTotal(String name) {
            var c = counters.get(name);
            return c == null ? 0 : c.total;
        }

        long upDownCounterTotal(String name) {
            var c = upDownCounters.get(name);
            return c == null ? 0 : c.value.get();
        }

        List<Double> histogramObservations(String name) {
            var h = histograms.get(name);
            return h == null ? List.of() : h.observations;
        }

        MetricAttributes lastAttributesFor(String name) {
            var c = counters.get(name);
            return c == null ? null : c.lastAttributes;
        }
    }

    static final class RecordingCounter implements LongCounter {
        long total = 0;
        MetricAttributes lastAttributes;

        @Override
        public void add(long value) { total += value; }

        @Override
        public void add(long value, MetricAttributes attributes) {
            total += value;
            lastAttributes = attributes;
        }
    }

    static final class RecordingHistogram implements DoubleHistogram {
        final List<Double> observations = new ArrayList<>();

        @Override
        public void record(double value) { observations.add(value); }

        @Override
        public void record(double value, MetricAttributes attributes) { observations.add(value); }
    }

    static final class RecordingUpDownCounter implements LongUpDownCounter {
        final AtomicLong value = new AtomicLong(0);

        @Override
        public void add(long delta) { value.addAndGet(delta); }

        @Override
        public void add(long delta, MetricAttributes attributes) { value.addAndGet(delta); }
    }
}
