package org.pangrova.observability.sdk.tracing.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;

/**
 * Verifies {@link FlightSpanInterceptor} creates OTel spans for Flight RPCs
 * with correct attributes: method, service, status, duration, and trace context propagation.
 */
class FlightSpanInterceptorTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private FlightSpanInterceptor interceptor;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        TracingProvider provider = new OtelTracingProvider(
                tracerProvider.get("pangrova-flight-test"));
        interceptor = new FlightSpanInterceptor(provider);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void doGetProducesSpanWithCorrectAttributes() {
        String fullMethod = "arrow.flight.protocol.FlightService/DoGet";
        CapturingCallHandler<String, String> handler = new CapturingCallHandler<>();
        ServerCall<String, String> rawCall = createCall(fullMethod);

        ServerCall.Listener<String> listener = interceptor.interceptCall(
                rawCall, new Metadata(), handler);
        listener.onMessage("ticket-payload");

        handler.capturedCall().close(Status.OK, new Metadata());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);

        SpanData span = spans.get(0);
        assertThat(span.getName()).isEqualTo(fullMethod);
        assertThat(span.getKind()).isEqualTo(SpanKind.SERVER);
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);

        assertThat(span.getAttributes().asMap())
                .containsEntry(
                        io.opentelemetry.api.common.AttributeKey.stringKey("rpc.system"), "grpc")
                .containsEntry(
                        io.opentelemetry.api.common.AttributeKey.stringKey("rpc.method"), "DoGet")
                .containsEntry(
                        io.opentelemetry.api.common.AttributeKey.stringKey("rpc.service"),
                        "arrow.flight.protocol.FlightService")
                .containsEntry(
                        io.opentelemetry.api.common.AttributeKey.stringKey("rpc.grpc.status_code"), "OK")
                .containsEntry(
                        io.opentelemetry.api.common.AttributeKey.stringKey("pangrova.flight.request_type"), "String");

        assertThat(span.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.longKey("pangrova.flight.duration_ms")))
                .isNotNull()
                .isGreaterThanOrEqualTo(0L);
    }

    @Test
    void errorStatusRecordsErrorOnSpan() {
        String fullMethod = "arrow.flight.protocol.FlightService/DoPut";
        CapturingCallHandler<String, String> handler = new CapturingCallHandler<>();
        ServerCall<String, String> rawCall = createCall(fullMethod);

        interceptor.interceptCall(rawCall, new Metadata(), handler);
        handler.capturedCall().close(Status.INTERNAL.withDescription("disk full"), new Metadata());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);

        SpanData span = spans.get(0);
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getStatus().getDescription()).isEqualTo("disk full");
        assertThat(span.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("rpc.grpc.status_code")))
                .isEqualTo("INTERNAL");
    }

    @Test
    void traceparentHeaderPropagatesTraceContext() {
        String fullMethod = "arrow.flight.protocol.FlightService/DoExchange";
        CapturingCallHandler<String, String> handler = new CapturingCallHandler<>();
        ServerCall<String, String> rawCall = createCall(fullMethod);

        Metadata headers = new Metadata();
        headers.put(
                Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");

        interceptor.interceptCall(rawCall, headers, handler);
        handler.capturedCall().close(Status.OK, new Metadata());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);

        SpanData span = spans.get(0);
        assertThat(span.getSpanContext().getTraceId())
                .isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(span.getParentSpanContext().getSpanId())
                .isEqualTo("00f067aa0ba902b7");
    }

    @Test
    void exceptionOnStatusRecordsEvent() {
        String fullMethod = "arrow.flight.protocol.FlightService/DoGet";
        CapturingCallHandler<String, String> handler = new CapturingCallHandler<>();
        ServerCall<String, String> rawCall = createCall(fullMethod);

        interceptor.interceptCall(rawCall, new Metadata(), handler);
        RuntimeException cause = new RuntimeException("connection reset");
        handler.capturedCall().close(Status.UNAVAILABLE.withCause(cause), new Metadata());

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);

        SpanData span = spans.get(0);
        assertThat(span.getEvents()).isNotEmpty();
        assertThat(span.getEvents().get(0).getName()).isEqualTo("exception");
    }

    // --- test infrastructure (no mock frameworks) ---

    private static ServerCall<String, String> createCall(String fullMethodName) {
        MethodDescriptor<String, String> descriptor = MethodDescriptor.<String, String>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(fullMethodName)
                .setRequestMarshaller(new StringMarshaller())
                .setResponseMarshaller(new StringMarshaller())
                .build();
        return new StubServerCall<>(descriptor);
    }

    /**
     * Minimal ServerCall that provides method descriptor and accepts close() calls.
     */
    private static final class StubServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {

        private final MethodDescriptor<ReqT, RespT> methodDescriptor;

        StubServerCall(MethodDescriptor<ReqT, RespT> methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return methodDescriptor;
        }

        @Override
        public void request(int numMessages) {
            // intentional no-op: test infrastructure
        }

        @Override
        public void sendHeaders(Metadata headers) {
            // intentional no-op: test infrastructure
        }

        @Override
        public void sendMessage(RespT message) {
            // intentional no-op: test infrastructure
        }

        @Override
        public void close(Status status, Metadata trailers) {
            // intentional no-op: base call close — interceptor wraps this
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    }

    /**
     * Handler that captures the wrapped ServerCall passed by the interceptor,
     * allowing tests to invoke close() on the instrumented call.
     */
    private static final class CapturingCallHandler<ReqT, RespT>
            implements ServerCallHandler<ReqT, RespT> {

        private ServerCall<ReqT, RespT> wrappedCall;

        @Override
        public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
            this.wrappedCall = call;
            return new ServerCall.Listener<>() {
                // intentional no-op: test only needs to verify interceptor behavior
            };
        }

        ServerCall<ReqT, RespT> capturedCall() {
            return wrappedCall;
        }
    }

    /**
     * Simple string marshaller for test method descriptors.
     */
    private static final class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public java.io.InputStream stream(String value) {
            return new java.io.ByteArrayInputStream(
                    value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String parse(java.io.InputStream stream) {
            try {
                return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
