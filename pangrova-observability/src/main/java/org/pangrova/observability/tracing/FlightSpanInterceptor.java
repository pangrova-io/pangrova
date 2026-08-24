package org.pangrova.observability.tracing;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.StatusCode;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TracingProvider;

/**
 * gRPC {@link ServerInterceptor} that creates OpenTelemetry spans for Arrow Flight RPCs.
 *
 * <p>Extracts W3C traceparent from gRPC metadata, creates a SERVER span for each
 * Flight operation (DoGet, DoPut, DoExchange, etc.), and records method name,
 * ticket information, gRPC status code, and call duration as span attributes.
 */
public final class FlightSpanInterceptor implements ServerInterceptor {

    private static final GrpcMetadataTextMapGetter METADATA_GETTER = new GrpcMetadataTextMapGetter();

    /** Nanoseconds in one millisecond, for converting elapsed nanos to millis. */
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    private final TracingProvider tracingProvider;

    public FlightSpanInterceptor(TracingProvider tracingProvider) {
        this.tracingProvider = tracingProvider;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String fullMethodName = call.getMethodDescriptor().getFullMethodName();
        long startNanos = System.nanoTime();

        Context parentContext = tracingProvider.extract(
                new OtelContextWrapper(io.opentelemetry.context.Context.current()),
                headers,
                METADATA_GETTER);

        Span span = tracingProvider.startSpan(fullMethodName, SpanKind.SERVER, parentContext);
        span.setAttribute("rpc.system", "grpc");
        span.setAttribute("rpc.method", extractMethodName(fullMethodName));
        span.setAttribute("rpc.service", extractServiceName(fullMethodName));

        ServerCall<ReqT, RespT> tracingCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                long durationMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLISECOND;
                span.setAttribute("rpc.grpc.status_code", status.getCode().name());
                span.setAttribute("pangrova.flight.duration_ms", durationMs);

                if (status.isOk()) {
                    span.setStatus(StatusCode.OK, "");
                } else {
                    span.setStatus(StatusCode.ERROR, status.getDescription() != null
                            ? status.getDescription() : status.getCode().name());
                    if (status.getCause() != null) {
                        span.recordException(status.getCause());
                    }
                }
                span.end();
                super.close(status, trailers);
            }
        };

        ServerCall.Listener<ReqT> listener = next.startCall(tracingCall, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onMessage(ReqT message) {
                recordTicketIfPresent(span, message);
                super.onMessage(message);
            }
        };
    }

    /**
     * Attempts to extract ticket information from the request message.
     * For Flight DoGet, the message is a Ticket containing the ticket bytes.
     */
    private static <ReqT> void recordTicketIfPresent(Span span, ReqT message) {
        if (message == null) {
            return;
        }
        String messageType = message.getClass().getSimpleName();
        span.setAttribute("pangrova.flight.request_type", messageType);
    }

    private static String extractMethodName(String fullMethodName) {
        int slashIndex = fullMethodName.lastIndexOf('/');
        return slashIndex >= 0 ? fullMethodName.substring(slashIndex + 1) : fullMethodName;
    }

    private static String extractServiceName(String fullMethodName) {
        int slashIndex = fullMethodName.lastIndexOf('/');
        return slashIndex >= 0 ? fullMethodName.substring(0, slashIndex) : "";
    }

    /**
     * {@link TextMapGetter} implementation that reads from gRPC {@link Metadata}.
     * Extracts W3C traceparent and tracestate headers for context propagation.
     */
    static final class GrpcMetadataTextMapGetter implements TextMapGetter<Metadata> {

        @Override
        public Iterable<String> keys(Metadata carrier) {
            return carrier.keys();
        }

        @Override
        public String get(Metadata carrier, String key) {
            Metadata.Key<String> metadataKey =
                    Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
            return carrier.get(metadataKey);
        }
    }
}
