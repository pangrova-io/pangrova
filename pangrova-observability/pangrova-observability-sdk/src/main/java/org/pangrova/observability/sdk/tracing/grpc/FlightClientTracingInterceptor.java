package org.pangrova.observability.sdk.tracing.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;

/**
 * gRPC {@link ClientInterceptor} that injects W3C trace context (traceparent/tracestate)
 * into outbound Arrow Flight RPC metadata.
 *
 * <p>Enables the Flight hop (worker-to-worker shuffle, coordinator-to-worker result fetch)
 * to propagate the distributed trace. Without this interceptor, outbound Flight RPCs
 * start a new trace root on the receiving side, breaking the end-to-end trace chain.
 *
 * <p>Paired with {@link FlightSpanInterceptor} on the server side which extracts the
 * injected context and creates a child SERVER span.
 */
public final class FlightClientTracingInterceptor implements ClientInterceptor {

    private static final GrpcMetadataTextMapSetter METADATA_SETTER = new GrpcMetadataTextMapSetter();

    private final TracingProvider tracingProvider;

    public FlightClientTracingInterceptor(TracingProvider tracingProvider) {
        this.tracingProvider = tracingProvider;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                tracingProvider.inject(null, headers, METADATA_SETTER);
                super.start(responseListener, headers);
            }
        };
    }

    /**
     * {@link TextMapSetter} implementation that writes to gRPC {@link Metadata}.
     * Injects W3C traceparent and tracestate headers for context propagation.
     */
    static final class GrpcMetadataTextMapSetter implements TextMapSetter<Metadata> {
        @Override
        public void set(Metadata carrier, String key, String value) {
            Metadata.Key<String> metadataKey =
                    Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
            carrier.put(metadataKey, value);
        }
    }
}
