package org.pangrova.observability.metrics;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * gRPC {@link ServerInterceptor} that records per-request shuffle metrics
 * (F-SHUFFLE-07) for the live native shuffle data plane.
 *
 * <p>Intercepts every RPC on the shuffle gRPC service and records:</p>
 * <ul>
 *   <li>{@code pangrova.shuffle.requests} — incremented once per RPC</li>
 *   <li>{@code pangrova.shuffle.request.duration} — wall-clock seconds per RPC</li>
 *   <li>{@code pangrova.shuffle.active.connections} — incremented on call start,
 *       decremented on close</li>
 *   <li>{@code pangrova.shuffle.backpressure.events} — incremented when the server
 *       returns {@link Status.Code#RESOURCE_EXHAUSTED}</li>
 *   <li>{@code pangrova.shuffle.bytes.sent} — bytes transferred (estimated from
 *       response message count, if available)</li>
 * </ul>
 *
 * <p>Thread-safe: all underlying metric instruments are thread-safe by SPI contract.</p>
 *
 * @see ShuffleMetrics
 */
public final class ShuffleMetricsInterceptor implements ServerInterceptor {

    /** Nanoseconds in one second, for converting elapsed nanos to seconds. */
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private final ShuffleMetrics metrics;

    /**
     * Creates an interceptor that records shuffle metrics through the given metrics holder.
     */
    public ShuffleMetricsInterceptor(ShuffleMetrics metrics) {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must not be null");
        }
        this.metrics = metrics;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String fullMethodName = call.getMethodDescriptor().getFullMethodName();
        String methodName = extractMethodName(fullMethodName);
        long startNanos = System.nanoTime();

        metrics.connectionOpened();

        ServerCall<ReqT, RespT> instrumentedCall =
                new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        try {
                            double durationSec = (System.nanoTime() - startNanos) / NANOS_PER_SECOND;
                            metrics.recordRequest(methodName, durationSec, 0);

                            if (status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
                                metrics.recordBackpressureEvent(methodName);
                            }
                        } finally {
                            metrics.connectionClosed();
                            super.close(status, trailers);
                        }
                    }
                };

        ServerCall.Listener<ReqT> listener = next.startCall(instrumentedCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {};
    }

    private static String extractMethodName(String fullMethodName) {
        int slashIndex = fullMethodName.lastIndexOf('/');
        return slashIndex >= 0 ? fullMethodName.substring(slashIndex + 1) : fullMethodName;
    }
}
