package org.pangrova.worker.transport;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.pangrova.spi.observability.TracingProvider;

/**
 * Data exchange client middleware that injects W3C trace context (traceparent/tracestate)
 * into outbound RPC headers for distributed trace propagation.
 *
 * <p>This middleware is instantiated per-call by the {@link Factory}. On each outgoing
 * data exchange request (DoGet, DoPut, DoExchange), it calls
 * {@link TracingProvider#inject(org.pangrova.spi.observability.Context, Object,
 * org.pangrova.spi.observability.TextMapSetter)} to serialize the current thread's
 * trace context into the gRPC metadata, enabling the receiving server's
 * {@link org.pangrova.observability.sdk.tracing.adapter.grpc.FlightSpanInterceptor} to extract it
 * and create a child span.
 */
final class TracingStreamMiddleware implements FlightClientMiddleware {

    private final TracingProvider tracingProvider;

    TracingStreamMiddleware(TracingProvider tracingProvider) {
        this.tracingProvider = tracingProvider;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        tracingProvider.inject(null, outgoingHeaders, CallHeaders::insert);
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        // no-op: we only inject, not extract on the client side
    }

    @Override
    public void onCallCompleted(CallStatus status) {
        // no-op: span lifecycle is managed by the caller
    }

    /**
     * Factory that creates a {@link TracingStreamMiddleware} per data exchange call.
     * Attach to {@link org.apache.arrow.flight.FlightClient.Builder#intercept(Factory...)}
     * to enable trace context propagation on all outbound data exchange RPCs.
     */
    static final class Factory implements FlightClientMiddleware.Factory {

        private final TracingProvider tracingProvider;

        Factory(TracingProvider tracingProvider) {
            this.tracingProvider = tracingProvider;
        }

        @Override
        public FlightClientMiddleware onCallStarted(CallInfo info) {
            return new TracingStreamMiddleware(tracingProvider);
        }
    }
}
