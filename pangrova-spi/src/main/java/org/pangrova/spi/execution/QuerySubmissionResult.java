package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.Optional;

/**
 * Result of an asynchronous query submission via
 * {@link AsyncQuerySubmission#submitAsync}.
 *
 * <p>Contains the assigned query ID, initial state, and an optional result
 * endpoint that the client can use for worker-redirect streaming once the
 * output stage is dispatched.</p>
 *
 * <p>This is the SPI-side equivalent of the protocol-layer
 * {@code SubmitQueryResponse}. The protocol DTO adds JSON serialization;
 * this type is a pure value object for internal use.</p>
 */
@Experimental(reason = "G-SCHED-01: result type for async query submission")
public record QuerySubmissionResult(
        QueryId queryId,
        QueryState initialState,
        ResultEndpointInfo resultEndpoint
) {

    /**
     * Creates a submission result.
     *
     * @throws NullPointerException if queryId or initialState is null
     */
    public QuerySubmissionResult {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(initialState, "initialState must not be null");
    }

    /**
     * Returns the result endpoint, if available.
     *
     * <p>The endpoint may be absent if the output stage has not yet been
     * dispatched (i.e., the query is still in {@link QueryState#QUEUED}
     * or {@link QueryState#PLANNING}).</p>
     *
     * @return optional result endpoint
     */
    public Optional<ResultEndpointInfo> endpoint() {
        return Optional.ofNullable(resultEndpoint);
    }

    /**
     * Transport endpoint where query results can be streamed from.
     *
     * @param host     the worker host serving results
     * @param port     the transport port
     * @param ticket   opaque ticket identifying the result stream
     * @param protocol transport protocol (e.g., "arrow-flight", "grpc")
     */
    public record ResultEndpointInfo(
            String host,
            int port,
            String ticket,
            String protocol
    ) {
        public ResultEndpointInfo {
            Objects.requireNonNull(host, "host must not be null");
            Objects.requireNonNull(ticket, "ticket must not be null");
            Objects.requireNonNull(protocol, "protocol must not be null");
        }
    }
}
