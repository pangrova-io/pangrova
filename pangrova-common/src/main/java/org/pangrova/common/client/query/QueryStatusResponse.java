package org.pangrova.common.client.query;

import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.ResultDisposition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable response body for querying the status of a submitted query.
 *
 * <p>The version and drain evidence describe the live owner projection.
 * Historical journal state does not carry either fact.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryStatusResponse(
    @JsonProperty("query_id") String queryId,
    @JsonProperty("state") String state,
    @JsonProperty("error") ErrorDetail error,
    @JsonProperty("stats") QueryStats stats,
    @JsonProperty("result_disposition") ResultDisposition resultDisposition,
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint,
    @JsonProperty("version") Long version,
    @JsonProperty("drained") Boolean drained
) {
    public static final long MAX_WAIT_MILLIS = 5_000;

    public QueryStatusResponse(String queryId, String state, ErrorDetail error, QueryStats stats,
                               ResultDisposition resultDisposition, ResultEndpoint resultEndpoint) {
        this(queryId, state, error, stats, requireLiveDisposition(resultDisposition), resultEndpoint, null, null);
    }

    /** Compact constructor enforcing the result disposition and endpoint invariant. */
    public QueryStatusResponse {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        state = requireCanonicalState(state);
        if (resultDisposition == null) {
            if (error != null || stats != null || version != null || drained != null || resultEndpoint != null) {
                throw new IllegalArgumentException("Historical status must contain only query_id and state");
            }
        } else {
            resultDisposition.validateEndpoint(resultEndpoint);
            if (resultDisposition == ResultDisposition.PENDING && isTerminalState(state)) {
                throw new IllegalArgumentException("PENDING must not survive terminal completion");
            }
        }
    }

    /** Factory for responses without version or drain evidence. */
    public static QueryStatusResponse of(
            String queryId, String state, ErrorDetail error,
            ResultDisposition resultDisposition, ResultEndpoint resultEndpoint) {
        return new QueryStatusResponse(queryId, state, error, null,
                requireLiveDisposition(resultDisposition), resultEndpoint);
    }

    /** Factory for durable diagnostic state without live result or drain authority. */
    public static QueryStatusResponse historical(String queryId, String state) {
        return new QueryStatusResponse(queryId, state, null, null, null, null, null, null);
    }

    /** Factory for status with statistics and an explicit result disposition. */
    public static QueryStatusResponse withoutEndpoint(
            String queryId, String state, ErrorDetail error, QueryStats stats,
            ResultDisposition resultDisposition) {
        return new QueryStatusResponse(queryId, state, error, stats,
                requireLiveDisposition(resultDisposition), null);
    }

    private static ResultDisposition requireLiveDisposition(ResultDisposition disposition) {
        if (disposition == null) {
            throw new IllegalArgumentException("Live status requires a result disposition");
        }
        return disposition;
    }

    static String requireCanonicalState(String state) {
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("state must not be null or blank");
        }
        return switch (state) {
            case "QUEUED", "PLANNING", "SCHEDULING", "RUNNING", "FINISHING",
                    "FINISHED", "FAILED", "CANCELLED" -> state;
            default -> throw new IllegalArgumentException("Unknown query state: " + state);
        };
    }

    static boolean isTerminalState(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
    }
}
