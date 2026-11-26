package org.pangrova.common.client.query;

import org.pangrova.common.client.result.ResultEndpoint;

import org.pangrova.common.client.error.ErrorDetail;

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
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint,
    @JsonProperty("version") Long version,
    @JsonProperty("drained") Boolean drained
) {
    public static final long MAX_WAIT_MILLIS = 5_000;

    public QueryStatusResponse(String queryId, String state, ErrorDetail error, QueryStats stats,
                               ResultEndpoint resultEndpoint) {
        this(queryId, state, error, stats, resultEndpoint, null, null);
    }

    /**
     * Compact constructor enforcing non-null mandatory fields.
     */
    public QueryStatusResponse {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("state must not be null or blank");
        }
    }

    /** Factory for responses without result endpoint or stats. */
    public static QueryStatusResponse of(
            String queryId, String state, ErrorDetail error, ResultEndpoint resultEndpoint) {
        return new QueryStatusResponse(queryId, state, error, null, resultEndpoint);
    }

    /** Factory for status with statistics and no result endpoint. */
    public static QueryStatusResponse withoutEndpoint(
            String queryId, String state, ErrorDetail error, QueryStats stats) {
        return new QueryStatusResponse(queryId, state, error, stats, null);
    }
}
