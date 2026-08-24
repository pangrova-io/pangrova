package org.pangrova.protocol.rest.query;

import org.pangrova.protocol.rest.error.ErrorDetail;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable response body for querying the status of a submitted query.
 *
 * <p>Includes result endpoint for transport data retrieval when available.
 * Null optional fields (error, stats, result_endpoint) are omitted from JSON
 * to maintain backward compatibility with existing clients.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryStatusResponse(
    @JsonProperty("query_id") String queryId,
    @JsonProperty("state") String state,
    @JsonProperty("error") ErrorDetail error,
    @JsonProperty("stats") QueryStats stats,
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint
) {

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

    /** Backward-compatible factory without resultEndpoint. */
    public static QueryStatusResponse withoutEndpoint(
            String queryId, String state, ErrorDetail error, QueryStats stats) {
        return new QueryStatusResponse(queryId, state, error, stats, null);
    }
}
