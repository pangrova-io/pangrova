package org.pangrova.query.api.submission;

import java.util.List;
import java.util.Objects;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.common.client.result.ResultEndpoint;

/**
 * Query owner's immutable observation when submission returns.
 * The state is an observation, not a promise that execution has completed. A nullable
 * endpoint denotes results not yet exposed (including control-plane write dispatch).
 * The endpoint is a borrowed address; returning this value transfers no buffer,
 * result stream or query-lifetime ownership. Callers observe later state separately.
 */
@Experimental
public record QuerySubmissionResult(
        String queryId, QueryState state, ResultEndpoint resultEndpoint, List<String> columnNames) {
    public QuerySubmissionResult {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(state, "state");
        columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }
}
