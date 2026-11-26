package org.pangrova.query.api.submission;

import java.util.List;
import java.util.Objects;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.common.client.result.ResultEndpoint;

/**
 * Query owner's immutable observation when submission returns.
 * The state is an observation, not a promise that execution has completed. The disposition
 * distinguishes a result that is not yet published from a query that intentionally has none.
 * The endpoint is a borrowed address; returning this value transfers no buffer,
 * result stream or query-lifetime ownership. Callers observe later state separately.
 */
@Experimental
public record QuerySubmissionResult(
        String queryId, QueryState state, ResultDisposition resultDisposition,
        ResultEndpoint resultEndpoint, List<String> columnNames,
        org.pangrova.query.api.session.BranchSelection branchSelection) {
    public QuerySubmissionResult(String queryId, QueryState state, ResultDisposition resultDisposition,
            ResultEndpoint resultEndpoint, List<String> columnNames) {
        this(queryId, state, resultDisposition, resultEndpoint, columnNames, null);
    }

    public QuerySubmissionResult {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(resultDisposition, "resultDisposition");
        resultDisposition.validateEndpoint(resultEndpoint);
        if (resultDisposition == ResultDisposition.PENDING && isTerminal(state)) {
            throw new IllegalArgumentException("PENDING must not survive terminal completion");
        }
        columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }

    private static boolean isTerminal(QueryState state) {
        return state == QueryState.FINISHED || state == QueryState.FAILED || state == QueryState.CANCELLED;
    }
}
