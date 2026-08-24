package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Snapshot of a query's current progress, returned by
 * {@link AsyncQuerySubmission#getQueryProgress}.
 *
 * <p>Provides a lightweight, non-blocking view of query state suitable
 * for client polling. Fields are populated progressively as the query
 * moves through lifecycle stages.</p>
 */
@Experimental(reason = "G-SCHED-01: progress snapshot for async query polling")
public record QueryProgress(
        QueryId queryId,
        QueryState state,
        OptionalLong processedRows,
        OptionalLong processedBytes,
        Optional<QuerySubmissionResult.ResultEndpointInfo> resultEndpoint,
        Optional<String> failureMessage
) {

    /**
     * Creates a progress snapshot.
     *
     * @throws NullPointerException if queryId or state is null
     */
    public QueryProgress {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (processedRows == null) {
            processedRows = OptionalLong.empty();
        }
        if (processedBytes == null) {
            processedBytes = OptionalLong.empty();
        }
        if (resultEndpoint == null) {
            resultEndpoint = Optional.empty();
        }
        if (failureMessage == null) {
            failureMessage = Optional.empty();
        }
    }

    /**
     * Returns whether the query has reached a terminal state
     * ({@link QueryState#FINISHED} or {@link QueryState#FAILED}).
     *
     * @return true if the query is in a terminal state
     */
    public boolean isTerminal() {
        return state == QueryState.FINISHED || state == QueryState.FAILED;
    }

    /**
     * Creates a minimal progress snapshot with just query ID and state.
     *
     * @param queryId the query identifier
     * @param state   the current state
     * @return a progress snapshot with no metrics or endpoint
     */
    public static QueryProgress of(QueryId queryId, QueryState state) {
        return new QueryProgress(queryId, state,
                OptionalLong.empty(), OptionalLong.empty(),
                Optional.empty(), Optional.empty());
    }

    /**
     * Creates a failed progress snapshot.
     *
     * @param queryId the query identifier
     * @param message the failure message
     * @return a progress snapshot in FAILED state
     */
    public static QueryProgress failed(QueryId queryId, String message) {
        return new QueryProgress(queryId, QueryState.FAILED,
                OptionalLong.empty(), OptionalLong.empty(),
                Optional.empty(), Optional.of(message));
    }
}
