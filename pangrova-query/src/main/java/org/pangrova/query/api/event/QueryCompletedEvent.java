package org.pangrova.query.api.event;

import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.lineage.QueryLineageMetadata;

import java.util.Objects;
import java.util.Optional;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Emitted once when a query reaches a terminal state.
 *
 * @param queryId               unique query identifier
 * @param sql                   the SQL text that was executed (may be {@code null} if not captured)
 * @param principal             the submitting principal ({@code "anonymous"} if unauthenticated)
 * @param finalState            terminal state — FINISHED, FAILED or CANCELLED
 * @param createTimeEpochMillis admission wall-clock time, epoch milliseconds
 * @param endTimeEpochMillis    terminal wall-clock time, epoch milliseconds
 * @param statistics            aggregate execution metrics
 * @param failureMessage        failure detail when {@code finalState == FAILED}, else empty
 * @param lineage               immutable datasets and columns from the authorized plan
 */
@Experimental
public record QueryCompletedEvent(
        String queryId,
        String sql,
        String principal,
        QueryState finalState,
        long createTimeEpochMillis,
        long endTimeEpochMillis,
        QueryStatistics statistics,
        Optional<String> failureMessage,
        QueryLineageMetadata lineage) {

    public QueryCompletedEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(finalState, "finalState must not be null");
        Objects.requireNonNull(statistics, "statistics must not be null");
        failureMessage = (failureMessage == null) ? Optional.empty() : failureMessage;
        Objects.requireNonNull(lineage, "lineage");
    }

    /** Query event producers without a relational plan supply empty logical dataset lineage. */
    public QueryCompletedEvent(String queryId, String sql, String principal, QueryState finalState,
            long createTimeEpochMillis, long endTimeEpochMillis, QueryStatistics statistics,
            Optional<String> failureMessage) {
        this(queryId, sql, principal, finalState, createTimeEpochMillis, endTimeEpochMillis,
                statistics, failureMessage, QueryLineageMetadata.empty());
    }

    /** @return true when the query terminated successfully */
    public boolean succeeded() {
        return finalState == QueryState.FINISHED;
    }
}
