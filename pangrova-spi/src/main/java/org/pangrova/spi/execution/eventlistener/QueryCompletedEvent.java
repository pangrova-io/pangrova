package org.pangrova.spi.execution.eventlistener;

import java.util.Objects;
import java.util.Optional;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.execution.QueryState;

/**
 * Emitted once when a query reaches a terminal state.
 *
 * @param queryId               unique query identifier
 * @param sql                   the SQL text that was executed (may be {@code null} if not captured)
 * @param principal             the submitting principal ({@code "anonymous"} if unauthenticated)
 * @param finalState            terminal state — {@link QueryState#FINISHED} or {@link QueryState#FAILED}
 * @param createTimeEpochMillis admission wall-clock time, epoch milliseconds
 * @param endTimeEpochMillis    terminal wall-clock time, epoch milliseconds
 * @param statistics            aggregate execution metrics
 * @param failureMessage        failure detail when {@code finalState == FAILED}, else empty
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
        Optional<String> failureMessage) {

    public QueryCompletedEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(finalState, "finalState must not be null");
        Objects.requireNonNull(statistics, "statistics must not be null");
        failureMessage = (failureMessage == null) ? Optional.empty() : failureMessage;
    }

    /** @return true when the query terminated successfully */
    public boolean succeeded() {
        return finalState == QueryState.FINISHED;
    }
}
