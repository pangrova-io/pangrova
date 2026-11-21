package org.pangrova.query.api.event;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Emitted once when a query is admitted for execution.
 *
 * @param queryId             unique query identifier
 * @param sql                 the SQL text being executed
 * @param principal           the submitting principal ({@code "anonymous"} if unauthenticated)
 * @param createTimeEpochMillis admission wall-clock time, epoch milliseconds
 */
@Experimental
public record QueryCreatedEvent(
        String queryId,
        String sql,
        String principal,
        long createTimeEpochMillis) {

    public QueryCreatedEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
    }
}
