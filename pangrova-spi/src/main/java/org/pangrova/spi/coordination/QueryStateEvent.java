package org.pangrova.spi.coordination;

import java.util.Objects;

/**
 * Immutable journal event for query state transitions.
 * Captures the essential state needed to rebuild a QueryStateMachine on HA recovery.
 *
 * @param queryId        unique query identifier (required)
 * @param sql            the SQL text submitted (nullable for non-QUEUED events)
 * @param sessionJson    serialized session context (nullable for non-QUEUED events)
 * @param state          the query state at this transition point (required)
 * @param timestampMs    epoch millis when the transition occurred
 * @param coordinatorId  the coordinator that owns this query (nullable for legacy events)
 */
public record QueryStateEvent(
        String queryId,
        String sql,
        String sessionJson,
        String state,
        long timestampMs,
        String coordinatorId
) {
    public QueryStateEvent {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestampMs must be non-negative");
        }
    }

    /** Backward-compatible constructor without coordinatorId. */
    public QueryStateEvent(String queryId, String sql, String sessionJson,
                           String state, long timestampMs) {
        this(queryId, sql, sessionJson, state, timestampMs, null);
    }
}
