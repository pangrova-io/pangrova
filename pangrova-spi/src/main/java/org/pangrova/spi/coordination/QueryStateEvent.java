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
 * @param context        immutable identity, original time and issuing owner
 */
public record QueryStateEvent(
        JournalEventContext context,
        String queryId,
        String sql,
        String sessionJson,
        String state
) {
    public QueryStateEvent {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(state, "state must not be null");
    }
}
