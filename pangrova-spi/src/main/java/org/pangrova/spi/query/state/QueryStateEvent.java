package org.pangrova.spi.query.state;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import org.pangrova.spi.query.lineage.QueryLineageFact;

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
@Experimental
public record QueryStateEvent(
        JournalEventContext context,
        String queryId,
        String sql,
        String sessionJson,
        String state,
        QueryLineageFact lineage
) {
    /** Existing producers without enabled governance have no fabricated lineage payload. */
    public QueryStateEvent(JournalEventContext context, String queryId, String sql, String sessionJson, String state) {
        this(context, queryId, sql, sessionJson, state, null);
    }

    public QueryStateEvent {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (lineage != null) {
            QueryLineageFact.Phase expected = switch (state) {
                case "QUEUED" -> QueryLineageFact.Phase.START;
                case "FINISHED" -> QueryLineageFact.Phase.COMPLETE;
                case "FAILED" -> QueryLineageFact.Phase.FAIL;
                case "CANCELLED" -> QueryLineageFact.Phase.ABORT;
                default -> throw new IllegalArgumentException("Intermediate query state cannot publish lineage");
            };
            if (!queryId.equals(lineage.queryId()) || lineage.phase() != expected) {
                throw new IllegalArgumentException("Query journal and lineage must describe the same logical fact");
            }
        }
    }
}
