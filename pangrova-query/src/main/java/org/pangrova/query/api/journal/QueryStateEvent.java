package org.pangrova.query.api.journal;

import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import org.pangrova.query.api.lineage.QueryLineageFact;

/**
 * Immutable admission or outcome fact. Terminal outcome metadata can prove a retired
 * receipt's result disposition without storing any Flight credentials.
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
        QueryLineageFact lineage,
        QueryAcceptance acceptance,
        QueryTerminalOutcome terminalOutcome
) {
    /** Existing producers without enabled governance have no fabricated lineage payload. */
    public QueryStateEvent(JournalEventContext context, String queryId, String sql, String sessionJson, String state) {
        this(context, queryId, sql, sessionJson, state, null, null, null);
    }

    public QueryStateEvent(JournalEventContext context, String queryId, String sql, String sessionJson, String state,
            QueryLineageFact lineage) {
        this(context, queryId, sql, sessionJson, state, lineage, null, null);
    }

    public QueryStateEvent(JournalEventContext context, String queryId, String sql, String sessionJson, String state,
            QueryLineageFact lineage, QueryAcceptance acceptance) {
        this(context, queryId, sql, sessionJson, state, lineage, acceptance, null);
    }

    public QueryStateEvent {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (acceptance != null
                && (!"QUEUED".equals(state) || !queryId.equals(acceptance.receipt().queryId()))) {
            throw new IllegalArgumentException("Acceptance must belong to the initial QUEUED event");
        }
        if (terminalOutcome != null && !terminalOutcome.matchesState(state)) {
            throw new IllegalArgumentException("Terminal outcome must match the query state");
        }
        if (lineage != null) {
            LineageEventType expected = switch (state) {
                case "QUEUED" -> LineageEventType.START;
                case "FINISHED" -> LineageEventType.COMPLETE;
                case "FAILED" -> LineageEventType.FAIL;
                case "CANCELLED" -> LineageEventType.ABORT;
                default -> throw new IllegalArgumentException("Intermediate query state cannot publish lineage");
            };
            if (!queryId.equals(lineage.queryId()) || lineage.phase() != expected) {
                throw new IllegalArgumentException("Query journal and lineage must describe the same logical fact");
            }
        }
    }
}
