package org.pangrova.query.api.state;

import java.util.Objects;
import java.util.UUID;
import org.pangrova.common.api.annotation.Stable;

/**
 * Immutable identity and observation context frozen before a journal event is first written.
 *
 * @param eventId unique identity reused with exactly the same event payload on every retry
 * @param timestampMs original event time in epoch milliseconds, not the retry or receipt time
 * @param coordinatorId issuing event owner, nullable for events without a coordinator owner
 */
@Stable(since = "1.0")
public record JournalEventContext(UUID eventId, long timestampMs, String coordinatorId) {
    public JournalEventContext {
        Objects.requireNonNull(eventId, "eventId must not be null");
        if (timestampMs < 0) {
            throw new IllegalArgumentException("timestampMs must be non-negative");
        }
    }

    /** Creates one context for a new transition; callers retain it rather than recreate it on retry. */
    public static JournalEventContext create(long timestampMs, String coordinatorId) {
        return new JournalEventContext(UUID.randomUUID(), timestampMs, coordinatorId);
    }
}
