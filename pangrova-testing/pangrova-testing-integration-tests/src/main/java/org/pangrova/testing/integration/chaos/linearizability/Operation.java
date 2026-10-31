package org.pangrova.testing.integration.chaos.linearizability;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single operation in a concurrent history.
 * Each operation has an invocation time, a response time, and a result.
 * The linearizability checker uses these to determine if a valid sequential ordering exists.
 *
 * @param clientId   identifies which client issued the operation
 * @param type       the operation type (e.g., "createTable", "dropTable")
 * @param argument   the operation argument (e.g., table name)
 * @param result     the operation result (null if pending, "ok", or error description)
 * @param invokedAt  when the operation was invoked
 * @param completedAt when the response was received (null if still pending)
 */
public record Operation(
        String clientId,
        String type,
        String argument,
        String result,
        Instant invokedAt,
        Instant completedAt
) {
    public Operation {
        Objects.requireNonNull(clientId, "clientId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(argument, "argument must not be null");
        Objects.requireNonNull(invokedAt, "invokedAt must not be null");
    }

    /**
     * Returns true if this operation's time interval overlaps with the other operation's interval.
     * Two operations are concurrent if neither completed before the other was invoked.
     */
    public boolean isConcurrentWith(Operation other) {
        if (this.completedAt == null || other.completedAt == null) {
            return true;
        }
        return !(this.completedAt.isBefore(other.invokedAt)
                || other.completedAt.isBefore(this.invokedAt));
    }

    /**
     * Returns true if this operation completed before the other was invoked.
     */
    public boolean happensBefore(Operation other) {
        if (this.completedAt == null) {
            return false;
        }
        return this.completedAt.isBefore(other.invokedAt)
                || this.completedAt.equals(other.invokedAt);
    }
}
