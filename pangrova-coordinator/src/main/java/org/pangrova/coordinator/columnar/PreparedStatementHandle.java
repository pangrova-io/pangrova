package org.pangrova.coordinator.columnar;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable handle representing a prepared statement in the columnar query service.
 * Stores the SQL text and creation timestamp for TTL-based expiration.
 */
public record PreparedStatementHandle(String handleId, String sql, Instant createdAt) {

    public PreparedStatementHandle {
        Objects.requireNonNull(handleId, "handleId");
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(createdAt, "createdAt");
    }

    /**
     * Returns true if this handle has expired based on the given TTL.
     */
    public boolean isExpired(long ttlMs) {
        return Instant.now().toEpochMilli() - createdAt.toEpochMilli() > ttlMs;
    }
}
