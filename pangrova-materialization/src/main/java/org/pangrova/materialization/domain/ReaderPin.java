package org.pangrova.materialization.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * Query-held pin that prevents garbage collection of a generation.
 *
 * <p>Pins are recorded when a query plans a scan against a generation and are
 * released when the query finishes. Long-running queries renew their pin
 * through a heartbeat.
 *
 * @param generation pinned generation
 * @param query owning query
 * @param owner query owner identity used for renewal and audit
 * @param expiresAt instant after which the pin no longer protects the generation
 * @param heartbeatAt last heartbeat instant
 */
public record ReaderPin(
        GenerationId generation,
        QueryId query,
        String owner,
        Instant expiresAt,
        Instant heartbeatAt) {

    public ReaderPin {
        if (generation == null || query == null) {
            throw new IllegalArgumentException("generation and query must not be null");
        }
        owner = Require.text(owner, "owner");
        if (expiresAt == null || heartbeatAt == null) {
            throw new IllegalArgumentException("expiresAt and heartbeatAt must not be null");
        }
        if (expiresAt.isBefore(heartbeatAt)) {
            throw new IllegalArgumentException("expiresAt must not precede heartbeatAt");
        }
    }

    /**
     * Renews the pin.
     *
     * @param leaseDuration new lease duration
     * @param now heartbeat instant
     * @return renewed pin
     */
    public ReaderPin renew(Duration leaseDuration, Instant now) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive");
        }
        return new ReaderPin(generation, query, owner, now.plus(leaseDuration), now);
    }

    /**
     * Returns whether the pin still protects the generation.
     *
     * @param now current instant
     * @return true when not expired
     */
    public boolean isActiveAt(Instant now) {
        return expiresAt.isAfter(now);
    }
}
