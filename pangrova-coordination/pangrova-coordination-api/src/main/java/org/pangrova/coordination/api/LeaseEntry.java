package org.pangrova.coordination.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable snapshot of a distributed lease.
 *
 * @param leaseKey   logical key identifying the protected resource
 * @param holderId   identifier of the node currently holding the lease
 * @param epoch      monotonic epoch for fencing stale holders
 * @param acquiredAt timestamp when the lease was acquired
 * @param expiresAt  absolute expiration timestamp
 */
public record LeaseEntry(
        String leaseKey,
        String holderId,
        long epoch,
        Instant acquiredAt,
        Instant expiresAt
) {

    public LeaseEntry {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(acquiredAt, "acquiredAt must not be null");
        Objects.requireNonNull(expiresAt, "expiresAt must not be null");
        if (leaseKey.isBlank() || holderId.isBlank()) {
            throw new IllegalArgumentException("leaseKey and holderId must not be blank");
        }
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive");
        }
    }

    /** Whether this lease has expired based on current time. */
    public boolean isExpired() {
        return isExpired(Instant.now());
    }

    /**
     * Determines whether this lease has expired at an explicit observation instant.
     *
     * @param observedAt instant at which the lease is observed
     * @return true when the expiration instant has been reached or passed
     */
    public boolean isExpired(Instant observedAt) {
        Objects.requireNonNull(observedAt, "observedAt must not be null");
        return !observedAt.isBefore(expiresAt);
    }
}
