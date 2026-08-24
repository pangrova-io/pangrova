package org.pangrova.spi.coordination;

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
    }

    /** Whether this lease has expired based on current time. */
    public boolean isExpired() {
        return Instant.now().isAfter(expiresAt);
    }
}
