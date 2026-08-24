package org.pangrova.coordinator.dynamictable.refresh.lock;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable record representing a refresh lock held for a Dynamic Table.
 *
 * <p>Used by {@link RefreshLockManager} to enforce mutual exclusion on
 * concurrent refresh operations for the same Dynamic Table.</p>
 *
 * @param dtName        Dynamic Table name this lock protects
 * @param coordinatorId identifier of the coordinator holding the lock
 * @param epoch         HA epoch for fencing stale leaders
 * @param acquiredAt    timestamp when the lock was acquired
 * @param timeout       maximum duration the lock may be held before expiry
 */
public record LockEntry(
        String dtName,
        String coordinatorId,
        long epoch,
        Instant acquiredAt,
        Duration timeout
) {

    public LockEntry {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        Objects.requireNonNull(acquiredAt, "acquiredAt must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
    }

    /**
     * Whether this lock has expired based on current time.
     */
    public boolean isExpired() {
        return acquiredAt.plus(timeout).isBefore(Instant.now());
    }
}
