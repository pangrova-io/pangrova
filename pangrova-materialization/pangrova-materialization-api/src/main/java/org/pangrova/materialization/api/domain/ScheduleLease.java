package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.exception.MaterializationException;

import java.time.Instant;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Lease held by a scheduler owner while it refreshes one materialization.
 *
 * @param owner scheduler owner identity
 * @param expiresAt instant after which the lease is void
 */
public record ScheduleLease(String owner, Instant expiresAt) {

    public ScheduleLease {
        owner = requireText(owner, "lease owner");
        if (expiresAt == null) {
            throw new IllegalArgumentException("lease expiry must not be null");
        }
    }

    /**
     * Returns whether the lease has expired.
     *
     * @param now current instant
     * @return true when the expiry is at or before {@code now}
     */
    public boolean isExpiredAt(Instant now) {
        return !expiresAt.isAfter(now);
    }

    /**
     * Requires that this lease belongs to the given owner and is still live.
     *
     * @param expectedOwner owner attempting the mutation
     * @param now current instant
     * @throws MaterializationException when ownership or liveness fails
     */
    public void requireHeldBy(String expectedOwner, Instant now) {
        if (!owner.equals(expectedOwner)) {
            throw MaterializationException.leaseExpired(
                    "schedule lease is held by " + owner + ", not " + expectedOwner);
        }
        if (isExpiredAt(now)) {
            throw MaterializationException.leaseExpired(
                    "schedule lease for " + owner + " expired at " + expiresAt);
        }
    }
}
