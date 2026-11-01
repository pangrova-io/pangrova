package org.pangrova.persistence.api.coordination;

import org.pangrova.spi.coordination.LeaseEntry;

import org.pangrova.common.api.annotation.Internal;

import java.time.Duration;
import java.util.Optional;

/**
 * Unified SPI for distributed lease-based coordination.
 *
 * <p>Provides mutual exclusion across distributed nodes via time-bounded
 * leases. The coordination backend allocates a strictly increasing fencing
 * epoch for each successful ownership change. Callers never choose epochs:
 * accepting caller-supplied epochs would let a stale or faulty process fence
 * the legitimate owner with an invented future value.</p>
 *
 * <p>Implementations must be thread-safe. Concurrent calls from different
 * nodes must be serialized by the backend (e.g., database row lock,
 * CAS on in-memory state).</p>
 *
 * <p>An implementation must retain the last allocated epoch for every lease
 * key after expiry and release. Reusing an epoch for the same logical resource
 * is forbidden. Resource lifecycle code may remove that high-water mark only
 * when the protected resource itself is permanently deleted.</p>
 */
@Internal
public interface DistributedLease {

    /**
     * Attempts to acquire a lease for the given key.
     *
     * <p>Succeeds if no active lease exists or the existing lease has expired.
     * A re-entrant call by the active holder returns the current lease without
     * changing its epoch or deadline. Use {@link #tryRenew} to extend it.</p>
     *
     * @param leaseKey  logical key identifying the protected resource
     * @param holderId  identifier of the requesting node
     * @param duration  lease TTL from now, with a minimum precision of one millisecond
     * @return the backend-issued lease, or empty when another active holder owns it
     */
    Optional<LeaseEntry> tryAcquire(String leaseKey, String holderId, Duration duration);

    /**
     * Renews an existing lease held by the specified holder.
     *
     * <p>Succeeds only if the holder and epoch exactly match the active lease.
     * Extends the expiration by the given duration from the backend clock.</p>
     *
     * @param leaseKey  logical key
     * @param holderId  current holder
     * @param epoch     exact backend-issued fencing epoch
     * @param duration  new TTL from now, with a minimum precision of one millisecond
     * @return true if the lease was renewed
     */
    boolean tryRenew(String leaseKey, String holderId, long epoch, Duration duration);

    /**
     * Voluntarily releases a lease.
     *
     * <p>Succeeds only if the holder and epoch exactly match the active lease.
     * Release expires the lease while retaining its epoch high-water mark.</p>
     *
     * @param leaseKey  logical key
     * @param holderId  holder releasing the lease
     * @param epoch     exact backend-issued fencing epoch
     * @return true if the lease was released
     */
    boolean release(String leaseKey, String holderId, long epoch);

    /**
     * Returns the current lease holder for the given key, if any
     * active (non-expired) lease exists.
     *
     * @param leaseKey logical key
     * @return current lease entry, or empty if no active lease
     */
    Optional<LeaseEntry> currentHolder(String leaseKey);

}
