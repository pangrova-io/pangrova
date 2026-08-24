package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Internal;

import java.time.Duration;
import java.util.Optional;

/**
 * Unified SPI for distributed lease-based coordination.
 *
 * <p>Provides mutual exclusion across distributed nodes via time-bounded
 * leases. Supports epoch-based fencing to prevent stale holders from
 * performing actions after a failover.</p>
 *
 * <p>Implementations must be thread-safe. Concurrent calls from different
 * nodes must be serialized by the backend (e.g., database row lock,
 * CAS on in-memory state).</p>
 *
 * <p>Use cases: HA leader election, DT refresh locking, index maintenance
 * leader election, index build claim coordination.</p>
 */
@Internal
public interface DistributedLease {

    /**
     * Attempts to acquire a lease for the given key.
     *
     * <p>Succeeds if no active lease exists, the existing lease has expired,
     * or the caller already holds the lease (re-entrant). Epoch fencing:
     * a higher epoch can force-replace a lower-epoch holder.</p>
     *
     * @param leaseKey  logical key identifying the protected resource
     * @param holderId  identifier of the requesting node
     * @param epoch     monotonic epoch for fencing (0 if unused)
     * @param duration  lease TTL from now
     * @return true if the lease was acquired
     */
    boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration);

    /**
     * Renews an existing lease held by the specified holder.
     *
     * <p>Succeeds only if the given holder currently owns the lease.
     * Extends the expiration by the given duration from now.</p>
     *
     * @param leaseKey  logical key
     * @param holderId  current holder
     * @param duration  new TTL from now
     * @return true if the lease was renewed
     */
    boolean tryRenew(String leaseKey, String holderId, Duration duration);

    /**
     * Voluntarily releases a lease.
     *
     * <p>Succeeds only if the given holder currently owns the lease.</p>
     *
     * @param leaseKey  logical key
     * @param holderId  holder releasing the lease
     * @return true if the lease was released
     */
    boolean release(String leaseKey, String holderId);

    /**
     * Returns the current lease holder for the given key, if any
     * active (non-expired) lease exists.
     *
     * @param leaseKey logical key
     * @return current lease entry, or empty if no active lease
     */
    Optional<LeaseEntry> currentHolder(String leaseKey);

    /**
     * Removes all expired lease entries.
     *
     * <p>Implementations should call this periodically to prevent
     * unbounded growth of stale entries.</p>
     *
     * @return number of expired entries removed
     */
    int expireStale();
}
