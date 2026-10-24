package org.pangrova.coordinator.query.lifecycle;

import java.time.Duration;
import java.util.Collection;

/**
 * Releases and renews the reader-pin leases held by queries on this coordinator.
 *
 * <p>Implemented by the materialization kernel; the query manager only needs
 * query identity so query lifecycle stays independent of materialization types.
 */
public interface ReaderPinLifecycle {

    /** Lifecycle used when no materialization authority is wired. */
    ReaderPinLifecycle NOOP = new ReaderPinLifecycle() {
        @Override
        public void release(String queryId) {
        }

        @Override
        public void renew(Collection<String> queryIds) {
        }
    };

    /**
     * Releases every reader pin held by a terminal query.
     *
     * @param queryId terminal query identity
     */
    void release(String queryId);

    /**
     * Renews reader pins of the listed live queries.
     *
     * @param queryIds live query identities
     */
    void renew(Collection<String> queryIds);

    /**
     * Returns how often live queries must renew their leases.
     *
     * @return renewal period in nanoseconds
     */
    default long renewIntervalNanos() {
        return Duration.ofSeconds(20).toNanos();
    }
}
