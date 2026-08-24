package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Internal;

/**
 * Lifecycle callback for components that participate in maintenance leader
 * election.
 *
 * <p>In a multi-coordinator deployment, only one coordinator holds the
 * maintenance lease at any time. Components that perform background
 * maintenance (index commits, garbage collection, metadata sync, stream
 * scheduling) implement this interface to start or stop work when
 * leadership changes.</p>
 *
 * <p>The coordinator bootstrap layer calls {@link #onLeaderElected()}
 * when the local node acquires the {@link DistributedLease} for
 * maintenance, and {@link #onLeadershipLost()} when the lease is
 * released or expires. Implementations must tolerate being called
 * multiple times in sequence (idempotent) and must not block
 * indefinitely.</p>
 *
 * <p>Implementations must be thread-safe: leadership transitions may
 * be triggered from the lease-renewal thread while maintenance work
 * is executing on other threads.</p>
 */
@Internal
public interface MaintenanceLeaderAware {

    /**
     * Called when this node becomes the maintenance leader.
     *
     * <p>Implementations should start or resume background maintenance
     * work (e.g., polling commit logs, scheduling garbage collection,
     * resuming stream pipelines). This method must return promptly;
     * long-running work should be dispatched to background threads.</p>
     */
    void onLeaderElected();

    /**
     * Called when this node loses maintenance leadership.
     *
     * <p>Implementations should stop or pause all background maintenance
     * work. In-progress operations should be cancelled or allowed to
     * complete their current unit of work, but no new work should be
     * initiated. This method must return promptly.</p>
     */
    void onLeadershipLost();
}
