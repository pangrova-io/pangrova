package org.pangrova.common.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe idempotent lifecycle guard backed by an {@link AtomicBoolean}.
 *
 * <p>Wraps the common {@code AtomicBoolean + compareAndSet} pattern used for
 * idempotent start/stop/close transitions across 20+ classes in 7 modules.
 * Provides semantic clarity: callers express intent ({@code tryActivate},
 * {@code tryDeactivate}) instead of raw CAS operations.
 *
 * <p>Typical usage:
 * <pre>{@code
 * private final LifecycleGuard guard = new LifecycleGuard();
 *
 * public void start() {
 *     if (!guard.tryActivate()) { return; } // already started
 *     // actual startup logic
 * }
 *
 * public void close() {
 *     if (!guard.tryDeactivate()) { return; } // already closed
 *     // actual cleanup logic
 * }
 * }</pre>
 *
 * <p>This class is thread-safe. All state transitions use
 * {@link AtomicBoolean#compareAndSet(boolean, boolean)}, guaranteeing that
 * exactly one thread succeeds for each transition.
 *
 * @see AtomicBoolean
 */
public final class LifecycleGuard {

    private final AtomicBoolean flag = new AtomicBoolean(false);

    /**
     * Attempts to transition from inactive to active state.
     *
     * <p>Uses {@code compareAndSet(false, true)} so that only the first caller
     * succeeds when multiple threads race.
     *
     * @return {@code true} if this call performed the activation (false→true),
     *         {@code false} if already active
     */
    public boolean tryActivate() {
        return flag.compareAndSet(false, true);
    }

    /**
     * Attempts to transition from active to inactive state.
     *
     * <p>Uses {@code compareAndSet(true, false)} so that only the first caller
     * succeeds when multiple threads race.
     *
     * @return {@code true} if this call performed the deactivation (true→false),
     *         {@code false} if already inactive
     */
    public boolean tryDeactivate() {
        return flag.compareAndSet(true, false);
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return {@code true} if active, {@code false} if inactive
     */
    public boolean isActive() {
        return flag.get();
    }
}
