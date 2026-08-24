package org.pangrova.metadata.dynamictable;

/**
 * Lifecycle states for a Dynamic Table.
 *
 * <p>State machine:
 * CREATING → ACTIVE ↔ SUSPENDED → DROPPING → DROPPED,
 * ACTIVE → PAUSED (user-initiated via ALTER DYNAMIC TABLE SUSPEND),
 * PAUSED → ACTIVE (user-initiated via ALTER DYNAMIC TABLE RESUME),
 * ACTIVE → FAILING → SUSPENDED (auto-suspend on consecutive failures),
 * ACTIVE → LAGGING (data lag exceeds target, does not block refresh).</p>
 */
public enum DynamicTableLifecycleState {

    /** Initial full refresh in progress. */
    CREATING,

    /** Normal operation — refresh is scheduled and running. */
    ACTIVE,

    /** User-initiated pause via ALTER DYNAMIC TABLE SUSPEND — no refresh scheduled until RESUME. */
    PAUSED,

    /** Automatically suspended due to consecutive failures — no refresh scheduled. */
    SUSPENDED,

    /** Consecutive failures in progress (transient state). */
    FAILING,

    /** Deletion in progress. */
    DROPPING,

    /** Fully deleted. */
    DROPPED,

    /** Data lag exceeds target — marker state, refresh continues. */
    LAGGING
}
