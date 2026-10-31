package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Declares the fault-tolerance guarantees a shuffle implementation offers (ADR-002 D3).
 *
 * <p>{@link #supportsTaskRetry()} is the single source of truth the FTE scheduler consults to decide
 * whether a failed downstream task may be retried <em>without</em> re-running its upstream stage. The
 * scheduler must never branch on the provider's name.
 *
 * <p>{@link #retentionPolicy()} tells the scheduler whether a worker's shuffle data survives
 * decommission/preemption, enabling safe scale-down decisions (G-RES-08).
 */
@Experimental(reason = "ADR-002 D3 shuffle FTE capabilities; evolving alongside D4 providers")
public interface ShuffleCapabilities {

    /** The durability tier this provider guarantees for committed partitions. */
    ShuffleDurability durability();

    /**
     * Whether the scheduler may retry a failed task by re-reading committed partitions instead of
     * re-running the upstream stage. True for {@code LOCAL_SPILL}/{@code DURABLE}; false for
     * {@code EPHEMERAL} (upstream data is gone, only a whole-query retry is possible).
     */
    default boolean supportsTaskRetry() {
        return durability() != ShuffleDurability.EPHEMERAL;
    }

    /**
     * The retention policy for committed shuffle data, controlling whether data outlives
     * the producing worker process (G-RES-08).
     *
     * <p>The scheduler consults this before decommissioning a worker:
     * <ul>
     *   <li>{@link ShuffleRetentionPolicy#EPHEMERAL} — data is lost on shutdown; the scheduler
     *       must migrate data or mark upstream stages for re-execution before scale-down.</li>
     *   <li>{@link ShuffleRetentionPolicy#DETACHED} — data survives graceful shutdown; safe for
     *       planned scale-down with a drain window.</li>
     *   <li>{@link ShuffleRetentionPolicy#PERSISTENT} — data is in an external store; safe for
     *       immediate scale-down and preemption.</li>
     * </ul>
     *
     * <p>Default derivation from durability:
     * {@code EPHEMERAL} durability maps to {@code EPHEMERAL} retention,
     * {@code LOCAL_SPILL} maps to {@code DETACHED},
     * {@code DURABLE} maps to {@code PERSISTENT}.
     */
    default ShuffleRetentionPolicy retentionPolicy() {
        return switch (durability()) {
            case EPHEMERAL -> ShuffleRetentionPolicy.EPHEMERAL;
            case LOCAL_SPILL -> ShuffleRetentionPolicy.DETACHED;
            case DURABLE -> ShuffleRetentionPolicy.PERSISTENT;
        };
    }

    /**
     * Whether a worker with committed shuffle data can be safely decommissioned
     * without losing data needed by downstream stages (G-RES-08).
     *
     * <p>Returns {@code true} for {@code DETACHED} and {@code PERSISTENT} retention policies,
     * meaning the data will survive worker shutdown. For {@code EPHEMERAL}, returns {@code false}
     * — the scheduler must either migrate shuffle data or mark affected stages for re-execution
     * before proceeding with scale-down.
     */
    default boolean supportsSafeScaleDown() {
        return retentionPolicy() != ShuffleRetentionPolicy.EPHEMERAL;
    }

    /**
     * The transport mode this provider uses for shuffle data exchange (ADR-002 D2).
     *
     * <p>PULL providers hold data in-process and let consumers fetch it (Arrow Flight).
     * PUSH providers send data to an external RSS as it's produced (Celeborn/object-store).
     * The scheduler uses this to select the correct data-plane contract.</p>
     *
     * <p>Default: {@link ShuffleTransportMode#PULL} for backward compatibility with
     * the native Arrow Flight shuffle.</p>
     */
    default ShuffleTransportMode transportMode() {
        return ShuffleTransportMode.PULL;
    }

    /** Returns a fixed-tier capabilities value with retention derived from durability. */
    static ShuffleCapabilities of(ShuffleDurability durability) {
        if (durability == null) {
            throw new IllegalArgumentException("durability must not be null");
        }
        return () -> durability;
    }

    /** Returns a capabilities value with explicit durability and retention policy. */
    static ShuffleCapabilities of(ShuffleDurability durability, ShuffleRetentionPolicy retention) {
        if (durability == null) {
            throw new IllegalArgumentException("durability must not be null");
        }
        if (retention == null) {
            throw new IllegalArgumentException("retention must not be null");
        }
        return new ShuffleCapabilities() {
            @Override
            public ShuffleDurability durability() {
                return durability;
            }

            @Override
            public ShuffleRetentionPolicy retentionPolicy() {
                return retention;
            }
        };
    }

    /**
     * Returns a capabilities value with explicit durability, retention, and transport mode.
     * Used by push-style providers (Celeborn, object-store) that need to declare
     * {@link ShuffleTransportMode#PUSH}.
     */
    static ShuffleCapabilities of(ShuffleDurability durability, ShuffleRetentionPolicy retention,
                                  ShuffleTransportMode transport) {
        if (durability == null) {
            throw new IllegalArgumentException("durability must not be null");
        }
        if (retention == null) {
            throw new IllegalArgumentException("retention must not be null");
        }
        if (transport == null) {
            throw new IllegalArgumentException("transport must not be null");
        }
        return new ShuffleCapabilities() {
            @Override
            public ShuffleDurability durability() {
                return durability;
            }

            @Override
            public ShuffleRetentionPolicy retentionPolicy() {
                return retention;
            }

            @Override
            public ShuffleTransportMode transportMode() {
                return transport;
            }
        };
    }
}
