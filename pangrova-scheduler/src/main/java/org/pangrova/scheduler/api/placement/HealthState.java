package org.pangrova.scheduler.api.placement;

/**
 * Worker health state based on heartbeat timing.
 * ACTIVE → SUSPECT (>20s no heartbeat) → DEAD (>60s no heartbeat).
 * SUSPECT → ACTIVE on heartbeat received.
 * DECOMMISSIONING: worker is draining tasks before shutdown.
 */
public enum HealthState {
    ACTIVE,
    SUSPECT,
    DEAD,
    DECOMMISSIONING,
    /** Worker not found in registry — distinct from DEAD (which means registered but unresponsive). */
    UNKNOWN
}
