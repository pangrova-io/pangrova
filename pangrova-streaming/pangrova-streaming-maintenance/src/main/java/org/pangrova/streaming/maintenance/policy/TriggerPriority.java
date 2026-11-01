package org.pangrova.streaming.maintenance.policy;

/**
 * Maintenance trigger priority (lower ordinal = higher priority).
 */
public enum TriggerPriority {
    THRESHOLD,
    INTERVAL,
    SCHEDULED_SCAN
}
