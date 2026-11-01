package org.pangrova.streaming.maintenance;

/**
 * Maintenance trigger priority (lower ordinal = higher priority).
 */
public enum TriggerPriority {
    THRESHOLD,
    INTERVAL,
    SCHEDULED_SCAN
}
