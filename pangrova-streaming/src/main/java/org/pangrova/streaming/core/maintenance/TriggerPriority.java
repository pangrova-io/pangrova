package org.pangrova.streaming.core.maintenance;

/**
 * Maintenance trigger priority (lower ordinal = higher priority).
 */
public enum TriggerPriority {
    THRESHOLD,
    INTERVAL,
    SCHEDULED_SCAN
}
