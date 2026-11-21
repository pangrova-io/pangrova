package org.pangrova.streaming.enums;

/**
 * Maintenance trigger priority (lower ordinal = higher priority).
 */
public enum TriggerPriority {
    THRESHOLD,
    INTERVAL,
    SCHEDULED_SCAN
}
