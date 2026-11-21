package org.pangrova.streaming.enums;

/**
 * Watermark state machine states (architecture 10 §5 DEF-143).
 */
public enum WatermarkState {
    INITIALIZING,
    ACTIVE,
    IDLE,
    STALE,
    FAILED
}
