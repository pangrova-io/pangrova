package org.pangrova.streaming.watermark;

/**
 * Watermark state machine states (architecture 10 §5 DEF-143).
 *
 * <p>A regression (non-advancing candidate) never moves the state machine:
 * the tracker logs it and holds the current watermark.</p>
 */
public enum WatermarkState {
    INITIALIZING,
    ACTIVE,
    IDLE,
    FAILED
}
