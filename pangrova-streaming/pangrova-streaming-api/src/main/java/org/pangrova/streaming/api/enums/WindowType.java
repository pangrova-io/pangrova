package org.pangrova.streaming.api.enums;

/**
 * Supported window types for streaming aggregation.
 *
 * <ul>
 *   <li>TUMBLE: fixed-size, non-overlapping windows</li>
 *   <li>HOP: fixed-size, overlapping windows (slide &lt; size)</li>
 *   <li>SESSION: gap-based windows that close after inactivity</li>
 *   <li>CUMULATE: expanding windows that emit at regular steps until max size</li>
 * </ul>
 */
public enum WindowType {
    TUMBLE,
    HOP,
    SESSION,
    CUMULATE
}
