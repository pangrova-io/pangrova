package org.pangrova.streaming.api.aggregation;

import java.time.Instant;

/**
 * Represents the time bounds of a window instance.
 *
 * @param start inclusive start of the window
 * @param end   exclusive end of the window
 */
public record WindowBounds(Instant start, Instant end) {

    public WindowBounds {
        if (end.isBefore(start)) {
            throw new IllegalArgumentException(
                    "Window end must not be before start: start=" + start + " end=" + end);
        }
    }

    /**
     * Returns true if the given event time falls within this window (start inclusive, end exclusive).
     */
    public boolean contains(Instant eventTime) {
        return !eventTime.isBefore(start) && eventTime.isBefore(end);
    }

    /**
     * Returns true if this window should be emitted given the current watermark.
     * A window is ready for emission when the watermark has passed the window end.
     */
    public boolean isClosedByWatermark(Instant watermark) {
        return !watermark.isBefore(end);
    }
}
