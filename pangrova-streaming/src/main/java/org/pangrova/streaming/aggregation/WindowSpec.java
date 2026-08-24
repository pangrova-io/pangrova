package org.pangrova.streaming.aggregation;

import java.time.Duration;

/**
 * Window specification defining the window type and its parameters.
 *
 * @param type     the window type (TUMBLE, HOP, SESSION, CUMULATE)
 * @param size     the window size (duration for time-based windows)
 * @param slide    the slide interval (for HOP windows; ignored for others)
 * @param gap      the session gap timeout (for SESSION windows; ignored for others)
 * @param step     the cumulate step (for CUMULATE windows; ignored for others)
 */
public record WindowSpec(
        WindowType type,
        Duration size,
        Duration slide,
        Duration gap,
        Duration step
) {
    public static WindowSpec tumble(Duration size) {
        return new WindowSpec(WindowType.TUMBLE, size, size, Duration.ZERO, Duration.ZERO);
    }

    /** Creates a hopping window spec with the given size and slide interval. */
    public static WindowSpec hop(Duration size, Duration slide) {
        if (slide.compareTo(size) > 0) {
            throw new IllegalArgumentException(
                    "HOP slide must not exceed size: slide=" + slide + " size=" + size);
        }
        return new WindowSpec(WindowType.HOP, size, slide, Duration.ZERO, Duration.ZERO);
    }

    public static WindowSpec session(Duration gap) {
        return new WindowSpec(WindowType.SESSION, Duration.ZERO, Duration.ZERO, gap, Duration.ZERO);
    }

    /** Creates a cumulating window spec with the given max size and step. */
    public static WindowSpec cumulate(Duration maxSize, Duration step) {
        if (step.compareTo(maxSize) > 0) {
            throw new IllegalArgumentException(
                    "CUMULATE step must not exceed maxSize: step=" + step + " maxSize=" + maxSize);
        }
        return new WindowSpec(WindowType.CUMULATE, maxSize, Duration.ZERO, Duration.ZERO, step);
    }
}
