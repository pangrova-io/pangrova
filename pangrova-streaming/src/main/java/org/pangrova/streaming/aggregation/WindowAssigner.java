package org.pangrova.streaming.aggregation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Assigns event timestamps to window instances based on a {@link WindowSpec}.
 *
 * <p>For each event time, returns the list of windows the event belongs to.
 * TUMBLE produces exactly one window per event. HOP may produce multiple
 * overlapping windows. SESSION and CUMULATE produce one window per event
 * (session merging is handled externally by the aggregation operator).</p>
 */
public final class WindowAssigner {

    private final WindowSpec spec;

    public WindowAssigner(WindowSpec spec) {
        this.spec = spec;
    }

    /**
     * Assigns the given event time to one or more windows.
     *
     * @param eventTime the event timestamp
     * @return list of windows this event belongs to (never empty)
     */
    public List<WindowBounds> assign(Instant eventTime) {
        return switch (spec.type()) {
            case TUMBLE -> assignTumble(eventTime);
            case HOP -> assignHop(eventTime);
            case SESSION -> assignSession(eventTime);
            case CUMULATE -> assignCumulate(eventTime);
        };
    }

    /**
     * Returns all windows that should be emitted (closed) given the current watermark.
     *
     * @param activeWindows the currently active (open) windows
     * @param watermark     the current watermark
     * @return windows that are closed by the watermark
     */
    public List<WindowBounds> closedWindows(List<WindowBounds> activeWindows, Instant watermark) {
        List<WindowBounds> closed = new ArrayList<>();
        for (WindowBounds window : activeWindows) {
            if (window.isClosedByWatermark(watermark)) {
                closed.add(window);
            }
        }
        return Collections.unmodifiableList(closed);
    }

    public WindowSpec spec() {
        return spec;
    }

    private List<WindowBounds> assignTumble(Instant eventTime) {
        long epochMillis = eventTime.toEpochMilli();
        long sizeMillis = spec.size().toMillis();
        long windowStart = epochMillis - (Math.floorMod(epochMillis, sizeMillis));
        Instant start = Instant.ofEpochMilli(windowStart);
        Instant end = start.plus(spec.size());
        return List.of(new WindowBounds(start, end));
    }

    private List<WindowBounds> assignHop(Instant eventTime) {
        long epochMillis = eventTime.toEpochMilli();
        long sizeMillis = spec.size().toMillis();
        long slideMillis = spec.slide().toMillis();
        long lastWindowStart = epochMillis - Math.floorMod(epochMillis, slideMillis);
        List<WindowBounds> windows = new ArrayList<>();
        for (long start = lastWindowStart; start > epochMillis - sizeMillis; start -= slideMillis) {
            Instant windowStart = Instant.ofEpochMilli(start);
            Instant windowEnd = windowStart.plus(spec.size());
            if (windowStart.toEpochMilli() <= epochMillis && epochMillis < windowEnd.toEpochMilli()) {
                windows.add(new WindowBounds(windowStart, windowEnd));
            }
        }
        return Collections.unmodifiableList(windows);
    }

    private List<WindowBounds> assignSession(Instant eventTime) {
        Instant start = eventTime;
        Instant end = eventTime.plus(spec.gap());
        return List.of(new WindowBounds(start, end));
    }

    private List<WindowBounds> assignCumulate(Instant eventTime) {
        long epochMillis = eventTime.toEpochMilli();
        long maxSizeMillis = spec.size().toMillis();
        long windowStart = epochMillis - Math.floorMod(epochMillis, maxSizeMillis);
        long stepMillis = spec.step().toMillis();
        long elapsed = epochMillis - windowStart;
        long stepsElapsed = elapsed / stepMillis + 1;
        Instant start = Instant.ofEpochMilli(windowStart);
        Duration cumulateEnd = spec.step().multipliedBy(stepsElapsed);
        if (cumulateEnd.compareTo(spec.size()) > 0) {
            cumulateEnd = spec.size();
        }
        Instant end = start.plus(cumulateEnd);
        return List.of(new WindowBounds(start, end));
    }
}
