package org.pangrova.common.time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Lightweight elapsed-time measurement based on {@link System#nanoTime()}.
 *
 * <p>Unlike {@link System#currentTimeMillis()}, {@code nanoTime} is monotonic
 * and immune to wall-clock adjustments, making it the correct choice for
 * measuring elapsed durations.
 *
 * <p>Usage:
 * <pre>{@code
 * Stopwatch sw = Stopwatch.start();
 * doWork();
 * long ms = sw.elapsedMs();
 * }</pre>
 *
 * <p>Instances are immutable after creation (the start time never changes).
 * This class is thread-safe: the same instance can be read from multiple
 * threads, though each read captures a different "now".
 */
public final class Stopwatch {

    private final long startNanos;

    private Stopwatch() {
        this.startNanos = System.nanoTime();
    }

    /**
     * Creates and starts a new stopwatch.
     *
     * @return a running stopwatch whose start time is the current {@link System#nanoTime()}
     */
    public static Stopwatch start() {
        return new Stopwatch();
    }

    /**
     * Returns the elapsed time in milliseconds since this stopwatch was started.
     *
     * @return elapsed milliseconds (always {@code >= 0})
     */
    public long elapsedMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /**
     * Returns the elapsed time in nanoseconds since this stopwatch was started.
     *
     * @return elapsed nanoseconds (always {@code >= 0})
     */
    public long elapsedNanos() {
        return System.nanoTime() - startNanos;
    }

    /**
     * Returns the elapsed time as a {@link Duration}.
     *
     * @return elapsed duration (never {@code null})
     */
    public Duration elapsed() {
        return Duration.ofNanos(elapsedNanos());
    }
}
