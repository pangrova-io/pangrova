package org.pangrova.common.concurrent;

import java.time.Duration;

/**
 * Thread-related utilities for common patterns such as quiet sleep.
 *
 * <p>This class centralizes the {@code Thread.sleep} + {@code InterruptedException}
 * catch + interrupt-flag restoration pattern that is duplicated across multiple modules.
 */
public final class ThreadUtils {

    private ThreadUtils() {}

    /**
     * Sleeps the current thread for the specified number of milliseconds.
     *
     * <p>If the thread is interrupted during sleep, the interrupt flag is restored
     * via {@link Thread#interrupt()} and the method returns immediately.
     * Non-positive values are treated as a no-op.
     *
     * @param ms the number of milliseconds to sleep; non-positive values are ignored
     */
    public static void sleepQuietly(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sleeps the current thread for the specified duration.
     *
     * <p>Delegates to {@link #sleepQuietly(long)} after converting the duration
     * to milliseconds. Non-positive durations are treated as a no-op.
     *
     * @param duration the duration to sleep; non-positive durations are ignored
     * @throws NullPointerException if {@code duration} is null
     */
    public static void sleepQuietly(Duration duration) {
        sleepQuietly(duration.toMillis());
    }
}
