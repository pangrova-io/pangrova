package org.pangrova.client.cli;

import java.time.Duration;

/**
 * Thrown when a query exceeds the configured polling timeout.
 * The shell sends a cancel request to the coordinator before raising this exception,
 * so the server-side query is not left running indefinitely.
 */
final class QueryTimeoutException extends RuntimeException {

    /** Number of seconds in one minute, used for duration formatting. */
    private static final long SECONDS_PER_MINUTE = 60;

    private final String queryId;
    private final Duration timeout;

    QueryTimeoutException(String queryId, Duration timeout) {
        super("Query " + queryId + " timed out after " + formatDuration(timeout)
                + "; cancel request sent to coordinator");
        this.queryId = queryId;
        this.timeout = timeout;
    }

    String queryId() {
        return queryId;
    }

    Duration timeout() {
        return timeout;
    }

    private static String formatDuration(Duration d) {
        long totalSeconds = d.getSeconds();
        if (totalSeconds < SECONDS_PER_MINUTE) {
            return totalSeconds + "s";
        }
        long minutes = totalSeconds / SECONDS_PER_MINUTE;
        long seconds = totalSeconds % SECONDS_PER_MINUTE;
        if (seconds == 0) {
            return minutes + "m";
        }
        return minutes + "m " + seconds + "s";
    }
}
