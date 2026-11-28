package org.pangrova.worker.heartbeat;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Configuration for the periodic heartbeat sender.
 */
public record HeartbeatConfig(
        String coordinatorUrl,
        String workerId,
        long intervalMs) {

    public HeartbeatConfig {
        checkNotNull(coordinatorUrl, "coordinatorUrl must not be null");
        checkNotNull(workerId, "workerId must not be null");
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive");
        }
    }

    public static final long DEFAULT_INTERVAL_MS = 5_000;
}
