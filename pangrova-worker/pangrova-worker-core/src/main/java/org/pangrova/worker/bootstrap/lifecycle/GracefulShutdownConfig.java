package org.pangrova.worker.bootstrap.lifecycle;

/**
 * Configuration for worker graceful shutdown behavior.
 *
 * @param drainTimeoutSeconds  maximum time to wait for running tasks to complete (default 30)
 * @param notifyCoordinator    whether to notify coordinator of pending task migration
 * @param deregisterFromDiscovery whether to deregister from service discovery on shutdown
 */
public record GracefulShutdownConfig(
        int drainTimeoutSeconds,
        boolean notifyCoordinator,
        boolean deregisterFromDiscovery) {

    public static final int DEFAULT_DRAIN_TIMEOUT_SECONDS = 30;

    public GracefulShutdownConfig {
        if (drainTimeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "drainTimeoutSeconds must be positive: " + drainTimeoutSeconds);
        }
    }

    public static GracefulShutdownConfig defaults() {
        return new GracefulShutdownConfig(DEFAULT_DRAIN_TIMEOUT_SECONDS, true, true);
    }
}
