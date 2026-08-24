package org.pangrova.spi.coordination;

import java.util.Objects;

/**
 * Immutable snapshot of a coordinator's registration state.
 *
 * <p>Captures the coordinator's identity, network endpoint, current role
 * within the HA cluster, and timing information. The {@code role} field
 * uses {@link String} rather than an enum so the SPI layer remains
 * independent of coordinator-internal types. Expected values are
 * {@code "LEADER"}, {@code "STANDBY"}, and {@code "STOPPED"}.
 *
 * @param coordinatorId  unique coordinator identifier (required)
 * @param host           hostname or IP address (required)
 * @param httpPort       HTTP API port
 * @param role           current HA role as string (LEADER / STANDBY / STOPPED)
 * @param epoch          leadership epoch counter
 * @param lastHeartbeatMs epoch millis of last heartbeat
 * @param registeredAtMs  epoch millis of initial registration
 * @param lastProcessedInvalidationId highest invalidation_log ID processed by this coordinator's
 *                                     {@code JdbcInvalidationPoller}; -1 if unknown
 */
public record CoordinatorSnapshot(
        String coordinatorId,
        String host,
        int httpPort,
        String role,
        long epoch,
        long lastHeartbeatMs,
        long registeredAtMs,
        long lastProcessedInvalidationId
) {

    public CoordinatorSnapshot {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        Objects.requireNonNull(host, "host must not be null");
    }

    /**
     * Backward-compatible constructor without invalidation tracking.
     */
    public CoordinatorSnapshot(String coordinatorId, String host, int httpPort,
                                String role, long epoch,
                                long lastHeartbeatMs, long registeredAtMs) {
        this(coordinatorId, host, httpPort, role, epoch,
                lastHeartbeatMs, registeredAtMs, -1);
    }
}
