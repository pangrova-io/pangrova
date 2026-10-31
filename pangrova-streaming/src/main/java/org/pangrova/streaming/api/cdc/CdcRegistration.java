package org.pangrova.streaming.api.cdc;

import java.time.Instant;

/**
 * Immutable representation of a CDC stream registration.
 *
 * @param streamId            unique stream identifier
 * @param consumerId          consumer that owns this registration
 * @param tenantId            tenant owning the stream (nullable)
 * @param state               current stream state
 * @param maxConcurrentWrites max concurrent write operations
 * @param maxPendingBytes     max pending bytes before backpressure
 * @param lastHeartbeat       timestamp of last heartbeat
 * @param createdAt           timestamp of registration creation
 */
public record CdcRegistration(
        String streamId,
        String consumerId,
        String tenantId,
        CdcStreamState state,
        int maxConcurrentWrites,
        long maxPendingBytes,
        Instant lastHeartbeat,
        Instant createdAt) {

    /** CDC stream lifecycle states. */
    public enum CdcStreamState {
        ACTIVE,
        PAUSED,
        EXPIRED
    }
}
