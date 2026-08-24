package org.pangrova.spi.streaming;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI contract for CDC stream lifecycle management.
 * Decouples the coordinator from the streaming module implementation.
 *
 * <p>Implementations handle stream registration, heartbeat renewal,
 * and stale stream expiration.</p>
 */
@Experimental
public interface CdcLifecycleManager {

    /**
     * Register a new CDC stream.
     *
     * @param consumerId         unique consumer identifier
     * @param tenantId           tenant owning the stream (nullable)
     * @param maxConcurrentWrites max concurrent write operations
     * @param maxPendingBytes    max pending bytes before backpressure
     * @return registration result with stream ID and lease parameters
     */
    CdcRegistrationResult register(String consumerId, String tenantId,
                                   int maxConcurrentWrites, long maxPendingBytes);

    /**
     * Renew the heartbeat for an active CDC stream.
     *
     * @param streamId the stream to renew
     * @throws IllegalArgumentException if stream not found
     */
    void heartbeat(String streamId);

    /**
     * Expire streams that have not sent a heartbeat within the lease TTL.
     */
    void expireStaleStreams();

    /**
     * Check if CDC infrastructure is available for the given table.
     *
     * <p>Used by the Dynamic Table refresh coordinator to validate
     * REFRESH_MODE = CDC at creation time and to detect CDC
     * unavailability for fallback to SNAPSHOT mode at runtime.</p>
     *
     * @param tableName fully qualified table name
     * @return {@code true} if CDC capture is available for this table
     */
    boolean isAvailable(String tableName);

    /**
     * Resolve the CDC offset corresponding to a snapshot checkpoint.
     *
     * <p>When the refresh coordinator falls back from CDC to SNAPSHOT
     * mode and later recovers, it needs to find the CDC offset that
     * corresponds to the latest SNAPSHOT checkpoint so consumption
     * can resume without data loss or duplication.</p>
     *
     * @param tableName          fully qualified table name
     * @param snapshotCheckpoint the snapshot checkpoint (opaque string)
     * @return the CDC offset corresponding to the snapshot checkpoint
     * @throws IllegalArgumentException if the checkpoint cannot be resolved
     */
    String resolveOffsetFromSnapshot(String tableName, String snapshotCheckpoint);

    /**
     * Immutable result of a CDC stream registration.
     */
    record CdcRegistrationResult(
            String streamId,
            int maxConcurrentWrites,
            long maxPendingBytes,
            long heartbeatIntervalMs,
            long leaseTtlMs) {}
}
