package org.pangrova.streaming.core.cdc;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.streaming.api.cdc.CdcLifecycleManager;
import org.pangrova.streaming.api.cdc.CdcRegistration;
import org.pangrova.streaming.api.cdc.CdcRegistration.CdcStreamState;
import org.pangrova.streaming.api.cdc.CdcRegistrationStore;
import org.pangrova.streaming.store.memory.InMemoryCdcRegistrationStore;

import java.time.Instant;
import java.util.UUID;

/**
 * Handles CDC stream registration and heartbeat management.
 * Implements {@link CdcLifecycleManager} SPI for coordinator decoupling.
 *
 * <p>Delegates persistence to a {@link CdcRegistrationStore}, enabling
 * pluggable backends (in-memory for single-node, JDBC for distributed).</p>
 */
@ThreadSafe
public class CdcRegistrationHandler implements CdcLifecycleManager {

    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
    private static final long DEFAULT_LEASE_TTL_MS = 90_000;

    private final CdcRegistrationStore store;

    /**
     * Creates a handler backed by the given registration store.
     *
     * @param store the store for persisting CDC registrations
     */
    public CdcRegistrationHandler(CdcRegistrationStore store) {
        this.store = store;
    }

    /** Convenience constructor using in-memory store (single-node). */
    public CdcRegistrationHandler() {
        this(new InMemoryCdcRegistrationStore());
    }

    /** Registers a CDC stream from the given request and returns the assigned limits. */
    public CdcRegistrationResponse register(CdcRegistrationRequest request) {
        validateRequest(request);
        CdcRegistrationResult result = register(
                request.consumerId(), request.tenantId(),
                request.maxConcurrentWrites(), request.maxPendingBytes());
        return new CdcRegistrationResponse(result.streamId(), result.maxConcurrentWrites(),
                result.maxPendingBytes(), result.heartbeatIntervalMs(), result.leaseTtlMs());
    }

    @Override
    public CdcRegistrationResult register(String consumerId, String tenantId,
                                           int maxConcurrentWrites, long maxPendingBytes) {
        if (consumerId == null || consumerId.isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                "consumerId is required");
        }
        String streamId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        CdcRegistration registration = new CdcRegistration(
                streamId, consumerId, tenantId, CdcStreamState.ACTIVE,
                maxConcurrentWrites, maxPendingBytes, now, now);
        store.register(registration);
        return new CdcRegistrationResult(streamId, maxConcurrentWrites,
            maxPendingBytes, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TTL_MS);
    }

    @Override
    public void heartbeat(String streamId) {
        CdcRegistration reg = store.getRegistration(streamId)
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                        "Stream not found: " + streamId));
        CdcRegistration refreshed = new CdcRegistration(
                reg.streamId(), reg.consumerId(), reg.tenantId(),
                CdcStreamState.ACTIVE, reg.maxConcurrentWrites(),
                reg.maxPendingBytes(), Instant.now(), reg.createdAt());
        store.register(refreshed);
    }

    @Override
    public void expireStaleStreams() {
        Instant threshold = Instant.now().minusMillis(DEFAULT_LEASE_TTL_MS);
        for (CdcRegistration reg : store.listRegistrations()) {
            if (reg.lastHeartbeat().isBefore(threshold)) {
                store.unregister(reg.streamId());
            }
        }
    }

    @Override
    public boolean isAvailable(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return false;
        }
        return store.listRegistrations().stream()
                .anyMatch(reg -> reg.state() == CdcStreamState.ACTIVE);
    }

    @Override
    public String resolveOffsetFromSnapshot(String tableName, String snapshotCheckpoint) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName is required");
        }
        if (snapshotCheckpoint == null || snapshotCheckpoint.isBlank()) {
            throw new IllegalArgumentException("snapshotCheckpoint is required");
        }
        // The snapshot checkpoint is the Kafka offset string from the snapshot metadata.
        // Return it directly — the CdcSourceReader.seekToOffsets() implementation
        // will parse it into topic-partition-offset entries.
        return snapshotCheckpoint;
    }

    public CdcStreamInfo getStream(String streamId) {
        return store.getRegistration(streamId)
                .map(CdcRegistrationHandler::toStreamInfo)
                .orElse(null);
    }

    private void validateRequest(CdcRegistrationRequest request) {
        if (request.consumerId() == null || request.consumerId().isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                "consumerId is required");
        }
    }

    private static CdcStreamInfo toStreamInfo(CdcRegistration reg) {
        return new CdcStreamInfo(
                reg.streamId(), reg.consumerId(), reg.tenantId(), reg.state(),
                reg.maxConcurrentWrites(), reg.maxPendingBytes(),
                reg.lastHeartbeat(), reg.createdAt());
    }
}
