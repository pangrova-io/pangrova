package org.pangrova.streaming.core.cdc;

import org.pangrova.streaming.api.cdc.CdcRegistration.CdcStreamState;

import java.time.Instant;

public record CdcStreamInfo(
    String streamId,
    String consumerId,
    String tenantId,
    CdcStreamState state,
    int maxConcurrentWrites,
    long maxPendingBytes,
    Instant lastHeartbeat,
    Instant createdAt
) {}
