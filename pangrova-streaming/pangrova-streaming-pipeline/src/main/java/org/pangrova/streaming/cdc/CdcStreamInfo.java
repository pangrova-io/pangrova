package org.pangrova.streaming.cdc;

import org.pangrova.streaming.api.enums.CdcStreamState;

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
