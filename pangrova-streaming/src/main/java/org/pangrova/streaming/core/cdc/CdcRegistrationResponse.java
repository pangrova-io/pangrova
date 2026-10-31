package org.pangrova.streaming.core.cdc;

public record CdcRegistrationResponse(
    String streamId,
    int allocatedMaxConcurrentWrites,
    long allocatedMaxPendingBytes,
    long heartbeatIntervalMs,
    long leaseTtlMs
) {}
