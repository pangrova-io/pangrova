package org.pangrova.streaming.cdc;

public record CdcRegistrationResponse(
    String streamId,
    int allocatedMaxConcurrentWrites,
    long allocatedMaxPendingBytes,
    long heartbeatIntervalMs,
    long leaseTtlMs
) {}
