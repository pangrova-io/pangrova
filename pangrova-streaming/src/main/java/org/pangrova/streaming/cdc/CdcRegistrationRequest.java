package org.pangrova.streaming.cdc;

import java.util.List;

public record CdcRegistrationRequest(
    String consumerId,
    String tenantId,
    List<String> targetTables,
    int maxConcurrentWrites,
    long maxPendingBytes
) {
    public CdcRegistrationRequest {
        targetTables = List.copyOf(targetTables);
    }

    @Override public List<String> targetTables() { return List.copyOf(targetTables); }
}
