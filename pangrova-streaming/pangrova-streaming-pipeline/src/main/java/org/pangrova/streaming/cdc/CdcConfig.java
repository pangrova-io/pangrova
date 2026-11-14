package org.pangrova.streaming.cdc;

import java.util.List;

/**
 * CDC consumer configuration.
 */
public record CdcConfig(
    String bootstrapServers,
    String groupId,
    List<String> topics,
    String consumerId,
    String tenantId,
    int maxConcurrentWrites,
    long maxPendingBytes
) {
    public CdcConfig {
        topics = List.copyOf(topics);
    }

    @Override public List<String> topics() { return List.copyOf(topics); }
}
