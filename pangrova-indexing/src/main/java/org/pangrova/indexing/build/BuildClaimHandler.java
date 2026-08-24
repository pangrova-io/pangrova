package org.pangrova.indexing.build;

import java.time.Instant;

public interface BuildClaimHandler {
    boolean tryClaim(String indexId, int generation, String ownerId, Instant claimedAt);

    void release(String indexId, int generation);

    void heartbeat(String indexId, int generation, Instant heartbeatAt);
}