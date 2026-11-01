package org.pangrova.coordinator.index.lifecycle;

import java.time.Instant;

import org.pangrova.indexing.build.BuildClaimHandler;
import org.pangrova.indexing.control.IndexMetadataStore;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Delegates build claim operations to the JDBC-backed metadata store.
 * Bridges the indexing module's BuildClaimHandler SPI to the coordinator's
 * metadata persistence layer, enabling distributed build mutual exclusion.
 */
public final class MetadataBackedBuildClaimHandler implements BuildClaimHandler {

    private final IndexMetadataStore metadataStore;

    public MetadataBackedBuildClaimHandler(IndexMetadataStore metadataStore) {
        this.metadataStore = checkNotNull(metadataStore, "metadataStore must not be null");
    }

    @Override
    public boolean tryClaim(String indexId, int generation, String ownerId, Instant now) {
        return metadataStore.claimBuild(indexId, generation, ownerId, now);
    }

    @Override
    public void release(String indexId, int generation) {
        metadataStore.releaseBuildClaim(indexId, generation);
    }

    @Override
    public void heartbeat(String indexId, int generation, Instant now) {
        metadataStore.heartbeatBuildClaim(indexId, generation, now);
    }
}
