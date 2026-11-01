package org.pangrova.generation.core.publication;

import org.pangrova.spi.generation.manifest.GenerationManifest;
import org.pangrova.persistence.api.generation.publication.GenerationPublicationStore;

import org.pangrova.spi.generation.job.GenerationJob;

import java.time.Instant;
import java.util.Map;

/** Strict artifact-type router for independent atomic publication adapters. */
public final class GenerationPublicationRouter implements GenerationPublicationStore {

    private final Map<String, GenerationPublicationStore> routes;

    /** Freezes the complete, non-empty artifact-type publication routing table. */
    public GenerationPublicationRouter(Map<String, GenerationPublicationStore> routes) {
        this.routes = Map.copyOf(routes);
        if (this.routes.isEmpty()
                || this.routes.entrySet().stream().anyMatch(entry ->
                entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null)) {
            throw new IllegalArgumentException("publication routes must be non-empty and complete");
        }
    }

    @Override
    public GenerationManifest publishClaimed(
            GenerationJob claimedJob, String owner, long ownerEpoch, Instant now) {
        GenerationPublicationStore route = routes.get(claimedJob.artifactType());
        if (route == null) {
            throw new IllegalStateException(
                    "No atomic publication adapter for artifact type "
                            + claimedJob.artifactType());
        }
        return route.publishClaimed(claimedJob, owner, ownerEpoch, now);
    }
}
