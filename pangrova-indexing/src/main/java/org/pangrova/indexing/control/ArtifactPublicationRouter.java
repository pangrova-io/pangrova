package org.pangrova.indexing.control;

import org.pangrova.indexing.api.ArtifactGenerationManifest;

import java.time.Instant;
import java.util.Map;

/** Strict artifact-type router for independent atomic publication adapters. */
public final class ArtifactPublicationRouter implements ArtifactPublicationStore {

    private final Map<String, ArtifactPublicationStore> routes;

    /** Freezes the complete, non-empty artifact-type publication routing table. */
    public ArtifactPublicationRouter(Map<String, ArtifactPublicationStore> routes) {
        this.routes = Map.copyOf(routes);
        if (this.routes.isEmpty()
                || this.routes.entrySet().stream().anyMatch(entry ->
                entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null)) {
            throw new IllegalArgumentException("publication routes must be non-empty and complete");
        }
    }

    @Override
    public ArtifactGenerationManifest publishClaimed(
            ArtifactBuildJob claimedJob, String owner, long ownerEpoch, Instant now) {
        ArtifactPublicationStore route = routes.get(claimedJob.artifactType());
        if (route == null) {
            throw new IllegalStateException(
                    "No atomic publication adapter for artifact type "
                            + claimedJob.artifactType());
        }
        return route.publishClaimed(claimedJob, owner, ownerEpoch, now);
    }
}
