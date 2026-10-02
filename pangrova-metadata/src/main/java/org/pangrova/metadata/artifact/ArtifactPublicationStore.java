package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;

import java.time.Instant;

/**
 * Atomic catalog publication adapter for one generic artifact type.
 *
 * <p>The durable build and lease contract is shared by every artifact. A
 * publication adapter owns only the catalog-specific pointer update that must
 * commit in the same transaction as the generic artifact manifest.</p>
 */
public interface ArtifactPublicationStore {

    ArtifactGenerationManifest publishClaimed(
            ArtifactBuildJob claimedJob,
            String owner,
            long ownerEpoch,
            Instant now);
}
