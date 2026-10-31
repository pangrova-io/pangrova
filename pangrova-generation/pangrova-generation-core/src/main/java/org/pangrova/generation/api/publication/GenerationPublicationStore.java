package org.pangrova.generation.api.publication;

import org.pangrova.generation.api.manifest.GenerationManifest;

import org.pangrova.generation.api.job.GenerationJob;

import java.time.Instant;

/**
 * Atomic catalog publication adapter for one generic artifact type.
 *
 * <p>The durable build and lease contract is shared by every artifact. A
 * publication adapter owns only the catalog-specific pointer update that must
 * commit in the same transaction as the generic artifact manifest.</p>
 */
public interface GenerationPublicationStore {

    GenerationManifest publishClaimed(
            GenerationJob claimedJob,
            String owner,
            long ownerEpoch,
            Instant now);
}
