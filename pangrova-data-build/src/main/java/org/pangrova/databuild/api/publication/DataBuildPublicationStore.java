package org.pangrova.databuild.api.publication;

import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;

import org.pangrova.databuild.api.build.DataBuildJob;

import java.time.Instant;

/**
 * Atomic catalog publication adapter for one generic artifact type.
 *
 * <p>The durable build and lease contract is shared by every artifact. A
 * publication adapter owns only the catalog-specific pointer update that must
 * commit in the same transaction as the generic artifact manifest.</p>
 */
public interface DataBuildPublicationStore {

    DataBuildGenerationManifest publishClaimed(
            DataBuildJob claimedJob,
            String owner,
            long ownerEpoch,
            Instant now);
}
