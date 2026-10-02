package org.pangrova.metadata.artifact.build;

import org.pangrova.spi.coordination.ExecutionAnchor;

/** Exact backend-issued execution identity carried by a Native task completion. */
public record ArtifactExecutionOwner(String ownerId, String ownerIncarnation, long fencingToken) {
    public ArtifactExecutionOwner {
        if (ownerId == null || ownerId.isBlank() || ownerIncarnation == null
                || ownerIncarnation.isBlank() || fencingToken <= 0) {
            throw new IllegalArgumentException("artifact execution owner must be explicit");
        }
    }

    /** Projects a real durable anchor without inventing a lease or process identity. */
    public static ArtifactExecutionOwner from(ExecutionAnchor anchor) {
        return new ArtifactExecutionOwner(anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
    }
}
