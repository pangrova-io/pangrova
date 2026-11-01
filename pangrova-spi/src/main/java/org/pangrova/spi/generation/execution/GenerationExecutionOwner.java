package org.pangrova.spi.generation.execution;

import org.pangrova.spi.query.coordination.ExecutionAnchor;

/** Exact backend-issued execution identity carried by a Native task completion. */
public record GenerationExecutionOwner(String ownerId, String ownerIncarnation, long fencingToken) {
    public GenerationExecutionOwner {
        if (ownerId == null || ownerId.isBlank() || ownerIncarnation == null
                || ownerIncarnation.isBlank() || fencingToken <= 0) {
            throw new IllegalArgumentException("artifact execution owner must be explicit");
        }
    }

    /** Projects a real durable anchor without inventing a lease or process identity. */
    public static GenerationExecutionOwner from(ExecutionAnchor anchor) {
        return new GenerationExecutionOwner(anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
    }
}
