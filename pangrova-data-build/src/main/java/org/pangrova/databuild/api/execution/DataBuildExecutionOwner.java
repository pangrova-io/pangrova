package org.pangrova.databuild.api.execution;

import org.pangrova.query.api.coordination.ExecutionAnchor;

/** Exact backend-issued execution identity carried by a Native task completion. */
public record DataBuildExecutionOwner(String ownerId, String ownerIncarnation, long fencingToken) {
    public DataBuildExecutionOwner {
        if (ownerId == null || ownerId.isBlank() || ownerIncarnation == null
                || ownerIncarnation.isBlank() || fencingToken <= 0) {
            throw new IllegalArgumentException("artifact execution owner must be explicit");
        }
    }

    /** Projects a real durable anchor without inventing a lease or process identity. */
    public static DataBuildExecutionOwner from(ExecutionAnchor anchor) {
        return new DataBuildExecutionOwner(anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
    }
}
