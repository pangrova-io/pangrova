package org.pangrova.metadata.artifact.build;

import java.time.Instant;
import java.util.List;

/** Fenced, immutable completion report for one artifact work page. */
public record ArtifactPageCompletion(
        String tenantId,
        String jobId,
        String pageId,
        long fencingToken,
        long ownerEpoch,
        byte[] reportBytes,
        String reportSha256,
        List<ArtifactBuildOutput> outputs,
        Instant completedAt,
        ArtifactExecutionOwner executionOwner) {

    /** Defensively copies mutable completion payloads. */
    public ArtifactPageCompletion {
        reportBytes = reportBytes == null ? null : reportBytes.clone();
        outputs = outputs == null ? null : List.copyOf(outputs);
        java.util.Objects.requireNonNull(executionOwner, "executionOwner");
    }

    @Override
    public byte[] reportBytes() {
        return reportBytes == null ? null : reportBytes.clone();
    }
}
