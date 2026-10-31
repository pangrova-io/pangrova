package org.pangrova.databuild.api.build;

import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;

import java.time.Instant;
import java.util.List;

/** Fenced, immutable completion report for one artifact work page. */
public record DataBuildPageCompletion(
        String tenantId,
        String jobId,
        String pageId,
        long fencingToken,
        long ownerEpoch,
        byte[] reportBytes,
        String reportSha256,
        List<DataBuildOutput> outputs,
        Instant completedAt,
        DataBuildExecutionOwner executionOwner) {

    /** Defensively copies mutable completion payloads. */
    public DataBuildPageCompletion {
        reportBytes = reportBytes == null ? null : reportBytes.clone();
        outputs = outputs == null ? null : List.copyOf(outputs);
        java.util.Objects.requireNonNull(executionOwner, "executionOwner");
    }

    @Override
    public byte[] reportBytes() {
        return reportBytes == null ? null : reportBytes.clone();
    }
}
