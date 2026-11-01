package org.pangrova.spi.generation.job;

import org.pangrova.spi.generation.execution.GenerationExecutionOwner;

import java.time.Instant;
import java.util.List;

/** Fenced, immutable completion report for one artifact work page. */
public record GenerationPageCompletion(
        String tenantId,
        String jobId,
        String pageId,
        long fencingToken,
        long ownerEpoch,
        byte[] reportBytes,
        String reportSha256,
        List<GenerationOutput> outputs,
        Instant completedAt,
        GenerationExecutionOwner executionOwner) {

    /** Defensively copies mutable completion payloads. */
    public GenerationPageCompletion {
        reportBytes = reportBytes == null ? null : reportBytes.clone();
        outputs = outputs == null ? null : List.copyOf(outputs);
        java.util.Objects.requireNonNull(executionOwner, "executionOwner");
    }

    @Override
    public byte[] reportBytes() {
        return reportBytes == null ? null : reportBytes.clone();
    }
}
