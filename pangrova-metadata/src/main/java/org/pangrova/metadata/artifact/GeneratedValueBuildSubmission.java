package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;

import java.util.List;
import java.util.Objects;

/** One immutable generated descriptor and its canonical durable graph, persisted as part of the write outbox. */
public record GeneratedValueBuildSubmission(ArtifactBuildJob job, ArtifactBuildDispatchSpec dispatchSpec,
                                            List<ArtifactBuildPage> pages, GeneratedValueArtifactDescriptor descriptor) {
    /** Requires matching identity across descriptor, job and dispatch; pages are copied before persistence. */
    public GeneratedValueBuildSubmission {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(dispatchSpec, "dispatchSpec");
        Objects.requireNonNull(descriptor, "descriptor");
        pages = List.copyOf(pages);
        if (!job.jobId().equals(dispatchSpec.jobId()) || !job.tenantId().equals(descriptor.tenantId())
                || !job.artifactId().equals(descriptor.artifactId())
                || !job.sourceId().equals(descriptor.sourceId())
                || !job.sourceSnapshot().equals(descriptor.targetSourceSnapshot())
                || !job.stagingGeneration().equals(descriptor.targetGeneration())) {
            throw new IllegalArgumentException("Generated descriptor differs from its canonical durable submission");
        }
    }
}
