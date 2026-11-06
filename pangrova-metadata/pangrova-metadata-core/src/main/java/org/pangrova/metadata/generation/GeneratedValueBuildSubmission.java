package org.pangrova.metadata.generation;

import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.page.GenerationPage;

import java.util.List;
import java.util.Objects;

/** One immutable generated descriptor and its canonical durable graph, persisted as part of the write outbox. */
public record GeneratedValueBuildSubmission(GenerationJob job, GenerationDispatchSpec dispatchSpec,
        List<GenerationPage> pages, GeneratedValueBuildDescriptor descriptor) {
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
