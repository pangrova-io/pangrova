package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;

import java.util.Objects;
import java.util.function.Function;

/** Validates durable Native index source pins before dispatch or publication. */
public final class ArtifactSnapshotPinValidator {
    private final GenerationJobStore store;
    private final Function<ConnectorSession, SnapshotPinProvider> pinProviders;

    public ArtifactSnapshotPinValidator(
            GenerationJobStore store,
            Function<ConnectorSession, SnapshotPinProvider> pinProviders) {
        this.store = Objects.requireNonNull(store, "store");
        this.pinProviders = Objects.requireNonNull(pinProviders, "pinProviders");
    }

    /** Revalidates the durable and connector pin before Native work can proceed. */
    public void validate(GenerationJob job) {
        if (!GenerationOutputTypeIds.VECTOR_INDEX.equals(job.artifactType())
                && !GenerationOutputTypeIds.TEXT_INDEX.equals(job.artifactType())) {
            return;
        }
        SnapshotPinIntent intent = store.findSnapshotPinForJob(job.tenantId(), job.jobId())
                .orElseThrow(() -> new SnapshotExpiredException(
                        job.sourceSnapshot(), "Artifact job has no durable snapshot pin"));
        if (intent.state() != SnapshotPinState.PINNED
                || !intent.indexId().equals(job.artifactId())
                || !Integer.toString(intent.generation()).equals(job.stagingGeneration())
                || !intent.sourceSnapshot().equals(job.sourceSnapshot())) {
            throw new SnapshotExpiredException(
                    job.sourceSnapshot(), "Artifact job snapshot pin identity differs from its generation");
        }
        if (org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT.equals(
                intent.sourceSnapshot())) {
            return;
        }
        var resolved = DeferredIndexAuthorityResolver.resolveSubmission(intent.authority());
        SnapshotPinProvider provider = pinProviders.apply(resolved.session());
        if (provider == null) {
            throw new IllegalStateException("Authorized catalog does not support durable snapshot pinning");
        }
        provider.validateSnapshotPin(
                resolved.table().catalog(), resolved.table().schema(), resolved.table().table(),
                intent.sourceSnapshot(), new SnapshotPinOwner(
                        intent.indexId(), intent.generation(), intent.attemptId()));
    }
}
