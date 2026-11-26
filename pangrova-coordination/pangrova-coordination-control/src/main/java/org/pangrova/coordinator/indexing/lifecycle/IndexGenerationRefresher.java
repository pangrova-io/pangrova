package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.build.BuildMappingPersister;
import org.pangrova.coordinator.indexing.IndexGenerationPublisher;
import org.pangrova.coordinator.indexing.ddl.DurableIndexBuildService;
import org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.build.IndexBuildResult;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.descriptor.IndexDescriptorTransitions;
import org.pangrova.indexing.api.enums.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashSet;
import java.util.List;

/** Builds and atomically publishes an immutable online replacement generation. */
final class IndexGenerationRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(IndexGenerationRefresher.class);

    private final IndexCommitContext deps;
    private final IndexLifecycleMetrics lifecycleMetrics;

    IndexGenerationRefresher(
            IndexCommitContext deps, IndexLifecycleMetrics lifecycleMetrics) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
    }

    boolean refresh(IndexDescriptor descriptor, IndexRefreshInput input) {
        String sourceSnapshot = input.sourceSnapshot();
        requireSourceSnapshot(sourceSnapshot);
        IndexMetadataStore store = deps.metadataStore();
        if (!input.forceReplacement()
                && isAlreadyPublished(store, descriptor.indexId(), sourceSnapshot)) {
            return true;
        }
        IndexBuildCoordinator coordinator = deps.buildServices().dataSkipping();
        boolean nativeArtifact = DurableIndexBuildService.supports(descriptor);
        if (!nativeArtifact && !input.addedFiles().isEmpty()
                && !coordinator.canAcceptBuild(descriptor.tenantId())) {
            return false;
        }
        int nextGeneration = Math.addExact(descriptor.generation(), 1);
        // Keep the reservation timestamp identical to the persisted publication fence.
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String nativeAttempt = nativeArtifact ? java.util.UUID.randomUUID().toString() : null;
        boolean started = nativeAttempt == null ? store.beginIndexGeneration(
                descriptor.indexId(), nextGeneration, sourceSnapshot,
                startedAt, descriptor.updatedAt(), descriptor.state(), descriptor.generation())
                : store.beginNativeIndexGeneration(descriptor.indexId(), nextGeneration, sourceSnapshot,
                        startedAt, descriptor.updatedAt(), descriptor.state(), descriptor.generation(),
                        null, nativeAttempt);
        if (!started) {
            return false;
        }
        IndexDescriptor rebuilding = IndexDescriptorTransitions.transition(
                descriptor.withGeneration(nextGeneration, startedAt)
                        .withBuildSnapshotId(sourceSnapshot, startedAt),
                IndexState.BUILDING, startedAt);
        try {
            if (!nativeArtifact) {
                IndexSourceAuthority.forDescriptor(
                        store, deps.buildServices(), deps.connectorAccessorResolver(), descriptor);
            }
            return refreshGeneration(
                    store, coordinator, descriptor, rebuilding, input, nativeArtifact, nativeAttempt);
        } catch (RuntimeException failure) {
            recordFailure(store, rebuilding, nativeAttempt, failure);
            LOG.error("action=index_generation_refresh_failed indexId={} generation={} error={}",
                    descriptor.indexId(), nextGeneration, failure.getMessage(), failure);
            return false;
        }
    }

    private boolean refreshGeneration(
            IndexMetadataStore store,
            IndexBuildCoordinator coordinator,
            IndexDescriptor descriptor,
            IndexDescriptor rebuilding,
            IndexRefreshInput input,
            boolean nativeArtifact, String nativeAttempt) {
        if (nativeArtifact) {
            submitNativeGeneration(descriptor, rebuilding, nativeAttempt);
            LOG.info("action=native_index_generation_queued indexId={} generation={} snapshot={}",
                    descriptor.indexId(), rebuilding.generation(), input.sourceSnapshot());
            return true;
        }
        long reused = store.findActiveIndexGeneration(descriptor.indexId())
                .map(active -> {
                    if (active.generation() != descriptor.generation()) {
                        throw new IllegalStateException(
                                "Active index generation differs from refresh base: " + descriptor.indexId());
                    }
                    return copyReusableMappings(store, descriptor, rebuilding, input);
                })
                .orElse(0L);
        long built = buildAddedFiles(store, coordinator, rebuilding, input.addedFiles());
        if (store.findIndex(descriptor.indexId()).isEmpty()) {
            throw new IllegalStateException(
                    "Index disappeared during refresh: " + descriptor.indexId());
        }
        IndexGenerationPublisher.publish(store, rebuilding, Math.addExact(reused, built));
        LOG.info("action=index_generation_refreshed indexId={} generation={} "
                        + "reused={} built={} snapshot={}",
                descriptor.indexId(), rebuilding.generation(), reused, built,
                input.sourceSnapshot());
        return true;
    }

    private static long copyReusableMappings(
            IndexMetadataStore store, IndexDescriptor descriptor,
            IndexDescriptor rebuilding, IndexRefreshInput input) {
        return store.copyIndexGenerationMappings(
                descriptor.indexId(), descriptor.generation(), rebuilding.generation(),
                List.copyOf(new LinkedHashSet<>(input.removedFiles())));
    }

    private void submitNativeGeneration(IndexDescriptor previous, IndexDescriptor rebuilding, String nativeAttempt) {
        var service = deps.buildServices().aiNative();
        if (service == null) {
            throw new IllegalStateException("Native index maintenance has no durable build service");
        }
        service.submitReplacement(previous, rebuilding, nativeAttempt);
    }

    private long buildAddedFiles(
            IndexMetadataStore store,
            IndexBuildCoordinator coordinator,
            IndexDescriptor rebuilding,
            List<DataFileInfo> dataFiles) {
        if (dataFiles.isEmpty()) {
            IndexBuildResult pinned = coordinator.submitBuildTask(
                    rebuilding, List.of(), null, null).join();
            if (!pinned.isFullySuccessful()) {
                throw new IllegalStateException("Empty replacement could not pin its source snapshot");
            }
            return 0;
        }
        var accessors = IndexSourceAuthority.forDescriptor(
                store, deps.buildServices(), deps.connectorAccessorResolver(), rebuilding);
        FileSystemAccessor fileSystem = accessors
                .resolve(rebuilding.catalogName());
        ColumnValueReader columnReader = accessors
                .resolveColumnValueReader(rebuilding.catalogName());
        IndexBuildResult result = coordinator.submitBuildTask(
                rebuilding, dataFiles, fileSystem, columnReader,
                batch -> BuildMappingPersister.persist(
                        store, batch, rebuilding.generation(),
                        rebuilding.buildSnapshotId()))
                .join();
        if (!result.isFullySuccessful() || result.successCount() != dataFiles.size()) {
            throw new IllegalStateException(
                    "Incremental generation build was incomplete: " + result);
        }
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordCommitBuildTriggered();
        }
        return result.successCount();
    }

    private static boolean isAlreadyPublished(
            IndexMetadataStore store, String indexId, String sourceSnapshot) {
        return store.findActiveIndexGeneration(indexId)
                .filter(manifest -> manifest.sourceSnapshot().equals(sourceSnapshot))
                .isPresent();
    }

    private static void recordFailure(
            IndexMetadataStore store,
            IndexDescriptor rebuilding,
            String nativeAttempt,
            RuntimeException failure) {
        if (store.findIndex(rebuilding.indexId())
                .filter(current -> current.state() != IndexState.DROPPED)
                .isEmpty()) {
            return;
        }
        String reason = failure.getMessage() == null
                ? "generation_refresh_failed" : failure.getMessage();
        boolean failed = nativeAttempt == null ? store.failIndexGeneration(
                rebuilding.indexId(), rebuilding.generation(), rebuilding.updatedAt(), Instant.now(), reason)
                : store.failNativeSubmissionAttempt(rebuilding.indexId(), rebuilding.generation(), nativeAttempt,
                        rebuilding.updatedAt(), Instant.now(), reason);
        if (!failed) {
            LOG.warn("action=index_generation_failure_fence_lost indexId={} generation={}",
                    rebuilding.indexId(), rebuilding.generation());
        }
    }

    private static void requireSourceSnapshot(String sourceSnapshot) {
        if (sourceSnapshot == null || sourceSnapshot.isBlank()) {
            throw new IllegalArgumentException(
                    "Online index refresh requires a non-blank source snapshot");
        }
    }
}
