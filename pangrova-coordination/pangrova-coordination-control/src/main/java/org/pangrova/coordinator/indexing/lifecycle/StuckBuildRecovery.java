package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildAbandonment;
import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildCandidate;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Recovers abandoned scalar initial and online builds through the existing
 * descriptor, staging-generation, mapping, publication and claim fences.
 *
 * <p>Recovery first commits the generation fence and expires the old claim. Only
 * then may it delete unreferenced staging objects, remove their mappings and ask
 * the normal generation refresher to rebuild. A DROP or replacement that wins a
 * fence race is never resurrected.</p>
 */
public final class StuckBuildRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(StuckBuildRecovery.class);
    private static final String RECOVERY_REASON = "scalar_build_timeout";

    private final IndexCommitContext deps;
    private final Duration buildTimeout;
    private final int stuckDetectionMultiplier;
    private final MaintenanceLeaderElection leaderElection;
    private final IndexGenerationRefresher generationRefresher;

    /** Creates scalar recovery over the same maintenance dependencies used by refresh. */
    public StuckBuildRecovery(
            IndexCommitContext deps,
            Duration buildTimeout,
            int stuckDetectionMultiplier,
            MaintenanceLeaderElection leaderElection) {
        this.deps = deps;
        this.buildTimeout = buildTimeout;
        this.stuckDetectionMultiplier = stuckDetectionMultiplier;
        this.leaderElection = leaderElection;
        this.generationRefresher = new IndexGenerationRefresher(deps, null);
    }

    /**
     * Fences abandoned scalar builds, cleans their staging objects and rebuilds
     * through the ordinary immutable generation path.
     *
     * @return number of generation fences committed or failed-build cleanups completed
     */
    public int recoverStuckBuilds() {
        if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
            LOG.debug("event=stuck_recovery_skip reason=not_leader");
            return 0;
        }
        Instant cutoff = computeStuckCutoff();
        IndexMetadataStore store = deps.metadataStore();
        List<IndexBuildCandidate> candidates = store.findIndexBuildRecoveryCandidates().stream()
                .filter(candidate -> isScalar(candidate.descriptor()))
                .filter(candidate -> candidate.lastActivityAt().isBefore(cutoff))
                .toList();
        int recovered = 0;
        for (IndexBuildCandidate candidate : candidates) {
            try {
                if ("BUILDING".equals(candidate.state())) {
                    recovered += recoverBuilding(candidate, cutoff) ? 1 : 0;
                } else if ("FAILED".equals(candidate.state())) {
                    recovered += recoverFailedCleanup(candidate) ? 1 : 0;
                }
            } catch (RuntimeException failure) {
                LOG.error("event=stuck_build_recovery_failed indexId={} generation={} error={}",
                        candidate.descriptor().indexId(), candidate.generation(),
                        failure.getMessage(), failure);
            }
        }
        if (recovered > 0) {
            LOG.info("event=stuck_build_recovery_completed recoveredCount={}", recovered);
        }
        return recovered;
    }

    private Instant computeStuckCutoff() {
        long stuckThresholdMillis = buildTimeout.toMillis() * stuckDetectionMultiplier;
        return Instant.now().minusMillis(stuckThresholdMillis);
    }

    private boolean recoverBuilding(IndexBuildCandidate candidate, Instant cutoff) {
        IndexMetadataStore store = deps.metadataStore();
        IndexDescriptor descriptor = candidate.descriptor();
        Instant abandonedAt = Instant.now();
        var abandonment = store.abandonIndexGeneration(
                descriptor.indexId(), candidate.generation(), descriptor.updatedAt(),
                cutoff, abandonedAt, RECOVERY_REASON);
        if (abandonment.isEmpty()) {
            LOG.debug("event=stuck_build_recovery_skipped indexId={} generation={} reason=fence_or_heartbeat_changed",
                    descriptor.indexId(), candidate.generation());
            return false;
        }
        IndexBuildAbandonment fenced = abandonment.orElseThrow();
        LOG.warn("event=stuck_build_recovered indexId={} generation={} state={} stuckDurationMs={}",
                descriptor.indexId(), candidate.generation(), descriptor.state(),
                Duration.between(candidate.lastActivityAt(), abandonedAt).toMillis());
        if (!cleanup(fenced.descriptor(), candidate.generation(), abandonedAt,
                fenced.mappings(), fenced.pathsToDelete())) {
            return true;
        }
        rebuild(fenced.descriptor().indexId());
        return true;
    }

    private boolean recoverFailedCleanup(IndexBuildCandidate candidate) {
        IndexMetadataStore store = deps.metadataStore();
        List<IndexFileMapping> mappings = store.getIndexFileMappings(candidate.descriptor().indexId()).stream()
                .filter(mapping -> mapping.generation() == candidate.generation())
                .toList();
        Set<String> abandonedPaths = mappings.stream().map(IndexFileMapping::indexFilePath)
                .collect(java.util.stream.Collectors.toSet());
        Set<String> retainedPaths = store.getIndexFileMappings(candidate.descriptor().indexId()).stream()
                .filter(mapping -> mapping.generation() != candidate.generation())
                .map(IndexFileMapping::indexFilePath)
                .collect(java.util.stream.Collectors.toSet());
        abandonedPaths.removeAll(retainedPaths);
        if (!cleanup(candidate.descriptor(), candidate.generation(), candidate.updatedAt(),
                mappings, abandonedPaths)) {
            return false;
        }
        rebuild(candidate.descriptor().indexId());
        return true;
    }

    private boolean cleanup(
            IndexDescriptor descriptor, int generation, Instant buildUpdatedAt,
            List<IndexFileMapping> mappings, Set<String> pathsToDelete) {
        if (!pathsToDelete.isEmpty()) {
            FileSystemAccessor fileSystem = IndexSourceAuthority.forDescriptor(
                    deps.metadataStore(), deps.buildServices(), deps.connectorAccessorResolver(), descriptor)
                    .resolve(descriptor.catalogName());
            for (String path : pathsToDelete) {
                try {
                    if (fileSystem.exists(path)) {
                        fileSystem.deleteFile(path);
                    }
                } catch (IOException failure) {
                    throw new IllegalStateException(
                            "Failed to clean fenced index staging: " + descriptor.indexId(), failure);
                }
            }
        }
        boolean completed = deps.metadataStore().completeIndexBuildCleanup(
                descriptor.indexId(), generation, descriptor.updatedAt(), buildUpdatedAt,
                descriptor.state(), descriptor.generation());
        if (!completed) {
            LOG.debug("event=stuck_build_cleanup_deferred indexId={} generation={} mappings={} reason=fence_changed",
                    descriptor.indexId(), generation, mappings.size());
        }
        return completed;
    }

    private static boolean isScalar(IndexDescriptor descriptor) {
        return !descriptor.indexType().requiresNativeArtifact();
    }

    private void rebuild(String indexId) {
        IndexMetadataStore store = deps.metadataStore();
        IndexDescriptor current = store.findIndex(indexId)
                .filter(descriptor -> descriptor.state() != IndexState.DROPPED)
                .filter(StuckBuildRecovery::isScalar)
                .orElse(null);
        if (current == null) {
            return;
        }
        SnapshotAwareMetadata snapshotMetadata = IndexSourceAuthority.forDescriptor(
                store, deps.buildServices(), deps.connectorAccessorResolver(), current)
                .resolveSnapshotAwareMetadata(current.catalogName(), current.schemaName(), current.tableName());
        if (snapshotMetadata == null) {
            LOG.warn("event=stuck_build_rebuild_deferred indexId={} reason=snapshot_metadata_unavailable", indexId);
            return;
        }
        String snapshot = snapshotMetadata.currentSnapshotId().filter(value -> !value.isBlank()).orElse(null);
        if (snapshot == null) {
            LOG.warn("event=stuck_build_rebuild_deferred indexId={} reason=no_current_snapshot", indexId);
            return;
        }
        List<SnapshotDataFile> files = snapshotMetadata.listFilesAtSnapshot(snapshot);
        IndexGenerationManifest active = store.findActiveIndexGeneration(indexId).orElse(null);
        Set<String> indexed = active == null ? Set.of()
                : store.getIndexedDataFilePaths(indexId, active.generation());
        if (active != null && active.generation() != current.generation()) {
            throw new IllegalStateException("Active generation differs from recovery base: " + indexId);
        }
        List<DataFileInfo> added = files.stream()
                .map(file -> new DataFileInfo(file.path(), file.rowCount(), file.sizeBytes(), file.format()))
                .toList();
        boolean started = generationRefresher.refresh(current,
                new IndexRefreshInput(snapshot, added, List.copyOf(indexed), true));
        if (!started) {
            LOG.info("event=stuck_build_rebuild_deferred indexId={} generation={} reason=concurrent_transition",
                    indexId, current.generation());
        }
    }
}
