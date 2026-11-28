package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.coordinator.indexing.ddl.IndexSourceAuthority;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.config.ConsistencyConfig;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.exception.SnapshotExpiredException;
import org.pangrova.connector.api.index.SnapshotFileDelta;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled consistency checker for index coverage.
 *
 * <p>Computes coverage ratio per READY index (indexed files / total data files).
 * When coverage drops below the configured threshold and auto-rebuild is enabled,
 * submits incremental build tasks for uncovered files only.</p>
 *
 * <p>Implements {@link AutoCloseable} to cancel the scheduled task on shutdown.</p>
 */
public final class IndexConsistencyChecker implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexConsistencyChecker.class);

    private final IndexConsistencyContext deps;
    private final IndexLifecycleMetrics lifecycleMetrics;
    private final IndexGenerationRefresher generationRefresher;
    private final IndexGenerationCoverageReader coverageReader;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    /** Creates the leader-gated index consistency checker. */
    public IndexConsistencyChecker(IndexConsistencyContext deps,
                                   IndexLifecycleMetrics lifecycleMetrics) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
        this.coverageReader = new IndexGenerationCoverageReader(deps.metadataStore());
        this.generationRefresher = new IndexGenerationRefresher(
                new IndexCommitContext(
                        deps.metadataStore(), deps.buildServices(), deps.accessorResolver()),
                lifecycleMetrics);
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("index-consistency");
    }

    /**
     * Starts the periodic consistency check at the configured interval.
     */
    public void start() {
        ConsistencyConfig config = deps.config().lifecycle().consistency();
        int intervalMinutes = config.checkIntervalMinutes();
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::runSafe, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
        LOG.info("action=consistency_checker_started intervalMinutes={} threshold={} autoRebuild={}",
                intervalMinutes, config.rebuildThreshold(), config.autoRebuildEnabled());
    }

    private void runSafe() {
        try {
            checkAll();
        } catch (Exception e) {
            LOG.error("action=consistency_check_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Runs a single consistency check cycle across all READY and STALE indexes.
     *
     * <p>STALE indexes (parked by {@link StuckBuildRecovery} or data changes) are
     * automatically rebuilt back to READY when auto-rebuild is enabled. This
     * prevents STALE from becoming a terminal state.</p>
     */
    @org.pangrova.common.VisibleForTesting
    void checkAll() {
        MaintenanceLeaderElection leaderElection = deps.leaderElection();
        if (!leaderElection.tryAcquireOrRenew()) {
            LOG.debug("action=consistency_check_skipped reason=not_leader");
            return;
        }

        IndexMetadataStore store = deps.metadataStore();
        ConsistencyConfig config = deps.config().lifecycle().consistency();

        // Phase 1: Check READY indexes for coverage degradation.
        paginateAndCheck(store, IndexState.READY, config);

        // Phase 2: Rebuild STALE indexes back to READY (if auto-rebuild enabled).
        if (config.autoRebuildEnabled()) {
            paginateAndCheck(store, IndexState.STALE, config);
        }
    }

    private void paginateAndCheck(IndexMetadataStore store, IndexState state,
                                  ConsistencyConfig config) {
        // Persist progress only after processing; a crash replays at most one page.
        while (deps.leaderElection().tryAcquireOrRenew()) {
            var page = store.loadMaintenancePage(state, null, 100);
            for (IndexDescriptor descriptor : page.candidates()) {
                reconcileIndex(descriptor, config, state == IndexState.STALE);
            }
            if (!store.completeMaintenancePage(page) || page.exhausted()) return;
        }
    }

    private void reconcileIndex(
            IndexDescriptor descriptor, ConsistencyConfig config, boolean fullReplacement) {
        try {
            SnapshotAwareMetadata snapshotMetadata = IndexSourceAuthority.forDescriptor(
                    deps.metadataStore(), deps.buildServices(), deps.accessorResolver(), descriptor)
                    .resolveSnapshotAwareMetadata(
                            descriptor.catalogName(), descriptor.schemaName(),
                            descriptor.tableName());
            if (snapshotMetadata == null) {
                verifyWithoutSnapshotIdentity(descriptor);
                return;
            }
            reconcileSnapshot(descriptor, config, fullReplacement, snapshotMetadata);
        } catch (Exception e) {
            LOG.error("action=consistency_reconcile_failed indexId={} error={}",
                    descriptor.indexId(), e.getMessage(), e);
        }
    }

    private void reconcileSnapshot(
            IndexDescriptor descriptor,
            ConsistencyConfig config,
            boolean fullReplacement,
            SnapshotAwareMetadata snapshotMetadata) {
        String currentSnapshot = snapshotMetadata.currentSnapshotId().orElse(null);
        if (currentSnapshot == null || currentSnapshot.isBlank()) {
            LOG.debug("action=consistency_skip indexId={} reason=no_current_snapshot",
                    descriptor.indexId());
            return;
        }
        IndexGenerationManifest active = deps.metadataStore()
                .findActiveIndexGeneration(descriptor.indexId()).orElse(null);
        if (active == null && descriptor.state() == IndexState.READY) {
            markReadyIndexStale(descriptor, "missing_active_generation_manifest");
            return;
        }
        SnapshotCoverage coverage = loadSnapshotCoverage(
                descriptor, snapshotMetadata, currentSnapshot, active);
        recordCoverage(descriptor, config, currentSnapshot, coverage);
        boolean currentManifest = active != null
                && active.sourceSnapshot().equals(currentSnapshot);
        if (!fullReplacement && coverage.isExact() && currentManifest) {
            return;
        }
        refreshSnapshot(descriptor, config, fullReplacement, snapshotMetadata,
                currentSnapshot, active, coverage);
    }

    private SnapshotCoverage loadSnapshotCoverage(
            IndexDescriptor descriptor,
            SnapshotAwareMetadata snapshotMetadata,
            String currentSnapshot,
            IndexGenerationManifest active) {
        List<SnapshotDataFile> snapshotFiles =
                snapshotMetadata.listFilesAtSnapshot(currentSnapshot);
        // A drift-retired STALE Native generation has no reusable active base.
        // Its recovery is a full replacement, not an assertion of exact coverage.
        boolean retiredForDrift = active == null && descriptor.state() == IndexState.STALE
                && descriptor.indexType().requiresNativeArtifact()
                && "native_source_snapshot_changed".equals(descriptor.staleReason());
        Set<String> indexedFiles = retiredForDrift ? Set.of() : coverageReader.read(descriptor, active);
        Set<String> currentFiles = snapshotFiles.stream()
                .map(SnapshotDataFile::path)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        return new SnapshotCoverage(snapshotFiles, currentFiles, indexedFiles);
    }

    private void recordCoverage(
            IndexDescriptor descriptor,
            ConsistencyConfig config,
            String currentSnapshot,
            SnapshotCoverage coverage) {
        double ratio = coverage.ratio();
        LOG.info("action=consistency_check indexId={} snapshot={} coverage={} total={} covered={}",
                descriptor.indexId(), currentSnapshot, String.format("%.3f", ratio),
                coverage.currentFiles().size(),
                countCovered(coverage.currentFiles(), coverage.indexedFiles()));
        if (ratio < config.rebuildThreshold() && lifecycleMetrics != null) {
            lifecycleMetrics.recordLowCoverage(descriptor.indexId(), ratio);
        }
    }

    private void refreshSnapshot(
            IndexDescriptor descriptor,
            ConsistencyConfig config,
            boolean fullReplacement,
            SnapshotAwareMetadata snapshotMetadata,
            String currentSnapshot,
            IndexGenerationManifest active,
            SnapshotCoverage coverage) {
        if (!config.autoRebuildEnabled()) {
            if (active != null && !active.sourceSnapshot().equals(currentSnapshot)
                    && descriptor.indexType().requiresNativeArtifact()
                    && deps.metadataStore().retireNativeGenerationForSourceDrift(
                            descriptor, active, currentSnapshot, Instant.now())) {
                LOG.info("action=consistency_native_generation_retired indexId={} generation={}",
                        descriptor.indexId(), active.generation());
                return;
            }
            LOG.warn("action=consistency_refresh_required indexId={} "
                            + "snapshotCurrent={} snapshotIndexed={} autoRebuild=disabled",
                    descriptor.indexId(), currentSnapshot,
                    active == null ? "none" : active.sourceSnapshot());
            return;
        }
        SnapshotDelta delta = fullReplacement || active == null
                || active.sourceSnapshot().isBlank()
                ? SnapshotDelta.full(coverage.snapshotFiles(), coverage.indexedFiles())
                : resolveDelta(snapshotMetadata, active, currentSnapshot,
                        coverage.snapshotFiles(), coverage.currentFiles(), coverage.indexedFiles());
        boolean currentManifest = active != null
                && active.sourceSnapshot().equals(currentSnapshot);
        boolean refreshed = generationRefresher.refresh(
                descriptor, new IndexRefreshInput(
                        currentSnapshot,
                        delta.added().stream()
                                .map(IndexConsistencyChecker::toDataFileInfo).toList(),
                        delta.removed(), fullReplacement || currentManifest));
        if (!refreshed) {
            LOG.info("action=consistency_refresh_deferred indexId={} snapshot={}",
                    descriptor.indexId(), currentSnapshot);
            return;
        }
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordConsistencyRebuild();
        }
    }

    private SnapshotDelta resolveDelta(
            SnapshotAwareMetadata snapshotMetadata,
            IndexGenerationManifest active,
            String currentSnapshot,
            List<SnapshotDataFile> currentSnapshotFiles,
            Set<String> currentFiles,
            Set<String> indexedFiles) {
        try {
            SnapshotRelation relation = snapshotMetadata.relation(
                    active.sourceSnapshot(), currentSnapshot);
            if (relation != SnapshotRelation.LEFT_ANCESTOR
                    && relation != SnapshotRelation.SAME) {
                LOG.warn("action=snapshot_lineage_changed indexId={} relation={} "
                                + "action=full_replacement",
                        active.indexId(), relation);
                return SnapshotDelta.full(currentSnapshotFiles, indexedFiles);
            }
            SnapshotFileDelta connectorDelta = snapshotMetadata.diffFiles(
                    active.sourceSnapshot(), currentSnapshot);
            Set<String> addedPaths = connectorDelta.added().stream()
                    .map(SnapshotDataFile::path)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            Set<String> removed = Set.copyOf(connectorDelta.removed());
            Set<String> predicted = new HashSet<>(indexedFiles);
            predicted.removeAll(removed);
            predicted.addAll(addedPaths);
            if (predicted.equals(currentFiles)) {
                return new SnapshotDelta(
                        connectorDelta.added(), connectorDelta.removed());
            }
            LOG.warn("action=snapshot_diff_validation_failed indexId={} "
                            + "predictedFiles={} currentFiles={} action=full_replacement",
                    active.indexId(), predicted.size(), currentFiles.size());
        } catch (SnapshotExpiredException expired) {
            LOG.warn("action=snapshot_diff_expired indexId={} snapshot={} "
                            + "action=full_replacement",
                    active.indexId(), expired.snapshotId());
        }
        return SnapshotDelta.full(currentSnapshotFiles, indexedFiles);
    }

    private void verifyWithoutSnapshotIdentity(IndexDescriptor descriptor) {
        IndexMetadataStore store = deps.metadataStore();
        IndexGenerationManifest active = store.findActiveIndexGeneration(
                descriptor.indexId()).orElse(null);
        if (active == null && descriptor.state() == IndexState.READY
                && descriptor.indexType().requiresNativeArtifact()) {
            markReadyIndexStale(descriptor, "missing_active_generation_manifest");
            return;
        }
        // A drift-retired STALE Native generation has no reusable active base.
        // Its recovery is a full replacement, not an assertion of exact coverage.
        boolean retiredForDrift = active == null && descriptor.state() == IndexState.STALE
                && descriptor.indexType().requiresNativeArtifact()
                && "native_source_snapshot_changed".equals(descriptor.staleReason());
        Set<String> indexedFiles = retiredForDrift ? Set.of() : coverageReader.read(descriptor, active);
        List<String> currentDataFiles = resolveDataFiles(
                descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        if (!indexedFiles.equals(Set.copyOf(currentDataFiles))
                && descriptor.state() == IndexState.READY) {
            markReadyIndexStale(descriptor, "snapshot_identity_unavailable_with_file_drift");
        }
    }

    private void markReadyIndexStale(IndexDescriptor descriptor, String reason) {
        boolean updated = deps.metadataStore().updateIndexState(
                descriptor.indexId(), IndexState.STALE.name(), java.time.Instant.now(),
                reason, descriptor.updatedAt(), descriptor.state().name(),
                descriptor.generation());
        LOG.warn("action=index_marked_stale indexId={} reason={} updated={}",
                descriptor.indexId(), reason, updated);
    }

    private static double computeCoverage(
            Set<String> dataFiles, Set<String> indexedFiles) {
        if (dataFiles.isEmpty()) {
            return indexedFiles.isEmpty() ? 1.0 : 0.0;
        }
        return (double) countCovered(dataFiles, indexedFiles) / dataFiles.size();
    }

    private static int countCovered(
            Set<String> dataFiles, Set<String> indexedFiles) {
        return Math.toIntExact(dataFiles.stream().filter(indexedFiles::contains).count());
    }

    private static DataFileInfo toDataFileInfo(SnapshotDataFile file) {
        return new DataFileInfo(
                file.path(), file.rowCount(), file.sizeBytes(), file.format());
    }

    private record SnapshotDelta(
            List<SnapshotDataFile> added, List<String> removed) {
        private SnapshotDelta {
            added = List.copyOf(added);
            removed = List.copyOf(removed);
        }

        private static SnapshotDelta full(
                List<SnapshotDataFile> currentFiles, Set<String> indexedFiles) {
            return new SnapshotDelta(
                    currentFiles, List.copyOf(indexedFiles));
        }
    }

    private record SnapshotCoverage(
            List<SnapshotDataFile> snapshotFiles,
            Set<String> currentFiles,
            Set<String> indexedFiles) {
        private double ratio() {
            return computeCoverage(currentFiles, indexedFiles);
        }

        private boolean isExact() {
            return indexedFiles.equals(currentFiles);
        }
    }

    private List<String> resolveDataFiles(String catalog, String schema, String table) {
        return deps.accessorResolver().listDataFiles(catalog, schema, table);
    }

    @Override
    public void close() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
        scheduler.shutdownNow();
        LOG.info("action=consistency_checker_shutdown");
    }
}
