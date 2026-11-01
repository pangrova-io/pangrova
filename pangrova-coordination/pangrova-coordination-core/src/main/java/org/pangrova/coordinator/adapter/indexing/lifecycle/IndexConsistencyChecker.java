package org.pangrova.coordinator.adapter.indexing.lifecycle;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.adapter.indexing.ddl.IndexSourceAuthority;
import org.pangrova.indexing.core.build.DataFileInfo;
import org.pangrova.indexing.core.config.ConsistencyConfig;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.indexing.store.IndexGenerationManifest;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotDataFile;
import org.pangrova.spi.connector.index.SnapshotExpiredException;
import org.pangrova.spi.connector.index.SnapshotFileDelta;
import org.pangrova.spi.connector.index.SnapshotRelation;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    /** Creates the leader-gated index consistency checker. */
    public IndexConsistencyChecker(IndexConsistencyContext deps,
                                   IndexLifecycleMetrics lifecycleMetrics) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
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
            paginateAndRebuildStale(store, config);
        }
    }

    private void paginateAndCheck(IndexMetadataStore store, IndexState state,
                                  ConsistencyConfig config) {
        int pageSize = 100;
        int offset = 0;
        Set<String> seen = new HashSet<>();
        while (true) {
            List<IndexDescriptor> page = store.findIndexesByStatePaginated(
                    state, offset, pageSize);
            if (page.isEmpty()) {
                break;
            }
            List<IndexDescriptor> unseen = page.stream()
                    .filter(descriptor -> seen.add(descriptor.indexId()))
                    .toList();
            for (IndexDescriptor descriptor : unseen) {
                checkSingleIndex(descriptor, config);
            }
            if (unseen.isEmpty()) {
                offset += page.size();
            } else {
                offset = 0;
            }
        }
    }

    /** Reconciles STALE indexes through the same immutable publication path. */
    private void paginateAndRebuildStale(IndexMetadataStore store, ConsistencyConfig config) {
        int pageSize = 100;
        int offset = 0;
        Set<String> seen = new HashSet<>();
        while (true) {
            List<IndexDescriptor> page = store.findIndexesByStatePaginated(
                    IndexState.STALE, offset, pageSize);
            if (page.isEmpty()) {
                break;
            }
            List<IndexDescriptor> unseen = page.stream()
                    .filter(descriptor -> seen.add(descriptor.indexId()))
                    .toList();
            for (IndexDescriptor descriptor : unseen) {
                reconcileIndex(descriptor, config, true);
            }
            if (unseen.isEmpty()) {
                offset += page.size();
            } else {
                offset = 0;
            }
        }
    }

    private void checkSingleIndex(IndexDescriptor descriptor, ConsistencyConfig config) {
        reconcileIndex(descriptor, config, false);
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
        int activeGeneration = active == null
                ? descriptor.generation() : active.generation();
        Set<String> indexedFiles = collectIndexedFiles(
                deps.metadataStore(), descriptor.indexId(), activeGeneration);
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
        int generation = active == null ? descriptor.generation() : active.generation();
        Set<String> indexedFiles = collectIndexedFiles(
                store, descriptor.indexId(), generation);
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

    private static Set<String> collectIndexedFiles(
            IndexMetadataStore store, String indexId, int generation) {
        Set<String> indexedFiles = new HashSet<>();
        for (IndexFileMapping mapping : store.getIndexFileMappings(indexId)) {
            if (mapping.generation() == generation) {
                indexedFiles.add(mapping.dataFilePath());
            }
        }
        return Set.copyOf(indexedFiles);
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
