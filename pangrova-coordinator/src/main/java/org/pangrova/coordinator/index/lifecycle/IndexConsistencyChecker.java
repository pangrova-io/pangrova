package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.index.BuildMappingPersister;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.config.ConsistencyConfig;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
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
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    public IndexConsistencyChecker(IndexConsistencyContext deps,
                                   IndexLifecycleMetrics lifecycleMetrics) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
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
        if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
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
        while (true) {
            List<IndexDescriptor> page = store.findIndexesByStatePaginated(
                    state, offset, pageSize);
            if (page.isEmpty()) {
                break;
            }
            for (IndexDescriptor descriptor : page) {
                checkSingleIndex(descriptor, config);
            }
            offset += page.size();
        }
    }

    /**
     * Scans STALE indexes and rebuilds them. Each STALE index is transitioned
     * to BUILDING, rebuilt, then advanced to READY on success.
     */
    private void paginateAndRebuildStale(IndexMetadataStore store, ConsistencyConfig config) {
        int pageSize = 100;
        int offset = 0;
        while (true) {
            List<IndexDescriptor> page = store.findIndexesByStatePaginated(
                    IndexState.STALE, offset, pageSize);
            if (page.isEmpty()) {
                break;
            }
            for (IndexDescriptor descriptor : page) {
                rebuildStaleIndex(store, descriptor);
            }
            offset += page.size();
        }
    }

    private void rebuildStaleIndex(IndexMetadataStore store, IndexDescriptor descriptor) {
        try {
            java.time.Instant now = java.time.Instant.now();
            // Transition STALE -> BUILDING
            store.updateIndexState(descriptor.indexId(), IndexState.BUILDING.name(), now);
            LOG.info("action=stale_rebuild_start indexId={}", descriptor.indexId());

            // Perform rebuild (resolve files and build index)
            List<String> currentDataFiles = resolveDataFiles(
                    descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
            if (!currentDataFiles.isEmpty()) {
                List<DataFileInfo> files = deps.accessorResolver().resolveDataFileInfos(
                        descriptor.catalogName(), descriptor.schemaName(),
                        descriptor.tableName(), currentDataFiles);
                if (!files.isEmpty()) {
                    FileSystemAccessor fsAccessor = deps.accessorResolver()
                            .resolve(descriptor.catalogName());
                    ColumnValueReader columnReader = deps.accessorResolver()
                            .resolveColumnValueReader(descriptor.catalogName());
                    deps.buildCoordinator().submitBuildTask(
                            descriptor, files, fsAccessor, columnReader,
                            batch -> BuildMappingPersister.persist(
                                    store, batch, descriptor.generation()));
                }
            }

            // Transition BUILDING -> READY
            store.updateIndexState(descriptor.indexId(), IndexState.READY.name(), now);
            LOG.info("action=stale_rebuild_complete indexId={}", descriptor.indexId());
            if (lifecycleMetrics != null) {
                lifecycleMetrics.recordConsistencyRebuild();
            }
        } catch (Exception e) {
            LOG.error("action=stale_rebuild_failed indexId={} error={}",
                    descriptor.indexId(), e.getMessage(), e);
        }
    }

    private void checkSingleIndex(IndexDescriptor descriptor, ConsistencyConfig config) {
        IndexMetadataStore store = deps.metadataStore();
        Set<String> indexedFiles = collectIndexedFiles(store, descriptor);
        List<String> currentDataFiles = resolveDataFiles(
                descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        if (currentDataFiles.isEmpty()) {
            LOG.debug("action=consistency_skip indexId={} reason=no_data_files", descriptor.indexId());
            return;
        }
        double coverageRatio = computeCoverage(currentDataFiles, indexedFiles);
        LOG.info("action=consistency_check indexId={} coverage={} total={} covered={}",
                descriptor.indexId(), String.format("%.3f", coverageRatio),
                currentDataFiles.size(), countCovered(currentDataFiles, indexedFiles));
        handleLowCoverage(descriptor, config, coverageRatio, currentDataFiles, indexedFiles);
    }

    private static Set<String> collectIndexedFiles(IndexMetadataStore store, IndexDescriptor descriptor) {
        List<IndexFileMapping> mappings = store.getIndexFileMappings(descriptor.indexId());
        Set<String> indexedFiles = new HashSet<>(mappings.size());
        for (IndexFileMapping m : mappings) {
            indexedFiles.add(m.dataFilePath());
        }
        return indexedFiles;
    }

    private static double computeCoverage(List<String> dataFiles, Set<String> indexedFiles) {
        return (double) countCovered(dataFiles, indexedFiles) / dataFiles.size();
    }

    private static int countCovered(List<String> dataFiles, Set<String> indexedFiles) {
        int count = 0;
        for (String f : dataFiles) {
            if (indexedFiles.contains(f)) {
                count++;
            }
        }
        return count;
    }

    private void handleLowCoverage(IndexDescriptor descriptor, ConsistencyConfig config,
                                   double coverageRatio, List<String> currentDataFiles,
                                   Set<String> indexedFiles) {
        if (coverageRatio >= config.rebuildThreshold()) {
            return;
        }
        LOG.warn("event=index_low_coverage indexId={} coverage={} threshold={}",
                descriptor.indexId(), String.format("%.3f", coverageRatio),
                config.rebuildThreshold());
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordLowCoverage(descriptor.indexId(), coverageRatio);
        }
        if (!config.autoRebuildEnabled()) {
            LOG.warn("action=consistency_low_coverage indexId={} coverage={} threshold={} autoRebuild=disabled",
                    descriptor.indexId(), String.format("%.3f", coverageRatio),
                    config.rebuildThreshold());
            return;
        }
        submitIncrementalBuild(descriptor, currentDataFiles, indexedFiles);
    }

    private void submitIncrementalBuild(
            IndexDescriptor descriptor,
            List<String> currentDataFiles,
            Set<String> indexedFiles) {
        try {
            List<DataFileInfo> uncoveredFiles = resolveUncoveredFiles(descriptor, currentDataFiles, indexedFiles);
            if (uncoveredFiles.isEmpty()) {
                return;
            }
            executeIncrementalBuild(descriptor, uncoveredFiles);
        } catch (Exception e) {
            LOG.error("action=consistency_rebuild_failed indexId={} error={}",
                    descriptor.indexId(), e.getMessage(), e);
        }
    }

    private List<DataFileInfo> resolveUncoveredFiles(IndexDescriptor descriptor,
                                                     List<String> currentDataFiles,
                                                     Set<String> indexedFiles) {
        List<String> uncoveredPaths = currentDataFiles.stream()
                .filter(f -> !indexedFiles.contains(f))
                .toList();
        if (uncoveredPaths.isEmpty()) {
            return List.of();
        }
        return deps.accessorResolver().resolveDataFileInfos(
                descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), uncoveredPaths);
    }

    private void executeIncrementalBuild(IndexDescriptor descriptor, List<DataFileInfo> uncoveredFiles) {
        FileSystemAccessor fsAccessor = deps.accessorResolver()
                .resolve(descriptor.catalogName());
        ColumnValueReader columnReader = deps.accessorResolver()
                .resolveColumnValueReader(descriptor.catalogName());
        IndexMetadataStore store = deps.metadataStore();
        deps.buildCoordinator().submitBuildTask(
                descriptor, uncoveredFiles, fsAccessor, columnReader,
                batch -> BuildMappingPersister.persist(
                        store, batch, descriptor.generation()));
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordConsistencyRebuild();
        }
        LOG.info("action=consistency_rebuild_submitted indexId={} uncoveredFiles={}",
                descriptor.indexId(), uncoveredFiles.size());
    }

    private List<String> resolveDataFiles(String catalog, String schema, String table) {
        try {
            return deps.accessorResolver().listDataFiles(catalog, schema, table);
        } catch (Exception e) {
            LOG.warn("action=list_data_files_failed catalog={} schema={} table={} error={}",
                    catalog, schema, table, e.getMessage());
            return List.of();
        }
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
