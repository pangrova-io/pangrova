package org.pangrova.coordinator.adapter.indexing.lifecycle.gc;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.adapter.indexing.lifecycle.IndexLifecycleMetrics;
import org.pangrova.coordinator.adapter.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.connector.spi.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled garbage collector for index files and metadata.
 *
 * <p>Phase 1 — Orphan cleanup: for each READY index, compares indexed
 * files against current data files to find and remove orphans.
 * Phase 2 — DROPPED purge: permanently deletes indexes past the
 * grace period.</p>
 *
 * <p>Implements {@link AutoCloseable} to cancel the scheduled task
 * on shutdown.</p>
 */
public final class IndexGarbageCollector implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexGarbageCollector.class);

    private final IndexGcContext deps;
    private final IndexLifecycleMetrics lifecycleMetrics;
    private final MaintenanceLeaderElection leaderElection;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    /** Creates a collector without a leader election gate. */
    public IndexGarbageCollector(IndexGcContext deps, IndexLifecycleMetrics lifecycleMetrics) {
        this(deps, lifecycleMetrics, null);
    }

    /** Creates a collector that runs only while holding the maintenance lease. */
    public IndexGarbageCollector(IndexGcContext deps, IndexLifecycleMetrics lifecycleMetrics,
                                 MaintenanceLeaderElection leaderElection) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
        this.leaderElection = leaderElection;
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("index-gc");
    }

    /**
     * Starts the periodic GC task at the configured interval.
     */
    public void start() {
        int intervalMinutes = deps.config().gcIntervalMinutes();
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::runSafe, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
        LOG.info("action=gc_started intervalMinutes={}", intervalMinutes);
    }

    /**
     * Cancels the periodic GC task. Called when this node loses maintenance leadership.
     * The scheduler thread pool remains alive for re-election.
     * Idempotent — safe to call multiple times.
     */
    public void stopSchedule() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
            scheduledTask = null;
        }
        LOG.info("action=gc_schedule_stopped");
    }

    /**
     * Re-starts the periodic GC task. Called when this node becomes maintenance leader.
     * Idempotent — cancels any existing schedule before starting a new one.
     */
    public void startSchedule() {
        stopSchedule();
        int intervalMinutes = deps.config().gcIntervalMinutes();
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::runSafe, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
        LOG.info("action=gc_schedule_started intervalMinutes={}", intervalMinutes);
    }

    private void runSafe() {
        try {
            GcResult result = runGc();
            if (lifecycleMetrics != null) {
                lifecycleMetrics.recordGcCycle(result.orphanedCount(), result.purgedCount());
            }
            LOG.info("action=gc_completed orphaned={} purged={}",
                    result.orphanedCount(), result.purgedCount());
        } catch (Exception e) {
            LOG.error("action=gc_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Executes a single GC cycle.
     *
     * @return the GC result with counts
     */
    @org.pangrova.common.VisibleForTesting
    GcResult runGc() {
        // Leader gate: only the maintenance leader runs GC (Req 4.AC2)
        if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
            LOG.debug("action=gc_skipped reason=not_leader");
            return new GcResult(0, 0);
        }
        int orphanedCount = cleanOrphans();
        int purgedCount = purgeDropped();
        return new GcResult(orphanedCount, purgedCount);
    }

    private int cleanOrphans() {
        IndexMetadataStore store = deps.metadataStore();
        Instant createdBefore = Instant.ofEpochMilli(
                System.currentTimeMillis()
                        - TimeUnit.MINUTES.toMillis(deps.config().gcGracePeriodMinutes()));
        List<IndexFileMapping> staleMappings = store.listStaleFileMappings(
                TimeUnit.MINUTES.toMillis(deps.config().gcGracePeriodMinutes()),
                createdBefore);
        Map<String, IndexDescriptor> descriptorMap = fetchDescriptors(store, staleMappings);
        return processOrphanMappings(store, staleMappings, descriptorMap);
    }

    private static Map<String, IndexDescriptor> fetchDescriptors(IndexMetadataStore store,
                                                                  List<IndexFileMapping> mappings) {
        List<String> indexIds = mappings.stream()
                .map(IndexFileMapping::indexId)
                .distinct()
                .toList();
        return indexIds.isEmpty() ? Map.of() : store.findIndexesByIds(indexIds);
    }

    private int processOrphanMappings(IndexMetadataStore store, List<IndexFileMapping> staleMappings,
                                      Map<String, IndexDescriptor> descriptorMap) {
        int totalOrphaned = 0;
        for (IndexFileMapping mapping : staleMappings) {
            IndexDescriptor descriptor = descriptorMap.get(mapping.indexId());
            if (shouldSkipMapping(mapping, descriptor)) {
                continue;
            }
            if (tryRemoveOrphan(store, mapping, descriptor)) {
                totalOrphaned++;
            }
        }
        return totalOrphaned;
    }

    private boolean tryRemoveOrphan(IndexMetadataStore store, IndexFileMapping mapping,
                                    IndexDescriptor descriptor) {
        IndexState observedState = descriptor.state();
        int observedGeneration = descriptor.generation();
        // Older v1 layouts can refer to one physical path from several generations.
        // Never delete a file while another mapping still owns that path.
        boolean sharedFile = store.getIndexFileMappings(mapping.indexId()).stream()
                .anyMatch(other -> other.generation() != mapping.generation()
                        && other.indexFilePath().equals(mapping.indexFilePath()));
        if (!sharedFile && !deleteIndexFile(mapping.indexFilePath(), descriptor)) {
            return false;
        }
        boolean removed = store.removeIndexFileMappingIfState(
                mapping, observedState, observedGeneration);
        if (removed) {
            return true;
        }
        LOG.info("action=gc_skip_state_changed indexId={} file={} observedState={}",
                mapping.indexId(), mapping.dataFilePath(), observedState);
        return false;
    }

    private boolean shouldSkipMapping(IndexFileMapping mapping, IndexDescriptor descriptor) {
        if (descriptor == null) {
            return true;
        }
        if (descriptor.state() == IndexState.BUILDING) {
            LOG.warn("action=gc_skip_building indexId={} file={}", mapping.indexId(), mapping.dataFilePath());
            return true;
        }
        if (mapping.generation() >= descriptor.generation() - 1) {
            LOG.debug("action=gc_skip_generation_fence indexId={} file={} mappingGen={} currentGen={}",
                    mapping.indexId(), mapping.dataFilePath(), mapping.generation(), descriptor.generation());
            return true;
        }
        return false;
    }

    private int purgeDropped() {
        IndexMetadataStore store = deps.metadataStore();
        long gracePeriodMs = TimeUnit.MINUTES.toMillis(deps.config().gcGracePeriodMinutes());
        long cutoffMs = System.currentTimeMillis() - gracePeriodMs;
        Instant cutoff = Instant.ofEpochMilli(cutoffMs);
        int purgedCount = 0;

        // Find all DROPPED indexes by scanning stale mappings won't work —
        // we need to find DROPPED descriptors directly.
        // Use a dedicated scan: list all indexes, filter DROPPED past grace period.
        List<IndexDescriptor> allDropped = findDroppedIndexesPastGrace(cutoff);

        for (IndexDescriptor descriptor : allDropped) {
            try {
                boolean[] purged = {false};
                store.withIndexLifecycleLock(descriptor.indexId(), () -> {
                    IndexDescriptor current = store.findIndex(descriptor.indexId()).orElse(null);
                    if (current != null && current.state() == IndexState.DROPPED
                            && current.updatedAt() != null && current.updatedAt().isBefore(cutoff)) {
                        purgeIndex(current);
                        purged[0] = true;
                    }
                });
                if (purged[0]) {
                    purgedCount++;
                }
            } catch (RuntimeException failure) {
                LOG.error("action=index_purge_deferred indexId={} error={}",
                        descriptor.indexId(), failure.getMessage(), failure);
            }
        }

        return purgedCount;
    }

    private List<IndexDescriptor> findDroppedIndexesPastGrace(Instant cutoff) {
        IndexMetadataStore store = deps.metadataStore();
        return store.findIndexesByState(IndexState.DROPPED).stream()
                .filter(desc -> desc.updatedAt() != null && desc.updatedAt().isBefore(cutoff))
                .toList();
    }

    private void purgeIndex(IndexDescriptor descriptor) {
        IndexMetadataStore store = deps.metadataStore();
        List<IndexFileMapping> mappings = store.getIndexFileMappings(descriptor.indexId());

        // Keep the durable descriptor for a later retry if the snapshot ref cannot be released.
        unpinSnapshot(descriptor);

        for (IndexFileMapping mapping : mappings) {
            if (!deleteIndexFile(mapping.indexFilePath(), descriptor)) {
                throw new IllegalStateException("Index file deletion failed; retaining dropped index "
                        + descriptor.indexId());
            }
        }

        store.purgeIndex(descriptor.indexId());
        LOG.info("action=index_purged indexId={} indexName={} fileCount={}",
                descriptor.indexId(), descriptor.indexName(), mappings.size());
    }

    /**
     * Unpins the snapshot previously pinned by this index, allowing compaction
     * to reclaim it if no other pins remain.
     */
    private void unpinSnapshot(IndexDescriptor descriptor) {
        SnapshotPinProvider pinProvider = deps.snapshotPinProvider();
        if (pinProvider == null) {
            if (descriptor.buildSnapshotId() != null && !descriptor.buildSnapshotId().isEmpty()
                    || descriptor.lastIndexedSnapshotId() != null
                    && !descriptor.lastIndexedSnapshotId().isEmpty()) {
                throw new IllegalStateException("Snapshot pin provider is required to purge index "
                        + descriptor.indexId());
            }
            return;
        }
        List<Long> snapshots = pinProvider.getPinnedSnapshotsForIndex(
                descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), descriptor.indexId());
        for (long snapshotId : snapshots) {
            pinProvider.unpinSnapshot(
                    descriptor.catalogName(), descriptor.schemaName(),
                    descriptor.tableName(), snapshotId, descriptor.indexId());
            LOG.info("action=snapshot_unpinned indexId={} snapshotId={}",
                    descriptor.indexId(), snapshotId);
        }
    }

    private boolean deleteIndexFile(String indexFilePath, IndexDescriptor descriptor) {
        try {
            FileSystemAccessor fsAccessor = deps.fileAccessor().apply(descriptor);
            if (fsAccessor.exists(indexFilePath)) {
                fsAccessor.deleteFile(indexFilePath);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("action=gc_delete_failed indexId={} file={} error={}",
                    descriptor.indexId(), indexFilePath, e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
        scheduler.shutdownNow();
        LOG.info("action=gc_shutdown");
    }
}
