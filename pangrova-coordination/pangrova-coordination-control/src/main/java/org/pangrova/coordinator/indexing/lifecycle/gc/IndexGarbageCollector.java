package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.indexing.lifecycle.IndexLifecycleMetrics;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexCleanup.MappedArtifactRetirement;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
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
    private static final int SCALAR_ORPHAN_BATCH_SIZE = 256;
    private static final int MAPPED_RETIREMENT_BATCH_SIZE = 256;

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
        int orphanedCount = cleanScalarArtifacts() + cleanOrphans();
        cleanMappedArtifacts();
        int purgedCount = purgeDropped();
        return new GcResult(orphanedCount, purgedCount);
    }

    /** Reclaims only exact, drained pre-mapping scalar writes, never expired writer heartbeats alone. */
    private int cleanScalarArtifacts() {
        IndexMetadataStore store = deps.metadataStore();
        Instant cutoff = Instant.ofEpochMilli(System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(deps.config().gcGracePeriodMinutes()));
        int removed = 0;
        for (var intent : store.pollScalarArtifactReclaimCandidates(cutoff, SCALAR_ORPHAN_BATCH_SIZE)) {
            try {
                boolean[] reclaimed = {false};
                store.withIndexLifecycleLock(intent.indexId(), () -> {
                    reclaimed[0] = reclaimScalarArtifact(store, intent);
                });
                if (reclaimed[0]) removed++;
            } catch (RuntimeException failure) {
                LOG.warn("action=scalar_orphan_reclaim_deferred indexId={}", intent.indexId(), failure);
            }
        }
        return removed;
    }

    private boolean reclaimScalarArtifact(IndexMetadataStore store, ScalarArtifactIntent intent) {
        var descriptor = store.findIndex(intent.indexId()).orElse(null);
        if (descriptor == null || !isCanonicalScalarArtifact(descriptor, intent)
                || !store.beginScalarArtifactReclaim(intent)) {
            return false;
        }
        if (!deleteIndexFile(intent.artifactPath(), descriptor)) {
            return false; // Durable DELETE_PENDING is retried, including an uncertain deletion.
        }
        store.completeScalarArtifactReclaim(intent);
        return true;
    }

    private boolean isCanonicalScalarArtifact(IndexDescriptor descriptor, ScalarArtifactIntent intent) {
        String expected = new IndexStorageLayout(deps.config().baseIndexPath()).indexFilePath(
                descriptor.withGeneration(intent.generation(), descriptor.updatedAt()),
                java.util.UUID.fromString(intent.attemptOwner()), intent.dataFilePath());
        if (!expected.equals(intent.artifactPath())) {
            LOG.warn("action=scalar_orphan_path_mismatch indexId={}", descriptor.indexId());
            return false;
        }
        return true;
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
        var candidatesByIndex = staleMappings.stream()
                .filter(mapping -> !shouldSkipMapping(mapping, descriptorMap.get(mapping.indexId())))
                .collect(Collectors.groupingBy(IndexFileMapping::indexId));
        int removed = 0;
        for (var entry : candidatesByIndex.entrySet()) {
            var descriptor = descriptorMap.get(entry.getKey());
            var result = store.removeOrphanMappings(descriptor.indexId(), descriptor.state(),
                    descriptor.generation(), entry.getValue());
            // The transaction durably schedules unreferenced paths; never delete its ephemeral return value.
            removed += result.mappingsRemoved();
        }
        return removed;
    }

    private void cleanMappedArtifacts() {
        IndexMetadataStore store = deps.metadataStore();
        for (var retirement : store.pollMappedArtifactRetirements(MAPPED_RETIREMENT_BATCH_SIZE)) {
            try {
                store.withIndexLifecycleLock(retirement.indexId(), () -> reclaimMappedArtifact(store, retirement));
            } catch (RuntimeException failure) {
                LOG.warn("action=mapped_artifact_reclaim_deferred retirementId={}", retirement.retirementId(), failure);
            }
        }
    }

    private void reclaimMappedArtifact(IndexMetadataStore store, MappedArtifactRetirement retirement) {
        IndexDescriptor descriptor = store.findIndex(retirement.indexId()).orElse(null);
        if (descriptor == null || !new IndexStorageLayout(deps.config().baseIndexPath()).isRetiredIndexFilePath(
                descriptor, retirement.dataFilePath(), retirement.retiredGeneration(), retirement.artifactPath())) {
            LOG.warn("action=mapped_artifact_path_unproven retirementId={}", retirement.retirementId());
            return;
        }
        if (store.beginMappedArtifactReclaim(retirement)
                && deleteIndexFile(retirement.artifactPath(), descriptor)) {
            store.completeMappedArtifactReclaim(retirement);
        }
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

        // The database filters the grace cutoff and persists progress between bounded pages.
        while (leaderElection == null || leaderElection.tryAcquireOrRenew()) {
            var page = store.loadMaintenancePage(IndexState.DROPPED, cutoff, 100);
            for (IndexDescriptor descriptor : page.candidates()) {
                if (purgeDroppedIndex(store, descriptor, cutoff)) purgedCount++;
            }
            if (!store.completeMaintenancePage(page) || page.exhausted()) break;
        }
        return purgedCount;
    }

    private boolean purgeDroppedIndex(IndexMetadataStore store, IndexDescriptor descriptor, Instant cutoff) {
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
            return purged[0];
        } catch (RuntimeException failure) {
            LOG.error("action=index_purge_deferred indexId={} error={}",
                    descriptor.indexId(), failure.getMessage(), failure);
            return false;
        }
    }

    private void purgeIndex(IndexDescriptor descriptor) {
        IndexMetadataStore store = deps.metadataStore();
        if (store.hasScalarArtifactIntents(descriptor.indexId())) {
            throw new IllegalStateException("Scalar artifact intents remain; retaining dropped index "
                    + descriptor.indexId());
        }
        if (store.hasPendingMappedArtifactRetirements(descriptor.indexId())) {
            throw new IllegalStateException("Mapped artifact retirements remain; retaining dropped index "
                    + descriptor.indexId());
        }
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
        List<org.pangrova.connector.api.index.SnapshotPinReference> snapshots = pinProvider.getSnapshotPinsForIndex(
                descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), descriptor.indexId());
        for (var snapshot : snapshots) {
            pinProvider.unpinSnapshot(
                    descriptor.catalogName(), descriptor.schemaName(),
                    descriptor.tableName(), snapshot.snapshotToken(), snapshot.owner());
            LOG.info("action=snapshot_unpinned indexId={} snapshotId={} owner={}",
                    descriptor.indexId(), snapshot.snapshotToken(), snapshot.owner().id());
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
