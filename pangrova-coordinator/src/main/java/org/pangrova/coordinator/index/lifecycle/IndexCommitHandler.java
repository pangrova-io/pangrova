package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.coordinator.index.BuildMappingPersister;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventListener;
import org.pangrova.metadata.index.CommitEventRequest;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Receives commit events from connectors and triggers incremental index builds.
 *
 * <p>{@link #onCommit(CommitEvent)} enqueues events to a bounded queue and
 * returns immediately (< 1ms). A background thread drains events in batches,
 * merges by table, and submits build tasks for uncovered files.</p>
 *
 * <p>Implements {@link AutoCloseable} for graceful shutdown.</p>
 */
public final class IndexCommitHandler implements CommitListener, MetadataEventListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCommitHandler.class);

    private final IndexCommitContext deps;
    private final IndexLifecycleMetrics lifecycleMetrics;
    private final MaintenanceLeaderElection leaderElection;
    private final ArrayBlockingQueue<CommitEvent> eventQueue;
    private final int batchMax;
    private final ExecutorService processingExecutor;
    private final CommitLogPoller commitLogPoller;
    private volatile boolean running = true;

    public IndexCommitHandler(IndexCommitContext deps, IndexLifecycleMetrics lifecycleMetrics,
                              int queueCapacity, int batchMax) {
        this(deps, lifecycleMetrics, null, queueCapacity, batchMax);
    }

    public IndexCommitHandler(IndexCommitContext deps, IndexLifecycleMetrics lifecycleMetrics,
                              MaintenanceLeaderElection leaderElection,
                              int queueCapacity, int batchMax) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
        this.leaderElection = leaderElection;
        this.eventQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchMax = batchMax;
        this.processingExecutor = ManagedExecutorFactory.newSingleThreadExecutor("index-commit-handler");
        this.processingExecutor.submit(this::processLoop);
        this.commitLogPoller = new CommitLogPoller(deps, leaderElection, batchMax, this::processTableCommit);
    }

    @Override
    public void onCommit(CommitEvent event) {
        if (!running) {
            return;
        }
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordCommitEventReceived();
        }
        // F-COORD-FEAT-07: onCommit only persists to the shared commit log.
        // Processing is exclusively driven by the CommitLogPoller, eliminating
        // the double-processing path where both the local queue and the poller
        // would process the same event.
        persistToCommitLog(event);
    }

    private void persistToCommitLog(CommitEvent event) {
        try {
            deps.metadataStore().recordCommitEvent(new CommitEventRequest(
                    event.catalogName(), event.schemaName(), event.tableName(),
                    event.snapshotId(),
                    String.join(",", event.addedFiles()),
                    String.join(",", event.removedFiles()),
                    event.eventType().name(), Instant.now()));
        } catch (Exception e) {
            LOG.warn("action=commit_log_write_failed catalog={} schema={} table={} error={}",
                    event.catalogName(), event.schemaName(), event.tableName(), e.getMessage());
        }
    }

    private void processLoop() {
        while (running) {
            try {
                // Block until at least one event is available
                CommitEvent first = eventQueue.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                if (first == null) {
                    continue;
                }

                List<CommitEvent> batch = new ArrayList<>(batchMax);
                batch.add(first);
                eventQueue.drainTo(batch, batchMax - 1);

                // Leader guard: only the maintenance leader processes local queue events.
                // Non-leaders still receive onCommit() and persist to commit_log (dual-write),
                // so the leader will pick them up via pollCommitLog().
                if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
                    LOG.debug("action=local_queue_processing_skipped reason=not_leader events={}",
                            batch.size());
                    continue;
                }

                processBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (lifecycleMetrics != null) {
                    lifecycleMetrics.recordCommitProcessingFailure();
                }
                LOG.error("action=commit_processing_failed error={}", e.getMessage(), e);
            }
        }
    }

    private void processBatch(List<CommitEvent> batch) {
        Map<String, MergedCommitFiles> merged = mergeByTable(batch);

        for (MergedCommitFiles files : merged.values()) {
            try {
                processTableCommit(files);
            } catch (Exception e) {
                if (lifecycleMetrics != null) {
                    lifecycleMetrics.recordCommitProcessingFailure();
                }
                LOG.error("action=commit_table_processing_failed table={}.{}.{} error={}",
                        files.catalog, files.schema, files.table, e.getMessage(), e);
            }
        }
    }

    void processTableCommit(MergedCommitFiles files) {
        IndexMetadataStore store = deps.metadataStore();
        boolean buildSucceeded = switch (files.eventType) {
            case APPEND -> handleAppend(store, files);
            case OVERWRITE -> handleOverwrite(store, files);
            case COMPACT -> handleCompact(store, files);
            case DELETE -> {
                markIndexesStale(store, files);
                yield false; // DELETE invalidates indexes; watermark must not advance
            }
        };
        // F-INDEX-02: only advance the snapshot watermark when the build succeeded.
        // Advancing on failure would leave stale/incorrect index state.
        if (buildSucceeded && !files.snapshotId.isEmpty()) {
            updateSnapshotBinding(store, files);
        }
    }

    private void updateSnapshotBinding(IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> indexes = store.findIndexesByTable(
                files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        for (IndexDescriptor desc : indexes) {
            try {
                store.updateLastIndexedSnapshotId(desc.indexId(), files.snapshotId);
            } catch (Exception e) {
                LOG.warn("action=snapshot_binding_failed indexId={} snapshotId={} error={}",
                        desc.indexId(), files.snapshotId, e.getMessage());
            }
        }
    }

    private boolean handleAppend(IndexMetadataStore store, MergedCommitFiles files) {
        if (!files.addedFiles.isEmpty()) {
            collectFileStatistics(store, files);
            return triggerIncrementalBuilds(files);
        }
        return true;
    }

    private boolean handleOverwrite(IndexMetadataStore store, MergedCommitFiles files) {
        if (!files.removedFiles.isEmpty()) {
            invalidateRemovedFiles(store, files);
        }
        if (!files.addedFiles.isEmpty()) {
            collectFileStatistics(store, files);
            return triggerIncrementalBuilds(files);
        }
        return true;
    }

    private boolean handleCompact(IndexMetadataStore store, MergedCommitFiles files) {
        if (!files.removedFiles.isEmpty()) {
            markCompactionStaleEntries(store, files);
            invalidateRemovedFiles(store, files);
        }
        if (!files.addedFiles.isEmpty()) {
            collectFileStatistics(store, files);
            return triggerIncrementalBuilds(files);
        }
        return true;
    }

    /**
     * Extracts file-level column statistics from the connector and persists them
     * to the metadata store for L0 pruning (Req 2.AC3).
     */
    void collectFileStatistics(IndexMetadataStore store, MergedCommitFiles files) {
        try {
            List<org.pangrova.spi.index.statistics.DataFileStatistics> statistics =
                    deps.connectorAccessorResolver().resolveFileStatistics(
                            files.catalog, files.schema, files.table, files.addedFiles);
            if (statistics.isEmpty()) {
                LOG.debug("action=no_file_statistics catalog={} schema={} table={} files={}",
                        files.catalog, files.schema, files.table, files.addedFiles.size());
                return;
            }
            store.batchInsertFileStatistics(
                    files.catalog, files.schema, files.table, statistics);
            LOG.info("action=file_statistics_collected catalog={} schema={} table={} "
                            + "filesRequested={} statsReturned={}",
                    files.catalog, files.schema, files.table,
                    files.addedFiles.size(), statistics.size());
        } catch (Exception e) {
            // Statistics collection failure must not block index builds (best-effort)
            LOG.warn("action=file_statistics_collection_failed catalog={} schema={} table={} error={}",
                    files.catalog, files.schema, files.table, e.getMessage());
        }
    }

    private void invalidateRemovedFiles(IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> indexes = store.findIndexesByTable(
                files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        for (IndexDescriptor desc : indexes) {
            store.markOrphanedByDataFiles(desc.indexId(), files.removedFiles);
        }
        // Clean up stale file column statistics for removed files (Req 2.AC4, 4.AC5)
        try {
            store.deleteFileStatistics(
                    files.catalog, files.schema, files.table, files.removedFiles);
            LOG.debug("action=file_statistics_deleted catalog={} schema={} table={} removedFiles={}",
                    files.catalog, files.schema, files.table, files.removedFiles.size());
        } catch (Exception e) {
            LOG.warn("action=file_statistics_delete_failed catalog={} schema={} table={} error={}",
                    files.catalog, files.schema, files.table, e.getMessage());
        }
    }

    private void markIndexesStale(IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> indexes = store.findIndexesByTable(
                files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        for (IndexDescriptor desc : indexes) {
            boolean updated = store.updateIndexState(
                    desc.indexId(), IndexState.STALE.name(),
                    Instant.now(), desc.updatedAt());
            if (updated) {
                LOG.info("action=index_marked_stale_by_delete indexId={} table={}.{}.{}",
                        desc.indexId(), files.catalog, files.schema, files.table);
            }
        }
    }

    /**
     * Marks index file mappings as STALE for files replaced during compaction (R11.5).
     * Compaction rewrites data files without logical change, so existing index entries
     * for the old files are stale and need rebuild for the new replacement files.
     */
    private void markCompactionStaleEntries(IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> indexes = store.findIndexesByTable(
                files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        int totalMarked = countAndRemoveStaleEntries(store, indexes, files.removedFiles);
        logCompactionStaleEntries(files, indexes.size(), totalMarked);
    }

    private static int countAndRemoveStaleEntries(IndexMetadataStore store,
                                                  List<IndexDescriptor> indexes,
                                                  List<String> removedFiles) {
        int totalMarked = 0;
        for (IndexDescriptor desc : indexes) {
            for (String removedFile : removedFiles) {
                if (store.removeIndexFileMappingIfState(desc.indexId(), removedFile, IndexState.READY)) {
                    totalMarked++;
                }
            }
        }
        return totalMarked;
    }

    private void logCompactionStaleEntries(MergedCommitFiles files, int indexCount, int totalMarked) {
        if (totalMarked > 0) {
            LOG.info("action=compaction_stale_entries_marked table={}.{}.{} "
                            + "removedFiles={} indexesAffected={} entriesMarked={}",
                    files.catalog, files.schema, files.table,
                    files.removedFiles.size(), indexCount, totalMarked);
            if (lifecycleMetrics != null) {
                lifecycleMetrics.recordCompactionStaleEntries(totalMarked);
            }
        }
    }

    private boolean triggerIncrementalBuilds(MergedCommitFiles files) {
        IndexBuildCoordinator buildCoordinator = deps.buildCoordinator();
        if (!buildCoordinator.canAcceptBuild()) {
            logBuildDeferred(files);
            return false;
        }
        IndexMetadataStore store = deps.metadataStore();
        List<IndexDescriptor> readyIndexes = store.findIndexesByTable(
                files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        boolean allSucceeded = true;
        for (IndexDescriptor descriptor : readyIndexes) {
            if (!buildSingleIndex(descriptor, files, store, buildCoordinator)) {
                allSucceeded = false;
            }
        }
        return allSucceeded;
    }

    private void logBuildDeferred(MergedCommitFiles files) {
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordCommitBuildDeferred();
        }
        LOG.info("action=commit_build_deferred catalog={} schema={} table={} reason=pool_saturated",
                files.catalog, files.schema, files.table);
    }

    private boolean buildSingleIndex(IndexDescriptor descriptor, MergedCommitFiles files,
                                     IndexMetadataStore store, IndexBuildCoordinator buildCoordinator) {
        try {
            List<String> unindexed = store.filterUnindexedFiles(
                    descriptor.indexId(), files.addedFiles);
            if (unindexed.isEmpty()) {
                return true;
            }
            submitIndexBuild(descriptor, files, unindexed, store, buildCoordinator);
            return true;
        } catch (Exception e) {
            if (lifecycleMetrics != null) {
                lifecycleMetrics.recordCommitProcessingFailure();
            }
            LOG.error("action=commit_build_failed indexId={} error={}",
                    descriptor.indexId(), e.getMessage(), e);
            return false;
        }
    }

    private void submitIndexBuild(IndexDescriptor descriptor, MergedCommitFiles files,
                                  List<String> unindexed, IndexMetadataStore store,
                                  IndexBuildCoordinator buildCoordinator) {
        List<DataFileInfo> dataFiles = deps.connectorAccessorResolver()
                .resolveDataFileInfos(files.catalog, files.schema, files.table, unindexed);
        FileSystemAccessor fsAccessor = deps.connectorAccessorResolver().resolve(files.catalog);
        ColumnValueReader columnReader = deps.connectorAccessorResolver()
                .resolveColumnValueReader(files.catalog);
        buildCoordinator.submitBuildTask(
                descriptor, dataFiles, fsAccessor, columnReader,
                batch -> BuildMappingPersister.persist(
                        store, batch, descriptor.generation(), files.snapshotId));
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordCommitBuildTriggered();
        }
        LOG.info("action=commit_build_triggered indexId={} files={}",
                descriptor.indexId(), unindexed.size());
    }

    static Map<String, MergedCommitFiles> mergeByTable(List<CommitEvent> batch) {
        Map<String, MergedCommitFiles> merged = new HashMap<>();
        for (CommitEvent event : batch) {
            String key = event.catalogName() + "." + event.schemaName() + "." + event.tableName();
            merged.compute(key, (k, existing) -> {
                if (existing == null) {
                    return new MergedCommitFiles(
                            event.catalogName(), event.schemaName(), event.tableName(),
                            new ArrayList<>(event.addedFiles()),
                            new ArrayList<>(event.removedFiles()),
                            event.eventType(), event.snapshotId());
                }
                existing.addedFiles.addAll(event.addedFiles());
                existing.removedFiles.addAll(event.removedFiles());
                // Escalate eventType: DELETE > OVERWRITE > COMPACT > APPEND (priority order)
                if (event.eventType().priority() > existing.eventType.priority()) {
                    existing.eventType = event.eventType();
                }
                // Keep the latest snapshot ID for token propagation
                return existing;
            });
        }
        return merged;
    }

    // ---- Schema evolution listener ----

    /**
     * Handles metadata events. On {@link MetadataEvent.SchemaEvolutionEvent}, marks
     * all indexes on the affected table for rebuild so they reflect the new schema.
     */
    @Override
    public void onEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.SchemaEvolutionEvent evolution) {
            markIndexesForRebuildOnSchemaChange(evolution);
        }
    }

    private void markIndexesForRebuildOnSchemaChange(MetadataEvent.SchemaEvolutionEvent evolution) {
        try {
            IndexMetadataStore metadataStore = deps.metadataStore();
            List<IndexDescriptor> indexes = metadataStore.findIndexesByTable(
                    evolution.tableRef().catalog(),
                    evolution.tableRef().schema(),
                    evolution.tableRef().table());
            Set<String> affectedColumns = collectAffectedColumns(evolution.schemaChanges());
            for (IndexDescriptor idx : indexes) {
                if (idx.state() != IndexState.READY && idx.state() != IndexState.BUILDING) {
                    continue;
                }
                if (!intersectsIndexColumns(idx, affectedColumns)) {
                    LOG.debug("event=index_unaffected_by_schema_change index={} indexCols={} "
                                    + "affected={}",
                            idx.indexId(), idx.columnNames(), affectedColumns);
                    continue;
                }
                metadataStore.updateIndexState(idx.indexId(),
                        IndexState.STALE.name(), Instant.now());
                LOG.info("event=index_marked_stale_schema_evolution index={} table={}.{}.{} "
                                + "affectedColumns={}",
                        idx.indexId(), evolution.catalogName(),
                        evolution.tableRef().schema(), evolution.tableRef().table(),
                        affectedColumns);
            }
        } catch (Exception e) {
            LOG.warn("event=schema_evolution_index_mark_failed table={} error={}",
                    evolution.tableRef(), e.getMessage());
        }
    }

    /**
     * Collects the lower-cased column names touched by a list of schema changes.
     * AddColumn alone never invalidates an existing index (new column was not indexed),
     * but is still recorded so callers may track structural drift.
     */
    static Set<String> collectAffectedColumns(List<SchemaChange> changes) {
        Set<String> affected = new HashSet<>();
        if (changes == null) {
            return affected;
        }
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.DropColumn drop) {
                affected.add(normalize(drop.name()));
            } else if (change instanceof SchemaChange.RenameColumn rename) {
                affected.add(normalize(rename.oldName()));
                affected.add(normalize(rename.newName()));
            } else if (change instanceof SchemaChange.ChangeType change2) {
                affected.add(normalize(change2.name()));
            } else if (change instanceof SchemaChange.ChangeNullability change3) {
                affected.add(normalize(change3.name()));
            }
            // AddColumn intentionally omitted: it never affects existing index builds.
        }
        return affected;
    }

    private static boolean intersectsIndexColumns(IndexDescriptor idx, Set<String> affected) {
        if (affected.isEmpty()) {
            return false;
        }
        for (String col : idx.columnNames()) {
            if (affected.contains(normalize(col))) {
                return true;
            }
        }
        return false;
    }

    private static String normalize(String name) {
        return name == null ? "" : name.toLowerCase(Locale.ROOT);
    }

    @Override
    public void close() {
        running = false;
        commitLogPoller.close();
        processingExecutor.shutdownNow();
        LOG.info("action=commit_handler_shutdown remainingEvents={}", eventQueue.size());
    }

    /**
     * Resumes commit-log polling. Delegates to the internal CommitLogPoller.
     * Called when this node becomes maintenance leader.
     */
    public void startPolling() {
        commitLogPoller.startPolling();
    }

    /**
     * Pauses commit-log polling. Delegates to the internal CommitLogPoller.
     * Called when this node loses maintenance leadership.
     */
    public void stopPolling() {
        commitLogPoller.stopPolling();
    }

    /** Merged added/removed files for a single table across multiple events. */
    static final class MergedCommitFiles {
        final String catalog;
        final String schema;
        final String table;
        final List<String> addedFiles;
        final List<String> removedFiles;
        CommitEventType eventType;
        final String snapshotId;

        MergedCommitFiles(String catalog, String schema, String table,
                          List<String> addedFiles, List<String> removedFiles,
                          CommitEventType eventType) {
            this(catalog, schema, table, addedFiles, removedFiles, eventType, "");
        }

        MergedCommitFiles(String catalog, String schema, String table,
                          List<String> addedFiles, List<String> removedFiles,
                          CommitEventType eventType, String snapshotId) {
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
            this.addedFiles = addedFiles;
            this.removedFiles = removedFiles;
            this.eventType = eventType;
            this.snapshotId = snapshotId != null ? snapshotId : "";
        }
    }
}
