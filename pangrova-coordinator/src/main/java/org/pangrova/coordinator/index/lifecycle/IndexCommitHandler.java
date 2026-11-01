package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.index.ddl.IndexSourceAuthority;
import org.pangrova.metadata.core.event.MetadataEvent;
import org.pangrova.metadata.core.event.MetadataEventListener;
import org.pangrova.indexing.control.CommitEventRequest;
import org.pangrova.indexing.control.IndexMetadataStore;
import org.pangrova.indexing.api.IndexFileMapping;
import org.pangrova.indexing.api.IndexGenerationManifest;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotDataFile;
import org.pangrova.spi.connector.index.SnapshotFileDelta;
import org.pangrova.spi.connector.index.SnapshotRelation;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Receives commit events from connectors and durably records index refresh work.
 *
 * <p>{@link #onCommit(CommitEvent)} only appends to the shared commit log.
 * The elected maintenance leader polls that log and refreshes immutable index
 * generations. There is deliberately no coordinator-local processing queue:
 * failover and retry semantics come from the durable log.</p>
 *
 * <p>Implements {@link AutoCloseable} for graceful shutdown.</p>
 */
public final class IndexCommitHandler implements CommitListener, MetadataEventListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCommitHandler.class);

    private final IndexCommitContext deps;
    private final IndexLifecycleMetrics lifecycleMetrics;
    private final MaintenanceLeaderElection leaderElection;
    private final CommitLogPoller commitLogPoller;
    private final IndexGenerationRefresher generationRefresher;
    private final java.util.concurrent.CopyOnWriteArrayList<CommitListener>
            durableCommitProcessors = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    /** Creates a commit handler without an external maintenance leader gate. */
    public IndexCommitHandler(IndexCommitContext deps, IndexLifecycleMetrics lifecycleMetrics,
                              int pollBatchSize) {
        this(deps, lifecycleMetrics, null, pollBatchSize);
    }

    /** Creates a commit handler whose poller is gated by the maintenance leader lease. */
    public IndexCommitHandler(IndexCommitContext deps, IndexLifecycleMetrics lifecycleMetrics,
                              MaintenanceLeaderElection leaderElection,
                              int pollBatchSize) {
        this.deps = deps;
        this.lifecycleMetrics = lifecycleMetrics;
        this.leaderElection = leaderElection;
        if (pollBatchSize <= 0) {
            throw new IllegalArgumentException("pollBatchSize must be positive");
        }
        this.generationRefresher = new IndexGenerationRefresher(deps, lifecycleMetrics);
        this.commitLogPoller = new CommitLogPoller(
                deps, leaderElection, pollBatchSize, this::processTableCommit);
    }

    @Override
    public void onCommit(CommitEvent event) {
        if (!running) {
            throw new IllegalStateException("Index commit handler is closed");
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
        deps.metadataStore().recordCommitEvent(new CommitEventRequest(
                event.tenantId(),
                event.catalogName(), event.schemaName(), event.tableName(),
                event.snapshotId(),
                CommitFileListCodec.encode(event.addedFiles()),
                CommitFileListCodec.encode(event.removedFiles()),
                event.eventType().name(), Instant.now(), event.commitFingerprint()));
    }

    void processTableCommit(MergedCommitFiles files) {
        IndexMetadataStore store = deps.metadataStore();
        boolean buildSucceeded = switch (files.eventType) {
            case APPEND -> refreshPublishedGenerations(store, files);
            case OVERWRITE, COMPACT -> {
                boolean refreshed = refreshPublishedGenerations(store, files);
                if (refreshed) deleteRemovedFileStatistics(store, files);
                yield refreshed;
            }
            case DELETE -> {
                markIndexesStale(store, files);
                yield true;
            }
        };
        if (!buildSucceeded) {
            throw new IllegalStateException(
                    "Index generation refresh did not complete for table "
                            + files.catalog + "." + files.schema + "." + files.table);
        }
        CommitEvent replayed = new CommitEvent(
                files.tenantId, files.catalog, files.schema, files.table,
                files.addedFiles, files.removedFiles, Instant.now(),
                files.snapshotId, files.eventType, files.commitFingerprint);
        for (CommitListener processor : durableCommitProcessors) {
            processor.onCommit(replayed);
        }
    }

    /**
     * Adds an idempotent processor to the durable commit-log barrier.
     * The log entry is acknowledged only after every processor succeeds.
     */
    public void registerDurableCommitProcessor(CommitListener processor) {
        durableCommitProcessors.add(java.util.Objects.requireNonNull(
                processor, "processor must not be null"));
    }

    private boolean refreshPublishedGenerations(
            IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> readyIndexes = store.findIndexesByTable(
                files.tenantId, files.catalog, files.schema, files.table).stream()
                .filter(descriptor -> descriptor.state() == IndexState.READY)
                .toList();
        if (readyIndexes.isEmpty()) {
            return true;
        }
        if (!files.addedFiles.isEmpty()) {
            var firstAccessors = IndexSourceAuthority.forDescriptor(
                    store, deps.buildServices(), deps.connectorAccessorResolver(), readyIndexes.get(0));
            collectFileStatistics(store, files, firstAccessors);
        }
        for (IndexDescriptor descriptor : readyIndexes) {
            var accessors = IndexSourceAuthority.forDescriptor(
                    store, deps.buildServices(), deps.connectorAccessorResolver(), descriptor);
            SnapshotAwareMetadata snapshotMetadata = accessors.resolveSnapshotAwareMetadata(
                    files.catalog, files.schema, files.table);
            if (snapshotMetadata == null) {
                markIndexStale(store, descriptor, "snapshot_metadata_unavailable");
                continue;
            }
            List<SnapshotDataFile> targetFiles = snapshotMetadata.listFilesAtSnapshot(files.snapshotId);
            IndexRefreshInput input = resolveRefreshInput(
                    store, snapshotMetadata, descriptor, files, targetFiles);
            if (input == null) {
                continue;
            }
            if (!generationRefresher.refresh(descriptor, input)) {
                logBuildDeferred(files);
                return false;
            }
        }
        return true;
    }

    private IndexRefreshInput resolveRefreshInput(
            IndexMetadataStore store,
            SnapshotAwareMetadata snapshotMetadata,
            IndexDescriptor descriptor,
            MergedCommitFiles event,
            List<SnapshotDataFile> targetFiles) {
        IndexGenerationManifest active = store.findActiveIndexGeneration(
                descriptor.indexId()).orElse(null);
        if (active == null) {
            markIndexStale(store, descriptor, "missing_active_generation_manifest");
            return null;
        }
        if (active.sourceSnapshot().equals(event.snapshotId)) {
            return null;
        }
        if (!active.sourceSnapshot().isBlank()) {
            SnapshotRelation relation = snapshotMetadata.relation(
                    active.sourceSnapshot(), event.snapshotId);
            if (relation == SnapshotRelation.RIGHT_ANCESTOR) {
                LOG.info("action=stale_commit_event_ignored indexId={} activeSnapshot={} "
                                + "eventSnapshot={}",
                        descriptor.indexId(), active.sourceSnapshot(), event.snapshotId);
                return null;
            }
            if (relation == SnapshotRelation.DIVERGED) {
                markIndexStale(store, descriptor, "source_snapshot_lineage_diverged");
                return null;
            }
        }
        SnapshotFileDelta delta = active.sourceSnapshot().isBlank()
                ? fullReplacement(store, descriptor, targetFiles)
                : validatedDelta(
                        store, snapshotMetadata, descriptor, active, event, targetFiles);
        return new IndexRefreshInput(
                event.snapshotId,
                delta.added().stream().map(IndexCommitHandler::toDataFileInfo).toList(),
                delta.removed(), active.sourceSnapshot().isBlank());
    }

    private SnapshotFileDelta validatedDelta(
            IndexMetadataStore store,
            SnapshotAwareMetadata snapshotMetadata,
            IndexDescriptor descriptor,
            IndexGenerationManifest active,
            MergedCommitFiles event,
            List<SnapshotDataFile> targetFiles) {
        SnapshotFileDelta delta = snapshotMetadata.diffFiles(
                active.sourceSnapshot(), event.snapshotId);
        Set<String> indexed = indexedFiles(
                store, descriptor.indexId(), active.generation());
        Set<String> predicted = new HashSet<>(indexed);
        predicted.removeAll(delta.removed());
        predicted.addAll(delta.added().stream().map(SnapshotDataFile::path).toList());
        Set<String> target = targetFiles.stream().map(SnapshotDataFile::path)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!predicted.equals(target)) {
            LOG.warn("action=commit_delta_validation_failed indexId={} "
                            + "predictedFiles={} targetFiles={} action=full_replacement",
                    descriptor.indexId(), predicted.size(), target.size());
            return fullReplacement(store, descriptor, targetFiles);
        }
        logEventDrift(descriptor.indexId(), event, delta);
        return delta;
    }

    private static SnapshotFileDelta fullReplacement(
            IndexMetadataStore store,
            IndexDescriptor descriptor,
            List<SnapshotDataFile> targetFiles) {
        return new SnapshotFileDelta(
                targetFiles,
                List.copyOf(indexedFiles(
                        store, descriptor.indexId(), descriptor.generation())));
    }

    private static Set<String> indexedFiles(
            IndexMetadataStore store, String indexId, int generation) {
        return store.getIndexFileMappings(indexId).stream()
                .filter(mapping -> mapping.generation() == generation)
                .map(IndexFileMapping::dataFilePath)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static DataFileInfo toDataFileInfo(SnapshotDataFile file) {
        return new DataFileInfo(
                file.path(), file.rowCount(), file.sizeBytes(), file.format());
    }

    private static void logEventDrift(
            String indexId, MergedCommitFiles event, SnapshotFileDelta actual) {
        Set<String> actualAdded = actual.added().stream()
                .map(SnapshotDataFile::path)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!actualAdded.equals(Set.copyOf(event.addedFiles))
                || !Set.copyOf(actual.removed()).equals(Set.copyOf(event.removedFiles))) {
            LOG.warn("action=commit_event_diff_mismatch indexId={} eventSnapshot={} "
                            + "eventAdded={} actualAdded={} eventRemoved={} actualRemoved={}",
                    indexId, event.snapshotId, event.addedFiles.size(), actualAdded.size(),
                    event.removedFiles.size(), actual.removed().size());
        }
    }

    /**
     * Extracts file-level column statistics from the connector and persists them
     * to the metadata store for L0 pruning (Req 2.AC3).
     */
    void collectFileStatistics(IndexMetadataStore store, MergedCommitFiles files) {
        collectFileStatistics(store, files, deps.connectorAccessorResolver());
    }

    private void collectFileStatistics(IndexMetadataStore store, MergedCommitFiles files,
                                       ConnectorAccessorResolver accessors) {
        try {
            List<org.pangrova.spi.index.statistics.DataFileStatistics> statistics =
                    accessors.resolveFileStatistics(
                            files.catalog, files.schema, files.table, files.addedFiles);
            if (statistics.isEmpty()) {
                LOG.debug("action=no_file_statistics catalog={} schema={} table={} files={}",
                        files.catalog, files.schema, files.table, files.addedFiles.size());
                return;
            }
            store.batchInsertFileStatistics(
                    files.tenantId, files.catalog, files.schema, files.table, statistics);
            LOG.info("action=file_statistics_collected catalog={} schema={} table={} "
                            + "filesRequested={} statsReturned={}",
                    files.catalog, files.schema, files.table,
                    files.addedFiles.size(), statistics.size());
        } catch (org.pangrova.spi.security.AccessDeniedException denied) {
            // Missing or revoked maintenance authority is not a best-effort
            // statistics failure: the durable commit event must remain pending.
            throw denied;
        } catch (Exception e) {
            // Statistics collection failure must not block index builds (best-effort)
            LOG.warn("action=file_statistics_collection_failed catalog={} schema={} table={} error={}",
                    files.catalog, files.schema, files.table, e.getMessage());
        }
    }

    private void deleteRemovedFileStatistics(
            IndexMetadataStore store, MergedCommitFiles files) {
        if (files.removedFiles.isEmpty()) {
            return;
        }
        try {
            store.deleteFileStatistics(
                    files.tenantId, files.catalog, files.schema, files.table, files.removedFiles);
            LOG.debug("action=file_statistics_deleted catalog={} schema={} table={} removedFiles={}",
                    files.catalog, files.schema, files.table, files.removedFiles.size());
        } catch (Exception e) {
            LOG.warn("action=file_statistics_delete_failed catalog={} schema={} table={} error={}",
                    files.catalog, files.schema, files.table, e.getMessage());
        }
    }

    private void markIndexesStale(IndexMetadataStore store, MergedCommitFiles files) {
        List<IndexDescriptor> indexes = store.findIndexesByTable(
                files.tenantId, files.catalog, files.schema, files.table).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
        markIndexesStale(store, indexes, "source_delete_commit");
    }

    private void markIndexesStale(
            IndexMetadataStore store,
            List<IndexDescriptor> indexes,
            String reason) {
        for (IndexDescriptor desc : indexes) {
            markIndexStale(store, desc, reason);
        }
    }

    private void markIndexStale(
            IndexMetadataStore store, IndexDescriptor descriptor, String reason) {
        boolean updated = store.updateIndexState(
                descriptor.indexId(), IndexState.STALE.name(), Instant.now(), reason,
                descriptor.updatedAt(), descriptor.state().name(), descriptor.generation());
        LOG.info("action=index_marked_stale indexId={} reason={} updated={}",
                descriptor.indexId(), reason, updated);
    }

    private void logBuildDeferred(MergedCommitFiles files) {
        if (lifecycleMetrics != null) {
            lifecycleMetrics.recordCommitBuildDeferred();
        }
        LOG.info("action=commit_build_deferred catalog={} schema={} table={} reason=pool_saturated",
                files.catalog, files.schema, files.table);
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
            List<IndexDescriptor> indexes = evolution.tenantScope().allTenants()
                    ? metadataStore.findIndexesByTableForGlobalMaintenance(
                            evolution.tableRef().catalog(), evolution.tableRef().schema(),
                            evolution.tableRef().table())
                    : evolution.tenantScope().tenantIds().stream()
                            .flatMap(tenantId -> metadataStore.findIndexesByTable(
                                    tenantId, evolution.tableRef().catalog(),
                                    evolution.tableRef().schema(),
                                    evolution.tableRef().table()).stream())
                            .toList();
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
                boolean updated = metadataStore.updateIndexState(
                        idx.indexId(), IndexState.STALE.name(), Instant.now(),
                        "schema_evolution", idx.updatedAt(), idx.state().name(),
                        idx.generation());
                LOG.info("event=index_marked_stale_schema_evolution index={} table={}.{}.{} "
                                + "affectedColumns={} updated={}",
                        idx.indexId(), evolution.catalogName(),
                        evolution.tableRef().schema(), evolution.tableRef().table(),
                        affectedColumns, updated);
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
        LOG.info("action=commit_handler_shutdown");
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

    void pollCommitLogOnce() {
        commitLogPoller.pollCommitLog();
    }

    /** Merged added/removed files for a single table across multiple events. */
    static final class MergedCommitFiles {
        final String tenantId;
        final String catalog;
        final String schema;
        final String table;
        final List<String> addedFiles;
        final List<String> removedFiles;
        final CommitEventType eventType;
        final String snapshotId;
        final String commitFingerprint;

        MergedCommitFiles(String tenantId, String catalog, String schema, String table,
                          List<String> addedFiles, List<String> removedFiles,
                          CommitEventType eventType, String snapshotId, String commitFingerprint) {
            if (tenantId == null || tenantId.isBlank()
                    || catalog == null || catalog.isBlank()
                    || schema == null || schema.isBlank()
                    || table == null || table.isBlank()
                    || snapshotId == null || snapshotId.isBlank()
                    || eventType == null) {
                throw new IllegalArgumentException(
                        "Commit table, snapshot, and event type are required");
            }
            this.tenantId = tenantId;
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
            this.addedFiles = List.copyOf(addedFiles);
            this.removedFiles = List.copyOf(removedFiles);
            this.eventType = eventType;
            this.snapshotId = snapshotId;
            this.commitFingerprint = commitFingerprint;
        }
    }
}
