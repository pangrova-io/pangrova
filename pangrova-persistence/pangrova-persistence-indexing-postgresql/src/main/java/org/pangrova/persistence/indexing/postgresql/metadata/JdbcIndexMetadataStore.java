package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.persistence.jdbc.api.security.policy.CurrentTableAuthorization;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;

import org.pangrova.persistence.indexing.api.metadata.CommitEventRequest;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.BreakerState;
import org.pangrova.persistence.indexing.api.metadata.CacheEvent;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;
import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildAbandonment;
import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildCandidate;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexFileSelection;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * JDBC-backed implementation of {@link IndexMetadataStore}.
 *
 * <p>Facade that delegates to focused sub-stores, each handling a single
 * responsibility domain:</p>
 * <ul>
 *   <li>{@link JdbcIndexDescriptorStore} — index CRUD and schema evolution</li>
 *   <li>{@link JdbcIndexDescriptorReadStore} — descriptor state and identity queries</li>
 *   <li>{@link JdbcIndexFileMappingStore} — file mapping CRUD and batch queries</li>
 *   <li>{@link JdbcIndexStatisticsStore} — file column statistics (L0 pruning)</li>
 *   <li>{@link JdbcIndexGcStore} — GC queries, cache events, breaker state</li>
 *   <li>{@link JdbcIndexCommitQueue} — durable commit-event queue and fenced heads</li>
 * </ul>
 */
public final class JdbcIndexMetadataStore implements IndexMetadataStore {

    private final JdbcIndexDescriptorStore descriptorStore;
    private final JdbcIndexDescriptorReadStore descriptorReadStore;
    private final JdbcIndexMaintenanceScanStore maintenanceScanStore;
    private final JdbcIndexFileMappingStore fileMappingStore;
    private final JdbcScalarArtifactIntentStore scalarArtifactStore;
    private final JdbcMappedArtifactRetirementStore mappedArtifactStore;
    private final JdbcIndexStatisticsStore statisticsStore;
    private final JdbcIndexGcStore gcStore;
    private final JdbcIndexCommitQueue commitQueue;
    private final JdbcIndexPublicationStore publicationStore;
    private final JdbcIndexGenerationCoverageStore coverageStore;
    private final JdbcIndexSchemaEvolutionStore schemaEvolutionStore;
    private final JdbcScalarSnapshotPinStore scalarSnapshotPinStore;
    private final ThreadLocal<Set<String>> heldLifecycleLocks = ThreadLocal.withInitial(HashSet::new);

    /** Creates the facade over the focused JDBC sub-stores backed by the given data source. */
    public JdbcIndexMetadataStore(DataSource dataSource, CurrentTableAuthorization authorization) {
        this.descriptorStore = new JdbcIndexDescriptorStore(dataSource, authorization);
        this.descriptorReadStore = new JdbcIndexDescriptorReadStore(dataSource);
        this.maintenanceScanStore = new JdbcIndexMaintenanceScanStore(dataSource);
        this.fileMappingStore = new JdbcIndexFileMappingStore(dataSource);
        this.scalarArtifactStore = new JdbcScalarArtifactIntentStore(dataSource);
        this.mappedArtifactStore = new JdbcMappedArtifactRetirementStore(dataSource);
        this.statisticsStore = new JdbcIndexStatisticsStore(dataSource);
        this.gcStore = new JdbcIndexGcStore(dataSource);
        this.commitQueue = new JdbcIndexCommitQueue(dataSource);
        this.publicationStore = new JdbcIndexPublicationStore(dataSource, descriptorStore);
        this.coverageStore = new JdbcIndexGenerationCoverageStore(dataSource);
        this.schemaEvolutionStore = new JdbcIndexSchemaEvolutionStore(dataSource);
        this.scalarSnapshotPinStore = new JdbcScalarSnapshotPinStore(dataSource);
    }

    @Override
    public boolean reserveScalarSnapshotPin(IndexDescriptor descriptor, String snapshot,
                                              SnapshotPinOwner owner, Instant now) {
        return scalarSnapshotPinStore.reserve(descriptor, snapshot, owner, now);
    }

    @Override
    public void markScalarSnapshotPinDrained(SnapshotPinOwner owner, String snapshot, Instant now) {
        scalarSnapshotPinStore.markDrained(owner, snapshot, now);
    }

    @Override
    public Optional<ScalarPin> findScalarSnapshotPin(SnapshotPinOwner owner) {
        return scalarSnapshotPinStore.find(owner);
    }

    @Override
    public List<ScalarPin> listScalarSnapshotPinCleanupCandidates(Instant now, int limit) {
        return scalarSnapshotPinStore.list(now, limit);
    }

    @Override
    public Optional<ScalarPin> claimScalarSnapshotPinCleanup(SnapshotPinOwner owner, Instant now) {
        return scalarSnapshotPinStore.claim(owner, now);
    }

    @Override
    public void markScalarSnapshotPinReleased(SnapshotPinOwner owner, String snapshot, Instant now) {
        scalarSnapshotPinStore.markReleased(owner, snapshot, now);
    }

    @Override
    public void beginScalarArtifactWrite(ScalarArtifactIntent intent) {
        scalarArtifactStore.beginWrite(intent);
    }

    @Override
    public void markScalarArtifactDrained(ScalarArtifactIntent intent) {
        scalarArtifactStore.markDrained(intent);
    }

    @Override
    public List<ScalarArtifactIntent> listReclaimableScalarArtifacts(Instant cutoff, int limit) {
        return scalarArtifactStore.listReclaimable(cutoff, limit);
    }

    @Override
    public List<ScalarArtifactIntent> pollScalarArtifactReclaimCandidates(Instant drainedBefore, int limit) {
        return scalarArtifactStore.pollReclaimable(drainedBefore, limit);
    }

    @Override
    public boolean beginScalarArtifactReclaim(ScalarArtifactIntent intent) {
        return scalarArtifactStore.beginReclaim(intent);
    }

    @Override
    public void completeScalarArtifactReclaim(ScalarArtifactIntent intent) {
        scalarArtifactStore.completeReclaim(intent);
    }

    @Override
    public boolean hasScalarArtifactIntents(String indexId) {
        return scalarArtifactStore.hasIntents(indexId);
    }

    // --- IndexDescriptor CRUD (delegate to descriptorStore) ---

    @Override
    public void registerIndex(IndexDescriptor descriptor) {
        descriptorStore.registerIndex(descriptor);
    }

    @Override
    public void registerNativeIndex(IndexDescriptor descriptor, String attemptId) {
        descriptorStore.registerNativeIndex(descriptor, attemptId);
    }

    @Override
    public void registerIndexWithBuildAuthority(IndexDescriptor descriptor, String principal) {
        descriptorStore.registerIndexWithBuildAuthority(descriptor, principal);
    }

    @Override
    public String loadAuthorizedBuildPrincipal(IndexDescriptor descriptor) {
        return descriptorStore.loadAuthorizedBuildPrincipal(descriptor);
    }

    @Override
    public String loadDroppedIndexCleanupPrincipal(IndexDescriptor descriptor) {
        return descriptorStore.loadDroppedIndexCleanupPrincipal(descriptor);
    }

    @Override
    public void withIndexLifecycleLock(String indexId, Runnable action) {
        if (heldLifecycleLocks.get().contains(indexId)) {
            action.run();
            return;
        }
        descriptorStore.withIndexLifecycleLock(indexId, () -> {
            heldLifecycleLocks.get().add(indexId);
            try {
                action.run();
            } finally {
                heldLifecycleLocks.get().remove(indexId);
                if (heldLifecycleLocks.get().isEmpty()) {
                    heldLifecycleLocks.remove();
                }
            }
        });
    }

    @Override
    public void updateIndexState(String indexId, String newState, Instant timestamp) {
        descriptorStore.updateIndexState(indexId, newState, timestamp);
    }

    @Override
    public void updateIndexState(String indexId, String newState,
                                 Instant timestamp, String staleReason) {
        descriptorStore.updateIndexState(indexId, newState, timestamp, staleReason);
    }

    @Override
    public boolean updateIndexState(String indexId, String newState,
                                    Instant timestamp, Instant expectedUpdatedAt) {
        return descriptorStore.updateIndexState(indexId, newState, timestamp, expectedUpdatedAt);
    }

    @Override
    public boolean updateIndexState(String indexId, String newState,
                                    Instant timestamp, Instant expectedUpdatedAt,
                                    String expectedState, int expectedGeneration) {
        return descriptorStore.updateIndexState(indexId, newState, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration);
    }

    @Override
    public boolean updateIndexState(
            String indexId,
            String newState,
            Instant timestamp,
            String staleReason,
            Instant expectedUpdatedAt,
            String expectedState,
            int expectedGeneration) {
        return descriptorStore.updateIndexState(
                indexId, newState, timestamp, staleReason,
                expectedUpdatedAt, expectedState, expectedGeneration);
    }

    @Override
    public boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) {
        return beginIndexGeneration(indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration, null);
    }

    @Override
    public boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration,
            String sourceSchemaFingerprint) {
        return descriptorStore.beginIndexGeneration(
                indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration, sourceSchemaFingerprint);
    }

    @Override
    public boolean beginNativeIndexGeneration(String indexId, int newGeneration, String buildSnapshotId,
                                              Instant timestamp, Instant expectedUpdatedAt, IndexState expectedState,
                                              int expectedGeneration, String sourceSchemaFingerprint,
                                              String attemptId) {
        return descriptorStore.beginNativeIndexGeneration(indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration, sourceSchemaFingerprint, attemptId);
    }

    @Override
    public boolean failIndexGeneration(
            String indexId,
            int generation,
            Instant expectedBuildUpdatedAt,
            Instant failedAt,
            String failureReason) {
        return descriptorStore.failIndexGeneration(
                indexId, generation, expectedBuildUpdatedAt, failedAt, failureReason);
    }

    @Override
    public boolean failNativeSubmissionAttempt(String indexId, int generation, String attemptId,
                                                Instant expectedUpdatedAt, Instant failedAt, String reason) {
        return descriptorStore.failNativeSubmissionAttempt(
                indexId, generation, attemptId, expectedUpdatedAt, failedAt, reason);
    }

    @Override
    public boolean isIndexGenerationBuildable(String indexId, int generation) {
        return descriptorStore.isIndexGenerationBuildable(indexId, generation);
    }

    @Override
    public List<NativeSubmissionCandidate> listAbandonedNativeReservations(int limit, Instant cutoff) {
        return descriptorStore.listAbandonedNativeReservations(limit, cutoff);
    }

    @Override
    public Optional<NativeSubmissionCandidate> findAbandonedNativeReservation(
            String indexId, int generation, Instant cutoff) {
        return descriptorStore.findAbandonedNativeReservation(indexId, generation, cutoff);
    }

    @Override
    public Optional<Instant> findIndexGenerationSnapshotReservation(String indexId, int generation, String snapshotId) {
        return descriptorStore.findIndexGenerationSnapshotReservation(indexId, generation, snapshotId);
    }

    @Override
    public Optional<String> findIndexGenerationSubmissionAttempt(String indexId, int generation, String snapshotId) {
        return descriptorStore.findIndexGenerationSubmissionAttempt(indexId, generation, snapshotId);
    }

    @Override
    public boolean isIndexGenerationSnapshotBuildable(
            String indexId, int generation, String snapshotId, Instant reservationUpdatedAt) {
        return descriptorStore.isIndexGenerationSnapshotBuildable(
                indexId, generation, snapshotId, reservationUpdatedAt);
    }

    @Override
    public boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        return descriptorStore.isIndexSnapshotBuildable(indexId, snapshotId);
    }

    @Override
    public List<IndexBuildCandidate> findIndexBuildRecoveryCandidates() {
        return descriptorStore.findIndexBuildRecoveryCandidates();
    }

    @Override
    public Optional<IndexBuildAbandonment> abandonIndexGeneration(
            String indexId, int generation, Instant expectedUpdatedAt,
            Instant cutoff, Instant abandonedAt, String reason) {
        return descriptorStore.abandonIndexGeneration(
                indexId, generation, expectedUpdatedAt, cutoff, abandonedAt, reason);
    }

    @Override
    public boolean completeIndexBuildCleanup(
            String indexId, int generation, Instant expectedUpdatedAt,
            Instant expectedBuildUpdatedAt, IndexState expectedState, int expectedGeneration) {
        return descriptorStore.completeIndexBuildCleanup(indexId, generation, expectedUpdatedAt,
                expectedBuildUpdatedAt, expectedState, expectedGeneration);
    }

    @Override
    public IndexGenerationManifest publishIndexGeneration(
            IndexPublicationRequest request) {
        return publicationStore.publish(request);
    }

    @Override
    public boolean retireNativeGenerationForSourceDrift(
            IndexDescriptor expected, IndexGenerationManifest active,
            String currentSnapshot, Instant timestamp) {
        return publicationStore.retireNativeGenerationForSourceDrift(
                expected, active, currentSnapshot, timestamp);
    }

    @Override
    public Optional<IndexGenerationManifest> findActiveIndexGeneration(
            String indexId) {
        return publicationStore.findActive(indexId);
    }

    @Override
    public Optional<IndexDescriptor> findIndex(String indexId) {
        return descriptorStore.findIndex(indexId);
    }

    @Override
    public List<IndexDescriptor> findIndexesByTable(
            String tenantId, String catalog, String schema, String table) {
        return descriptorStore.findIndexesByTable(tenantId, catalog, schema, table);
    }

    @Override
    public List<IndexDescriptor> findIndexesByTableForGlobalMaintenance(
            String catalog, String schema, String table) {
        return descriptorStore.findIndexesByTableForGlobalMaintenance(catalog, schema, table);
    }

    @Override
    public List<IndexDescriptor> findReadyIndexesForColumns(
            String tenantId, String catalog, String schema, String table,
            List<String> columns, boolean includeStale) {
        return descriptorStore.findReadyIndexesForColumns(
                tenantId, catalog, schema, table, columns, includeStale);
    }

    @Override
    public void dropIndex(String indexId, Instant timestamp) {
        descriptorStore.dropIndex(indexId, timestamp);
    }

    @Override
    public boolean retireDroppedIndexMappings(String indexId, Instant expectedUpdatedAt, int expectedGeneration) {
        return descriptorStore.retireDroppedIndexMappings(indexId, expectedUpdatedAt, expectedGeneration);
    }

    @Override
    public void purgeIndex(String indexId) {
        withIndexLifecycleLock(indexId, () -> {
            if (scalarSnapshotPinStore.hasUnreleased(indexId)) {
                throw new IllegalStateException("Scalar snapshot pin cleanup is incomplete for " + indexId);
            }
            if (mappedArtifactStore.hasPending(indexId)) {
                throw new IllegalStateException("Mapped artifact retirement is incomplete for " + indexId);
            }
            descriptorStore.purgeIndex(indexId);
        });
    }

    @Override
    public List<IndexDescriptor> findIndexesByColumn(
            String tenantId, String catalog, String schema, String table, String column) {
        return descriptorStore.findIndexesByColumn(tenantId, catalog, schema, table, column);
    }

    @Override
    public void renameColumnInIndexes(
            String tenantId, String catalog, String schema, String table,
            String oldColumn, String newColumn) {
        descriptorStore.renameColumnInIndexes(
                tenantId, catalog, schema, table, oldColumn, newColumn);
    }

    @Override
    public void dropAllIndexesForTable(
            String tenantId, String catalog, String schema, String table, Instant timestamp) {
        descriptorStore.dropAllIndexesForTable(tenantId, catalog, schema, table, timestamp);
    }

    @Override
    public void renameTable(
            String tenantId, String catalog, String schema, String oldTable, String newTable) {
        descriptorStore.renameTable(tenantId, catalog, schema, oldTable, newTable);
    }

    @Override
    public List<IndexDescriptor> findIndexesByState(IndexState state) {
        return descriptorReadStore.findIndexesByState(state);
    }

    @Override
    public Map<String, IndexDescriptor> findIndexesByIds(List<String> indexIds) {
        return descriptorReadStore.findIndexesByIds(indexIds);
    }

    @Override
    public void updateLastIndexedSnapshotId(String indexId, String snapshotId) {
        descriptorStore.updateLastIndexedSnapshotId(indexId, snapshotId);
    }

    @Override
    public MaintenancePage loadMaintenancePage(IndexState state, Instant cutoff, int limit) {
        return maintenanceScanStore.loadMaintenancePage(state, cutoff, limit);
    }

    @Override
    public boolean completeMaintenancePage(MaintenancePage page) {
        return maintenanceScanStore.completeMaintenancePage(page);
    }

    // --- IndexFileMapping CRUD (delegate to fileMappingStore) ---

    @Override
    public void batchRegisterIndexFiles(String attemptOwner, List<IndexFileMapping> mappings) {
        fileMappingStore.batchRegisterIndexFiles(attemptOwner, mappings);
    }

    @Override
    public long copyIndexGenerationMappings(
            String indexId,
            int sourceGeneration,
            int targetGeneration,
            List<String> excludedDataFiles) {
        return fileMappingStore.copyIndexGenerationMappings(
                indexId, sourceGeneration, targetGeneration, excludedDataFiles);
    }

    @Override
    public List<IndexFileMapping> getIndexFileMappings(String indexId) {
        return fileMappingStore.getIndexFileMappings(indexId);
    }

    @Override
    public Set<String> getIndexedDataFilePaths(String indexId, int generation) {
        return fileMappingStore.getIndexedDataFilePaths(indexId, generation);
    }

    @Override
    public Set<String> getNativeIndexSourcePaths(
            IndexDescriptor descriptor, IndexGenerationManifest active) {
        return coverageStore.getNativeIndexSourcePaths(descriptor, active);
    }

    @Override
    public Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths) {
        return fileMappingStore.batchGetIndexFileMappings(indexIds, dataFilePaths);
    }

    @Override
    public Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths, String querySnapshotToken) {
        return fileMappingStore.batchGetIndexFileMappings(indexIds, dataFilePaths, querySnapshotToken);
    }

    @Override
    public Map<String, Map<String, IndexFileMapping>> batchGetIndexArtifacts(
            Map<String, IndexFileSelection> selections,
            List<String> dataFilePaths) {
        return fileMappingStore.batchGetIndexArtifacts(selections, dataFilePaths);
    }

    @Override
    public void removeIndexFileMapping(String indexId, String dataFilePath) {
        fileMappingStore.removeIndexFileMapping(indexId, dataFilePath);
    }

    @Override
    public OrphanRemoval removeOrphanMappings(String indexId, IndexState expectedState,
                                              int expectedGeneration, List<IndexFileMapping> mappings) {
        return gcStore.removeOrphanMappings(indexId, expectedState, expectedGeneration, mappings);
    }

    @Override
    public Map<String, Integer> getIndexFileMappingCounts(
            List<String> indexIds, List<String> dataFilePaths) {
        return fileMappingStore.getIndexFileMappingCounts(indexIds, dataFilePaths);
    }

    @Override
    public List<String> filterUnindexedFiles(String indexId, List<String> dataFilePaths) {
        return fileMappingStore.filterUnindexedFiles(indexId, dataFilePaths);
    }

    @Override
    public void updateLastVerifiedAt(String indexId, String dataFilePath, long timestamp) {
        fileMappingStore.updateLastVerifiedAt(indexId, dataFilePath, timestamp);
    }

    @Override
    public void deleteFileMappingsByGeneration(String indexId, int maxGeneration) {
        fileMappingStore.deleteFileMappingsByGeneration(indexId, maxGeneration);
    }

    // --- GC queries (delegate to gcStore) ---

    @Override
    public List<MappedArtifactRetirement> pollMappedArtifactRetirements(int limit) {
        return mappedArtifactStore.poll(limit);
    }

    @Override
    public boolean beginMappedArtifactReclaim(MappedArtifactRetirement retirement) {
        return mappedArtifactStore.begin(retirement);
    }

    @Override
    public void completeMappedArtifactReclaim(MappedArtifactRetirement retirement) {
        mappedArtifactStore.complete(retirement);
    }

    @Override
    public boolean hasPendingMappedArtifactRetirements(String indexId) {
        return mappedArtifactStore.hasPending(indexId);
    }

    @Override
    public void markOrphanedByDataFiles(String indexId, List<String> dataFilePaths) {
        gcStore.markOrphanedByDataFiles(indexId, dataFilePaths);
    }

    @Override
    public List<IndexFileMapping> listStaleFileMappings(long olderThanMillis) {
        return gcStore.listStaleFileMappings(olderThanMillis);
    }

    @Override
    public List<IndexFileMapping> listStaleFileMappings(long olderThanMillis, Instant createdBefore) {
        return gcStore.listStaleFileMappings(olderThanMillis, createdBefore);
    }

    // --- Commit log (delegate to commitQueue) ---

    @Override
    public void recordCommitEvent(CommitEventRequest request) {
        commitQueue.recordCommitEvent(request);
    }

    @Override
    public List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
        return commitQueue.pollUnprocessedCommitEvents(limit, leaseEpoch);
    }

    @Override
    public void markCommitEventProcessed(long eventId, long leaseEpoch) {
        commitQueue.markCommitEventProcessed(eventId, leaseEpoch);
    }

    // --- Cache events (delegate to gcStore) ---

    @Override
    public void insertCacheEvent(String eventType, String indexId,
                                 String catalog, String schema, String table, Instant now) {
        gcStore.insertCacheEvent(eventType, indexId, catalog, schema, table, now);
    }

    @Override
    public List<CacheEvent> pollCacheEvents(long afterEventId, int limit) {
        return gcStore.pollCacheEvents(afterEventId, limit);
    }

    // --- Circuit breaker state (delegate to gcStore) ---

    @Override
    public Optional<BreakerState> getBreakerState(String indexId) {
        return gcStore.getBreakerState(indexId);
    }

    @Override
    public void updateBreakerState(String indexId, int failureCount,
                                   Instant lastFailureAt, Instant lastSuccessAt, String state) {
        gcStore.updateBreakerState(indexId, failureCount, lastFailureAt, lastSuccessAt, state);
    }

    @Override
    public UUID prepareSchemaEvolution(SchemaChangeEvent event, Instant now) {
        return schemaEvolutionStore.prepare(event, now);
    }

    @Override
    public UUID prepareSchemaEvolution(SchemaChangeEvent event,
            Optional<org.pangrova.connector.api.metadata.table.TableIdentity> sourceIdentity, Instant now) {
        return schemaEvolutionStore.prepare(event, sourceIdentity, now);
    }

    @Override
    public void markSchemaEvolutionCommitted(UUID operationId, Instant now) {
        schemaEvolutionStore.markCommitted(operationId, now);
    }

    @Override
    public void markSchemaEvolutionCommitted(UUID operationId,
            Optional<org.pangrova.connector.api.metadata.table.TableIdentity> targetIdentity, Instant now) {
        schemaEvolutionStore.markCommitted(operationId, targetIdentity, now);
    }

    @Override
    public void markSchemaEvolutionUnknown(UUID operationId, Instant now) {
        schemaEvolutionStore.markUnknown(operationId, now);
    }

    @Override
    public Optional<IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord> findSchemaEvolution(UUID operationId) {
        return schemaEvolutionStore.find(operationId);
    }

    @Override
    public List<IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord> listUnresolvedSchemaEvolution(int limit) {
        return schemaEvolutionStore.listUnresolved(limit);
    }

    @Override
    public void deferSchemaEvolutionRetry(UUID operationId, Instant now) {
        schemaEvolutionStore.deferRetry(operationId, now);
    }

    @Override
    public List<IndexDescriptor> listSchemaEvolutionTargets(UUID operationId) {
        return schemaEvolutionStore.targets(operationId);
    }

    @Override
    public void markSchemaEvolutionAborted(UUID operationId, Instant now) {
        schemaEvolutionStore.markAborted(operationId, now);
    }

    @Override
    public List<IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord> listPendingSchemaEvolution(int limit) {
        return schemaEvolutionStore.listPending(limit);
    }

    @Override
    public void applySchemaEvolution(
            UUID operationId, List<IndexSchemaEvolutionStore.IndexSchemaTransition> transitions, Instant now) {
        schemaEvolutionStore.apply(operationId, transitions, now);
    }

    @Override
    public Optional<UUID> fenceIndexSchemaMismatch(IndexDescriptor descriptor, String reason, Instant now) {
        return schemaEvolutionStore.fenceMismatch(descriptor, reason, now);
    }

    // --- Statistics (delegate to statisticsStore) ---

    @Override
    public void batchInsertFileStatistics(
            String tenantId, String catalog, String schema, String table,
            List<DataFileStatistics> statistics) {
        statisticsStore.batchInsertFileStatistics(tenantId, catalog, schema, table, statistics);
    }

    @Override
    public void deleteFileStatistics(String tenantId, String catalog, String schema, String table,
                                     List<String> dataFilePaths) {
        statisticsStore.deleteFileStatistics(tenantId, catalog, schema, table, dataFilePaths);
    }

    @Override
    public Map<String, List<FileColumnStatistics>> getFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotId,
            List<String> dataFilePaths, List<String> columns) {
        return statisticsStore.getFileStatistics(tenantId, catalog, schema, table, snapshotId, dataFilePaths, columns);
    }
}
