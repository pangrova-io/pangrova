package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.index.CommitEventRequest;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.BreakerState;
import org.pangrova.metadata.index.model.CacheEvent;
import org.pangrova.metadata.index.model.CommitLogEntry;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.metadata.index.model.IndexFileSelection;
import org.pangrova.metadata.index.model.IndexGenerationManifest;
import org.pangrova.metadata.index.model.IndexPublicationRequest;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * JDBC-backed implementation of {@link IndexMetadataStore}.
 *
 * <p>Facade that delegates to focused sub-stores, each handling a single
 * responsibility domain:</p>
 * <ul>
 *   <li>{@link JdbcIndexDescriptorStore} — index CRUD and schema evolution</li>
 *   <li>{@link JdbcIndexDescriptorReadStore} — descriptor state and identity queries</li>
 *   <li>{@link JdbcIndexFileMappingStore} — file mapping CRUD and batch queries</li>
 *   <li>{@link JdbcIndexStateStore} — build claim coordination</li>
 *   <li>{@link JdbcIndexStatisticsStore} — file column statistics (L0 pruning)</li>
 *   <li>{@link JdbcIndexGcStore} — GC queries, cache events, breaker state</li>
 *   <li>{@link JdbcIndexCommitQueue} — durable commit-event queue and fenced heads</li>
 * </ul>
 */
public final class JdbcIndexMetadataStore implements IndexMetadataStore {

    private final JdbcIndexDescriptorStore descriptorStore;
    private final JdbcIndexDescriptorReadStore descriptorReadStore;
    private final JdbcIndexFileMappingStore fileMappingStore;
    private final JdbcIndexStateStore stateStore;
    private final JdbcIndexStatisticsStore statisticsStore;
    private final JdbcIndexGcStore gcStore;
    private final JdbcIndexCommitQueue commitQueue;
    private final JdbcIndexPublicationStore publicationStore;
    private final ThreadLocal<Set<String>> heldLifecycleLocks = ThreadLocal.withInitial(HashSet::new);

    /** Creates the facade over the focused JDBC sub-stores backed by the given data source. */
    public JdbcIndexMetadataStore(DataSource dataSource) {
        this.descriptorStore = new JdbcIndexDescriptorStore(dataSource);
        this.descriptorReadStore = new JdbcIndexDescriptorReadStore(dataSource);
        this.fileMappingStore = new JdbcIndexFileMappingStore(dataSource);
        this.stateStore = new JdbcIndexStateStore(dataSource);
        this.statisticsStore = new JdbcIndexStatisticsStore(dataSource);
        this.gcStore = new JdbcIndexGcStore(dataSource);
        this.commitQueue = new JdbcIndexCommitQueue(dataSource);
        this.publicationStore = new JdbcIndexPublicationStore(dataSource);
    }

    // --- IndexDescriptor CRUD (delegate to descriptorStore) ---

    @Override
    public void registerIndex(IndexDescriptor descriptor) {
        descriptorStore.registerIndex(descriptor);
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
        return descriptorStore.beginIndexGeneration(
                indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration);
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
    public boolean isIndexGenerationBuildable(String indexId, int generation) {
        return descriptorStore.isIndexGenerationBuildable(indexId, generation);
    }

    @Override
    public boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        return descriptorStore.isIndexSnapshotBuildable(indexId, snapshotId);
    }

    @Override
    public IndexGenerationManifest publishIndexGeneration(
            IndexPublicationRequest request) {
        return publicationStore.publish(request);
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
    public void purgeIndex(String indexId) {
        withIndexLifecycleLock(indexId, () -> descriptorStore.purgeIndex(indexId));
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
    public List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        return descriptorReadStore.findIndexesByStatePaginated(state, offset, limit);
    }

    // --- IndexFileMapping CRUD (delegate to fileMappingStore) ---

    @Override
    public void registerIndexFile(IndexFileMapping mapping) {
        fileMappingStore.registerIndexFile(mapping);
    }

    @Override
    public void batchRegisterIndexFiles(List<IndexFileMapping> mappings) {
        fileMappingStore.batchRegisterIndexFiles(mappings);
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
        Map<String, IndexFileSelection> activeSelections = new java.util.HashMap<>();
        for (var selection : selections.entrySet()) {
            Optional<IndexGenerationManifest> active =
                    publicationStore.findActive(selection.getKey());
            if (active.isPresent() && selection.getValue().matches(active.get())) {
                activeSelections.put(selection.getKey(), selection.getValue());
            }
        }
        return fileMappingStore.batchGetIndexArtifacts(
                Map.copyOf(activeSelections), dataFilePaths);
    }

    @Override
    public void removeIndexFileMapping(String indexId, String dataFilePath) {
        fileMappingStore.removeIndexFileMapping(indexId, dataFilePath);
    }

    @Override
    public boolean removeIndexFileMappingIfState(IndexFileMapping mapping,
                                                  IndexState expectedState,
                                                  int expectedGeneration) {
        return fileMappingStore.removeIndexFileMappingIfState(
                mapping, expectedState, expectedGeneration);
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

    // --- Build claims (delegate to stateStore) ---

    @Override
    public boolean claimBuild(String indexId, int generation, String ownerId, Instant now) {
        return stateStore.claimBuild(indexId, generation, ownerId, now);
    }

    @Override
    public void releaseBuildClaim(String indexId, int generation) {
        stateStore.releaseBuildClaim(indexId, generation);
    }

    @Override
    public void heartbeatBuildClaim(String indexId, int generation, Instant now) {
        stateStore.heartbeatBuildClaim(indexId, generation, now);
    }

    @Override
    public int expireStaleBuilds(Instant olderThan) {
        return stateStore.expireStaleBuilds(olderThan);
    }

    // --- GC queries (delegate to gcStore) ---

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
