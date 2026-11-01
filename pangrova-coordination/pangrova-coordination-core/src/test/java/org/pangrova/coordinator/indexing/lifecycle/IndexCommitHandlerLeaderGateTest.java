package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.assembly.serde.DefaultTypeSerializers;

import org.pangrova.coordinator.indexing.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.registry.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.indexing.store.CommitEventRequest;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.CommitLogEntry;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.persistence.api.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all entry points of {@link IndexCommitHandler} are leader-gated.
 *
 * <p>Two processing paths exist:
 * <ol>
 *   <li>{@code pollCommitLog()} — scheduled poller reading from shared commit log</li>
 *   <li>{@code onCommit()} → local queue → {@code processLoop()} — fast-path from connector callbacks</li>
 * </ol>
 * Both must check {@link MaintenanceLeaderElection#tryAcquireOrRenew()} before processing.
 */
@DisplayName("IndexCommitHandler Leader Gate")
class IndexCommitHandlerLeaderGateTest {

    private IndexCommitHandler handler;
    private CommitLogPoller poller;

    @AfterEach
    void tearDown() {
        if (poller != null) {
            poller.close();
        }
        if (handler != null) {
            handler.close();
        }
    }

    @Test
    @DisplayName("pollCommitLog() is no-op when lease not held")
    void pollCommitLogSkipsWhenNotLeader() {
        var env = createEnv(false);
        handler = env.handler;
        poller = env.poller;

        env.store.seedCommitLogEntry(new CommitLogEntry(
                1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));

        env.poller.pollCommitLog();

        assertThat(env.store.processedEventIds)
                .as("pollCommitLog must skip processing when not leader")
                .isEmpty();
    }

    @Test
    @DisplayName("pollCommitLog() processes events when leader")
    void pollCommitLogProcessesWhenLeader() {
        var env = createEnv(true);
        handler = env.handler;
        poller = env.poller;

        env.store.seedCommitLogEntry(new CommitLogEntry(
                1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));

        env.poller.pollCommitLog();

        assertThat(env.store.processedEventIds)
                .as("pollCommitLog must process events when leader")
                .containsExactly(1L);
    }

    @Test
    void readyIndexWithoutMaintenanceAuthorityLeavesCommitPending() {
        var env = createEnv(true);
        handler = env.handler;
        poller = env.poller;
        env.store.readyIndexes = List.of(new IndexDescriptor(
                "index-1", "test-index", "cat", "sch", "tbl", List.of("col1"),
                org.pangrova.spi.indexing.descriptor.IndexType.BLOOM_FILTER, Map.of(), IndexState.READY,
                null, 0, "snap-0", "snap-0", Instant.now(), Instant.now(), "tenant-a"));
        env.store.seedCommitLogEntry(new CommitLogEntry(
                1L, "tenant-a", "cat", "sch", "tbl", "snap-1",
                "[\"file1.parquet\"]", "[]", "APPEND", Instant.now(), null));

        env.poller.pollCommitLog();

        assertThat(env.store.processedEventIds).isEmpty();
        assertThat(env.store.pollUnprocessedCommitEvents(8, 1L))
                .extracting(CommitLogEntry::eventId).containsExactly(1L);
    }

    @Test
    @DisplayName("onCommit + pollCommitLog skips processing when not leader (F-COORD-FEAT-07)")
    void processLoopSkipsWhenNotLeader() {
        var env = createEnv(false);
        handler = env.handler;
        poller = env.poller;

        handler.onCommit(new CommitEvent(
                "tenant-a",
                "cat", "sch", "tbl",
                List.of("file1.parquet"), List.of(),
                Instant.now(), "snap-1", CommitEventType.APPEND, null));

        // F-COORD-FEAT-07: processing now goes through CommitLogPoller, not processLoop
        env.poller.pollCommitLog();

        // processTableCommit should NOT have been called (leader gate)
        assertThat(env.store.findIndexesByTableCallCount.get())
                .as("poller must not process events when not leader")
                .isZero();
    }

    @Test
    @DisplayName("onCommit + pollCommitLog processes events when leader (F-COORD-FEAT-07)")
    void processLoopProcessesWhenLeader() {
        var env = createEnv(true);
        handler = env.handler;
        poller = env.poller;

        handler.onCommit(new CommitEvent(
                "tenant-a",
                "cat", "sch", "tbl",
                List.of("file1.parquet"), List.of(),
                Instant.now(), "snap-1", CommitEventType.APPEND, null));

        // F-COORD-FEAT-07: processing now goes through CommitLogPoller, not processLoop
        env.poller.pollCommitLog();

        // processTableCommit should have been called via the poller
        assertThat(env.store.findIndexesByTableCallCount.get())
                .as("poller must process events from commit log when leader")
                .isPositive();
    }

    @Test
    @DisplayName("onCommit() always persists to commit log regardless of leader status")
    void onCommitAlwaysPersistsToCommitLog() {
        var env = createEnv(false);
        handler = env.handler;
        poller = env.poller;

        handler.onCommit(new CommitEvent(
                "tenant-a",
                "cat", "sch", "tbl",
                List.of("file1.parquet"), List.of(),
                Instant.now(), "snap-1", CommitEventType.APPEND, null));

        // Commit log write should happen even when not leader
        assertThat(env.store.commitLogWriteCount.get())
                .as("onCommit must persist to commit log even when not leader")
                .isEqualTo(1);
    }

    // ── Test infrastructure ─────────────────────────────────────────────

    private TestEnv createEnv(boolean isLeader) {
        var store = new TrackingMetadataStore();
        var lease = new ControllableLease(isLeader);
        var leaderElection = new MaintenanceLeaderElection(lease, "test-coord");

        var catDesc = new CatalogDescriptor(
                "cat", CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        var connector = new StubConnector();
        var catalogHandle = new ConnectorCatalogHandle(catDesc, connector);
        var persistence = new StubMetadataStore(catDesc);
        var registry = new CatalogRegistry(
                persistence, desc -> catalogHandle, null, null);
        registry.initialize();
        var resolver = new ConnectorAccessorResolver(registry);

        var config = new IndexingConfig("/tmp/test-index", false, null, null);
        var engineRegistry = new IndexEngineRegistry(List.of());
        var storageManager = new IndexStorageManager();
        var serializer = new IndexSerializer(DefaultTypeSerializers.createRegistry());
        var buildCtx = new IndexBuildContext(
                config, engineRegistry, storageManager, serializer);
        var buildCoordinator = new IndexBuildCoordinator(buildCtx, null);

        var deps = new IndexCommitContext(
                store,
                new org.pangrova.coordinator.indexing.ddl.IndexBuildServices(
                        buildCoordinator, null),
                resolver);
        var handler = new IndexCommitHandler(
                deps, null, leaderElection, 8);
        // F-COORD-FEAT-07: wire poller to handler's processTableCommit so the
        // end-to-end path (onCommit → commit log → poller → processing) works.
        var poller = new CommitLogPoller(deps, leaderElection, 16, handler::processTableCommit);
        return new TestEnv(handler, poller, store);
    }

    record TestEnv(IndexCommitHandler handler, CommitLogPoller poller, TrackingMetadataStore store) {}

    /**
     * DistributedLease that always grants or always denies the lease.
     * Used to control leader election outcome in tests.
     */
    static final class ControllableLease implements DistributedLease {
        private final boolean allowAcquire;

        ControllableLease(boolean allowAcquire) {
            this.allowAcquire = allowAcquire;
        }

        @Override
        public Optional<LeaseEntry> tryAcquire(String k, String h, Duration d) {
            return allowAcquire
                    ? Optional.of(new LeaseEntry(k, h, 1, Instant.now(), Instant.now().plus(d)))
                    : Optional.empty();
        }

        @Override
        public boolean tryRenew(String k, String h, long e, Duration d) {
            return allowAcquire;
        }

        @Override
        public boolean release(String k, String h, long e) { return true; }

        @Override
        public Optional<LeaseEntry> currentHolder(String k) {
            return Optional.empty();
        }

    }

    /** Metadata store that tracks calls for assertion. */
    static final class TrackingMetadataStore implements IndexMetadataStore {
        final CopyOnWriteArrayList<Long> processedEventIds = new CopyOnWriteArrayList<>();
        final AtomicInteger findIndexesByTableCallCount = new AtomicInteger();
        final AtomicInteger commitLogWriteCount = new AtomicInteger();
        volatile List<IndexDescriptor> readyIndexes = List.of();
        private final CopyOnWriteArrayList<CommitLogEntry> commitLogEntries =
                new CopyOnWriteArrayList<>();

        void seedCommitLogEntry(CommitLogEntry entry) {
            commitLogEntries.add(entry);
        }

        private final AtomicLong eventIdGenerator = new AtomicLong(1000);

        @Override
        public void recordCommitEvent(CommitEventRequest request) {
            commitLogWriteCount.incrementAndGet();
            // F-COORD-FEAT-07: store the entry so CommitLogPoller can read it
            commitLogEntries.add(new CommitLogEntry(
                    eventIdGenerator.getAndIncrement(), request.tenantId(),
                    request.catalogName(), request.schemaName(),
                    request.tableName(), request.snapshotId(), request.addedFilesJson(),
                    request.removedFilesJson(), request.eventType(), request.createdAt(), request.commitFingerprint()));
        }

        @Override
        public List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
            var result = List.copyOf(commitLogEntries);
            return result.subList(0, Math.min(limit, result.size()));
        }

        @Override
        public void markCommitEventProcessed(long eventId, long leaseEpoch) {
            processedEventIds.add(eventId);
            commitLogEntries.removeIf(e -> e.eventId() == eventId);
        }

        @Override
        public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) {
            findIndexesByTableCallCount.incrementAndGet();
            return readyIndexes;
        }

        // ── Remaining no-op implementations ──
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) {
            return Optional.empty();
        }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean b) {
            return List.of();
        }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) {
            return List.of();
        }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public List<String> filterUnindexedFiles(
                String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(
                String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) {
            return List.of();
        }
        @Override public void updateLastVerifiedAt(
                String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(
                String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(
                String tenantId, String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) {
            return List.of();
        }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }

    /** Minimal connector stub. */
    static final class StubConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession s) {
            return new StubConnectorMetadata();
        }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            return null;
        }
    }

    /** Minimal connector metadata stub. */
    static final class StubConnectorMetadata implements ConnectorMetadata {
        @Override public List<String> listSchemaNames(ConnectorSession s) {
            return List.of();
        }
        @Override public List<org.pangrova.spi.connector.metadata.table.SchemaTableName> listTables(
                ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableHandle getTableHandle(
                ConnectorSession s, org.pangrova.spi.connector.metadata.table.SchemaTableName n) {
            return null;
        }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata getTableMetadata(
                ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) {
            return null;
        }
        @Override public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) {
            return Map.of();
        }
        @Override public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(
                ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) {
            return org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN;
        }
    }

    /** Minimal metadata store stub for CatalogRegistry. */
    static final class StubMetadataStore implements MetadataStore {
        private final CatalogDescriptor descriptor;
        StubMetadataStore(CatalogDescriptor d) { this.descriptor = d; }
        @Override public void saveTable(org.pangrova.metadata.api.object.TableMetadata m) {}
        @Override public Optional<org.pangrova.metadata.api.object.TableMetadata> loadTable(
                String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<org.pangrova.metadata.api.object.QualifiedTableRef> searchTables(
                String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return descriptor.name().equals(n) ? Optional.of(descriptor) : Optional.empty();
        }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of(descriptor);
        }
        @Override public Optional<Instant> getLastSyncTime(String c) {
            return Optional.empty();
        }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(org.pangrova.metadata.api.catalog.SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }
}
