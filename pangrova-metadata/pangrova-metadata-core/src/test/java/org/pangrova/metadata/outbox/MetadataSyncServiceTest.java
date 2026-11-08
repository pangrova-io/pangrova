package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.error.MetadataSyncException;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.api.catalog.SyncResult;

/**
 * Tests for {@link MetadataSyncService} covering incremental sync, full sync,
 * retry logic, error accumulation, and cache invalidation.
 */
class MetadataSyncServiceTest {

    private RecordingProjectionTransactions store;
    private RecordingCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService syncService;
    private RecordingMetadataSyncRepository synchronization;

    private static CatalogDescriptor deployment(String name) {
        return new CatalogDescriptor(name, org.pangrova.metadata.api.catalog.CatalogType.FEDERATION, "fixture",
                org.pangrova.metadata.api.catalog.CatalogState.ACTIVE, Map.of(), "", java.util.Set.of(), name + "-incarnation", 1);
    }

    @BeforeEach
    void setUp() {
        store = new RecordingProjectionTransactions(deployment("cat"));
        cache = new RecordingCache();
        dispatcher = new MetadataEventDispatcher();
        synchronization = new RecordingMetadataSyncRepository();
        syncService = new MetadataSyncService(store, store, synchronization, cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        dispatcher.shutdown();
    }

    // ── Helpers ──

    private static TableMetadata table(String catalog, String schema, String name) {
        return new TableMetadata(
                catalog, schema, name,
                List.of(), Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());
    }

    // ── Incremental sync ──

    @Test
    void incrementalSyncAppliesAddChanges() {
        TableMetadata meta = table("cat", "sch", "t1");
        MetadataChange addChange = new MetadataChange(
                MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("cat", "sch", "t1"), meta);
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(addChange);

        SyncResult result = syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(result.catalogName()).isEqualTo("cat");
        assertThat(result.syncType()).isEqualTo("INCREMENTAL");
        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(result.errors()).isZero();
        assertThat(store.loadTable("cat", "sch", "t1")).isPresent();
    }

    @Test
    void incrementalSyncAppliesDeleteChanges() {
        TableMetadata meta = table("cat", "sch", "t1");
        store.saveTable(meta);
        MetadataChange deleteChange = new MetadataChange(
                MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("cat", "sch", "t1"), null);
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(deleteChange);

        SyncResult result = syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(store.loadTable("cat", "sch", "t1")).isEmpty();
    }

    @Test
    void incrementalSyncInvalidatesCache() {
        TableMetadata meta = table("cat", "sch", "t1");
        MetadataChange change = new MetadataChange(
                MetadataChange.ChangeType.UPDATE,
                new QualifiedTableRef("cat", "sch", "t1"), meta);
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(change);

        syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(cache.invalidatedTables).contains("cat:sch:t1");
    }

    @Test
    void incrementalSyncRecordsSyncHistory() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of();

        syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(synchronization.completions).hasSize(1);
        assertThat(synchronization.completions.get(0).result().catalogName()).isEqualTo("cat");
    }

    @Test
    void successfulUnboundedSyncDoesNotInventCheckpoint() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(new MetadataChange(
                MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("cat", "s1", "t1"),
                table("cat", "s1", "t1")));

        syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(synchronization.completions.get(0).nextCheckpoint()).isEmpty();
    }

    // ── Full sync ──

    @Test
    void fullSyncSavesAllTablesFromAllSchemas() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.schemas = List.of("s1", "s2");
        adapter.tablesBySchema.put("s1", List.of("t1"));
        adapter.tablesBySchema.put("s2", List.of("t2", "t3"));
        adapter.tableMetadata.put("s1:t1", table("cat", "s1", "t1"));
        adapter.tableMetadata.put("s2:t2", table("cat", "s2", "t2"));
        adapter.tableMetadata.put("s2:t3", table("cat", "s2", "t3"));

        SyncResult result = syncService.fullSync(deployment("cat"), adapter);

        assertThat(result.syncType()).isEqualTo("FULL");
        assertThat(result.changesApplied()).isEqualTo(3);
        assertThat(result.errors()).isZero();
    }

    @Test
    void fullSyncSavesSchemasToStore() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.schemas = List.of("s1", "s2");

        syncService.fullSync(deployment("cat"), adapter);

        assertThat(store.savedSchemas.get("cat")).containsExactly("s1", "s2");
    }

    @Test
    void fullSyncInvalidatesCacheForEachTable() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.schemas = List.of("s1");
        adapter.tablesBySchema.put("s1", List.of("t1", "t2"));
        adapter.tableMetadata.put("s1:t1", table("cat", "s1", "t1"));
        adapter.tableMetadata.put("s1:t2", table("cat", "s1", "t2"));

        syncService.fullSync(deployment("cat"), adapter);

        assertThat(cache.invalidatedTables).containsExactlyInAnyOrder("cat:s1:t1", "cat:s1:t2");
    }

    // ── Error accumulation ──

    @Test
    void fullSyncAccumulatesErrorsWithoutStopping() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.schemas = List.of("s1");
        adapter.tablesBySchema.put("s1", List.of("t1", "t2"));
        adapter.tableMetadata.put("s1:t1", table("cat", "s1", "t1"));
        // t2 missing from tableMetadata -> getTable will throw

        SyncResult result = syncService.fullSync(deployment("cat"), adapter);

        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(result.errors()).isEqualTo(1);
        assertThat(result.errorMessages()).hasSize(1);
    }

    @Test
    void incrementalSyncAccumulatesErrorsPerChange() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        TableMetadata meta = table("cat", "sch", "t1");
        MetadataChange goodChange = new MetadataChange(
                MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("cat", "sch", "t1"), meta);
        MetadataChange badChange = new MetadataChange(
                MetadataChange.ChangeType.UPDATE,
                new QualifiedTableRef("cat", "sch", "t2"), null);
        adapter.changes = List.of(goodChange, badChange);
        // badChange has null metadata -> store.saveTable(null) will throw

        SyncResult result = syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(result.errors()).isEqualTo(1);
    }

    // ── Retry logic ──

    @Test
    void syncWithRetrySucceedsOnFirstAttempt() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of();

        SyncResult result = syncService.syncWithRetry(deployment("cat"), adapter);

        assertThat(result.syncType()).isEqualTo("INCREMENTAL");
    }

    @Test
    void syncWithRetryExhaustsRetriesAndThrows() {
        FailingCatalogAdapter adapter = new FailingCatalogAdapter(4);

        assertThatThrownBy(() -> syncService.syncWithRetry(deployment("cat"), adapter))
                .isInstanceOf(MetadataSyncException.class);
    }

    @Test
    void syncWithRetryPreservesOriginalExceptionAsCause() {
        FailingCatalogAdapter adapter = new FailingCatalogAdapter(4);

        assertThatThrownBy(() -> syncService.syncWithRetry(deployment("cat"), adapter))
                .isInstanceOf(MetadataSyncException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .cause()
                .hasMessageContaining("Simulated failure");
    }

    @Test
    void syncWithRetryPreservesCheckedExceptionCauseChain() {
        TypedFailingCatalogAdapter adapter = new TypedFailingCatalogAdapter(
                new java.io.IOException("Network timeout connecting to catalog backend"));

        assertThatThrownBy(() -> syncService.syncWithRetry(deployment("cat"), adapter))
                .isInstanceOf(MetadataSyncException.class)
                .hasRootCauseInstanceOf(java.io.IOException.class)
                .rootCause()
                .hasMessageContaining("Network timeout");
    }

    @Test
    void syncWithRetryExceptionCarriesCatalogNameAndAttemptCount() {
        FailingCatalogAdapter adapter = new FailingCatalogAdapter(4);

        var catalogStore = new RecordingProjectionTransactions(deployment("my-catalog"));
        var catalogService = new MetadataSyncService(catalogStore, catalogStore, synchronization, cache, dispatcher);
        assertThatThrownBy(() -> catalogService.syncWithRetry(deployment("my-catalog"), adapter))
                .isInstanceOf(MetadataSyncException.class)
                .satisfies(thrown -> {
                    MetadataSyncException ex = (MetadataSyncException) thrown;
                    assertThat(ex.getCatalogName()).isEqualTo("my-catalog");
                    assertThat(ex.getAttemptsExhausted()).isEqualTo(3);
                    assertThat(ex.getErrorCode().getCode()).isEqualTo("METADATA-0009");
                });
    }

    // ── Duration tracking ──

    @Test
    void syncResultContainsNonNegativeDuration() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of();

        SyncResult result = syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(result.durationMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void incrementalSyncUsesPersistentStore() {
        TableMetadata meta = table("cat", "sch", "t1");
        MetadataChange change = new MetadataChange(
                MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("cat", "sch", "t1"), meta);
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(change);

        SyncResult result = syncService.incrementalSync(deployment("cat"), adapter);

        assertThat(result.changesApplied()).isEqualTo(1);
    }

    @Test
    void partialFailureIsRetriedAndNeverReturnedAsSuccessfulFullSync() {
        StubCatalogAdapter adapter = new StubCatalogAdapter();
        adapter.changes = List.of(new MetadataChange(MetadataChange.ChangeType.ADD,
                new QualifiedTableRef("cat", "sch", "broken"), null));
        adapter.schemas = List.of("sch");
        adapter.tablesBySchema.put("sch", List.of("broken"));
        // Both incremental projection and subsequent full enumeration fail for this table.
        assertThatThrownBy(() -> syncService.syncWithRetry(deployment("cat"), adapter))
                .isInstanceOf(MetadataSyncException.class)
                .hasMessageContaining("catalog=cat after 3 attempts")
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(synchronization.completions).hasSize(4).allSatisfy(completion -> {
            assertThat(completion.result().errors()).isEqualTo(1);
            assertThat(completion.nextCheckpoint()).isEmpty();
        });
        assertThat(synchronization.completions.get(3).result().syncType()).isEqualTo("FULL");
    }

    // ── Test doubles ──

    static final class StubCatalogAdapter implements FederatedCatalog {
        List<MetadataChange> changes = List.of();
        List<String> schemas = List.of();
        final Map<String, List<String>> tablesBySchema = new HashMap<>();
        final Map<String, TableMetadata> tableMetadata = new HashMap<>();

        @Override
        public List<String> listSchemas() { return schemas; }

        @Override
        public List<String> listTables(String schema) {
            return tablesBySchema.getOrDefault(schema, List.of());
        }

        @Override
        public TableMetadata getTable(String schema, String table) {
            TableMetadata meta = tableMetadata.get(schema + ":" + table);
            if (meta == null) {
                throw new RuntimeException("Table not found: " + schema + "." + table);
            }
            return meta;
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) { return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(changes, java.util.Optional.empty()); }

        @Override
        public void close() { }
    }

    static final class FailingCatalogAdapter implements FederatedCatalog {
        private final int failCount;
        private int callCount;

        FailingCatalogAdapter(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public List<String> listSchemas() {
            // Full sync also fails — simulates a catalog that is completely unreachable
            throw new RuntimeException("Simulated failure: catalog unreachable");
        }

        @Override
        public List<String> listTables(String schema) { return List.of(); }

        @Override
        public TableMetadata getTable(String schema, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            callCount++;
            if (callCount <= failCount) {
                throw new RuntimeException("Simulated failure #" + callCount);
            }
            return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(List.of(), java.util.Optional.empty());
        }

        @Override
        public void close() { }
    }

    /**
     * Adapter that wraps a specific exception type to verify cause chain preservation
     * for non-RuntimeException failures (e.g., IOException, checked exceptions).
     */
    static final class TypedFailingCatalogAdapter implements FederatedCatalog {
        private final Exception exceptionToThrow;

        TypedFailingCatalogAdapter(Exception exceptionToThrow) {
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        public List<String> listSchemas() {
            // Full sync also fails — simulates a catalog that is completely unreachable
            if (exceptionToThrow instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(exceptionToThrow);
        }

        @Override
        public List<String> listTables(String schema) { return List.of(); }

        @Override
        public TableMetadata getTable(String schema, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            if (exceptionToThrow instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(exceptionToThrow);
        }

        @Override
        public void close() { }
    }

    static final class RecordingCache implements MetadataCache {
        final List<String> invalidatedTables = new CopyOnWriteArrayList<>();

        @Override
        public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void putTable(String catalog, String schema, String table, TableMetadata metadata) { }

        @Override
        public void invalidateTable(String catalog, String schema, String table) {
            invalidatedTables.add(catalog + ":" + schema + ":" + table);
        }

        @Override
        public void invalidateCatalog(String catalog) { }

        @Override
        public CacheStats getStats() {
            return new CacheStats(0L, 0L, 0L, 0L);
        }
    }
}
