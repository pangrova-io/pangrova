package org.pangrova.metadata.core.outbox;

import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.api.catalog.SyncResult;

/**
 * Legacy component checks for sync fallback (incremental failure → full sync).
 * Drift detection/correction is exercised against actual PostgreSQL inventory in
 * {@link MetadataDriftSamplingIntegrationTest}.
 */
@DisplayName("MetadataSync Fallback")
class MetadataSyncFallbackAndDriftTest {

    private InMemoryStore store;
    private RecordingCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService syncService;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        cache = new RecordingCache();
        dispatcher = new MetadataEventDispatcher();
        syncService = new MetadataSyncService(store, cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        dispatcher.shutdown();
    }

    // ── Sync fallback tests ──

    @Test
    @DisplayName("syncWithRetry falls back to fullSync when all incremental attempts fail")
    void syncWithRetryFallsBackToFullSync() {
        // Adapter that fails incremental (getChangesSince throws) but succeeds full sync
        var adapter = new IncrementalFailingAdapter();
        adapter.schemas = List.of("s1");
        adapter.tablesBySchema.put("s1", List.of("t1"));
        adapter.tableMetadata.put("s1:t1", table("cat", "s1", "t1"));

        SyncResult result = syncService.syncWithRetry("cat", adapter);

        assertThat(result.syncType()).isEqualTo("FULL");
        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(store.loadTable("cat", "s1", "t1")).isPresent();
    }

    @Test
    @DisplayName("syncWithRetry succeeds on second incremental attempt")
    void syncWithRetrySucceedsOnRetry() {
        var adapter = new FailNTimesAdapter(1);

        SyncResult result = syncService.syncWithRetry("cat", adapter);

        assertThat(result.syncType()).isEqualTo("INCREMENTAL");
    }

    // ── Helpers ──

    private static TableMetadata table(String catalog, String schema, String name) {
        return new TableMetadata(
                catalog, schema, name,
                List.of(), Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());
    }

    // ── Test doubles ──

    /** Adapter that always fails incremental sync but succeeds full sync. */
    static final class IncrementalFailingAdapter implements FederatedCatalog {
        List<String> schemas = List.of();
        final Map<String, List<String>> tablesBySchema = new HashMap<>();
        final Map<String, TableMetadata> tableMetadata = new HashMap<>();

        @Override public List<String> listSchemas() { return schemas; }
        @Override public List<String> listTables(String schema) {
            return tablesBySchema.getOrDefault(schema, List.of());
        }
        @Override public TableMetadata getTable(String schema, String table) {
            TableMetadata meta = tableMetadata.get(schema + ":" + table);
            if (meta == null) throw new RuntimeException("Not found: " + schema + "." + table);
            return meta;
        }
        @Override public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }
        @Override public List<MetadataChange> getChangesSince(Instant since) {
            throw new RuntimeException("Incremental sync unavailable");
        }
        @Override public void close() {}
    }

    /** Adapter that fails N times then succeeds. */
    static final class FailNTimesAdapter implements FederatedCatalog {
        private final int failCount;
        private int callCount;

        FailNTimesAdapter(int failCount) { this.failCount = failCount; }

        @Override public List<String> listSchemas() { return List.of(); }
        @Override public List<String> listTables(String schema) { return List.of(); }
        @Override public TableMetadata getTable(String schema, String table) {
            throw new UnsupportedOperationException();
        }
        @Override public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }
        @Override public List<MetadataChange> getChangesSince(Instant since) {
            callCount++;
            if (callCount <= failCount) throw new RuntimeException("Fail #" + callCount);
            return List.of();
        }
        @Override public void close() {}
    }

    static final class InMemoryStore implements MetadataStore {
        private final Map<String, TableMetadata> tables = new HashMap<>();
        private final Map<String, Instant> syncTimes = new HashMap<>();
        final List<SyncResult> syncHistory = new ArrayList<>();
        final Map<String, List<String>> savedSchemas = new HashMap<>();

        @Override public void saveTable(TableMetadata metadata) {
            if (metadata == null) throw new NullPointerException("metadata must not be null");
            tables.put(metadata.catalog() + ":" + metadata.schema() + ":" + metadata.table(), metadata);
        }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) {
            return Optional.ofNullable(tables.get(c + ":" + s + ":" + t));
        }
        @Override public void deleteTable(String c, String s, String t) { tables.remove(c + ":" + s + ":" + t); }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.ofNullable(syncTimes.get(c)); }
        @Override public void updateSyncTime(String c, Instant t) { syncTimes.put(c, t); }
        @Override public void recordSyncHistory(SyncResult r) { syncHistory.add(r); }
        @Override public void saveSchemas(String c, List<String> s) { savedSchemas.put(c, List.copyOf(s)); }
    }

    static final class RecordingCache implements MetadataCache {
        final List<String> invalidatedTables = new CopyOnWriteArrayList<>();

        @Override public Optional<TableMetadata> getTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void putTable(String c, String s, String t, TableMetadata m) {}
        @Override public void invalidateTable(String c, String s, String t) {
            invalidatedTables.add(c + ":" + s + ":" + t);
        }
        @Override public void invalidateCatalog(String c) {}
        @Override public CacheStats getStats() { return new CacheStats(0L, 0L, 0L, 0L); }
    }
}
