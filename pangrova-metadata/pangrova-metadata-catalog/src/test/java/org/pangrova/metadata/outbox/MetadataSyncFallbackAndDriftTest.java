package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.SyncResult;

/**
 * Legacy component checks for sync fallback (incremental failure → full sync).
 * Drift detection/correction is exercised against actual PostgreSQL inventory in
 * {@link MetadataDriftSamplingIntegrationTest}.
 */
@DisplayName("MetadataSync Fallback")
class MetadataSyncFallbackAndDriftTest {

    private RecordingProjectionTransactions store;
    private RecordingCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService syncService;
    private RecordingMetadataSyncRepository synchronization;

    private static CatalogDescriptor deployment(String name) {
        return new CatalogDescriptor(name, org.pangrova.metadata.api.enums.CatalogType.FEDERATION, "fixture",
                org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", java.util.Set.of(), name + "-incarnation", 1);
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

    @Test
    void cancelledSourceCannotFallBackToFullSyncOrAdvanceTheCursor() {
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var stopped = new java.util.concurrent.CancellationException("source closed");
        var catalog = new FailingIncrementalCatalog() {
            @Override public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
                reads.incrementAndGet();
                throw stopped;
            }
            @Override public List<String> listSchemas() {
                throw new AssertionError("Full scan after cancellation");
            }
        };
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> syncService.syncWithRetry(deployment("cat"), catalog))
                .isSameAs(stopped);
        assertThat(reads.get()).isOne();
        assertThat(synchronization.completions).isEmpty();
    }

    // ── Sync fallback tests ──

    @Test
    @DisplayName("syncWithRetry falls back to fullSync when all incremental attempts fail")
    void syncWithRetryFallsBackToFullSync() {
        // Adapter that fails incremental (readChangesSince throws) but succeeds full sync
        var adapter = new FailingIncrementalCatalog();
        adapter.schemas = List.of("s1");
        adapter.tablesBySchema.put("s1", List.of("t1"));
        adapter.tableMetadata.put("s1:t1", table("cat", "s1", "t1"));

        SyncResult result = syncService.syncWithRetry(deployment("cat"), adapter);

        assertThat(result.syncType()).isEqualTo("FULL");
        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(store.loadTable("cat", "s1", "t1")).isPresent();
    }

    @Test
    @DisplayName("syncWithRetry succeeds on second incremental attempt")
    void syncWithRetrySucceedsOnRetry() {
        var adapter = new RetryableTestCatalog(1);

        SyncResult result = syncService.syncWithRetry(deployment("cat"), adapter);

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
    static class FailingIncrementalCatalog implements FederatedCatalog {
        List<String> schemas = List.of();
        final Map<String, List<String>> tablesBySchema = new HashMap<>();
        final Map<String, TableMetadata> tableMetadata = new HashMap<>();

        @Override public List<String> listSchemas() { return schemas; }
        @Override public List<String> listTables(String schema) {
            return tablesBySchema.getOrDefault(schema, List.of());
        }
        @Override public java.util.Optional<TableMetadata> observeTable(String schema, String table) {
            return java.util.Optional.of(getTable(schema, table));
        }

        @Override public TableMetadata getTable(String schema, String table) {
            TableMetadata meta = tableMetadata.get(schema + ":" + table);
            if (meta == null) throw new RuntimeException("Not found: " + schema + "." + table);
            return meta;
        }
        @Override public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }
        @Override public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            throw new RuntimeException("Incremental sync unavailable");
        }
        @Override public void close() {}
    }

    /** Adapter that fails N times then succeeds. */
    static final class RetryableTestCatalog implements FederatedCatalog {
        private final int failCount;
        private int callCount;

        RetryableTestCatalog(int failCount) { this.failCount = failCount; }

        @Override public List<String> listSchemas() { return List.of(); }
        @Override public List<String> listTables(String schema) { return List.of(); }
        @Override public TableMetadata getTable(String schema, String table) {
            throw new UnsupportedOperationException();
        }
        @Override public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }
        @Override public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            callCount++;
            if (callCount <= failCount) throw new RuntimeException("Fail #" + callCount);
            return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(List.of(), java.util.Optional.empty());
        }
        @Override public void close() {}
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
