package org.pangrova.metadata.outbox;

import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.metadata.api.catalog.SyncResult;

/**
 * Validates G-META-04 fix: sync watermark only advances after at least one
 * change is successfully applied.
 */
class MetadataSyncWatermarkTest {

    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new MetadataEventDispatcher();
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    @Test
    void watermarkDoesNotAdvanceOnZeroAppliedChanges() {
        TrackingMetadataStore store = new TrackingMetadataStore();
        NoOpCache cache = new NoOpCache();

        MetadataSyncService service = new MetadataSyncService(store, cache, dispatcher);

        // Adapter returns empty changes — nothing to apply
        EmptyAdapter adapter = new EmptyAdapter();
        SyncResult result = service.incrementalSync("test-catalog", adapter);

        assertThat(result.changesApplied()).isEqualTo(0);
        // Watermark should NOT have been updated
        assertThat(store.syncTimeUpdated).isFalse();
    }

    @Test
    void watermarkAdvancesWhenChangesApplied() {
        TrackingMetadataStore store = new TrackingMetadataStore();
        NoOpCache cache = new NoOpCache();

        MetadataSyncService service = new MetadataSyncService(store, cache, dispatcher);

        // Adapter returns one change
        OneChangeAdapter adapter = new OneChangeAdapter();
        SyncResult result = service.incrementalSync("test-catalog", adapter);

        assertThat(result.changesApplied()).isEqualTo(1);
        // Watermark SHOULD have been updated
        assertThat(store.syncTimeUpdated).isTrue();
    }

    // ── Stubs ─────────────────────────────────────────────────────

    static final class TrackingMetadataStore implements MetadataStore {
        boolean syncTimeUpdated = false;

        @Override
        public void saveTable(TableMetadata metadata) {}

        @Override
        public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) {}

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {}

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.empty();
        }

        @Override
        public void deleteCatalogDescriptor(String name) {}

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.of(Instant.EPOCH);
        }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) {
            syncTimeUpdated = true;
        }

        @Override
        public void recordSyncHistory(SyncResult result) {}

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) {}
    }

    static final class NoOpCache implements MetadataCache {
        @Override
        public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void putTable(String catalog, String schema, String table, TableMetadata metadata) {}

        @Override
        public void invalidateTable(String catalog, String schema, String table) {}

        @Override
        public void invalidateCatalog(String catalog) {}

        @Override
        public CacheStats getStats() { return new CacheStats(0, 0, 0, 0); }
    }

    static final class EmptyAdapter implements FederatedCatalog {
        @Override
        public List<String> listSchemas() { return List.of(); }

        @Override
        public List<String> listTables(String schema) { return List.of(); }

        @Override
        public TableMetadata getTable(String schema, String table) { return null; }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<MetadataChange> getChangesSince(Instant since) {
            return List.of();
        }

        @Override
        public void close() {}
    }

    static final class OneChangeAdapter implements FederatedCatalog {
        @Override
        public List<String> listSchemas() { return List.of("public"); }

        @Override
        public List<String> listTables(String schema) { return List.of("orders"); }

        @Override
        public TableMetadata getTable(String schema, String table) {
            return new TableMetadata("test-catalog", schema, table,
                    List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                    Map.of(), List.of(), Map.of(), null, null);
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return new TableStatistics(100L, 1024L, -1, Map.of());
        }

        @Override
        public List<MetadataChange> getChangesSince(Instant since) {
            TableMetadata meta = getTable("public", "orders");
            return List.of(new MetadataChange(
                    MetadataChange.ChangeType.ADD,
                    new QualifiedTableRef("test-catalog", "public", "orders"),
                    meta));
        }

        @Override
        public void close() {}
    }
}
