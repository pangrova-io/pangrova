package org.pangrova.coordinator.query.cache;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.coordinator.query.cache.CacheWarmupService.WarmupResult;
import org.pangrova.coordinator.query.statement.TableWarmupExecutor;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.SplitProperties;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheWarmupService} (G-CACHE-06: proactive cache warmup
 * and hot-table pin policy).
 *
 * <p>Coverage:
 * <ul>
 *   <li>Single-table warmup via FQN.</li>
 *   <li>Multi-table warmup via comma-separated session property value.</li>
 *   <li>Invalid FQN rejected gracefully.</li>
 *   <li>Per-table failure isolation (one bad table doesn't prevent others).</li>
 *   <li>Pin/unpin lifecycle and policy queries.</li>
 *   <li>warmupAndPinTables auto-pins on success, skips on failure.</li>
 *   <li>Session property constant.</li>
 *   <li>FQN parser edge cases.</li>
 * </ul>
 */
class CacheWarmupServiceTest {

    private static org.pangrova.coordinator.query.session.CoordinatorSessionContext statementSession() {
        return context();
    }

    private static final String CATALOG = "testcat";
    private static final String SCHEMA = "analytics";
    private static final String TABLE = "orders";
    private static final String FQN = CATALOG + "." + SCHEMA + "." + TABLE;

    private MemTableStore store;
    private CatalogRegistry registry;
    private CacheWarmupService service;

    @BeforeEach
    void setUp() {
        store = new MemTableStore();
        store.addTable(SCHEMA, TABLE, List.of("id", "amount"), 10, 2048L);
        store.addTable(SCHEMA, "customers", List.of("cid", "name"), 5, 1024L);
        registry = buildRegistry(store);
        service = new CacheWarmupService(new TableWarmupExecutor(registry));
    }

    // --- Warmup tests ---

    @Test
    void warmupSingleTable_success() {
        WarmupResult result = service.warmupSingleTable(FQN, statementSession());

        assertThat(result.success()).isTrue();
        assertThat(result.tableName()).isEqualTo(FQN);
        assertThat(result.errorMessage()).isNull();
        assertThat(result.splitsProcessed()).isEqualTo(1L);
        assertThat(result.rowsTouched()).isEqualTo(10L);
        assertThat(result.bytesPrefetched()).isEqualTo(2048L);
        assertThat(result.durationMs()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void warmupTables_multipleTablesCommaSeparated() {
        String tableList = FQN + " , " + CATALOG + ".analytics.customers";

        List<WarmupResult> results = service.warmupTables(tableList, statementSession());

        assertThat(results).hasSize(2);
        assertThat(results.get(0).success()).isTrue();
        assertThat(results.get(0).tableName()).isEqualTo(FQN);
        assertThat(results.get(0).rowsTouched()).isEqualTo(10L);
        assertThat(results.get(1).success()).isTrue();
        assertThat(results.get(1).tableName()).isEqualTo(CATALOG + ".analytics.customers");
        assertThat(results.get(1).rowsTouched()).isEqualTo(5L);
    }

    @Test
    void warmupTables_emptyString_returnsEmptyList() {
        assertThat(service.warmupTables("", statementSession())).isEmpty();
        assertThat(service.warmupTables(null, statementSession())).isEmpty();
        assertThat(service.warmupTables("  ", statementSession())).isEmpty();
    }

    @Test
    void warmupSingleTable_invalidFqn_returnsFailure() {
        WarmupResult result = service.warmupSingleTable("just_a_name", statementSession());

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).contains("catalog.schema.table");
    }

    @Test
    void warmupSingleTable_tableNotFound_returnsFailure() {
        WarmupResult result = service.warmupSingleTable(CATALOG + ".analytics.nonexistent", statementSession());

        assertThat(result.success()).isFalse();
        assertThat(result.errorMessage()).isNotNull();
    }

    @Test
    void warmupTables_oneFailsOtherSucceeds() {
        String tableList = CATALOG + ".analytics.nonexistent," + FQN;

        List<WarmupResult> results = service.warmupTables(tableList, statementSession());

        assertThat(results).hasSize(2);
        assertThat(results.get(0).success()).isFalse();
        assertThat(results.get(1).success()).isTrue();
        assertThat(results.get(1).rowsTouched()).isEqualTo(10L);
    }

    // --- Pin policy tests ---

    @Test
    void pinTable_hotTable_registersPolicy() {
        service.pinTable(FQN, CachePinPolicy.HOT_TABLE);

        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.HOT_TABLE);
        assertThat(service.hasPinnedTables()).isTrue();
        assertThat(service.pinnedTableNames()).contains(FQN.toLowerCase());
    }

    @Test
    void pinTable_none_removesPin() {
        service.pinTable(FQN, CachePinPolicy.HOT_TABLE);
        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.HOT_TABLE);

        service.pinTable(FQN, CachePinPolicy.NONE);
        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.NONE);
        assertThat(service.hasPinnedTables()).isFalse();
    }

    @Test
    void unpinTable_removesExistingPin() {
        service.pinTable(FQN, CachePinPolicy.EXCLUSIVE);
        service.unpinTable(FQN);

        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.NONE);
    }

    @Test
    void getPinPolicy_unknownTable_returnsNone() {
        assertThat(service.getPinPolicy("unknown.schema.table"))
                .isEqualTo(CachePinPolicy.NONE);
    }

    @Test
    void getPinPolicy_null_returnsNone() {
        assertThat(service.getPinPolicy(null)).isEqualTo(CachePinPolicy.NONE);
    }

    @Test
    void pinnedTableSnapshot_returnsImmutableCopy() {
        service.pinTable(FQN, CachePinPolicy.HOT_TABLE);
        Map<String, CachePinPolicy> snapshot = service.pinnedTableSnapshot();

        assertThat(snapshot).hasSize(1);
        assertThat(snapshot.get(FQN.toLowerCase())).isEqualTo(CachePinPolicy.HOT_TABLE);
    }

    @Test
    void pinTable_caseInsensitive() {
        service.pinTable("CAT.SCHEMA.TABLE", CachePinPolicy.HOT_TABLE);
        assertThat(service.getPinPolicy("cat.schema.table"))
                .isEqualTo(CachePinPolicy.HOT_TABLE);
    }

    // --- Warmup + pin combined tests ---

    @Test
    void warmupAndPinTables_pinsSuccessfulWarmups() {
        List<WarmupResult> results = service.warmupAndPinTables(FQN, statementSession());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).success()).isTrue();
        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.HOT_TABLE);
    }

    @Test
    void warmupAndPinTables_doesNotPinFailedWarmups() {
        String tableList = CATALOG + ".analytics.ghost," + FQN;

        List<WarmupResult> results = service.warmupAndPinTables(tableList, statementSession());

        assertThat(results).hasSize(2);
        // Failed table not pinned.
        assertThat(service.getPinPolicy(CATALOG + ".analytics.ghost"))
                .isEqualTo(CachePinPolicy.NONE);
        // Successful table pinned.
        assertThat(service.getPinPolicy(FQN)).isEqualTo(CachePinPolicy.HOT_TABLE);
    }

    // --- Session property constant ---

    @Test
    void sessionPropertyConstant_isCorrect() {
        assertThat(CacheWarmupService.SESSION_PROPERTY)
                .isEqualTo("cache_warmup_tables");
    }

    // --- FQN parser tests ---

    @Test
    void parseFqn_validThreePart() {
        String[] parts = CacheWarmupService.parseFullyQualifiedName("cat.sch.tbl");
        assertThat(parts).containsExactly("cat", "sch", "tbl");
    }

    @Test
    void parseFqn_whitespace_trimmed() {
        String[] parts = CacheWarmupService.parseFullyQualifiedName("  cat.sch.tbl  ");
        assertThat(parts).containsExactly("cat", "sch", "tbl");
    }

    @Test
    void parseFqn_noDot_returnsNull() {
        assertThat(CacheWarmupService.parseFullyQualifiedName("table_only")).isNull();
    }

    @Test
    void parseFqn_oneDot_returnsNull() {
        assertThat(CacheWarmupService.parseFullyQualifiedName("schema.table")).isNull();
    }

    @Test
    void parseFqn_fourParts_returnsNull() {
        assertThat(CacheWarmupService.parseFullyQualifiedName("a.b.c.d")).isNull();
    }

    @Test
    void parseFqn_emptyParts_returnsNull() {
        assertThat(CacheWarmupService.parseFullyQualifiedName(".schema.table")).isNull();
        assertThat(CacheWarmupService.parseFullyQualifiedName("catalog..table")).isNull();
        assertThat(CacheWarmupService.parseFullyQualifiedName("catalog.schema.")).isNull();
    }

    @Test
    void parseFqn_nullOrBlank_returnsNull() {
        assertThat(CacheWarmupService.parseFullyQualifiedName(null)).isNull();
        assertThat(CacheWarmupService.parseFullyQualifiedName("")).isNull();
        assertThat(CacheWarmupService.parseFullyQualifiedName("   ")).isNull();
    }

    // --- CachePinPolicy.fromString tests ---

    @Test
    void cachePinPolicy_fromString_caseInsensitive() {
        assertThat(CachePinPolicy.fromString("hot_table")).isEqualTo(CachePinPolicy.HOT_TABLE);
        assertThat(CachePinPolicy.fromString("HOT_TABLE")).isEqualTo(CachePinPolicy.HOT_TABLE);
        assertThat(CachePinPolicy.fromString("Hot_Table")).isEqualTo(CachePinPolicy.HOT_TABLE);
    }

    @Test
    void cachePinPolicy_fromString_none() {
        assertThat(CachePinPolicy.fromString("none")).isEqualTo(CachePinPolicy.NONE);
        assertThat(CachePinPolicy.fromString("NONE")).isEqualTo(CachePinPolicy.NONE);
    }

    @Test
    void cachePinPolicy_fromString_exclusive() {
        assertThat(CachePinPolicy.fromString("exclusive")).isEqualTo(CachePinPolicy.EXCLUSIVE);
    }

    @Test
    void cachePinPolicy_fromString_nullOrBlank_returnsNone() {
        assertThat(CachePinPolicy.fromString(null)).isEqualTo(CachePinPolicy.NONE);
        assertThat(CachePinPolicy.fromString("")).isEqualTo(CachePinPolicy.NONE);
        assertThat(CachePinPolicy.fromString("  ")).isEqualTo(CachePinPolicy.NONE);
    }

    @Test
    void cachePinPolicy_fromString_invalid_throws() {
        try {
            CachePinPolicy.fromString("bogus");
            assertThat(false).as("should have thrown").isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("bogus");
        }
    }

    // --- Test fixtures (mirrors TableWarmupExecutorTest structure) ---

    private static CatalogRegistry buildRegistry(MemTableStore store) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new MemConnector(store));
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        reg.register(new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "memory",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        return reg;
    }

    private static final class MemTableStore {
        record TableSpec(List<String> columns, int rowsPerSplit, long sizeHint) { }

        final Map<String, TableSpec> tables = new LinkedHashMap<>();
        final AtomicInteger pageSourcesOpened = new AtomicInteger();
        final AtomicInteger pageSourcesClosed = new AtomicInteger();

        void addTable(String schema, String table, List<String> columns,
                      int rowsPerSplit, long sizeHint) {
            tables.put(schema + "." + table, new TableSpec(columns, rowsPerSplit, sizeHint));
        }
    }

    private record MemTableHandle(String key) implements ConnectorTableHandle { }
    private record MemColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }
    private record MemSplit(String key, long sizeHint) implements ConnectorSplit {
        @Override public SplitProperties typedSplitProperties() {
            return new SplitProperties("/data/" + key + "/split-0",
                    0L, sizeHint, Map.of(), Map.of());
        }
    }

    private static final class MemConnector implements Connector {
        private final MemTableStore store;
        MemConnector(MemTableStore s) { this.store = s; }
        @Override public ConnectorMetadata getMetadata(ConnectorSession s) {
            return new MemMetadata(store);
        }
        @Override public ConnectorSplitManager getSplitManager() {
            return new MemSplitManager(store);
        }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            return new MemPageSourceProvider(store);
        }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class MemMetadata implements ConnectorMetadata {
        private final MemTableStore store;
        MemMetadata(MemTableStore s) { this.store = s; }
        @Override public List<String> listSchemaNames(ConnectorSession s) {
            return List.of(SCHEMA);
        }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            List<SchemaTableName> out = new ArrayList<>();
            for (String key : store.tables.keySet()) {
                int dot = key.indexOf('.');
                out.add(new SchemaTableName(key.substring(0, dot), key.substring(dot + 1)));
            }
            return out;
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName name) {
            String key = name.schema() + "." + name.table();
            return store.tables.containsKey(key) ? new MemTableHandle(key) : null;
        }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            int dot = h.key.indexOf('.');
            return new ConnectorTableMetadata(
                    new SchemaTableName(h.key.substring(0, dot), h.key.substring(dot + 1)),
                    store.tables.get(h.key).columns(), Map.of());
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            Map<String, ColumnHandle> out = new LinkedHashMap<>();
            for (String c : store.tables.get(h.key).columns()) {
                out.put(c, new MemColumnHandle(c));
            }
            return out;
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle t) {
            return TableStatistics.UNKNOWN;
        }
        @Override public ConnectorOutputTableHandle beginInsert(ConnectorSession s, ConnectorTableHandle t) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class MemSplitManager implements ConnectorSplitManager {
        private final MemTableStore store;
        MemSplitManager(MemTableStore s) { this.store = s; }
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorTableHandle t, DynamicFilter df, Constraint c) {
            String key = ((MemTableHandle) t).key;
            long sizeHint = store.tables.get(key).sizeHint();
            return new ConnectorSplitSource() {
                boolean done;
                @Override public List<ConnectorSplit> getNextBatch(int max) {
                    if (done) return List.of();
                    done = true;
                    return List.of(new MemSplit(key, sizeHint));
                }
                @Override public boolean isFinished() { return done; }
                @Override public void close() { }
            };
        }
    }

    private static final class MemPageSourceProvider implements ConnectorPageSourceProvider {
        private final MemTableStore store;
        MemPageSourceProvider(MemTableStore s) { this.store = s; }
        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorSplit split, ConnectorTableHandle t, List<ColumnHandle> cols, DynamicFilter df) {
            MemSplit ms = (MemSplit) split;
            int rows = store.tables.get(ms.key).rowsPerSplit();
            store.pageSourcesOpened.incrementAndGet();
            return new ConnectorPageSource() {
                boolean done;
                @Override public Page getNextPage() {
                    if (done) return null;
                    done = true;
                    if (rows == 0) return null;
                    List<TypedRow> rowList = new ArrayList<>(rows);
                    for (int i = 0; i < rows; i++) {
                        rowList.add(TypedRow.builder().putLong("v", i).build());
                    }
                    return new Page(List.of("v"), rowList);
                }
                @Override public boolean isFinished() { return done || rows == 0; }
                @Override public void close() { store.pageSourcesClosed.incrementAndGet(); }
            };
        }
    }

    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, List<String> s) { }
    }
}
