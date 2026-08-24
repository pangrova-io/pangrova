package org.pangrova.coordinator.query.statement;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.exception.PangrovaException;
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
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.page.Page;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link TableWarmupExecutor} (G-CACHE-06: Trino-style CACHE SELECT
 * equivalent — proactive table warmup).
 *
 * <p>Coverage:
 * <ul>
 *   <li>Two-arg form (schema, table) and one-arg form ('schema.table').</li>
 *   <li>Splits are enumerated and each is read through the page-source path.</li>
 *   <li>Native-page format counts rows + observed bytes.</li>
 *   <li>Row-page format counts rows; bytes fall back to the split size hint.</li>
 *   <li>Per-split failures are tolerated; failure-only invocations throw.</li>
 *   <li>Non-existent table raises METADATA_TABLE_NOT_FOUND.</li>
 *   <li>Argument validation: missing arg0; single-arg without dot.</li>
 *   <li>Split cap honoured.</li>
 *   <li>Each split's page source is closed (release semantics).</li>
 * </ul>
 */
class TableWarmupExecutorTest {

    private static final String CATALOG = "warm";
    private static final String SCHEMA = "default";
    private static final String TABLE = "orders";

    private MemTableStore store;
    private CatalogRegistry registry;

    @BeforeEach
    void setUp() {
        store = new MemTableStore();
        store.addTable(SCHEMA, TABLE, List.of("id", "name"), 5, 1024L);
        registry = buildRegistry(store);
    }

    @Test
    void warmup_twoArgForm_drainsRowsAndReturnsSummary() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);

        List<TypedRow> rows = exec.execute(CATALOG,
                Map.of("arg0", SCHEMA, "arg1", TABLE));

        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(row.getString("catalog")).isEqualTo(CATALOG);
        assertThat(row.getString("schema")).isEqualTo(SCHEMA);
        assertThat(row.getString("table")).isEqualTo(TABLE);
        assertThat(row.getLong("splits_processed")).isEqualTo(1L);
        assertThat(row.getLong("splits_failed")).isEqualTo(0L);
        assertThat(row.getLong("rows_touched")).isEqualTo(5L);
        // bytes_prefetched falls back to split size hint when row-format pages are used
        assertThat(row.getLong("bytes_prefetched")).isEqualTo(1024L);
        assertThat(row.getLong("duration_ms")).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void warmup_singleArgForm_acceptsSchemaDotTable() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);

        List<TypedRow> rows = exec.execute(CATALOG,
                Map.of("arg0", SCHEMA + "." + TABLE));

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong("rows_touched")).isEqualTo(5L);
    }

    @Test
    void warmup_resultColumnsContractMatchesTypedRow() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = exec.execute(CATALOG, Map.of("arg0", SCHEMA, "arg1", TABLE)).get(0);
        assertThat(TableWarmupExecutor.resultColumns())
                .containsExactlyElementsOf(row.columnNames());
    }

    @Test
    void warmup_iteratesAllSplits_andClosesEachPageSource() {
        store.addTable("default", "multi", List.of("v"), 7, 256L);
        store.setSplitsPerTable("default", "multi", 4); // 4 splits, each contributes rows

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        List<TypedRow> rows = exec.execute(CATALOG, Map.of("arg0", "default", "arg1", "multi"));

        assertThat(rows.get(0).getLong("splits_processed")).isEqualTo(4L);
        assertThat(rows.get(0).getLong("rows_touched")).isEqualTo(4L * 7L);
        assertThat(rows.get(0).getLong("bytes_prefetched")).isEqualTo(4L * 256L);
        assertThat(store.pageSourcesOpened.get()).isEqualTo(store.pageSourcesClosed.get())
                .as("every page source created must be closed");
        assertThat(store.splitSourcesOpened.get()).isEqualTo(store.splitSourcesClosed.get())
                .as("split source must be closed");
    }

    @Test
    void warmup_nativePageFormat_creditsRealBytes() {
        store.addTable("default", "native_t", List.of("v"), 9, 0L /* size hint missing */);
        store.setNativeBytesPerSplit("default", "native_t", 4096);

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = exec.execute(CATALOG, Map.of("arg0", "default", "arg1", "native_t")).get(0);

        assertThat(row.getLong("rows_touched")).isEqualTo(9L);
        // No size hint, but native page reported bytes — those are credited.
        assertThat(row.getLong("bytes_prefetched")).isEqualTo(4096L);
    }

    @Test
    void warmup_perSplitFailure_continuesRemainingSplits() {
        store.addTable("default", "flaky", List.of("v"), 3, 128L);
        store.setSplitsPerTable("default", "flaky", 3);
        store.failPageSourceForSplitIndex("default", "flaky", 1); // 2nd split fails

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = exec.execute(CATALOG, Map.of("arg0", "default", "arg1", "flaky")).get(0);

        assertThat(row.getLong("splits_processed")).isEqualTo(2L);
        assertThat(row.getLong("splits_failed")).isEqualTo(1L);
        assertThat(row.getLong("rows_touched")).isEqualTo(2L * 3L);
    }

    @Test
    void warmup_allSplitsFail_throwsConnectorReadError() {
        store.addTable("default", "doomed", List.of("v"), 3, 128L);
        store.setSplitsPerTable("default", "doomed", 2);
        store.failAllSplitsForTable("default", "doomed");

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() ->
                exec.execute(CATALOG, Map.of("arg0", "default", "arg1", "doomed")))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("warmup_table");
    }

    @Test
    void warmup_unknownTable_throwsTableNotFound() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> exec.execute(CATALOG,
                Map.of("arg0", SCHEMA, "arg1", "ghost")))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("table not found");
    }

    @Test
    void warmup_missingArg0_throws() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> exec.execute(CATALOG, Map.of()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("warmup_table requires arguments");
    }

    @Test
    void warmup_singleArgWithoutDot_throws() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> exec.execute(CATALOG, Map.of("arg0", "no_dot")))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("'schema.table'");
    }

    @Test
    void warmup_splitCap_stopsEarly() {
        store.addTable("default", "huge", List.of("v"), 1, 64L);
        store.setSplitsPerTable("default", "huge", 50);

        TableWarmupExecutor exec = new TableWarmupExecutor(registry, /* maxSplits = */ 5);
        TypedRow row = exec.execute(CATALOG,
                Map.of("arg0", "default", "arg1", "huge")).get(0);

        assertThat(row.getLong("splits_processed")).isEqualTo(5L);
    }

    @Test
    void warmup_emptyTable_returnsZeroes() {
        store.addTable("default", "empty", List.of("v"), 0, 0L);
        store.setSplitsPerTable("default", "empty", 0);

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = exec.execute(CATALOG,
                Map.of("arg0", "default", "arg1", "empty")).get(0);

        assertThat(row.getLong("splits_processed")).isEqualTo(0L);
        assertThat(row.getLong("splits_failed")).isEqualTo(0L);
        assertThat(row.getLong("rows_touched")).isEqualTo(0L);
        assertThat(row.getLong("bytes_prefetched")).isEqualTo(0L);
    }

    // --- Test fixtures ---

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
        final Map<String, Integer> splitsPerTable = new LinkedHashMap<>();
        final Map<String, Integer> nativeBytesPerSplit = new LinkedHashMap<>();
        final Map<String, Integer> failSplitIndex = new LinkedHashMap<>();
        final Set<String> failAllTables = new java.util.HashSet<>();

        // Lifecycle counters.
        final AtomicInteger pageSourcesOpened = new AtomicInteger();
        final AtomicInteger pageSourcesClosed = new AtomicInteger();
        final AtomicInteger splitSourcesOpened = new AtomicInteger();
        final AtomicInteger splitSourcesClosed = new AtomicInteger();

        void addTable(String schema, String table, List<String> columns,
                      int rowsPerSplit, long sizeHint) {
            tables.put(schema + "." + table, new TableSpec(columns, rowsPerSplit, sizeHint));
            splitsPerTable.putIfAbsent(schema + "." + table, 1);
        }

        void setSplitsPerTable(String schema, String table, int n) {
            splitsPerTable.put(schema + "." + table, n);
        }

        void setNativeBytesPerSplit(String schema, String table, int bytes) {
            nativeBytesPerSplit.put(schema + "." + table, bytes);
        }

        void failPageSourceForSplitIndex(String schema, String table, int idx) {
            failSplitIndex.put(schema + "." + table, idx);
        }

        void failAllSplitsForTable(String schema, String table) {
            failAllTables.add(schema + "." + table);
        }
    }

    private record MemTableHandle(String key) implements ConnectorTableHandle { }

    private record MemColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }

    private record MemSplit(String key, int index, long sizeHint) implements ConnectorSplit {
        @Override public SplitProperties typedSplitProperties() {
            return new SplitProperties("/data/" + key + "/split-" + index,
                    0L, sizeHint, Map.of(), Map.of());
        }
    }

    private static final class MemConnector implements Connector {
        private final MemTableStore store;
        MemConnector(MemTableStore s) { this.store = s; }

        @Override public org.pangrova.spi.connector.metadata.ConnectorMetadata getMetadata(ConnectorSession s) {
            return new MemMetadata(store);
        }
        @Override public ConnectorSplitManager getSplitManager() {
            return new MemSplitManager(store);
        }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            return new MemPageSourceProvider(store);
        }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new UnsupportedOperationException("not used");
        }
    }

    private static final class MemMetadata implements org.pangrova.spi.connector.metadata.ConnectorMetadata {
        private final MemTableStore store;
        MemMetadata(MemTableStore s) { this.store = s; }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(SCHEMA); }
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
            throw new UnsupportedOperationException("not used");
        }
    }

    private static final class MemSplitManager implements ConnectorSplitManager {
        private final MemTableStore store;
        MemSplitManager(MemTableStore s) { this.store = s; }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorTableHandle t, DynamicFilter df, Constraint c) {
            String key = ((MemTableHandle) t).key;
            int splits = store.splitsPerTable.getOrDefault(key, 1);
            long sizeHint = store.tables.get(key).sizeHint();
            store.splitSourcesOpened.incrementAndGet();
            return new ConnectorSplitSource() {
                int produced;
                @Override public List<ConnectorSplit> getNextBatch(int max) {
                    if (produced >= splits) return List.of();
                    int take = Math.min(max, splits - produced);
                    List<ConnectorSplit> out = new ArrayList<>(take);
                    for (int i = 0; i < take; i++) {
                        out.add(new MemSplit(key, produced + i, sizeHint));
                    }
                    produced += take;
                    return out;
                }
                @Override public boolean isFinished() { return produced >= splits; }
                @Override public void close() { store.splitSourcesClosed.incrementAndGet(); }
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
            String key = ms.key;
            if (store.failAllTables.contains(key)) {
                throw new RuntimeException("forced failure for table " + key);
            }
            Integer failIdx = store.failSplitIndex.get(key);
            if (failIdx != null && failIdx == ms.index) {
                throw new RuntimeException("forced failure for split " + ms.index);
            }
            int rowsPerSplit = store.tables.get(key).rowsPerSplit();
            Integer nativeBytes = store.nativeBytesPerSplit.get(key);
            store.pageSourcesOpened.incrementAndGet();
            return new ConnectorPageSource() {
                boolean done;
                @Override public Page getNextPage() {
                    if (nativeBytes != null) return null; // force native path
                    if (done) return null;
                    done = true;
                    if (rowsPerSplit == 0) return null;
                    List<TypedRow> rows = new ArrayList<>(rowsPerSplit);
                    for (int i = 0; i < rowsPerSplit; i++) {
                        rows.add(TypedRow.builder().putLong("v", i).build());
                    }
                    return new Page(store.tables.get(key).columns(), rows);
                }
                @Override public NativePage getNextNativePage() {
                    if (nativeBytes == null) return null;
                    if (done) return null;
                    done = true;
                    if (rowsPerSplit == 0) return null;
                    return new NativePage(new byte[nativeBytes], rowsPerSplit, 1, BatchFormat.ARROW_IPC);
                }
                @Override public boolean isFinished() {
                    return done || (rowsPerSplit == 0);
                }
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
