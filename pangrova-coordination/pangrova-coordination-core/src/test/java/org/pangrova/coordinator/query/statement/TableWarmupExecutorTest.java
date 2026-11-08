package org.pangrova.coordinator.query.statement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.spi.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.BatchFormat;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.SplitProperties;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
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

    private static org.pangrova.query.api.session.QuerySession statementSession() {
        return context();
    }

    private static List<TypedRow> execute(TableWarmupExecutor executor, String catalog,
            Map<String, String> arguments, org.pangrova.query.api.session.QuerySession session) {
        var literals = new java.util.ArrayList<org.pangrova.parser.api.ast.Expression>();
        for (int i = 0; i < arguments.size(); i++) {
            literals.add(org.pangrova.parser.api.ast.Literal.ofString(arguments.get("arg" + i)));
        }
        var call = org.pangrova.query.core.statement.TableProcedureCall.bind(
                new org.pangrova.parser.api.ast.statement.maintenance.CallStatement(
                        org.pangrova.parser.api.ast.Identifier.of(catalog, "warmup_table"), literals));
        return executor.execute(call.target(), session);
    }

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

        List<TypedRow> rows = execute(exec, CATALOG,
                Map.of("arg0", SCHEMA, "arg1", TABLE), statementSession());

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

        List<TypedRow> rows = execute(exec, CATALOG,
                Map.of("arg0", SCHEMA + "." + TABLE), statementSession());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong("rows_touched")).isEqualTo(5L);
    }

    @Test
    void warmup_resultColumnsContractMatchesTypedRow() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = execute(exec, CATALOG, Map.of("arg0", SCHEMA, "arg1", TABLE), statementSession()).get(0);
        assertThat(TableWarmupExecutor.resultColumns())
                .containsExactlyElementsOf(row.columnNames());
    }

    @Test
    void warmup_iteratesAllSplits_andClosesEachPageSource() {
        store.addTable("default", "multi", List.of("v"), 7, 256L);
        store.setSplitsPerTable("default", "multi", 4); // 4 splits, each contributes rows

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        List<TypedRow> rows = execute(exec, CATALOG, Map.of("arg0", "default", "arg1", "multi"), statementSession());

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
        TypedRow row = execute(exec, CATALOG, Map.of("arg0", "default", "arg1", "native_t"), statementSession()).get(0);

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
        TypedRow row = execute(exec, CATALOG, Map.of("arg0", "default", "arg1", "flaky"), statementSession()).get(0);

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
                execute(exec, CATALOG, Map.of("arg0", "default", "arg1", "doomed"), statementSession()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("warmup_table");
    }

    @Test
    void warmup_unknownTable_throwsTableNotFound() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> execute(exec, CATALOG,
                Map.of("arg0", SCHEMA, "arg1", "ghost"), statementSession()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("table not found");
    }

    @Test
    void warmup_missingArg0_throws() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> execute(exec, CATALOG, Map.of(), statementSession()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("warmup_table requires arguments");
    }

    @Test
    void warmup_singleArgWithoutDot_throws() {
        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        assertThatThrownBy(() -> execute(exec, CATALOG, Map.of("arg0", "no_dot"), statementSession()))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("'schema.table'");
    }

    @Test
    void warmup_splitCap_stopsEarly() {
        store.addTable("default", "huge", List.of("v"), 1, 64L);
        store.setSplitsPerTable("default", "huge", 50);

        TableWarmupExecutor exec = new TableWarmupExecutor(registry, /* maxSplits = */ 5);
        TypedRow row = execute(exec, CATALOG,
                Map.of("arg0", "default", "arg1", "huge"), statementSession()).get(0);

        assertThat(row.getLong("splits_processed")).isEqualTo(5L);
    }

    @Test
    void warmup_emptyTable_returnsZeroes() {
        store.addTable("default", "empty", List.of("v"), 0, 0L);
        store.setSplitsPerTable("default", "empty", 0);

        TableWarmupExecutor exec = new TableWarmupExecutor(registry);
        TypedRow row = execute(exec, CATALOG,
                Map.of("arg0", "default", "arg1", "empty"), statementSession()).get(0);

        assertThat(row.getLong("splits_processed")).isEqualTo(0L);
        assertThat(row.getLong("splits_failed")).isEqualTo(0L);
        assertThat(row.getLong("rows_touched")).isEqualTo(0L);
        assertThat(row.getLong("bytes_prefetched")).isEqualTo(0L);
    }

    // --- Test fixtures ---

    private static CatalogRegistry buildRegistry(MemTableStore store) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new MemConnector(store));
        CatalogRegistry reg = new CatalogRegistry(new EmptyCatalogDeployments(), chf, null, null);
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

        @Override public org.pangrova.connector.spi.metadata.ConnectorMetadata getMetadata(ConnectorSession s) {
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

    private static final class MemMetadata implements org.pangrova.connector.spi.metadata.ConnectorMetadata {
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

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
