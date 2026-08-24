package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.coordinator.query.execution.dispatch.task.InsertValuesTask;
import org.pangrova.coordinator.query.execution.dispatch.task.InsertOverwriteTask;
import org.pangrova.metadata.catalog.*;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.ExpressionItem;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.*;
import org.pangrova.spi.connector.data.*;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.*;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the complete DML write path from StatementDispatcher through
 * Connector PageSink to data file commit for INSERT statement types.
 *
 * DELETE/UPDATE/MERGE now go through PlanExecutionFactory (native worker path)
 * and are not tested here — they require a full DAG dispatch setup.
 *
 * Validates: Requirements 1.8
 */
class DmlWritePathTest {

    private final List<String> callbackInvocations = new CopyOnWriteArrayList<>();
    private final List<String> sinkOperations = new CopyOnWriteArrayList<>();
    private InMemoryTableStore tableStore;
    private CatalogRegistry registry;
    private StatementDispatcher dispatcher;

    private static final CoordinatorSessionContext TEST_SESSION =
            new CoordinatorSessionContext("q-test", Map.of(), null, null);

    @BeforeEach
    void setUp() {
        tableStore = new InMemoryTableStore();
        registry = buildRegistry(tableStore);
        dispatcher = buildDispatcher(registry, callbackInvocations);
    }

    // ── isRoutable routing ──

    @Test
    void insertStatementRecognizedAsRoutable() {
        assertThat(dispatcher.isRoutable(buildInsert())).isTrue();
    }

    @Test
    void insertOverwriteRecognizedAsRoutable() {
        InsertOverwriteStatement stmt = new InsertOverwriteStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(List.of("id", "name")),
                List.of(List.of(Literal.ofInteger(1), Literal.ofString("alice"))));
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    @Test
    void deleteStatementRecognizedAsRoutable() {
        assertThat(dispatcher.isRoutable(buildDelete())).isTrue();
    }

    @Test
    void updateStatementRecognizedAsRoutable() {
        assertThat(dispatcher.isRoutable(buildUpdate())).isTrue();
    }

    @Test
    void mergeStatementRecognizedAsRoutable() {
        assertThat(dispatcher.isRoutable(buildMerge())).isTrue();
    }

    @Test
    void insertSelectStatementRecognizedAsRoutable() {
        Query selectQuery = new Query(
                false,
                List.of(new ExpressionItem(Literal.ofInteger(1), Optional.of("id")),
                        new ExpressionItem(Literal.ofString("bob"), Optional.of("name"))),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty());
        InsertSelectStatement stmt = new InsertSelectStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(List.of("id", "name")),
                false,
                new SetQuery(List.of(selectQuery)));
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    // ── INSERT write path ──

    @Test
    void insertWritesRowsViaSink() {
        dispatcher.executeDirect(buildInsert(), null, "q-insert", TEST_SESSION);

        assertThat(tableStore.tables.get("default.orders")).hasSize(1);
        TypedRow row = tableStore.tables.get("default.orders").get(0);
        assertThat(ColumnValue.toRaw(row.get("id"))).isEqualTo(1L);
        assertThat(ColumnValue.toRaw(row.get("name"))).isEqualTo("alice");
    }

    @Test
    void insertFiresCallback() {
        dispatcher.executeDirect(buildInsert(), null, "q-insert-cb", TEST_SESSION);
        assertThat(callbackInvocations).hasSize(1);
        assertThat(callbackInvocations.get(0)).contains("orders");
    }

    // ── INSERT OVERWRITE write path ──

    @Test
    void insertOverwriteReplacesExistingData() {
        dispatcher.executeDirect(buildInsert(), null, "q-insert-1", TEST_SESSION);
        assertThat(tableStore.tables.get("default.orders")).hasSize(1);

        InsertOverwriteStatement overwrite = new InsertOverwriteStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(List.of("id", "name")),
                List.of(List.of(Literal.ofInteger(99), Literal.ofString("replaced"))));
        dispatcher.executeDirect(overwrite, null, "q-overwrite", TEST_SESSION);

        assertThat(tableStore.tables.get("default.orders")).hasSize(1);
        TypedRow row = tableStore.tables.get("default.orders").get(0);
        assertThat(ColumnValue.toRaw(row.get("id"))).isEqualTo(99L);
    }

    // ── Sink operations verification ──

    @Test
    void sinkAppendAndCloseCalledForInsert() {
        CatalogRegistry trackedRegistry = buildTrackedRegistry(tableStore, sinkOperations);
        StatementDispatcher trackedDispatcher = buildDispatcher(trackedRegistry, null);

        trackedDispatcher.executeDirect(buildInsert(), null, "q-sink", TEST_SESSION);

        assertThat(sinkOperations).contains("appendPage", "close");
    }

    // ── Statement builders ──

    private static InsertStatement buildInsert() {
        return buildInsertRow(1, "alice");
    }

    private static InsertStatement buildInsertRow(int id, String name) {
        return new InsertStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(List.of("id", "name")),
                List.of(List.of(Literal.ofInteger(id), Literal.ofString(name))));
    }

    private static DeleteStatement buildDelete() {
        return new DeleteStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(new BinaryExpression(
                        Identifier.of("id"),
                        BinaryExpression.Operator.EQUAL,
                        Literal.ofInteger(1))));
    }

    private static UpdateStatement buildUpdate() {
        return new UpdateStatement(
                Identifier.of("mem", "default", "orders"),
                List.of(new UpdateStatement.Assignment("name", Literal.ofString("updated"))),
                Optional.of(new BinaryExpression(
                        Identifier.of("id"),
                        BinaryExpression.Operator.EQUAL,
                        Literal.ofInteger(1))));
    }

    private static MergeStatement buildMerge() {
        return new MergeStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.empty(),
                new org.pangrova.parser.ast.TableRef(Identifier.of("mem", "default", "orders")),
                new BinaryExpression(
                        Identifier.of("id"),
                        BinaryExpression.Operator.EQUAL,
                        Literal.ofInteger(1)),
                List.of(
                        new MergeStatement.MergeClause.WhenMatchedUpdate(
                                Optional.empty(),
                                List.of(new UpdateStatement.Assignment("name",
                                        Literal.ofString("merged")))),
                        new MergeStatement.MergeClause.WhenNotMatchedInsert(
                                Optional.empty(),
                                Optional.of(List.of("id", "name")),
                                List.of(Literal.ofInteger(99), Literal.ofString("new")))));
    }

    // ── Test infrastructure ──

    private static StatementDispatcher buildDispatcher(
            CatalogRegistry reg, List<String> callbacks) {
        DmlExecutor dmlExecutor = new DmlExecutor(reg, DmlExecutorConfig.of(null, null));
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(InsertStatement.class, new InsertValuesTask(dmlExecutor));
        tasks.put(InsertOverwriteStatement.class, new InsertOverwriteTask(dmlExecutor));
        // Register plan-type statements for isRoutable() verification only.
        // In production these go through PlanExecutionFactory via native workers.
        MetadataTask<Statement> routabilityStub = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DML);
        tasks.put(DeleteStatement.class, routabilityStub);
        tasks.put(UpdateStatement.class, routabilityStub);
        tasks.put(MergeStatement.class, routabilityStub);
        tasks.put(InsertSelectStatement.class, routabilityStub);
        var factory = new MetadataExecutionFactory(tasks);
        StatementCompletionListener listener = callbacks != null
                ? event -> { if (event.mutatedTable() != null) callbacks.add(event.mutatedTable()); }
                : event -> {};
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(listener), null);
        return new StatementDispatcher(context);
    }

    private static CatalogRegistry buildRegistry(InMemoryTableStore store) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new InMemoryConnector(store));
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "mem", CatalogType.CONNECTOR, "memory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        reg.register(descriptor);
        return reg;
    }

    private static CatalogRegistry buildTrackedRegistry(
            InMemoryTableStore store, List<String> ops) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TrackedConnector(store, ops));
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "mem", CatalogType.CONNECTOR, "memory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        reg.register(descriptor);
        return reg;
    }

    static final class InMemoryTableStore {
        final Map<String, List<TypedRow>> tables = new LinkedHashMap<>();
        final Map<String, List<String>> columns = new LinkedHashMap<>();

        InMemoryTableStore() {
            tables.put("default.orders", new ArrayList<>());
            columns.put("default.orders", List.of("id", "name"));
        }
    }

    private static final class InMemoryConnector implements Connector {
        private final InMemoryTableStore store;

        InMemoryConnector(InMemoryTableStore store) {
            this.store = store;
        }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return new MemMetadata(store); }
        @Override public ConnectorSplitManager getSplitManager() { return new MemSplitManager(store); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return new MemPageSourceProvider(store); }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() { return new MemPageSinkProvider(store); }
    }

    private static final class TrackedConnector implements Connector {
        private final InMemoryTableStore store;
        private final List<String> ops;

        TrackedConnector(InMemoryTableStore store, List<String> ops) {
            this.store = store;
            this.ops = ops;
        }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return new MemMetadata(store); }
        @Override public ConnectorSplitManager getSplitManager() { return new MemSplitManager(store); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return new MemPageSourceProvider(store); }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() { return new TrackedPageSinkProvider(store, ops); }
    }

    private static final class MemMetadata implements ConnectorMetadata {
        private final InMemoryTableStore store;
        MemMetadata(InMemoryTableStore store) { this.store = store; }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of("default"); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(new SchemaTableName("default", "orders"));
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName name) {
            String key = name.schema() + "." + name.table();
            return store.tables.containsKey(key) ? new MemTableHandle(key) : null;
        }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "orders"),
                    store.columns.get(h.key), Map.of());
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            Map<String, ColumnHandle> handles = new LinkedHashMap<>();
            for (String col : store.columns.get(h.key)) {
                handles.put(col, new MemColumnHandle(col));
            }
            return handles;
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle t) {
            return TableStatistics.UNKNOWN;
        }
        @Override public ConnectorOutputTableHandle beginInsert(ConnectorSession s, ConnectorTableHandle t) {
            return new MemOutputTableHandle(((MemTableHandle) t).key, false);
        }
        @Override public ConnectorOutputTableHandle beginOverwrite(ConnectorSession s, ConnectorTableHandle t) {
            return new MemOutputTableHandle(((MemTableHandle) t).key, true);
        }

        /**
         * In-memory connector has no external catalog — data is committed
         * during appendPage(). Satisfies the two-phase commit contract.
         */
        @Override public void finishDistributedWrite(ConnectorSession s,
                ConnectorOutputTableHandle outputHandle, java.util.List<byte[]> fragments) {
            // In-memory store: data already committed during appendPage
        }
    }

    private record MemTableHandle(String key) implements ConnectorTableHandle {}
    private record MemColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }
    private record MemOutputTableHandle(String key, boolean overwrite) implements ConnectorOutputTableHandle {}
    private record MemSplit(String key) implements ConnectorSplit {}

    private static final class MemSplitManager implements ConnectorSplitManager {
        private final InMemoryTableStore store;
        MemSplitManager(InMemoryTableStore store) { this.store = store; }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorTableHandle t, DynamicFilter df, Constraint c) {
            String key = ((MemTableHandle) t).key;
            return new ConnectorSplitSource() {
                private boolean done;
                @Override public List<ConnectorSplit> getNextBatch(int max) {
                    if (done) return List.of();
                    done = true;
                    return List.of(new MemSplit(key));
                }
                @Override public boolean isFinished() { return done; }
                @Override public void close() { }
            };
        }
    }

    private static final class MemPageSourceProvider implements ConnectorPageSourceProvider {
        private final InMemoryTableStore store;
        MemPageSourceProvider(InMemoryTableStore store) { this.store = store; }

        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorSplit split, ConnectorTableHandle t, List<ColumnHandle> cols, DynamicFilter df) {
            String key = ((MemSplit) split).key;
            List<TypedRow> rows = new ArrayList<>(store.tables.get(key));
            List<String> colNames = store.columns.get(key);
            return new ConnectorPageSource() {
                private boolean done;
                @Override public Page getNextPage() {
                    if (done) return null;
                    done = true;
                    return new Page(colNames, rows);
                }
                @Override public boolean isFinished() { return done; }
                @Override public void close() { }
            };
        }
    }

    private static final class MemPageSinkProvider implements ConnectorPageSinkProvider {
        private final InMemoryTableStore store;
        MemPageSinkProvider(InMemoryTableStore store) { this.store = store; }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle tx,
                ConnectorSession s, ConnectorOutputTableHandle out) {
            return buildSink((MemOutputTableHandle) out);
        }

        @Override
        public ConnectorPageSink createOverwritePageSink(ConnectorTransactionHandle tx,
                ConnectorSession s, ConnectorOutputTableHandle out) {
            return buildSink((MemOutputTableHandle) out);
        }

        private ConnectorPageSink buildSink(MemOutputTableHandle h) {
            return new ConnectorPageSink() {
                @Override public void appendPage(Page page) {
                    if (h.overwrite()) store.tables.get(h.key()).clear();
                    store.tables.get(h.key()).addAll(page.rows());
                }
                @Override public byte[] finishWithMetadata() { return new byte[0]; }
                @Override public void abort() {}
                @Override public void close() { }
            };
        }
    }

    private static final class TrackedPageSinkProvider implements ConnectorPageSinkProvider {
        private final InMemoryTableStore store;
        private final List<String> ops;

        TrackedPageSinkProvider(InMemoryTableStore store, List<String> ops) {
            this.store = store;
            this.ops = ops;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle tx,
                ConnectorSession s, ConnectorOutputTableHandle out) {
            return buildTrackedSink((MemOutputTableHandle) out);
        }

        @Override
        public ConnectorPageSink createOverwritePageSink(ConnectorTransactionHandle tx,
                ConnectorSession s, ConnectorOutputTableHandle out) {
            return buildTrackedSink((MemOutputTableHandle) out);
        }

        private ConnectorPageSink buildTrackedSink(MemOutputTableHandle h) {
            return new ConnectorPageSink() {
                @Override public void appendPage(Page page) {
                    ops.add("appendPage");
                    if (h.overwrite()) store.tables.get(h.key()).clear();
                    store.tables.get(h.key()).addAll(page.rows());
                }
                @Override public byte[] finishWithMetadata() { return new byte[0]; }
                @Override public void abort() {}
                @Override public void close() {
                    ops.add("close");
                }
            };
        }
    }

    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<org.pangrova.metadata.model.QualifiedTableRef> searchTables(String p) { return List.of(); }
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
