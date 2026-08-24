package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.coordinator.query.execution.dispatch.task.InsertValuesTask;
import org.pangrova.coordinator.query.execution.dispatch.task.InsertOverwriteTask;
import org.pangrova.metadata.catalog.*;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.spi.connector.*;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies StatementDispatcher fires completion events (write callback)
 * after successful DML operations.
 * Uses real CatalogRegistry with in-memory connector catalog — no mocks.
 */
class DmlWriteCallbackTest {

    private final List<String> callbackInvocations = new CopyOnWriteArrayList<>();
    private CatalogRegistry registry;
    private DmlExecutor dmlExecutor;

    private static final CoordinatorSessionContext TEST_SESSION =
            new CoordinatorSessionContext("q-test", Map.of(), null, null);

    @BeforeEach
    void setUp() {
        registry = buildRegistryWithInMemoryCatalog();
        dmlExecutor = new DmlExecutor(registry, DmlExecutorConfig.of(null, null));
    }

    private StatementDispatcher buildDispatcher(StatementCompletionListener listener) {
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(InsertStatement.class, new InsertValuesTask(dmlExecutor));
        tasks.put(InsertOverwriteStatement.class, new InsertOverwriteTask(dmlExecutor));
        var factory = new MetadataExecutionFactory(tasks);
        var listeners = listener != null ? List.of(listener) : List.<StatementCompletionListener>of();
        var context = new StatementDispatcherContext(null, null, List.of(factory), listeners, null);
        return new StatementDispatcher(context);
    }

    private StatementCompletionListener recordingListener() {
        return event -> {
            if (event.mutatedTable() != null) {
                callbackInvocations.add(event.mutatedTable());
            }
        };
    }

    @Test
    void insertFiresWriteCallback() {
        StatementDispatcher dispatcher = buildDispatcher(recordingListener());
        InsertStatement insert = buildInsertStatement();
        dispatcher.executeDirect(insert, null, "q-insert", TEST_SESSION);

        assertThat(callbackInvocations).hasSize(1);
        assertThat(callbackInvocations.get(0)).contains("orders");
    }

    @Test
    void nullCallbackDoesNotThrow() {
        StatementDispatcher dispatcher = buildDispatcher(null);
        dispatcher.executeDirect(buildInsertStatement(), null, "q-null-cb", TEST_SESSION);
        // No exception — no listener registered
    }

    private static InsertStatement buildInsertStatement() {
        return new InsertStatement(
                Identifier.of("mem", "default", "orders"),
                Optional.of(List.of("id", "name")),
                List.of(List.of(Literal.ofInteger(1), Literal.ofString("alice"))));
    }

    private CatalogRegistry buildRegistryWithInMemoryCatalog() {
        InMemoryTableStore tableStore = new InMemoryTableStore();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new InMemoryConnector(tableStore));
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "mem", CatalogType.CONNECTOR, "memory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        reg.register(descriptor);
        return reg;
    }

    // ── In-memory connector implementation ──

    private static final class InMemoryTableStore {
        private final Map<String, List<TypedRow>> tables = new LinkedHashMap<>();
        private final Map<String, List<String>> columns = new LinkedHashMap<>();

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

    private static final class MemMetadata implements ConnectorMetadata {
        private final InMemoryTableStore store;
        MemMetadata(InMemoryTableStore store) { this.store = store; }

        @Override
        public List<String> listSchemaNames(ConnectorSession s) { return List.of("default"); }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(new SchemaTableName("default", "orders"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName name) {
            String key = name.schema() + "." + name.table();
            return store.tables.containsKey(key) ? new MemTableHandle(key) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "orders"),
                    store.columns.get(h.key), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle t) {
            MemTableHandle h = (MemTableHandle) t;
            Map<String, ColumnHandle> handles = new LinkedHashMap<>();
            for (String col : store.columns.get(h.key)) {
                handles.put(col, new MemColumnHandle(col));
            }
            return handles;
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle t) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession s, ConnectorTableHandle t) {
            return new MemOutputTableHandle(((MemTableHandle) t).key, false);
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession s, ConnectorTableHandle t) {
            return new MemOutputTableHandle(((MemTableHandle) t).key, true);
        }

        /**
         * In-memory connector has no external catalog — data is committed
         * during appendPage(). This override satisfies the two-phase commit
         * contract without performing additional work.
         */
        @Override
        public void finishDistributedWrite(ConnectorSession s,
                ConnectorOutputTableHandle outputHandle, java.util.List<byte[]> fragments) {
            // In-memory store: data already committed during appendPage
        }
    }

    private record MemTableHandle(String key) implements ConnectorTableHandle { }
    private record MemColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }
    private record MemOutputTableHandle(String key, boolean overwrite) implements ConnectorOutputTableHandle { }
    private record MemSplit(String key) implements ConnectorSplit { }

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
            MemOutputTableHandle h = (MemOutputTableHandle) out;
            return buildSink(h);
        }

        @Override
        public ConnectorPageSink createOverwritePageSink(ConnectorTransactionHandle tx,
                ConnectorSession s, ConnectorOutputTableHandle out) {
            MemOutputTableHandle h = (MemOutputTableHandle) out;
            return buildSink(h);
        }

        private ConnectorPageSink buildSink(MemOutputTableHandle h) {
            return new ConnectorPageSink() {
                @Override public void appendPage(Page page) {
                    if (h.overwrite) {
                        store.tables.get(h.key).clear();
                    }
                    store.tables.get(h.key).addAll(page.rows());
                }
                @Override public byte[] finishWithMetadata() { return new byte[0]; }
                @Override public void abort() {}
                @Override public void close() { }
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
