package org.pangrova.coordinator.query.execution.dispatch.task;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
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
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies CallProcedureTask dispatches the engine-level {@code warmup_table}
 * procedure to {@link TableWarmupExecutor} (G-CACHE-06) instead of forwarding
 * to the connector. Other procedures still hit
 * {@code ConnectorMetadata.executeProcedure}.
 */
class CallProcedureWarmupDispatchTest {

    private static final String CATALOG = "warmcat";

    @Test
    void warmupTable_isHandledByEngineExecutor_notConnector() {
        ConnectorMetadataSpy metadataSpy = new ConnectorMetadataSpy();
        CatalogRegistry registry = buildRegistry(metadataSpy);
        CallProcedureTask task = new CallProcedureTask(registry);

        CallStatement stmt = new CallStatement(
                Identifier.of(CATALOG, TableWarmupExecutor.PROCEDURE_NAME),
                List.of(Literal.ofString("default"), Literal.ofString("orders")));

        StatementResult result = task.execute(stmt,
                org.pangrova.coordinator.query.statement.StatementTestSessions.executionContext());

        assertThat(result.category()).isEqualTo(StatementCategory.MAINTENANCE);
        assertThat(result.columnNames())
                .containsExactlyElementsOf(TableWarmupExecutor.resultColumns());
        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().get(0).getString("table")).isEqualTo("orders");
        assertThat(result.rows().get(0).getLong("rows_touched")).isEqualTo(2L);
        // Crucially: connector executeProcedure was NOT called for warmup_table.
        assertThat(metadataSpy.executeProcedureInvocations).isEqualTo(0);
    }

    @Test
    void unknownProcedure_stillRoutesToConnector() {
        ConnectorMetadataSpy metadataSpy = new ConnectorMetadataSpy();
        CatalogRegistry registry = buildRegistry(metadataSpy);
        CallProcedureTask task = new CallProcedureTask(registry);

        CallStatement stmt = new CallStatement(
                Identifier.of(CATALOG, "do_something_else"),
                List.of(Literal.ofString("x")));

        StatementResult result = task.execute(stmt,
                org.pangrova.coordinator.query.statement.StatementTestSessions.executionContext());
        assertThat(result.category()).isEqualTo(StatementCategory.MAINTENANCE);
        assertThat(metadataSpy.executeProcedureInvocations).isEqualTo(1);
        assertThat(metadataSpy.lastProcedure).isEqualTo("do_something_else");
    }

    // --- fixtures ---

    private static CatalogRegistry buildRegistry(ConnectorMetadataSpy spy) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new SpyConnector(spy));
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        reg.register(new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "memory",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        return reg;
    }

    private static final class ConnectorMetadataSpy {
        int executeProcedureInvocations;
        String lastProcedure;
    }

    private record SpyTableHandle() implements ConnectorTableHandle { }
    private record SpyColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }
    private record SpySplit() implements ConnectorSplit {
        @Override public SplitProperties typedSplitProperties() {
            return new SplitProperties("/data/orders/split-0", 0L, 64L, Map.of(), Map.of());
        }
    }

    private static final class SpyConnector implements Connector {
        private final ConnectorMetadataSpy spy;
        SpyConnector(ConnectorMetadataSpy s) { this.spy = s; }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return new SpyMetadata(spy); }
        @Override public ConnectorSplitManager getSplitManager() { return new SpySplitManager(); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return new SpyPageSourceProvider(); }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class SpyMetadata implements ConnectorMetadata {
        private final ConnectorMetadataSpy spy;
        SpyMetadata(ConnectorMetadataSpy s) { this.spy = s; }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of("default"); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(new SchemaTableName("default", "orders"));
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName name) {
            return ("default".equals(name.schema()) && "orders".equals(name.table()))
                    ? new SpyTableHandle() : null;
        }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle t) {
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "orders"), List.of("id"), Map.of());
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle t) {
            Map<String, ColumnHandle> out = new LinkedHashMap<>();
            out.put("id", new SpyColumnHandle("id"));
            return out;
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle t) {
            return TableStatistics.UNKNOWN;
        }
        @Override public ConnectorOutputTableHandle beginInsert(ConnectorSession s, ConnectorTableHandle t) {
            throw new UnsupportedOperationException();
        }
        @Override public List<TypedRow> executeProcedure(
                ConnectorSession session, String procedureName, Map<String, String> args) {
            spy.executeProcedureInvocations++;
            spy.lastProcedure = procedureName;
            return List.of(TypedRow.builder().putString("ok", procedureName).build());
        }
    }

    private static final class SpySplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorTableHandle t, DynamicFilter df, Constraint c) {
            AtomicInteger produced = new AtomicInteger();
            return new ConnectorSplitSource() {
                @Override public List<ConnectorSplit> getNextBatch(int max) {
                    if (produced.getAndSet(1) == 0) return List.of(new SpySplit());
                    return List.of();
                }
                @Override public boolean isFinished() { return produced.get() == 1; }
                @Override public void close() { }
            };
        }
    }

    private static final class SpyPageSourceProvider implements ConnectorPageSourceProvider {
        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle tx, ConnectorSession s,
                ConnectorSplit split, ConnectorTableHandle t, List<ColumnHandle> cols, DynamicFilter df) {
            return new ConnectorPageSource() {
                boolean done;
                @Override public Page getNextPage() {
                    if (done) return null;
                    done = true;
                    List<TypedRow> rows = new ArrayList<>();
                    rows.add(TypedRow.builder().putLong("id", 1L).build());
                    rows.add(TypedRow.builder().putLong("id", 2L).build());
                    return new Page(List.of("id"), rows);
                }
                @Override public boolean isFinished() { return done; }
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
