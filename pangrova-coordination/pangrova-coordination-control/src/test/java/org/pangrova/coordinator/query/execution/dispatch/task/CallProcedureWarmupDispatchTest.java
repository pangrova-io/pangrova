package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableWarmupExecutor;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.statement.maintenance.CallStatement;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.data.ConnectorPageSource;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.SplitProperties;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies CallProcedureTask dispatches the engine-level {@code warmup_table}
 * procedure to {@link TableWarmupExecutor} (G-CACHE-06) instead of forwarding
 * to the connector. Mutation procedures must pass the table-control maintenance boundary.
 */
class CallProcedureWarmupDispatchTest {

    private static final String CATALOG = "warmcat";

    @Test
    void warmupTable_isHandledByEngineExecutor_notConnector() {
        ConnectorMetadataSpy metadataSpy = new ConnectorMetadataSpy();
        CatalogRegistry registry = buildRegistry(metadataSpy);
        CallProcedureTask task = new CallProcedureTask(registry, maintenance(registry));

        CallStatement stmt = new CallStatement(
                Identifier.of(CATALOG, org.pangrova.query.statement.TableProcedureCall.WARMUP_PROCEDURE_NAME),
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
    void unknownProcedureCannotBypassMaintenancePolicy() {
        ConnectorMetadataSpy spy = new ConnectorMetadataSpy();
        CatalogRegistry registry = buildRegistry(spy);
        CallProcedureTask task = new CallProcedureTask(registry, maintenance(registry));
        CallStatement stmt = new CallStatement(Identifier.of(CATALOG, "rollback_to_snapshot"),
                List.of(Literal.ofString("default.orders"), Literal.ofString("123")));

        assertThatThrownBy(() -> task.execute(stmt,
                org.pangrova.coordinator.query.statement.StatementTestSessions.executionContext()))
                .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class)
                .hasMessageContaining("no table-control maintenance policy");
        assertThat(spy.executeProcedureInvocations).isZero();
    }

    @Test
    void compactCannotMutateAnObservedTable() {
        ConnectorMetadataSpy spy = new ConnectorMetadataSpy();
        CatalogRegistry registry = buildRegistry(spy);
        CallProcedureTask task = new CallProcedureTask(registry, maintenance(registry));
        CallStatement stmt = new CallStatement(Identifier.of(CATALOG, "compact"),
                List.of(Literal.ofString("default.orders")));

        assertThatThrownBy(() -> task.execute(stmt,
                org.pangrova.coordinator.query.statement.StatementTestSessions.executionContext()))
                .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(spy.executeProcedureInvocations).isZero();
    }

    private static org.pangrova.coordinator.query.statement.ControlledTableMaintenance maintenance(
            CatalogRegistry registry) {
        return new org.pangrova.coordinator.query.statement.ControlledTableMaintenance(registry,
                new org.pangrova.table.maintenance.TableControlMaintenanceService(
                        new org.pangrova.persistence.table.memory.maintenance.InMemoryTableControlMaintenanceStore()),
                null, "test", new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }

    // --- fixtures ---

    private static CatalogRegistry buildRegistry(ConnectorMetadataSpy spy) {
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new SpyConnector(spy));
        CatalogRegistry reg = new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments(), chf, null, null);
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

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
