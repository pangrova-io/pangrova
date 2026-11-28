package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.parser.api.enums.BinaryExpressionOperator;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.query.api.enums.QueryState;

import java.util.ArrayList;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.security.api.identity.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.query.dispatch.StatementExecutionContext;

/** Hand-written connector guards and real query/write state for DELETE routing tests. */
final class DeleteDispatchFixture {

    static final String CATALOG = "delete_catalog";
    static final String SCHEMA = "analytics";
    static final String QUERY_ID = "query-delete";
    static final SchemaTableName TABLE = new SchemaTableName(SCHEMA, "orders");
    static final Identifier TABLE_NAME = Identifier.of(CATALOG, SCHEMA, TABLE.table());
    static final String QUALIFIED_TABLE = CATALOG + "." + SCHEMA + "." + TABLE.table();

    private DeleteDispatchFixture() {}

    static DeleteStatement deleteAll() {
        return new DeleteStatement(TABLE_NAME, Optional.empty());
    }

    static DeleteStatement deleteWhere(Expression predicate) {
        return new DeleteStatement(TABLE_NAME, Optional.of(predicate));
    }

    static BinaryExpression equalToSeven(String column) {
        return new BinaryExpression(Identifier.of(column), BinaryExpressionOperator.EQUAL,
                Literal.ofInteger(7));
    }

    static BinaryExpression arithmeticPredicate() {
        return new BinaryExpression(
                new BinaryExpression(Identifier.of("id"), BinaryExpressionOperator.ADD, Literal.ofInteger(1)),
                BinaryExpressionOperator.EQUAL, Literal.ofInteger(7));
    }

    static void assertDmlResult(StatementResult result) {
        assertThat(result).isEqualTo(new StatementResult(
                List.of(), List.of(), QUALIFIED_TABLE, StatementCategory.DML));
    }

    static final class Fixture {
        final NativeWriteCommitTracker tracker = new NativeWriteCommitTracker();
        final QueryStateMachine stateMachine = new QueryStateMachine();
        final QuerySession session = new QuerySession(
                QUERY_ID, Map.of(), new UserIdentity("delete-user", Set.of(), Map.of(),
                        Instant.EPOCH), "delete-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        final StatementExecutionContext context = new StatementExecutionContext(
                stateMachine, QUERY_ID, session);
        final List<String> events = new ArrayList<>();
        final List<Statement> dispatched = new ArrayList<>();
        final DeleteMetadata metadata;
        final CatalogRegistry registry;
        Consumer<Statement> onDispatch = statement -> {};
        DispatchResult dispatchResult = new DispatchResult("worker.invalid", "delete-task",
                new ResultEndpoint("worker.invalid", 0, "delete-ticket", "grpc"));

        Fixture(boolean partitioned) {
            metadata = new DeleteMetadata(partitioned, events);
            registry = new DeleteCatalogRegistry(metadata);
            assertThat(stateMachine.tryTransition(QueryState.PLANNING)).isTrue();
            assertThat(stateMachine.getState()).isEqualTo(QueryState.PLANNING);
        }

        StatementResult execute(DeleteStatement statement) {
            var factory = new PlanExecutionFactory(new PlanExecutionFactoryContext(
                    (incomingState, incomingSession, nativeStatement) -> {
                        assertThat(incomingState).isSameAs(stateMachine);
                        assertThat(incomingSession).isSameAs(session);
                        assertNotAllocated();
                        assertThat(stateMachine.getState()).isEqualTo(QueryState.PLANNING);
                        events.add("dispatch");
                        dispatched.add(nativeStatement);
                        onDispatch.accept(nativeStatement);
                        return java.util.concurrent.CompletableFuture.completedFuture(dispatchResult);
                    },
                    null,
                    (sql, incomingSession) -> {
                        throw new AssertionError("DELETE must not use the EXPLAIN compiler");
                    }, registry, null));
            return factory.execute(statement, context);
        }

        void assertNotAllocated() {
            assertThat(metadata.overwriteTables).isEmpty();
            assertThat(tracker.hasWriteContext(QUERY_ID)).isFalse();
            assertThat(tracker.peekWriteState(QUERY_ID)).isNull();
        }

        void assertNativePending(String... expectedEvents) {
            assertThat(events).containsExactly(expectedEvents);
            assertThat(dispatched).hasSize(1);
            assertNotAllocated();
            assertThat(stateMachine.getState()).isEqualTo(QueryState.PLANNING);
            assertThat(stateMachine.isTerminal()).isFalse();
        }

        void assertNotDispatched() {
            assertThat(dispatched).isEmpty();
            assertThat(tracker.hasWriteContext(QUERY_ID)).isFalse();
            assertThat(tracker.peekWriteState(QUERY_ID)).isNull();
            assertThat(stateMachine.getState()).isEqualTo(QueryState.PLANNING);
        }
    }

    private static final class DeleteCatalogRegistry extends CatalogRegistry {
        private final ConnectorCatalogHandle handle;

        DeleteCatalogRegistry(DeleteMetadata metadata) {
            // getCatalog is the only registry operation used; no store or health monitor is started.
            super(memoryTableStores(), null);
            var descriptor = new CatalogDescriptor(CATALOG, CatalogType.CONNECTOR, "delete-test",
                    CatalogState.ACTIVE, Map.of(), "", Set.of());
            handle = new ConnectorCatalogHandle(descriptor, new GuardedDeleteConnector(metadata));
        }

        @Override
        public CatalogHandle getCatalog(String name) {
            assertThat(name).isEqualTo(CATALOG);
            return handle;
        }
    }

    private record GuardedDeleteConnector(DeleteMetadata metadata) implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            assertThat(session.getCatalog()).contains(CATALOG);
            return metadata;
        }

        @Override
        public ConnectorOutputHandleCodec getOutputHandleCodec() {
            return new FixtureOutputHandleCodec(
                    "pangrova.io/test/delete-output-handle", TABLE,
                    Set.of("OVERWRITE"), List.of("id", "region"),
                    mode -> new DeleteOutput(metadata.table));
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new AssertionError("Coordinator DELETE must not enumerate or read splits");
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new AssertionError("Coordinator DELETE must not read pages");
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new AssertionError("Coordinator DELETE must not create a page writer");
        }
    }

    record DeleteTable(SchemaTableName name, List<String> partitionKeys) implements ConnectorTableHandle {
        DeleteTable {
            partitionKeys = List.copyOf(partitionKeys);
        }
    }

    record DeleteOutput(DeleteTable table) implements ConnectorOutputTableHandle {
        @Override
        public Optional<SchemaTableName> targetTable() {
            return Optional.of(table.name());
        }

        @Override
        public String writeMode() {
            return "OVERWRITE";
        }
    }

    private record DeleteColumn(String columnName) implements ColumnHandle {}

    static final class DeleteMetadata implements ConnectorMetadata {
        final DeleteTable table;
        final DeleteOutput output;
        final List<DeleteFilter> filters = new ArrayList<>();
        final List<ConnectorTableHandle> overwriteTables = new ArrayList<>();
        private final List<String> events;
        RuntimeException deleteFailure;
        RuntimeException overwriteFailure;
        ConnectorSession overwriteSession;

        DeleteMetadata(boolean partitioned, List<String> events) {
            table = new DeleteTable(TABLE, partitioned ? List.of("region") : List.of());
            output = new DeleteOutput(table);
            this.events = events;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of(SCHEMA);
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return schemaName.isEmpty() || schemaName.get().equals(SCHEMA) ? List.of(TABLE) : List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            assertThat(session.getCatalog()).contains(CATALOG);
            assertThat(name).isEqualTo(TABLE);
            return table;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle handle) {
            assertThat(handle).isSameAs(table);
            return new ConnectorTableMetadata(TABLE, List.of("id", "region"), List.of(
                    new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("region", PangrovaType.VARCHAR)), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
            assertThat(handle).isSameAs(table);
            return Map.of("id", new DeleteColumn("id"), "region", new DeleteColumn("region"));
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle handle) {
            assertThat(handle).isSameAs(table);
            return TableStatistics.UNKNOWN;
        }

        @Override
        public void deleteByFilter(ConnectorSession session, ConnectorTableHandle handle, DeleteFilter filter) {
            assertThat(handle).isSameAs(table);
            assertOperationalSession(session);
            events.add("deleteByFilter");
            filters.add(filter);
            if (deleteFailure != null) {
                throw deleteFailure;
            }
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle handle) {
            assertThat(handle).isSameAs(table);
            assertOperationalSession(session);
            events.add("beginOverwrite");
            overwriteTables.add(handle);
            overwriteSession = session;
            if (overwriteFailure != null) {
                throw overwriteFailure;
            }
            return output;
        }

        private static void assertOperationalSession(ConnectorSession session) {
            assertThat(session.getCatalog()).contains(CATALOG);
            assertThat(session.getSchema()).contains(SCHEMA);
            assertThat(session.getUser()).isEqualTo("delete-user");
            assertThat(session.getProperties()).containsEntry("tenant_id", "delete-tenant");
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle handle,
                                                        Map<String, String> staticPartition) {
            throw new AssertionError("DELETE must register full-table overwrite, not a static partition overwrite");
        }

        @Override
        public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle handle) {
            throw new AssertionError("Native DELETE writes survivors, not delete markers");
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle handle) {
            throw new AssertionError("Native DELETE must overwrite, not append");
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session, ConnectorOutputTableHandle handle,
                                           List<byte[]> fragments) {
            throw new AssertionError("DELETE dispatch must not commit before workers finish");
        }
    }
}
