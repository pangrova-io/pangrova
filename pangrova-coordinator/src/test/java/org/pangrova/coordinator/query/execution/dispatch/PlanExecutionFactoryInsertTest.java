package org.pangrova.coordinator.query.execution.dispatch;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.expression.ArrayConstructor;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies that literal DML is admitted and dispatched without a Coordinator row writer. */
class PlanExecutionFactoryInsertTest {

    private static final Identifier TABLE = Identifier.of("test", "analytics", "events");
    private static final SchemaTableName SCHEMA_TABLE = new SchemaTableName("analytics", "events");

    @Test
    void insertValuesDispatchesWithoutAllocatingBeforeProtectedCompilation() {
        Fixture fixture = new Fixture();
        List<List<Expression>> rows = List.of(
                List.of(Literal.ofInteger(1), Literal.ofDecimal(new BigDecimal("1.25")),
                        Literal.ofTimestamp("2026-09-19 08:30:00"),
                        new ArrayConstructor(List.of(Literal.ofDecimal(new BigDecimal("0.5")),
                                Literal.ofDecimal(new BigDecimal("1.5")))),
                        new FunctionCall(Identifier.of("FILE_FROM_URL"),
                                List.of(Literal.ofString("https://example.invalid/a.pdf")))),
                List.of(Literal.ofInteger(2), Literal.ofNull(), Literal.ofNull(),
                        new ArrayConstructor(List.of(Literal.ofDecimal(new BigDecimal("2.5")),
                                Literal.ofDecimal(new BigDecimal("3.5")))), Literal.ofNull()));
        InsertStatement statement = new InsertStatement(TABLE,
                Optional.of(List.of("id", "amount", "created_at", "embedding", "asset")), rows);

        StatementResult result = fixture.execute(statement);

        assertThat(fixture.dispatched).containsExactly(statement);
        assertThat(fixture.metadata.beginInsertCount).isZero();
        assertThat(fixture.metadata.beginOverwriteCount).isZero();
        assertThat(fixture.tracker.hasWriteContext("insert-query")).isFalse();
        assertThat(fixture.events).containsExactly("dispatch", "callback");
        assertThat(result.mutatedTable()).isEqualTo("test.analytics.events");
    }

    @Test
    void staticOverwriteUsesPartitionScopedContextAndSameNativeStatement() {
        Fixture fixture = new Fixture();
        InsertOverwriteStatement statement = new InsertOverwriteStatement(
                TABLE, Optional.of(List.of("id")),
                Optional.of(Map.of("region", Literal.ofString("cn"),
                        "day", Literal.ofDate("2026-09-19"))),
                List.of(List.of(Literal.ofInteger(7))));

        fixture.execute(statement);

        assertThat(fixture.dispatched).containsExactly(statement);
        assertThat(fixture.metadata.staticPartition).isEmpty();
        assertThat(statement.partitionSpec()).contains(Map.of("day", Literal.ofDate("2026-09-19"),
                "region", Literal.ofString("cn")));
        assertThat(fixture.tracker.hasWriteContext("insert-query")).isFalse();
        assertThat(fixture.events).containsExactly("dispatch", "callback");
    }

    @Test
    void insertOnlyMergeUsesAppendContextMatchingTheNativeWriteMode() {
        Fixture fixture = new Fixture();
        MergeStatement statement = new MergeStatement(TABLE, Optional.empty(),
                new TableRef(Identifier.of("test", "analytics", "source")),
                Literal.ofBoolean(true),
                List.of(new MergeStatement.MergeClause.WhenNotMatchedInsert(
                        Optional.empty(), Optional.of(List.of("id")),
                        List.of(Literal.ofInteger(7)))));

        fixture.execute(statement);

        assertThat(fixture.metadata.beginInsertCount).isZero();
        assertThat(fixture.metadata.beginOverwriteCount).isZero();
        assertThat(fixture.dispatched).containsExactly(statement);
        assertThat(fixture.tracker.hasWriteContext("insert-query")).isFalse();
    }

    @Test
    void emptyValuesFailsBeforeWriteRegistrationOrDispatch() {
        Fixture fixture = new Fixture();
        InsertStatement empty = new InsertStatement(TABLE, Optional.empty(), List.of());

        assertThatThrownBy(() -> fixture.execute(empty))
                .hasMessageContaining("requires at least one row");
        assertThat(fixture.dispatched).isEmpty();
        assertThat(fixture.events).isEmpty();
        assertThat(fixture.tracker.hasWriteContext("insert-query")).isFalse();
    }

    @Test
    void postCommitCallbackFailureDoesNotTurnCommittedWriteIntoRetryableFailure() {
        Fixture fixture = new Fixture();
        fixture.callback = ignored -> { throw new IllegalStateException("maintenance unavailable"); };
        InsertStatement statement = new InsertStatement(
                TABLE, Optional.of(List.of("id")),
                List.of(List.of(Literal.ofInteger(1))));

        StatementResult result = fixture.execute(statement);

        assertThat(result.mutatedTable()).isEqualTo("test.analytics.events");
        assertThat(fixture.dispatched).containsExactly(statement);
    }

    private static final class Fixture {
        final List<String> events = new ArrayList<>();
        final List<Statement> dispatched = new ArrayList<>();
        final InsertMetadata metadata = new InsertMetadata(events);
        final NativeWriteCommitTracker tracker = new NativeWriteCommitTracker();
        final CatalogRegistry registry = new InsertCatalogRegistry(metadata);
        java.util.function.Consumer<String> callback = ignored -> events.add("callback");
        final CoordinatorSessionContext session = new CoordinatorSessionContext(
                "insert-query", Map.of(), new org.pangrova.spi.security.UserIdentity(
                        "insert-user", Set.of(), Map.of(), java.time.Instant.EPOCH), "tenant");

        StatementResult execute(Statement statement) {
            PlanExecutionFactory factory = new PlanExecutionFactory(new PlanExecutionFactoryContext(
                    (state, incomingSession, nativeStatement) -> {
                        dispatched.add(nativeStatement);
                        events.add("dispatch");
                        return new DispatchResult("worker.invalid", "task-1",
                                new ResultEndpoint("worker.invalid", 1, "ticket", "grpc"));
                    }, callback, (sql, ignored) -> sql,
                    registry, null));
            QueryStateMachine state = new QueryStateMachine();
            state.tryTransition(QueryStateMachine.State.PLANNING);
            return factory.execute(statement, new StatementExecutionContext(
                    state, "insert-query", session, null, null));
        }
    }

    private static final class InsertCatalogRegistry extends CatalogRegistry {
        private final CatalogHandle handle;

        InsertCatalogRegistry(InsertMetadata metadata) {
            super(null);
            CatalogDescriptor descriptor = new CatalogDescriptor(
                    "test", CatalogType.CONNECTOR, "insert-test", CatalogState.ACTIVE,
                    Map.of(), "", Set.of());
            handle = new ConnectorCatalogHandle(descriptor, new InsertConnector(metadata));
        }

        @Override
        public CatalogHandle getCatalog(String name) {
            assertThat(name).isEqualTo("test");
            return handle;
        }
    }

    private record InsertConnector(InsertMetadata metadata) implements Connector {
        @Override public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }
        @Override public ConnectorOutputHandleCodec getOutputHandleCodec() {
            return new FixtureOutputHandleCodec(
                    "pangrova.io/test/insert-output-handle", SCHEMA_TABLE,
                    Set.of("INSERT", "OVERWRITE"),
                    List.of("id", "amount", "created_at", "embedding", "asset"),
                    InsertOutput::new);
        }
        @Override public ConnectorSplitManager getSplitManager() {
            throw new AssertionError("Coordinator INSERT must not scan splits");
        }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new AssertionError("Coordinator INSERT must not read pages");
        }
        @Override public ConnectorPageSinkProvider getPageSinkProvider() {
            throw new AssertionError("Coordinator INSERT must not open page sinks");
        }
    }

    private record InsertTable() implements ConnectorTableHandle {}

    private record InsertColumn(String columnName) implements ColumnHandle {}

    private record InsertOutput(String writeMode) implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() { return Optional.of(SCHEMA_TABLE); }
    }

    private static final class InsertMetadata implements ConnectorMetadata {
        private final List<String> events;
        private final InsertTable table = new InsertTable();
        int beginInsertCount;
        int beginOverwriteCount;
        Map<String, String> staticPartition = Map.of();

        InsertMetadata(List<String> events) { this.events = events; }
        @Override public List<String> listSchemaNames(ConnectorSession session) { return List.of("analytics"); }
        @Override public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of(SCHEMA_TABLE);
        }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return name.equals(SCHEMA_TABLE) ? table : null;
        }
        @Override public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle handle) {
            List<String> names = List.of("id", "amount", "created_at", "embedding", "asset");
            return new ConnectorTableMetadata(SCHEMA_TABLE, names, List.of(
                    new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("amount", PangrovaType.DECIMAL),
                    new ConnectorTableMetadata.Column("created_at", PangrovaType.TIMESTAMP),
                    new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR),
                    new ConnectorTableMetadata.Column("asset", PangrovaType.STRUCT)), Map.of());
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle handle) {
            return Map.of("id", new InsertColumn("id"), "amount", new InsertColumn("amount"),
                    "created_at", new InsertColumn("created_at"),
                    "embedding", new InsertColumn("embedding"), "asset", new InsertColumn("asset"));
        }
        @Override public TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle handle) { return TableStatistics.UNKNOWN; }
        @Override public ConnectorOutputTableHandle beginInsert(
                ConnectorSession session, ConnectorTableHandle handle) {
            beginInsertCount++;
            events.add("beginInsert");
            return new InsertOutput("INSERT");
        }
        @Override public ConnectorOutputTableHandle beginOverwrite(
                ConnectorSession session, ConnectorTableHandle handle) {
            beginOverwriteCount++;
            events.add("beginOverwrite");
            return new InsertOutput("OVERWRITE");
        }
        @Override public ConnectorOutputTableHandle beginOverwrite(
                ConnectorSession session, ConnectorTableHandle handle,
                Map<String, String> partition) {
            staticPartition = Map.copyOf(partition);
            return beginOverwrite(session, handle);
        }
    }
}
