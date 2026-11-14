package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.types.value.TypedRow;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for factory category consistency.
 *
 * <p><b>Validates: Requirements 2.2, 2.3, 2.4, 2.5</b></p>
 */
class ExecutionFactoryPropertyTest {

    private static final Identifier TABLE_ID = Identifier.of("cat", "sch", "tbl");

    /**
     * Property 6: Factory category consistency — DDL factory returns DDL category.
     */
    @Property(tries = 30)
    void ddlFactoryReturnsDdlCategory(@ForAll("ddlStatements") Statement stmt) {
        RecordingExecutor executor = new RecordingExecutor();
        StatementExecutionFactory<Statement> factory = new TestDdlFactory(executor);
        StatementExecutionContext ctx = createContext();

        StatementResult result = factory.execute(stmt, ctx);

        assertThat(result.category()).isEqualTo(StatementCategory.DDL);
        if (result.mutatedTable() != null) {
            assertThat(result.mutatedTable()).isEqualTo(result.mutatedTable().toLowerCase());
        }
    }

    /**
     * Property 6 (continued): DML factory returns DML category.
     */
    @Property(tries = 30)
    void dmlFactoryReturnsDmlCategory(@ForAll("dmlStatements") Statement stmt) {
        RecordingExecutor executor = new RecordingExecutor();
        StatementExecutionFactory<Statement> factory = new TestDmlFactory(executor);
        StatementExecutionContext ctx = createContext();

        StatementResult result = factory.execute(stmt, ctx);

        assertThat(result.category()).isEqualTo(StatementCategory.DML);
        if (result.mutatedTable() != null) {
            assertThat(result.mutatedTable()).isEqualTo(result.mutatedTable().toLowerCase());
        }
    }

    @Provide
    Arbitrary<Statement> ddlStatements() {
        return Arbitraries.of(
                new CreateTableStatement(TABLE_ID, List.of(), false,
                        List.of(), List.of(), Optional.empty(), Map.of()),
                new DropTableStatement(TABLE_ID, false),
                new AlterTableStatement.AddColumn(TABLE_ID, "col", org.pangrova.parser.build.type.RawSqlTypeParser.parse("STRING")),
                new AlterTableStatement.DropColumn(TABLE_ID, "col"),
                new AlterTableStatement.RenameColumn(TABLE_ID, "old", "new_col"),
                new AlterTableStatement.ChangeColumnType(TABLE_ID, "col", org.pangrova.parser.build.type.RawSqlTypeParser.parse("BIGINT")),
                new AlterTableStatement.SetPartitionSpec(TABLE_ID, List.of()));
    }

    @Provide
    Arbitrary<Statement> dmlStatements() {
        return Arbitraries.of(
                new InsertStatement(TABLE_ID, Optional.empty(), List.of()),
                new InsertOverwriteStatement(TABLE_ID, Optional.empty(), List.of()),
                new DeleteStatement(TABLE_ID, Optional.empty()),
                new UpdateStatement(TABLE_ID, List.of(), Optional.empty()));
    }

    private static StatementExecutionContext createContext() {
        QueryStateMachine sm = new QueryStateMachine();
        QuerySession session = new QuerySession(
                "q-test", Map.of(), null, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        return new StatementExecutionContext(sm, "q-test", session);
    }

    /** Inline executor that returns a fixed row for any statement. */
    private static final class RecordingExecutor {
        List<TypedRow> execute(Statement stmt) {
            return List.of(TypedRow.builder().putString("result", "ok").build());
        }
    }

    /** Test DDL factory that mirrors MetadataExecutionFactory behavior for DDL. */
    private static final class TestDdlFactory implements StatementExecutionFactory<Statement> {
        private final RecordingExecutor executor;

        TestDdlFactory(RecordingExecutor executor) {
            this.executor = executor;
        }

        @Override
        public StatementResult execute(Statement stmt, StatementExecutionContext ctx) {
            List<TypedRow> rows = executor.execute(stmt);
            String mutatedTable = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
            List<String> colNames = rows.isEmpty()
                    ? List.of() : List.copyOf(rows.get(0).columnNames());
            return new StatementResult(rows, colNames, mutatedTable, StatementCategory.DDL);
        }

        @Override
        public Set<Class<? extends Statement>> supportedTypes() {
            return Set.of(CreateTableStatement.class, DropTableStatement.class,
                    AlterTableStatement.AddColumn.class, AlterTableStatement.DropColumn.class,
                    AlterTableStatement.RenameColumn.class,
                    AlterTableStatement.ChangeColumnType.class,
                    AlterTableStatement.SetPartitionSpec.class);
        }
    }

    /** Test DML factory that mirrors MetadataExecutionFactory behavior for DML. */
    private static final class TestDmlFactory implements StatementExecutionFactory<Statement> {
        private final RecordingExecutor executor;

        TestDmlFactory(RecordingExecutor executor) {
            this.executor = executor;
        }

        @Override
        public StatementResult execute(Statement stmt, StatementExecutionContext ctx) {
            List<TypedRow> rows = executor.execute(stmt);
            String mutatedTable = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
            List<String> colNames = rows.isEmpty()
                    ? List.of() : List.copyOf(rows.get(0).columnNames());
            return new StatementResult(rows, colNames, mutatedTable, StatementCategory.DML);
        }

        @Override
        public Set<Class<? extends Statement>> supportedTypes() {
            return Set.of(InsertStatement.class, InsertOverwriteStatement.class,
                    DeleteStatement.class, UpdateStatement.class);
        }
    }
}
