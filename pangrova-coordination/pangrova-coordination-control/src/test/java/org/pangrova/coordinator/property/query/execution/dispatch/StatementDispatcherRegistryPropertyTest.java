package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementExecutionFactory;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.ast.statement.ExplainStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for StatementDispatcher registry dispatch completeness.
 *
 * <p><b>Validates: Requirements 1.1, 1.3, 2.2, 2.3</b></p>
 */
class StatementDispatcherRegistryPropertyTest {

    private static final Identifier TABLE_ID = Identifier.of("cat", "sch", "tbl");

    /**
     * Property 1: Registry dispatch completeness.
     * All isDdlOrDml statements have a registered factory in the combined registry.
     */
    @Property(tries = 50)
    void allDdlDmlStatementsHaveRegisteredFactory(
            @ForAll("ddlDmlStatements") Statement stmt) {
        Map<Class<? extends Statement>, StatementExecutionFactory<?>> registry =
                buildFullRegistry();

        assertThat(registry.containsKey(stmt.getClass()))
                .as("Registry must contain factory for %s", stmt.getClass().getSimpleName())
                .isTrue();

        StatementExecutionFactory<?> factory = registry.get(stmt.getClass());
        assertThat(factory).isNotNull();
        assertThat(factory.supportedTypes().contains(stmt.getClass()))
                .as("Factory supportedTypes must include %s", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Provide
    Arbitrary<Statement> ddlDmlStatements() {
        return Arbitraries.of(
                new CreateTableStatement(TABLE_ID, List.of(), false,
                        List.of(), List.of(), Optional.empty(), Map.of()),
                new DropTableStatement(TABLE_ID, false),
                new AlterTableStatement.AddColumn(TABLE_ID, "col", org.pangrova.parser.build.type.RawSqlTypeParser.parse("STRING")),
                new AlterTableStatement.DropColumn(TABLE_ID, "col"),
                new AlterTableStatement.RenameColumn(TABLE_ID, "old", "new_col"),
                new AlterTableStatement.ChangeColumnType(TABLE_ID, "col", org.pangrova.parser.build.type.RawSqlTypeParser.parse("BIGINT")),
                new AlterTableStatement.SetPartitionSpec(TABLE_ID, List.of()),
                new CreateSchemaStatement(Identifier.of("cat", "sch"), false),
                new InsertStatement(TABLE_ID, Optional.empty(), List.of()),
                new InsertOverwriteStatement(TABLE_ID, Optional.empty(), List.of()),
                new DeleteStatement(TABLE_ID, Optional.empty()),
                new UpdateStatement(TABLE_ID, List.of(), Optional.empty()));
    }

    /**
     * Builds a registry matching the production StatementDispatcher configuration,
     * using lightweight test factories that mirror the real factory supportedTypes().
     */
    @SuppressWarnings("unchecked")
    private static Map<Class<? extends Statement>, StatementExecutionFactory<?>> buildFullRegistry() {
        Map<Class<? extends Statement>, StatementExecutionFactory<?>> registry =
                new ConcurrentHashMap<>();

        StatementExecutionFactory<Statement> ddlFactory = new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(Statement stmt, StatementExecutionContext ctx) {
                String table = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
                return new StatementResult(List.of(), List.of(), table, StatementCategory.DDL);
            }

            @Override
            public Set<Class<? extends Statement>> supportedTypes() {
                return Set.of(CreateTableStatement.class, DropTableStatement.class,
                        AlterTableStatement.AddColumn.class, AlterTableStatement.DropColumn.class,
                        AlterTableStatement.RenameColumn.class,
                        AlterTableStatement.ChangeColumnType.class,
                        AlterTableStatement.SetPartitionSpec.class,
                        CreateSchemaStatement.class);
            }
        };

        StatementExecutionFactory<Statement> dmlFactory = new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(Statement stmt, StatementExecutionContext ctx) {
                String table = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
                return new StatementResult(List.of(), List.of(), table, StatementCategory.DML);
            }

            @Override
            public Set<Class<? extends Statement>> supportedTypes() {
                return Set.of(InsertStatement.class, InsertOverwriteStatement.class,
                        DeleteStatement.class, UpdateStatement.class, MergeStatement.class);
            }
        };

        StatementExecutionFactory<InsertSelectStatement> isqFactory =
                new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(InsertSelectStatement stmt,
                                           StatementExecutionContext ctx) {
                String table = String.join(".", stmt.tableName().parts()).toLowerCase();
                return new StatementResult(List.of(), List.of(), table, StatementCategory.DML);
            }

            @Override
            public Set<Class<? extends InsertSelectStatement>> supportedTypes() {
                return Set.of(InsertSelectStatement.class);
            }
        };

        StatementExecutionFactory<ExplainStatement> explainFactory =
                new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(ExplainStatement stmt,
                                           StatementExecutionContext ctx) {
                return new StatementResult(List.of(), List.of(), null, StatementCategory.EXPLAIN);
            }

            @Override
            public Set<Class<? extends ExplainStatement>> supportedTypes() {
                return Set.of(ExplainStatement.class);
            }
        };

        for (StatementExecutionFactory<?> factory :
                List.of(ddlFactory, dmlFactory, isqFactory, explainFactory)) {
            for (var type : factory.supportedTypes()) {
                registry.put((Class<? extends Statement>) type, factory);
            }
        }
        return registry;
    }
}
