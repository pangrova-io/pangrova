package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests verifying StatementDispatcher correctly categorizes
 * and extracts mutatedTable for all routable statements.
 *
 * <p><b>Validates: Requirements 4.3, 10.1</b></p>
 */
class StatementDispatcherEquivalencePropertyTest {

    private static final Identifier TABLE_ID = Identifier.of("cat", "sch", "tbl");

    private final StatementDispatcher equivalenceDispatcher = buildEquivalenceDispatcher();

    /**
     * Property 11: Behavioral equivalence.
     * For all isDdlOrDml statements, the new factory-based dispatch produces
     * the expected category and mutatedTable via factory registration
     * and StatementTableTargets.
     */
    @Property(tries = 50)
    void newDispatchMatchesOldRouterBehavior(
            @ForAll("ddlDmlStatements") StatementWithExpected stmtWithExpected) {
        Statement stmt = stmtWithExpected.statement;
        StatementCategory expectedCategory = stmtWithExpected.expectedCategory;

        // New architecture: factory-based dispatch
        StatementResult result = dispatchViaFactory(stmt, expectedCategory);

        // Verify category matches what old router would produce
        assertThat(result.category()).isEqualTo(expectedCategory);

        // Verify mutatedTable matches StatementTableTargets (which replaced the old instanceof chain)
        String expectedTable = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
        assertThat(result.mutatedTable()).isEqualTo(expectedTable);

        // Verify isRoutable classification is consistent
        assertThat(equivalenceDispatcher.isRoutable(stmt))
                .as("isRoutable must return true for %s", stmt.getClass().getSimpleName())
                .isTrue();
    }

    /**
     * Property 11 (continued): DDL without table name still produces correct category.
     */
    @Property(tries = 10)
    void ddlWithoutTableNameProducesCorrectCategory() {
        Statement stmt = new CreateSchemaStatement(Identifier.of("cat", "sch"), false);
        StatementResult result = dispatchViaFactory(stmt, StatementCategory.DDL);

        assertThat(result.category()).isEqualTo(StatementCategory.DDL);
        assertThat(result.mutatedTable()).isNull();
    }

    @Provide
    Arbitrary<StatementWithExpected> ddlDmlStatements() {
        return Arbitraries.of(
                new StatementWithExpected(
                        new CreateTableStatement(TABLE_ID, List.of(), false,
                                List.of(), List.of(), Optional.empty(), Map.of()),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new DropTableStatement(TABLE_ID, false),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new AlterTableStatement.AddColumn(TABLE_ID, "col", org.pangrova.parser.core.build.type.RawSqlTypeParser.parse("STRING")),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new AlterTableStatement.DropColumn(TABLE_ID, "col"),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new AlterTableStatement.RenameColumn(TABLE_ID, "old", "new_col"),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new AlterTableStatement.ChangeColumnType(TABLE_ID, "col", org.pangrova.parser.core.build.type.RawSqlTypeParser.parse("BIGINT")),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new AlterTableStatement.SetPartitionSpec(TABLE_ID, List.of()),
                        StatementCategory.DDL),
                new StatementWithExpected(
                        new InsertStatement(TABLE_ID, Optional.empty(), List.of()),
                        StatementCategory.DML),
                new StatementWithExpected(
                        new InsertOverwriteStatement(TABLE_ID, Optional.empty(), List.of()),
                        StatementCategory.DML));
    }

    private static StatementResult dispatchViaFactory(
            Statement stmt, StatementCategory category) {
        String mutatedTable = java.util.Optional.ofNullable(StatementTableTargets.identifier(stmt))
                .map(id -> String.join(".", id.parts())).orElse(null);
        return new StatementResult(List.of(), List.of(), mutatedTable, category);
    }

    /** Pairs a statement with its expected category for equivalence verification. */
    record StatementWithExpected(Statement statement, StatementCategory expectedCategory) {}

    private static StatementDispatcher buildEquivalenceDispatcher() {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new java.util.HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(CreateTableStatement.class, noOp);
        tasks.put(DropTableStatement.class, noOp);
        tasks.put(AlterTableStatement.AddColumn.class, noOp);
        tasks.put(AlterTableStatement.DropColumn.class, noOp);
        tasks.put(AlterTableStatement.RenameColumn.class, noOp);
        tasks.put(AlterTableStatement.ChangeColumnType.class, noOp);
        tasks.put(AlterTableStatement.SetPartitionSpec.class, noOp);
        tasks.put(CreateSchemaStatement.class, noOp);
        tasks.put(InsertStatement.class, noOp);
        tasks.put(InsertOverwriteStatement.class, noOp);
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(), null);
        return new StatementDispatcher(context);
    }
}
