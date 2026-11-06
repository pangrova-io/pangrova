package org.pangrova.query.core.statement;

import java.util.List;
import java.util.Optional;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;
import org.junit.jupiter.api.Test;

import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for StatementTableTargets consistency.
 *
 * <p><b>Validates: Requirement 3.1</b></p>
 */
class StatementTableTargetsPropertyTest {

    @Test
    void everyAlterTableVariantRetainsItsTargetTable() {
        Identifier target = Identifier.of("Catalog", "Schema", "Table");
        List<Statement> statements = List.of(
                new AlterTableStatement.SetSortOrder(target, List.of()),
                new AlterTableStatement.CreateBranch(target, "dev"),
                new AlterTableStatement.DropBranch(target, "dev"),
                new AlterTableStatement.CreateTag(target, "v1"),
                new AlterTableStatement.DropTag(target, "v1"),
                new AlterTableStatement.SetColumnTag(target, "col", "key", "value"),
                new AlterTableStatement.UnsetColumnTag(target, "col", "key"),
                new AlterTableStatement.Adopt(target, java.util.Map.of()),
                new AlterTableStatement.Unlink(target));
        assertThat(statements).allSatisfy(statement ->
                assertThat(StatementTableTargets.identifier(statement)).isEqualTo(target));
    }

    /** Every generated target retains exact components, including case and literal dots. */
    @Property(tries = 50)
    void identifierPreservesTheOriginalComponents(@ForAll("targetedStatements") TargetedStatement target) {
        assertThat(StatementTableTargets.identifier(target.statement())).isEqualTo(target.identifier());
    }

    @Property(tries = 20)
    void noTargetForUnregisteredTypes(@ForAll("unregisteredStatements") Statement stmt) {
        assertThat(StatementTableTargets.identifier(stmt)).isNull();
    }

    @Provide
    Arbitrary<TargetedStatement> targetedStatements() {
        return qualifiedTableIdentifiers().flatMap(id -> Arbitraries.of(List.<Statement>of(
                new CreateTableStatement(id, List.of(), false, List.of(), List.of(),
                        Optional.empty(), java.util.Map.of()),
                new DropTableStatement(id, false),
                new AlterTableStatement.AddColumn(id, "col", org.pangrova.parser.core.build.type.RawSqlTypeParser.parse("STRING")),
                new AlterTableStatement.DropColumn(id, "col"),
                new AlterTableStatement.RenameColumn(id, "old", "new_col"),
                new AlterTableStatement.ChangeColumnType(id, "col", org.pangrova.parser.core.build.type.RawSqlTypeParser.parse("BIGINT")),
                new AlterTableStatement.SetPartitionSpec(id, List.of()),
                new InsertStatement(id, Optional.empty(), List.of()),
                new InsertOverwriteStatement(id, Optional.empty(), List.of()),
                new DeleteStatement(id, Optional.empty()),
                new UpdateStatement(id, List.of(), Optional.empty())))
                .map(statement -> new TargetedStatement(id, statement)));
    }

    record TargetedStatement(Identifier identifier, Statement statement) { }

    @Provide
    Arbitrary<Statement> unregisteredStatements() {
        return Arbitraries.just(
                new org.pangrova.parser.api.ast.statement.CreateSchemaStatement(
                        Identifier.of("catalog", "my_schema"), false));
    }

    private Arbitrary<Identifier> qualifiedTableIdentifiers() {
        Arbitrary<String> catalog = Arbitraries.of("catalog", "Cat1", "PROD");
        Arbitrary<String> schema = Arbitraries.of("schema", "public", "DB", "Schema.With.Dot");
        Arbitrary<String> table = Arbitraries.of("users", "Orders", "DATA", "Table.With.Dot");
        return Combinators.combine(catalog, schema, table)
                .as((c, s, t) -> Identifier.of(c, s, t));
    }
}
