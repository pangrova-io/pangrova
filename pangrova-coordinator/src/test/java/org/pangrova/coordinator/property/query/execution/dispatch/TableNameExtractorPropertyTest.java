package org.pangrova.coordinator.property.query.execution.dispatch;

import java.util.List;
import java.util.Optional;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;
import org.junit.jupiter.api.Test;

import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for TableNameExtractor consistency.
 *
 * <p><b>Validates: Requirement 3.1</b></p>
 */
class TableNameExtractorPropertyTest {

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
                new AlterTableStatement.UnsetColumnTag(target, "col", "key"));
        assertThat(statements).allSatisfy(statement ->
                assertThat(TableNameExtractor.extract(statement)).isEqualTo("catalog.schema.table"));
    }

    /**
     * Property 8: Table name extraction consistency.
     * For any registered statement type with a qualified table name,
     * extract() returns a lowercase string containing ".".
     */
    @Property(tries = 50)
    void extractReturnsLowercaseDottedString(
            @ForAll("registeredStatements") Statement stmt) {
        String result = TableNameExtractor.extract(stmt);

        assertThat(result).isNotNull();
        assertThat(result).contains(".");
        assertThat(result).isEqualTo(result.toLowerCase());
    }

    /**
     * Property 8 (continued): extract() returns null for unregistered types.
     */
    @Property(tries = 20)
    void extractReturnsNullForUnregisteredTypes(
            @ForAll("unregisteredStatements") Statement stmt) {
        String result = TableNameExtractor.extract(stmt);
        assertThat(result).isNull();
    }

    @Provide
    Arbitrary<Statement> registeredStatements() {
        Arbitrary<Identifier> tableId = qualifiedTableIdentifiers();

        Arbitrary<Statement> createTable = tableId.map(id ->
                new CreateTableStatement(id, List.of(), false, List.of(), List.of(),
                        Optional.empty(), java.util.Map.of()));

        Arbitrary<Statement> dropTable = tableId.map(id ->
                new DropTableStatement(id, false));

        Arbitrary<Statement> addColumn = tableId.map(id ->
                new AlterTableStatement.AddColumn(id, "col", DataTypeSpec.fromRawTypeString("STRING")));

        Arbitrary<Statement> dropColumn = tableId.map(id ->
                new AlterTableStatement.DropColumn(id, "col"));

        Arbitrary<Statement> renameColumn = tableId.map(id ->
                new AlterTableStatement.RenameColumn(id, "old", "new_col"));

        Arbitrary<Statement> changeType = tableId.map(id ->
                new AlterTableStatement.ChangeColumnType(id, "col", DataTypeSpec.fromRawTypeString("BIGINT")));

        Arbitrary<Statement> setPartition = tableId.map(id ->
                new AlterTableStatement.SetPartitionSpec(id, List.of()));

        Arbitrary<Statement> insert = tableId.map(id ->
                new InsertStatement(id, Optional.empty(), List.of()));

        Arbitrary<Statement> insertOverwrite = tableId.map(id ->
                new InsertOverwriteStatement(id, Optional.empty(), List.of()));

        Arbitrary<Statement> delete = tableId.map(id ->
                new DeleteStatement(id, Optional.empty()));

        Arbitrary<Statement> update = tableId.map(id ->
                new UpdateStatement(id, List.of(), Optional.empty()));

        return Arbitraries.oneOf(
                createTable, dropTable, addColumn, dropColumn,
                renameColumn, changeType, setPartition,
                insert, insertOverwrite, delete, update);
    }

    @Provide
    Arbitrary<Statement> unregisteredStatements() {
        return Arbitraries.just(
                new org.pangrova.parser.ast.statement.CreateSchemaStatement(
                        Identifier.of("catalog", "my_schema"), false));
    }

    private Arbitrary<Identifier> qualifiedTableIdentifiers() {
        Arbitrary<String> catalog = Arbitraries.of("catalog", "Cat1", "PROD");
        Arbitrary<String> schema = Arbitraries.of("schema", "public", "DB");
        Arbitrary<String> table = Arbitraries.of("users", "Orders", "DATA");
        return Combinators.combine(catalog, schema, table)
                .as((c, s, t) -> Identifier.of(c, s, t));
    }
}
