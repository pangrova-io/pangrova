package org.pangrova.parser.parse;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.format.AstPrinter;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL grammar and printer boundary, not catalog mutation or Native execution. */
class AlterTableSortOrderParserTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @ParameterizedTest
    @ValueSource(strings = {"SET SORT ORDER", "set sort order", "SeT SoRt OrDeR"})
    void preservesColumnOrderDirectionsAndPrintRoundTrip(String clause) {
        var statement = (AlterTableStatement.SetSortOrder) parser.parse(
                "ALTER TABLE catalog.schema.events " + clause + " (id DESC, ts, sort ASC)");
        assertThat(statement.tableName()).isEqualTo(Identifier.of("catalog", "schema", "events"));
        assertThat(statement.sortColumns()).containsExactly(
                new AlterTableStatement.SortColumn("id", false),
                new AlterTableStatement.SortColumn("ts", true),
                new AlterTableStatement.SortColumn("sort", true));
        assertThat(statement.useZOrder()).isFalse();
        assertThat(parser.parse(new AstPrinter().print(statement))).isEqualTo(statement);
    }

    @Test
    void preservesZOrderAndSortAsNonReservedIdentifier() {
        var statement = (AlterTableStatement.SetSortOrder) parser.parse(
                "ALTER TABLE sort SET SORT ORDER (sort DESC) USING ZORDER");
        assertThat(statement.tableName()).isEqualTo(Identifier.of("sort"));
        assertThat(statement.sortColumns()).isEqualTo(
                List.of(new AlterTableStatement.SortColumn("sort", false)));
        assertThat(statement.useZOrder()).isTrue();
        assertThat(parser.parse(new AstPrinter().print(statement))).isEqualTo(statement);
        var created = (CreateTableStatement) parser.parse("CREATE TABLE sort (sort BIGINT)");
        assertThat(created.tableName()).isEqualTo(Identifier.of("sort"));
        assertThat(created.columns()).hasSize(1);
        assertThat(created.columns().get(0).name()).isEqualTo("sort");
    }

    @ParameterizedTest
    @ValueSource(strings = {"()", "(id DESC,)", "(id SIDEWAYS)"})
    void rejectsMalformedSortColumns(String columns) {
        assertThatThrownBy(() -> parser.parse("ALTER TABLE events SET SORT ORDER " + columns))
                .isInstanceOf(ParseException.class);
    }
}
