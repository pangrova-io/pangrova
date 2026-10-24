package org.pangrova.parser;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;
import org.pangrova.parser.ast.statement.ShowMaterializationDependenciesStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationGcHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationRefreshHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationsStatement;

import static org.assertj.core.api.Assertions.assertThat;

/** Materialization observability SQL parsing and printing. */
class MaterializationObservabilityParserTest {

    private final SqlParser parser = new SqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Test
    void parsesTenantWideShows() {
        var objects = parse("SHOW MATERIALIZATIONS");
        assertThat(objects).isInstanceOf(ShowMaterializationsStatement.class);
        assertThat(objects.type()).isEqualTo(StatementType.SHOW_MATERIALIZATIONS);

        var refresh = parse("SHOW MATERIALIZATION REFRESH HISTORY");
        assertThat(refresh).isInstanceOf(ShowMaterializationRefreshHistoryStatement.class);
        assertThat(((ShowMaterializationRefreshHistoryStatement) refresh).object()).isEmpty();
        assertThat(refresh.type())
                .isEqualTo(StatementType.SHOW_MATERIALIZATION_REFRESH_HISTORY);

        var dependencies = parse("SHOW MATERIALIZATION DEPENDENCIES");
        assertThat(dependencies).isInstanceOf(ShowMaterializationDependenciesStatement.class);
        assertThat(((ShowMaterializationDependenciesStatement) dependencies).object()).isEmpty();
        assertThat(dependencies.type())
                .isEqualTo(StatementType.SHOW_MATERIALIZATION_DEPENDENCIES);

        var gc = parse("SHOW MATERIALIZATION GC HISTORY");
        assertThat(gc).isInstanceOf(ShowMaterializationGcHistoryStatement.class);
        assertThat(((ShowMaterializationGcHistoryStatement) gc).object()).isEmpty();
        assertThat(gc.type()).isEqualTo(StatementType.SHOW_MATERIALIZATION_GC_HISTORY);
    }

    @Test
    void parsesScopedShows() {
        var refresh = (ShowMaterializationRefreshHistoryStatement) parse(
                "SHOW MATERIALIZATION REFRESH HISTORY FOR cat.sch.totals");
        assertThat(refresh.object()).get()
                .satisfies(name -> assertThat(name.parts())
                        .containsExactly("cat", "sch", "totals"));

        var dependencies = (ShowMaterializationDependenciesStatement) parse(
                "SHOW MATERIALIZATION DEPENDENCIES FOR cat.sch.totals");
        assertThat(dependencies.object()).get()
                .satisfies(name -> assertThat(name.parts())
                        .containsExactly("cat", "sch", "totals"));

        var gc = (ShowMaterializationGcHistoryStatement) parse(
                "SHOW MATERIALIZATION GC HISTORY FOR cat.sch.totals");
        assertThat(gc.object()).get()
                .satisfies(name -> assertThat(name.parts())
                        .containsExactly("cat", "sch", "totals"));
    }

    @Test
    void printsAndReparsesEveryStatement() {
        for (String sql : new String[]{
                "SHOW MATERIALIZATIONS",
                "SHOW MATERIALIZATION REFRESH HISTORY",
                "SHOW MATERIALIZATION REFRESH HISTORY FOR cat.sch.totals",
                "SHOW MATERIALIZATION DEPENDENCIES",
                "SHOW MATERIALIZATION DEPENDENCIES FOR cat.sch.totals",
                "SHOW MATERIALIZATION GC HISTORY",
                "SHOW MATERIALIZATION GC HISTORY FOR cat.sch.totals"
        }) {
            Statement statement = parse(sql);
            assertThat(parse(printer.print(statement))).isEqualTo(statement);
        }
    }

    private Statement parse(String sql) {
        return parser.parse(sql);
    }
}
