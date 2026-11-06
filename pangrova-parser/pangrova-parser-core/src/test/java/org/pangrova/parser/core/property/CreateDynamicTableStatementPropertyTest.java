package org.pangrova.parser.core.property;

import net.jqwik.api.*;
import org.pangrova.parser.core.format.AstPrinter;
import org.pangrova.parser.core.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 1: Dynamic Table SQL round-trip correctness.
 *
 * <p><b>Validates: Requirements 1.9</b></p>
 *
 * <p>For any valid Dynamic Table SQL statement, {@code parse(print(parse(sql)))}
 * must produce a semantically equivalent AST to {@code parse(sql)}.</p>
 */
class CreateDynamicTableStatementPropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Property(tries = 10)
    void dynamicTableRoundTripProducesEquivalentAst(
            @ForAll("dynamicTableSql") String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2)
                .as("Round-trip for [%s] -> [%s]", sql, printed)
                .isEqualTo(ast1);
    }

    @Provide
    Arbitrary<String> dynamicTableSql() {
        return Arbitraries.oneOf(
                createStatements(),
                alterStatements(),
                dropStatements(),
                showDescribeStatements()
        );
    }

    private Arbitrary<String> createStatements() {
        Arbitrary<String> names = Arbitraries.of("my_dt", "sales_summary", "catalog1.schema1.dt1");
        Arbitrary<String> lags = Arbitraries.of("PT5M", "PT1H", "PT30S", "PT10M", "PT2H");
        Arbitrary<String> modes = Arbitraries.of("", " REFRESH_MODE = SNAPSHOT",
                " REFRESH_MODE = CDC", " REFRESH_MODE = AUTO");
        Arbitrary<String> warehouses = Arbitraries.of("", " WAREHOUSE = 'wh1'",
                " WAREHOUSE = 'compute_xl'");
        Arbitrary<String> queries = Arbitraries.of(
                "SELECT * FROM source",
                "SELECT a, b FROM t WHERE (a > 1)",
                "SELECT a, SUM(b) FROM t GROUP BY a",
                "SELECT id, name FROM orders"
        );

        return Combinators.combine(names, lags, modes, warehouses, queries)
                .as((name, lag, mode, wh, q) ->
                        "CREATE DYNAMIC TABLE " + name
                                + " TARGET_LAG = '" + lag + "'"
                                + mode + wh + " AS " + q);
    }

    private Arbitrary<String> alterStatements() {
        Arbitrary<String> names = Arbitraries.of("my_dt", "catalog1.schema1.dt1");
        return Arbitraries.oneOf(
                names.map(n -> "ALTER DYNAMIC TABLE " + n + " SUSPEND"),
                names.map(n -> "ALTER DYNAMIC TABLE " + n + " RESUME"),
                names.map(n -> "ALTER DYNAMIC TABLE " + n + " REFRESH"),
                names.map(n -> "ALTER DYNAMIC TABLE " + n + " SET TARGET_LAG = 'PT10M'"),
                names.map(n -> "ALTER DYNAMIC TABLE " + n + " SET TARGET_LAG = 'PT1H'"),
                names.map(n -> "ALTER DYNAMIC TABLE " + n
                        + " REDEFINE AS SELECT a, b FROM new_source")
        );
    }

    private Arbitrary<String> dropStatements() {
        Arbitrary<String> names = Arbitraries.of("my_dt", "catalog1.schema1.dt1");
        return Arbitraries.oneOf(
                names.map(n -> "DROP DYNAMIC TABLE " + n),
                names.map(n -> "DROP DYNAMIC TABLE " + n + " CASCADE")
        );
    }

    private Arbitrary<String> showDescribeStatements() {
        return Arbitraries.of(
                "SHOW DYNAMIC TABLES",
                "DESCRIBE DYNAMIC TABLE my_dt",
                "DESCRIBE DYNAMIC TABLE catalog1.schema1.dt1"
        );
    }
}
