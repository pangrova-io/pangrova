package org.pangrova.parser.parse;

import org.pangrova.parser.api.enums.IncrementalStrategy;
import org.pangrova.parser.api.enums.RefreshMode;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.format.AstPrinter;

import org.pangrova.parser.api.ast.*;
import org.pangrova.parser.api.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.api.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.api.ast.statement.ShowDynamicTablesStatement;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for Dynamic Table SQL parsing.
 * Covers R1 AC1-AC8 syntax variants and error scenarios.
 */
class DynamicTableParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Nested
    class CreateDynamicTable {

        /** R1 AC1: minimal CREATE DYNAMIC TABLE with required fields. */
        @Test
        void parsesMinimalCreateDynamicTable() {
            var stmt = parse("CREATE DYNAMIC TABLE my_dt TARGET_LAG = 'PT5M' AS SELECT * FROM source");

            assertThat(stmt).isInstanceOf(CreateDynamicTableStatement.class);
            var dt = (CreateDynamicTableStatement) stmt;
            assertThat(dt.name()).isEqualTo(Identifier.of("my_dt"));
            assertThat(dt.targetLag()).isEqualTo(Duration.ofMinutes(5));
            assertThat(dt.refreshMode()).isEmpty();
            assertThat(dt.warehouse()).isEmpty();
            assertThat(dt.query()).isNotNull();
        }

        /** R1 AC1: qualified name with catalog.schema.table. */
        @Test
        void parsesQualifiedName() {
            var stmt = parse("CREATE DYNAMIC TABLE cat.sch.my_dt TARGET_LAG = 'PT1H' AS SELECT 1");
            var dt = (CreateDynamicTableStatement) stmt;
            assertThat(dt.name().parts()).containsExactly("cat", "sch", "my_dt");
        }

        /** Omitting TARGET_LAG is the explicit manual-refresh form. */
        @Test
        void omitsTargetLagForManualRefresh() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE manual_dt AS SELECT * FROM source");

            assertThat(dt.targetLag()).isZero();
            assertThat(dt.refreshMode()).isEmpty();
        }

        @Test
        void rejectsZeroTargetLag() {
            assertThatThrownBy(() -> parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT0S' AS SELECT * FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("TARGET_LAG must be a positive duration");
        }

        /** R1 AC2: REFRESH_MODE = CDC. */
        @Test
        void parsesRefreshModeCdc() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT10S' REFRESH_MODE = CDC AS SELECT * FROM t");
            assertThat(dt.refreshMode()).contains(RefreshMode.CDC);
        }

        /** R1 AC2: REFRESH_MODE = SNAPSHOT. */
        @Test
        void parsesRefreshModeSnapshot() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' REFRESH_MODE = SNAPSHOT AS SELECT * FROM t");
            assertThat(dt.refreshMode()).contains(RefreshMode.SNAPSHOT);
        }

        /** R1 AC2: REFRESH_MODE = AUTO. */
        @Test
        void parsesRefreshModeAuto() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' REFRESH_MODE = AUTO AS SELECT * FROM t");
            assertThat(dt.refreshMode()).contains(RefreshMode.AUTO);
        }

        /** REFRESH_MODE = INCREMENTAL with a pinned strategy. */
        @Test
        void parsesIncrementalStrategy() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' REFRESH_MODE = INCREMENTAL "
                            + "INCREMENTAL_STRATEGY = APPEND_ONLY AS SELECT * FROM t");
            assertThat(dt.refreshMode()).contains(RefreshMode.INCREMENTAL);
            assertThat(dt.incrementalStrategy()).contains(IncrementalStrategy.APPEND_ONLY);
        }

        @Test
        void parsesAutomaticIncrementalStrategy() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 INCREMENTAL_STRATEGY = AUTO AS SELECT * FROM t");
            assertThat(dt.incrementalStrategy()).contains(IncrementalStrategy.AUTO);
        }

        /** R1 AC3: optional WAREHOUSE clause. */
        @Test
        void parsesWarehouseClause() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' WAREHOUSE = 'compute_xl' AS SELECT * FROM t");
            assertThat(dt.warehouse()).contains("compute_xl");
        }

        /** R1 AC2 + AC3: both REFRESH_MODE and WAREHOUSE. */
        @Test
        void parsesRefreshModeAndWarehouse() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' REFRESH_MODE = CDC WAREHOUSE = 'wh1' AS SELECT * FROM t");
            assertThat(dt.refreshMode()).contains(RefreshMode.CDC);
            assertThat(dt.warehouse()).contains("wh1");
        }

        /** R1 AC1: various ISO-8601 durations. */
        @Test
        void parsesVariousDurations() {
            assertThat(parseDt("PT30S").targetLag()).isEqualTo(Duration.ofSeconds(30));
            assertThat(parseDt("PT1H").targetLag()).isEqualTo(Duration.ofHours(1));
            assertThat(parseDt("PT2H30M").targetLag()).isEqualTo(Duration.ofHours(2).plusMinutes(30));
        }

        /** R1 AC5: invalid TARGET_LAG format. */
        @Test
        void rejectsInvalidTargetLagFormat() {
            assertThatThrownBy(() -> parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = '5 minutes' AS SELECT * FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Invalid TARGET_LAG duration")
                    .hasMessageContaining("ISO-8601");
        }

        /** R1 AC5: another invalid format. */
        @Test
        void rejectsNonIso8601Duration() {
            assertThatThrownBy(() -> parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = '1 hour' AS SELECT * FROM t"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Invalid TARGET_LAG duration");
        }

        /** R1 AC1: complex query with WHERE and GROUP BY. */
        @Test
        void parsesComplexQuery() {
            var dt = (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE summary TARGET_LAG = 'PT5M' AS "
                            + "SELECT region, SUM(amount) FROM orders WHERE (status = 'active') GROUP BY region");
            assertThat(dt.query().groupBy()).hasSize(1);
            assertThat(dt.query().where()).isPresent();
        }

        private CreateDynamicTableStatement parseDt(String lag) {
            return (CreateDynamicTableStatement) parse(
                    "CREATE DYNAMIC TABLE dt1 TARGET_LAG = '" + lag + "' AS SELECT * FROM t");
        }
    }

    @Nested
    class AlterDynamicTable {

        /** R1 AC6: ALTER DYNAMIC TABLE SUSPEND. */
        @Test
        void parsesSuspend() {
            var stmt = (AlterDynamicTableStatement) parse("ALTER DYNAMIC TABLE my_dt SUSPEND");
            assertThat(stmt.name()).isEqualTo(Identifier.of("my_dt"));
            assertThat(stmt.action()).isInstanceOf(AlterDynamicTableAction.Suspend.class);
        }

        /** ALTER DYNAMIC TABLE RESUME. */
        @Test
        void parsesResume() {
            var stmt = (AlterDynamicTableStatement) parse("ALTER DYNAMIC TABLE my_dt RESUME");
            assertThat(stmt.action()).isInstanceOf(AlterDynamicTableAction.Resume.class);
        }

        /** R1 AC6: ALTER DYNAMIC TABLE REFRESH (manual trigger). */
        @Test
        void parsesRefresh() {
            var stmt = (AlterDynamicTableStatement) parse("ALTER DYNAMIC TABLE my_dt REFRESH");
            assertThat(stmt.action()).isInstanceOf(AlterDynamicTableAction.Refresh.class);
        }

        /** ALTER DYNAMIC TABLE SET TARGET_LAG. */
        @Test
        void parsesSetTargetLag() {
            var stmt = (AlterDynamicTableStatement) parse(
                    "ALTER DYNAMIC TABLE my_dt SET TARGET_LAG = 'PT10M'");
            assertThat(stmt.action()).isInstanceOf(AlterDynamicTableAction.SetTargetLag.class);
            var action = (AlterDynamicTableAction.SetTargetLag) stmt.action();
            assertThat(action.newLag()).isEqualTo(Duration.ofMinutes(10));
        }

        /** ALTER DYNAMIC TABLE SET TARGET_LAG with invalid duration. */
        @Test
        void rejectsInvalidSetTargetLag() {
            assertThatThrownBy(() -> parse(
                    "ALTER DYNAMIC TABLE my_dt SET TARGET_LAG = 'bad'"))
                    .isInstanceOf(ParseException.class)
                    .hasMessageContaining("Invalid TARGET_LAG duration");
        }

        @Test
        void parsesSetRefreshMode() {
            var stmt = (AlterDynamicTableStatement) parse(
                    "ALTER DYNAMIC TABLE my_dt SET REFRESH_MODE = INCREMENTAL");
            assertThat(stmt.action())
                    .isInstanceOfSatisfying(AlterDynamicTableAction.SetRefreshMode.class,
                            action -> assertThat(action.mode())
                                    .isEqualTo(RefreshMode.INCREMENTAL));
        }

        @Test
        void parsesRename() {
            var stmt = (AlterDynamicTableStatement) parse(
                    "ALTER DYNAMIC TABLE my_dt RENAME TO cat.sch.dt2");
            assertThat(stmt.action())
                    .isInstanceOfSatisfying(AlterDynamicTableAction.Rename.class,
                            action -> assertThat(action.newName().parts())
                                    .containsExactly("cat", "sch", "dt2"));
        }

        /** R1 AC7: ALTER DYNAMIC TABLE REDEFINE AS new_query. */
        @Test
        void parsesRedefine() {
            var stmt = (AlterDynamicTableStatement) parse(
                    "ALTER DYNAMIC TABLE my_dt REDEFINE AS SELECT a, b FROM new_source");
            assertThat(stmt.action()).isInstanceOf(AlterDynamicTableAction.Redefine.class);
            var action = (AlterDynamicTableAction.Redefine) stmt.action();
            assertThat(action.newQuery()).isNotNull();
        }

        /** ALTER with qualified name. */
        @Test
        void parsesAlterWithQualifiedName() {
            var stmt = (AlterDynamicTableStatement) parse(
                    "ALTER DYNAMIC TABLE cat.sch.my_dt REFRESH");
            assertThat(stmt.name().parts()).containsExactly("cat", "sch", "my_dt");
        }
    }

    @Nested
    class DropDynamicTable {

        @Test
        void parsesDrop() {
            var stmt = (DropDynamicTableStatement) parse("DROP DYNAMIC TABLE my_dt");
            assertThat(stmt.name()).isEqualTo(Identifier.of("my_dt"));
            assertThat(stmt.cascade()).isFalse();
        }

        @Test
        void parsesDropCascade() {
            var stmt = (DropDynamicTableStatement) parse("DROP DYNAMIC TABLE my_dt CASCADE");
            assertThat(stmt.name()).isEqualTo(Identifier.of("my_dt"));
            assertThat(stmt.cascade()).isTrue();
        }

        @Test
        void parsesDropWithQualifiedName() {
            var stmt = (DropDynamicTableStatement) parse("DROP DYNAMIC TABLE cat.sch.dt1 CASCADE");
            assertThat(stmt.name().parts()).containsExactly("cat", "sch", "dt1");
        }
    }

    @Nested
    class ShowAndDescribe {

        @Test
        void parsesShowDynamicTables() {
            var stmt = parse("SHOW DYNAMIC TABLES");
            assertThat(stmt).isInstanceOf(ShowDynamicTablesStatement.class);
        }

        @Test
        void parsesDescribeDynamicTable() {
            var stmt = (DescribeDynamicTableStatement) parse("DESCRIBE DYNAMIC TABLE my_dt");
            assertThat(stmt.name()).isEqualTo(Identifier.of("my_dt"));
        }
    }

    @Nested
    class AstPrinterRoundTrip {

        /** R1 AC8: AstPrinter formats CreateDynamicTableStatement back to valid SQL. */
        @Test
        void printsCreateDynamicTable() {
            var stmt = parse("CREATE DYNAMIC TABLE my_dt TARGET_LAG = 'PT5M' AS SELECT * FROM t");
            String printed = printer.print(stmt);
            assertThat(printed).startsWith("CREATE DYNAMIC TABLE");
            assertThat(printed).contains("TARGET_LAG");
            // Verify round-trip
            var reparsed = parse(printed);
            assertThat(reparsed).isEqualTo(stmt);
        }

        @Test
        void printsCreateWithAllClauses() {
            var stmt = parse("CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' "
                    + "REFRESH_MODE = CDC WAREHOUSE = 'wh1' AS SELECT * FROM t");
            String printed = printer.print(stmt);
            assertThat(printed).contains("REFRESH_MODE = CDC");
            assertThat(printed).contains("WAREHOUSE = 'wh1'");
            assertThat(parse(printed)).isEqualTo(stmt);
        }

        @Test
        void printsManualCreateWithoutTargetLag() {
            var stmt = parse("CREATE DYNAMIC TABLE manual_dt AS SELECT * FROM t");
            String printed = printer.print(stmt);
            assertThat(printed).doesNotContain("TARGET_LAG");
            assertThat(parse(printed)).isEqualTo(stmt);
        }

        @Test
        void printsIncrementalStrategy() {
            var stmt = parse("CREATE DYNAMIC TABLE dt1 TARGET_LAG = 'PT5M' "
                    + "REFRESH_MODE = INCREMENTAL INCREMENTAL_STRATEGY = APPEND_ONLY "
                    + "AS SELECT * FROM t");
            String printed = printer.print(stmt);
            assertThat(printed).contains("INCREMENTAL_STRATEGY = APPEND_ONLY");
            assertThat(parse(printed)).isEqualTo(stmt);
        }

        @Test
        void printsAlterDynamicTable() {
            for (String sql : new String[]{
                    "ALTER DYNAMIC TABLE dt1 SUSPEND",
                    "ALTER DYNAMIC TABLE dt1 RESUME",
                    "ALTER DYNAMIC TABLE dt1 REFRESH",
                    "ALTER DYNAMIC TABLE dt1 SET TARGET_LAG = 'PT10M'",
                    "ALTER DYNAMIC TABLE dt1 SET REFRESH_MODE = INCREMENTAL",
                    "ALTER DYNAMIC TABLE dt1 RENAME TO cat.sch.dt2",
                    "ALTER DYNAMIC TABLE dt1 REDEFINE AS SELECT a FROM t"
            }) {
                var stmt = parse(sql);
                assertThat(parse(printer.print(stmt))).isEqualTo(stmt);
            }
        }

        @Test
        void printsDropDynamicTable() {
            var stmt = parse("DROP DYNAMIC TABLE dt1 CASCADE");
            assertThat(parse(printer.print(stmt))).isEqualTo(stmt);
        }

        @Test
        void printsShowAndDescribe() {
            assertThat(printer.print(parse("SHOW DYNAMIC TABLES"))).isEqualTo("SHOW DYNAMIC TABLES");
            var desc = parse("DESCRIBE DYNAMIC TABLE my_dt");
            assertThat(parse(printer.print(desc))).isEqualTo(desc);
        }
    }

    private Statement parse(String sql) {
        return parser.parse(sql);
    }
}
