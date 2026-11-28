package org.pangrova.parser.syntax;

import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.format.AstPrinter;

import org.pangrova.parser.api.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.enums.IncrementalStrategy;
import org.pangrova.parser.api.enums.RefreshMode;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;
import org.pangrova.parser.api.ast.statement.AlterMaterializedViewAction;
import org.pangrova.parser.api.ast.statement.AlterMaterializedViewStatement;
import org.pangrova.parser.api.ast.statement.CreateViewStatement;
import org.pangrova.parser.api.ast.statement.DescribeMaterializedViewStatement;
import org.pangrova.parser.api.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.api.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.api.ast.statement.ShowMaterializedViewsStatement;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Materialized View SQL parsing and printing. */
class MaterializedViewParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Test
    void parsesCreateWithoutQualification() {
        var stmt = (CreateMaterializedViewStatement) parse(
                "CREATE MATERIALIZED VIEW totals AS SELECT region, SUM(amount) FROM orders GROUP BY region");

        assertThat(stmt.name()).isEqualTo(Identifier.of("totals"));
        assertThat(stmt.query().groupBy()).hasSize(1);
        assertThat(stmt.targetLag()).isZero();
        assertThat(stmt.refreshMode()).isEmpty();
        assertThat(stmt.type()).isEqualTo(StatementType.CREATE_MATERIALIZED_VIEW);
    }

    @Test
    void parsesCreateWithFreshnessOptions() {
        var stmt = (CreateMaterializedViewStatement) parse(
                "CREATE MATERIALIZED VIEW cat.sch.totals TARGET_LAG = 'PT5M' "
                        + "REFRESH_MODE = AUTO AS SELECT 1");

        assertThat(stmt.targetLag()).isEqualTo(Duration.ofMinutes(5));
        assertThat(stmt.refreshMode()).contains(RefreshMode.AUTO);
    }

    @Test
    void parsesOrReplaceAndIfNotExists() {
        var replace = (CreateMaterializedViewStatement) parse(
                "CREATE OR REPLACE MATERIALIZED VIEW cat.sch.totals AS SELECT 1");
        assertThat(replace.orReplace()).isTrue();
        assertThat(replace.ifNotExists()).isFalse();

        var ifNotExists = (CreateMaterializedViewStatement) parse(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS cat.sch.totals AS SELECT 1");
        assertThat(ifNotExists.ifNotExists()).isTrue();
        assertThat(ifNotExists.orReplace()).isFalse();
    }

    @Test
    void parsesIncrementalStrategy() {
        var stmt = (CreateMaterializedViewStatement) parse(
                "CREATE MATERIALIZED VIEW cat.sch.totals TARGET_LAG = 'PT5M' "
                        + "REFRESH_MODE = INCREMENTAL INCREMENTAL_STRATEGY = AFFECTED_KEY "
                        + "AS SELECT 1");

        assertThat(stmt.refreshMode()).contains(RefreshMode.INCREMENTAL);
        assertThat(stmt.incrementalStrategy()).contains(IncrementalStrategy.AFFECTED_KEY);
    }

    @Test
    void parsesDropIfExists() {
        var drop = (DropMaterializedViewStatement) parse(
                "DROP MATERIALIZED VIEW IF EXISTS cat.sch.totals");
        assertThat(drop.ifExists()).isTrue();
        assertThat(drop.cascade()).isFalse();
    }

    @Test
    void rejectsZeroTargetLag() {
        assertThatThrownBy(() -> parse(
                "CREATE MATERIALIZED VIEW totals TARGET_LAG = 'PT0S' AS SELECT 1"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void parsesCreateWithQualifiedName() {
        var stmt = (CreateMaterializedViewStatement) parse(
                "CREATE MATERIALIZED VIEW cat.sch.totals AS SELECT 1");

        assertThat(stmt.name().parts()).containsExactly("cat", "sch", "totals");
    }

    @Test
    void requiresQueryForCreate() {
        assertThatThrownBy(() -> parse("CREATE MATERIALIZED VIEW totals"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void parsesRefreshDropShowAndDescribe() {
        var refresh = parse("REFRESH MATERIALIZED VIEW cat.sch.totals");
        assertThat(refresh).isInstanceOf(RefreshMaterializedViewStatement.class);
        assertThat(((RefreshMaterializedViewStatement) refresh).name().parts())
                .containsExactly("cat", "sch", "totals");
        assertThat(refresh.type()).isEqualTo(StatementType.REFRESH_MATERIALIZED_VIEW);

        var drop = parse("DROP MATERIALIZED VIEW cat.sch.totals");
        assertThat(drop).isInstanceOf(DropMaterializedViewStatement.class);
        assertThat(((DropMaterializedViewStatement) drop).cascade()).isFalse();
        assertThat(drop.type()).isEqualTo(StatementType.DROP_MATERIALIZED_VIEW);

        var cascade = (DropMaterializedViewStatement) parse(
                "DROP MATERIALIZED VIEW cat.sch.totals CASCADE");
        assertThat(cascade.cascade()).isTrue();

        var show = parse("SHOW MATERIALIZED VIEWS");
        assertThat(show).isInstanceOf(ShowMaterializedViewsStatement.class);
        assertThat(show.type()).isEqualTo(StatementType.SHOW_MATERIALIZED_VIEWS);
        assertThat(((ShowMaterializedViewsStatement) show).pattern()).isNull();

        var showLike = (ShowMaterializedViewsStatement) parse(
                "SHOW MATERIALIZED VIEWS LIKE 'daily_%'");
        assertThat(showLike.pattern()).isEqualTo("daily_%");

        var describe = parse("DESCRIBE MATERIALIZED VIEW cat.sch.totals");
        assertThat(describe).isInstanceOf(DescribeMaterializedViewStatement.class);
        assertThat(describe.type()).isEqualTo(StatementType.DESCRIBE_MATERIALIZED_VIEW);
    }

    @Test
    void doesNotChangePlainViewGrammar() {
        assertThat(parse("CREATE VIEW v AS SELECT 1")).isInstanceOf(CreateViewStatement.class);
    }

    @Test
    void parsesAlterActions() {
        var suspend = (AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW cat.sch.totals SUSPEND");
        assertThat(suspend.name().parts()).containsExactly("cat", "sch", "totals");
        assertThat(suspend.action()).isInstanceOf(AlterMaterializedViewAction.Suspend.class);
        assertThat(suspend.type()).isEqualTo(StatementType.ALTER_MATERIALIZED_VIEW);

        assertThat(((AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW totals RESUME")).action())
                .isInstanceOf(AlterMaterializedViewAction.Resume.class);
        assertThat(((AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW totals REFRESH")).action())
                .isInstanceOf(AlterMaterializedViewAction.Refresh.class);

        var setLag = (AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW totals SET TARGET_LAG = 'PT2H'");
        var action = (AlterMaterializedViewAction.SetTargetLag) setLag.action();
        assertThat(action.newLag()).isEqualTo(Duration.ofHours(2));

        var setMode = (AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW totals SET REFRESH_MODE = SNAPSHOT");
        assertThat(setMode.action())
                .isInstanceOfSatisfying(AlterMaterializedViewAction.SetRefreshMode.class,
                        mode -> assertThat(mode.mode()).isEqualTo(RefreshMode.SNAPSHOT));

        var rename = (AlterMaterializedViewStatement) parse(
                "ALTER MATERIALIZED VIEW cat.sch.totals RENAME TO cat.sch.totals_v2");
        assertThat(rename.action())
                .isInstanceOfSatisfying(AlterMaterializedViewAction.Rename.class,
                        renamed -> assertThat(renamed.newName().parts())
                                .containsExactly("cat", "sch", "totals_v2"));
    }

    @Test
    void rejectsZeroTargetLagOnAlter() {
        assertThatThrownBy(() -> parse(
                "ALTER MATERIALIZED VIEW totals SET TARGET_LAG = 'PT0S'"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void printsAndReparsesEveryStatement() {
        for (String sql : new String[]{
                "CREATE MATERIALIZED VIEW cat.sch.totals AS SELECT region, SUM(amount) FROM orders GROUP BY region",
                "CREATE MATERIALIZED VIEW cat.sch.totals TARGET_LAG = 'PT5M' REFRESH_MODE = AUTO AS SELECT 1",
                "CREATE MATERIALIZED VIEW cat.sch.totals TARGET_LAG = 'PT1H' REFRESH_MODE = SNAPSHOT AS SELECT 1",
                "CREATE OR REPLACE MATERIALIZED VIEW cat.sch.totals AS SELECT 1",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS cat.sch.totals AS SELECT 1",
                "CREATE MATERIALIZED VIEW cat.sch.totals TARGET_LAG = 'PT5M' REFRESH_MODE = INCREMENTAL INCREMENTAL_STRATEGY = APPEND_ONLY AS SELECT 1",
                "ALTER MATERIALIZED VIEW cat.sch.totals SUSPEND",
                "ALTER MATERIALIZED VIEW cat.sch.totals RESUME",
                "ALTER MATERIALIZED VIEW cat.sch.totals REFRESH",
                "ALTER MATERIALIZED VIEW cat.sch.totals SET TARGET_LAG = 'PT30M'",
                "ALTER MATERIALIZED VIEW cat.sch.totals SET REFRESH_MODE = INCREMENTAL",
                "ALTER MATERIALIZED VIEW cat.sch.totals RENAME TO cat.sch.totals_v2",
                "REFRESH MATERIALIZED VIEW cat.sch.totals",
                "DROP MATERIALIZED VIEW cat.sch.totals",
                "DROP MATERIALIZED VIEW cat.sch.totals CASCADE",
                "DROP MATERIALIZED VIEW IF EXISTS cat.sch.totals",
                "SHOW MATERIALIZED VIEWS",
                "SHOW MATERIALIZED VIEWS LIKE 'daily_%'",
                "DESCRIBE MATERIALIZED VIEW cat.sch.totals"
        }) {
            Statement statement = parse(sql);
            assertThat(parse(printer.print(statement))).isEqualTo(statement);
        }
    }

    private Statement parse(String sql) {
        return parser.parse(sql);
    }
}
