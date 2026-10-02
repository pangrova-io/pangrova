package org.pangrova.planner;

import org.junit.jupiter.api.Test;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.parse.AstToSqlNodeConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SystemTableSqlConversionTest {

    @Test
    void calciteIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse(
                "SELECT snapshot_id FROM lake.analytics.orders$snapshots");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$snapshots");
    }

    @Test
    void unsupportedSystemTableFailsBeforePhysicalTableLookup() {
        var ast = new SqlParser().parse("SELECT * FROM lake.analytics.orders$files");

        assertThatThrownBy(() -> new AstToSqlNodeConverter().convert(ast))
                .isInstanceOf(PlannerException.class)
                .hasMessageContaining("no Native reader: files");
    }

    @Test
    void historyIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT snapshot_id FROM lake.analytics.orders$history");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$history");
    }
}
