package org.pangrova.planner;

import org.junit.jupiter.api.Test;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.parse.AstToSqlNodeConverter;

import static org.assertj.core.api.Assertions.assertThat;

class SystemTableSqlConversionTest {

    @Test
    void refsRetainsTheBaseTableAndMetadataSuffix() {
        var ast = new SqlParser().parse("SELECT name, type FROM lake.analytics.orders$refs");
        var select = (org.apache.calcite.sql.SqlSelect) new AstToSqlNodeConverter().convert(ast);
        assertThat(((org.apache.calcite.sql.SqlIdentifier) select.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$refs");
    }

    @Test
    void calciteIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse(
                "SELECT snapshot_id FROM lake.analytics.orders$snapshots");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$snapshots");
    }

    @Test
    void entriesIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT * FROM lake.analytics.orders$entries");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$entries");
    }

    @Test
    void historyIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT snapshot_id FROM lake.analytics.orders$history");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$history");
    }

    @Test
    void tagsIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT tag_name FROM lake.analytics.orders$tags");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$tags");
    }

    @Test
    void optionsIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT * FROM lake.analytics.orders$options");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$options");
    }

    @Test
    void manifestsIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT path FROM lake.analytics.orders$manifests");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$manifests");
    }

    @Test
    void filesIdentifierRetainsSystemTableSuffix() {
        var ast = new SqlParser().parse("SELECT file_path FROM lake.analytics.orders$files");

        var calcite = (SqlSelect) new AstToSqlNodeConverter().convert(ast);

        assertThat(((SqlIdentifier) calcite.getFrom()).names)
                .containsExactly("lake", "analytics", "orders$files");
    }
}
