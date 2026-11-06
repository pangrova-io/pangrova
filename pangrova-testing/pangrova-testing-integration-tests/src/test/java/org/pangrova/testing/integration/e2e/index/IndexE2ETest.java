package org.pangrova.testing.integration.e2e.index;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.types.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T10: Index — 6 E2E scenarios.
 */
@E2ETest
@DisplayName("T10: Index")
class IndexE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: Data skipping via filter")
    class DataSkipping {
        @Test void equalityFilterSkipsIrrelevantData() {
            String tbl = CATALOG + "." + SCHEMA + ".t10_skip_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, category VARCHAR, val DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 100; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",'cat_").append(i%5).append("',").append(i*1.0).append(")"); }
            executeDml(sb.toString());
            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl + " WHERE category = 'cat_0'");
            assertThat(rows).hasSize(20);
        }
    }

    @Nested @DisplayName("Scenario 2: Filter with AND")
    class CompoundFilter {
        @Test void andFilterNarrowsResults() {
            String tbl = CATALOG + "." + SCHEMA + ".t10_and_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, a INT, b INT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2)");
            assertQueryReturnsRows("SELECT * FROM " + tbl + " WHERE a = 1 AND b = 2", 1);
        }
    }

    @Nested @DisplayName("Scenario 3: IN-list filter")
    class InListFilter {
        @Test void inListFilterWorks() {
            String tbl = CATALOG + "." + SCHEMA + ".t10_in_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, status VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'active'),(2,'inactive'),(3,'active'),(4,'pending')");
            assertQueryReturnsRows("SELECT * FROM " + tbl + " WHERE status IN ('active','pending')", 3);
        }
    }

    @Nested @DisplayName("Scenario 4: Range filter")
    class RangeFilter {
        @Test void betweenFilterWorks() {
            String tbl = CATALOG + "." + SCHEMA + ".t10_range_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, score DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 50; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",").append(i*2.0).append(")"); }
            executeDml(sb.toString());
            List<TypedRow> rows = executeQuery("SELECT * FROM " + tbl + " WHERE score BETWEEN 10.0 AND 30.0");
            assertThat(rows).hasSize(11); // 5..15
        }
    }

    @Nested @DisplayName("Scenario 5: Pre-filter pushdown")
    class PreFilterPushdown {
        @Test void filterBeforeJoinReducesWork() {
            String f = CATALOG + "." + SCHEMA + ".t10_f_" + System.nanoTime();
            String d = CATALOG + "." + SCHEMA + ".t10_d_" + System.nanoTime();
            executeDdl("CREATE TABLE " + f + " (id BIGINT, key INT)");
            executeDdl("CREATE TABLE " + d + " (key INT, label VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + f + " VALUES ");
            for (int i = 0; i < 30; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(",").append(i%3).append(")"); }
            executeDml(sb.toString());
            executeDml("INSERT INTO " + d + " VALUES (0,'zero'),(1,'one'),(2,'two')");
            List<TypedRow> rows = executeQuery(
                    "SELECT f.id, d.label FROM " + f + " f JOIN " + d + " d ON f.key=d.key WHERE d.label='zero'");
            assertThat(rows).hasSize(10);
        }
    }

    @Nested @DisplayName("Scenario 6: UDF fallback (no index)")
    class UdfFallback {
        @Test void complexExpressionStillWorks() {
            String tbl = CATALOG + "." + SCHEMA + ".t10_udf_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'hello'),(2,'world'),(3,'hello world')");
            // Length-based filter — no index can help, full scan
            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + tbl + " WHERE LENGTH(val) > 5");
            assertThat(rows).hasSize(1); // 'hello world'
        }
    }
}
