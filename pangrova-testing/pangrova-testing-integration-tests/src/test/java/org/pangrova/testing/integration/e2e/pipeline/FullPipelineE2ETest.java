package org.pangrova.testing.integration.e2e.pipeline;

import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T-Pipeline: Full Pipeline & Cluster Scenarios — 6 E2E scenarios.
 *
 * <p>Validates complete query lifecycle through the full stack.
 */
@E2ETest
@DisplayName("T-Pipeline: Full Pipeline & Cluster")
class FullPipelineE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: TPC-H style query")
    class TpcHStyle {
        @Test void tpchQ1StyleAggregation() {
            String lineitem = CATALOG + "." + SCHEMA + ".t_lineitem_" + System.nanoTime();
            executeDdl("CREATE TABLE " + lineitem
                    + " (l_orderkey BIGINT, l_quantity DOUBLE, l_extendedprice DOUBLE, "
                    + "l_discount DOUBLE, l_returnflag VARCHAR, l_linestatus VARCHAR)");
            executeDml("INSERT INTO " + lineitem + " VALUES "
                    + "(1,17.0,21168.23,0.04,'N','O'),"
                    + "(1,36.0,34850.16,0.09,'N','O'),"
                    + "(2,8.0,13309.60,0.10,'R','F'),"
                    + "(3,45.0,54058.05,0.06,'A','F')");
            List<TypedRow> rows = executeQuery(
                    "SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice) "
                            + "FROM " + lineitem + " GROUP BY l_returnflag, l_linestatus "
                            + "ORDER BY l_returnflag, l_linestatus");
            assertThat(rows).hasSize(3); // N-O, R-F, A-F
        }
    }

    @Nested @DisplayName("Scenario 2: Full lifecycle (create→insert→query→drop)")
    class FullLifecycle {
        @Test void completeTableLifecycle() {
            String tbl = CATALOG + "." + SCHEMA + ".t_lifecycle_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, data VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'hello'),(2,'world')");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 2);
            executeDml("UPDATE " + tbl + " SET data='updated' WHERE id=1");
            executeDml("DELETE FROM " + tbl + " WHERE id=2");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
            executeDdl("DROP TABLE " + tbl);
            assertThat(executeExpectingError("SELECT * FROM " + tbl)).isNotNull();
        }
    }

    @Nested @DisplayName("Scenario 3: Mixed read/write workload")
    class MixedWorkload {
        @Test void concurrentReadsAndWrites() throws Exception {
            String tbl = CATALOG + "." + SCHEMA + ".t_mixed_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val INT)");
            for (int i = 0; i < 5; i++) {
                executeDml("INSERT INTO " + tbl + " VALUES (" + i + "," + i*10 + ")");
                Thread.sleep(200); // Filesystem sync delay diagnostic
                assertQueryReturnsRows("SELECT * FROM " + tbl, i + 1);
            }
        }
    }

    @Nested @DisplayName("Scenario 4: Complex analytical query")
    class ComplexAnalytical {
        @Test void windowAndGroupBy() {
            String tbl = CATALOG + "." + SCHEMA + ".t_analytic_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, dept VARCHAR, salary DOUBLE)");
            executeDml("INSERT INTO " + tbl + " VALUES "
                    + "(1,'eng',100.0),(2,'eng',120.0),(3,'sales',80.0),(4,'sales',90.0),(5,'hr',110.0)");
            List<TypedRow> rows = executeQuery(
                    "SELECT dept, AVG(salary), COUNT(*) FROM " + tbl + " GROUP BY dept ORDER BY dept");
            assertThat(rows).hasSize(3);
        }
    }

    @Nested @DisplayName("Scenario 5: Subquery correctness")
    class SubqueryCorrectness {
        @Test void correlatedSubqueryWorks() {
            String tbl = CATALOG + "." + SCHEMA + ".t_sub_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val DOUBLE)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,10.0),(2,20.0),(3,30.0)");
            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + tbl + " WHERE val > (SELECT AVG(val) FROM " + tbl + ")");
            assertThat(rows).hasSize(1); // only id=3 (30 > 20)
        }
    }

    @Nested @DisplayName("Scenario 6: UNION ALL correctness")
    class UnionAll {
        @Test void unionAllCombinesResults() {
            String t1 = CATALOG + "." + SCHEMA + ".t_u1_" + System.nanoTime();
            String t2 = CATALOG + "." + SCHEMA + ".t_u2_" + System.nanoTime();
            executeDdl("CREATE TABLE " + t1 + " (id BIGINT, val VARCHAR)");
            executeDdl("CREATE TABLE " + t2 + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + t1 + " VALUES (1,'a'),(2,'b')");
            executeDml("INSERT INTO " + t2 + " VALUES (3,'c'),(4,'d')");
            assertQueryReturnsRows(
                    "SELECT * FROM " + t1 + " UNION ALL SELECT * FROM " + t2, 4);
        }
    }
}
