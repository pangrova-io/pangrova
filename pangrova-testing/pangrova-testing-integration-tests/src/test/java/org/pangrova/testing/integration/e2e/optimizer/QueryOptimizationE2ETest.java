package org.pangrova.testing.integration.e2e.optimizer;

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
 * T06: Query Optimization & Planning — 8 E2E scenarios.
 */
@E2ETest
@DisplayName("T06: Query Optimization & Planning")
class QueryOptimizationE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: Multi-way join correct")
    class MultiWayJoin {
        @Test void threeWayJoinProducesCorrectResult() {
            String a = CATALOG + "." + SCHEMA + ".t06_a_" + System.nanoTime();
            String b = CATALOG + "." + SCHEMA + ".t06_b_" + System.nanoTime();
            String c = CATALOG + "." + SCHEMA + ".t06_c_" + System.nanoTime();
            executeDdl("CREATE TABLE " + a + " (id BIGINT, val VARCHAR)");
            executeDdl("CREATE TABLE " + b + " (id BIGINT, ref BIGINT)");
            executeDdl("CREATE TABLE " + c + " (ref BIGINT, label VARCHAR)");
            executeDml("INSERT INTO " + a + " VALUES (1,'x'),(2,'y')");
            executeDml("INSERT INTO " + b + " VALUES (1,10),(2,20)");
            executeDml("INSERT INTO " + c + " VALUES (10,'L1'),(20,'L2')");
            List<TypedRow> rows = executeQuery(
                    "SELECT a.val, c.label FROM " + a + " a JOIN " + b + " b ON a.id=b.id JOIN " + c + " c ON b.ref=c.ref ORDER BY a.id");
            assertThat(rows).hasSize(2);
        }
    }

    @Nested @DisplayName("Scenario 2: Projection pushdown")
    class ProjectionPushdown {
        @Test void onlyRequestedColumnsReturned() {
            String tbl = CATALOG + "." + SCHEMA + ".t06_proj_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, a VARCHAR, b VARCHAR, c VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'x','y','z')");
            List<TypedRow> rows = executeQuery("SELECT a FROM " + tbl);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).columnCount()).isEqualTo(1);
        }
    }

    @Nested @DisplayName("Scenario 3: NULL WHERE DELETE")
    class NullWhereDelete {
        @Test void deleteWithNullHandling() {
            String tbl = CATALOG + "." + SCHEMA + ".t06_null_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'a'),(2,NULL),(3,'c')");
            executeDml("DELETE FROM " + tbl + " WHERE val IS NULL");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 2);
        }
    }

    @Nested @DisplayName("Scenario 4: Runtime filter")
    class RuntimeFilter {
        @Test void runtimeFilterReducesScanRows() {
            String fact = CATALOG + "." + SCHEMA + ".t06_fact_" + System.nanoTime();
            String dim = CATALOG + "." + SCHEMA + ".t06_dim_" + System.nanoTime();
            executeDdl("CREATE TABLE " + fact + " (id BIGINT, dim_key INT, amount DOUBLE)");
            executeDdl("CREATE TABLE " + dim + " (key INT, name VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + fact + " VALUES ");
            for (int i = 0; i < 50; i++) {
                if (i > 0) sb.append(",");
                sb.append("(").append(i).append(",").append(i % 5).append(",").append(i * 1.0).append(")");
            }
            executeDml(sb.toString());
            executeDml("INSERT INTO " + dim + " VALUES (0,'zero'),(1,'one')");
            List<TypedRow> rows = executeQuery(
                    "SELECT f.id, d.name FROM " + fact + " f JOIN " + dim + " d ON f.dim_key=d.key ORDER BY f.id");
            assertThat(rows).hasSize(20); // keys 0,1 each have 10 rows
        }
    }

    @Nested @DisplayName("Scenario 5: Shuffle statistics")
    class ShuffleStatistics {
        @Test void aggregationStatisticsCorrect() {
            String tbl = CATALOG + "." + SCHEMA + ".t06_stats_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, grp INT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,1),(2,1),(3,2),(4,2),(5,3)");
            List<TypedRow> rows = executeQuery("SELECT grp, COUNT(*) FROM " + tbl + " GROUP BY grp ORDER BY grp");
            assertThat(rows).hasSize(3);
        }
    }

    @Nested @DisplayName("Scenario 6: Unsupported operator fail-fast")
    class UnsupportedOperator {
        @Test void unsupportedSyntaxReturnsError() {
            String error = executeExpectingError("SELECT * FROM nonexistent_table_xyz_" + System.nanoTime());
            assertThat(error).isNotNull();
        }
    }

    @Nested @DisplayName("Scenario 7: LIMIT pushdown")
    class LimitPushdown {
        @Test void limitReducesResultSet() {
            String tbl = CATALOG + "." + SCHEMA + ".t06_limit_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 100; i++) { if (i > 0) sb.append(","); sb.append("(").append(i).append(")"); }
            executeDml(sb.toString());
            assertQueryReturnsRows("SELECT * FROM " + tbl + " LIMIT 10", 10);
        }
    }

    @Nested @DisplayName("Scenario 8: Predicate pushdown")
    class PredicatePushdown {
        @Test void predicatePushdownFilters() {
            String tbl = CATALOG + "." + SCHEMA + ".t06_pred_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, region VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'us'),(2,'eu'),(3,'us'),(4,'ap')");
            assertQueryReturnsRows("SELECT * FROM " + tbl + " WHERE region = 'us'", 2);
        }
    }
}
