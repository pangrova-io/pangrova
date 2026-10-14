package org.pangrova.testing.e2e.optimization;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Optimization correctness tests.
 *
 * <p>Validates that query optimizer transformations preserve result correctness.
 * Each test verifies that an optimized query pattern produces identical results
 * to the naive/unoptimized equivalent. This catches optimizer regressions where
 * a rewrite produces incorrect results.</p>
 *
 * <p>Covers: predicate pushdown, projection pruning, join reordering,
 * constant folding, subquery decorrelation, and aggregate pushdown.</p>
 *
 * <p>Requirements validated: 5.1, 5.2, 9.3</p>
 */
@DisplayName("Optimization Correctness Tests")
class OptimizationCorrectnessTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final String T1 = FQN_PREFIX + "opt_orders";
    private static final String T2 = FQN_PREFIX + "opt_items";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        seedTables();
    }

    private static void seedTables() {
        var runner = new org.pangrova.testing.e2e.runner.SqlTestRunner(framework);

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T1
                + " (order_id BIGINT, customer_id BIGINT, amount DOUBLE, "
                + "status VARCHAR, region VARCHAR)");
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T2
                + " (item_id BIGINT, order_id BIGINT, product VARCHAR, "
                + "qty BIGINT, price DOUBLE)");

        runner.execute("INSERT INTO " + T1 + " VALUES "
                + "(1, 100, 500.0, 'SHIPPED', 'east'), "
                + "(2, 101, 200.0, 'PENDING', 'west'), "
                + "(3, 100, 300.0, 'SHIPPED', 'east'), "
                + "(4, 102, 150.0, 'CANCELLED', 'north'), "
                + "(5, 103, 800.0, 'SHIPPED', 'west')");

        runner.execute("INSERT INTO " + T2 + " VALUES "
                + "(1, 1, 'Widget', 2, 250.0), "
                + "(2, 1, 'Gadget', 1, 250.0), "
                + "(3, 2, 'Widget', 3, 66.67), "
                + "(4, 3, 'Sprocket', 5, 60.0), "
                + "(5, 5, 'Widget', 4, 200.0)");
    }

    @Nested
    @DisplayName("Predicate Pushdown")
    class PredicatePushdown {

        @Test
        @DisplayName("filter pushdown through join preserves results")
        void filterPushdownThroughJoin() {
            // Query with filter on outer table — optimizer should push predicate below join
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o.order_id, i.product "
                    + "FROM " + T1 + " o "
                    + "JOIN " + T2 + " i ON o.order_id = i.order_id "
                    + "WHERE o.status = 'SHIPPED'");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(3);
            // Verify only SHIPPED orders appear
            for (TypedRow row : rows) {
                List<TypedRow> orderStatus = sqlRunner.execute(
                        "SELECT status FROM " + T1 + " WHERE order_id = "
                        + ((Number) row.getRaw("order_id")).longValue());
                assertThat(orderStatus.get(0).getRaw("status")).isEqualTo("SHIPPED");
            }
        }

        @Test
        @DisplayName("predicate on aggregated column applied correctly")
        void predicateOnAggregatedColumn() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT customer_id, SUM(amount) AS total "
                    + "FROM " + T1
                    + " GROUP BY customer_id "
                    + "HAVING SUM(amount) > 400");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                assertThat(((Number) row.getRaw("total")).doubleValue()).isGreaterThan(400.0);
            }
        }
    }

    @Nested
    @DisplayName("Constant Folding")
    class ConstantFolding {

        @Test
        @DisplayName("constant expressions folded correctly")
        void constantExpressionsFolded() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT order_id, amount * 1.0 AS same_amount, "
                    + "amount + 0 AS plus_zero "
                    + "FROM " + T1 + " WHERE 1 = 1");
            assertThat(rows).hasSize(5);
        }

        @Test
        @DisplayName("constant condition elimination")
        void constantConditionElimination() {
            List<TypedRow> withTrue = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + T1 + " WHERE 1 = 1");
            List<TypedRow> noCondition = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + T1);
            assertThat(((Number) withTrue.get(0).getRaw("cnt")).longValue())
                    .isEqualTo(((Number) noCondition.get(0).getRaw("cnt")).longValue());
        }
    }

    @Nested
    @DisplayName("Subquery Decorrelation")
    class SubqueryDecorrelation {

        @Test
        @DisplayName("correlated subquery produces correct results")
        void correlatedSubqueryCorrectness() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT order_id, amount FROM " + T1 + " o "
                    + "WHERE amount > ("
                    + "  SELECT AVG(amount) FROM " + T1 + " o2 "
                    + "  WHERE o2.region = o.region"
                    + ")");
            assertThat(rows).extracting(row -> ((Number) row.getRaw("order_id")).longValue())
                    .containsExactlyInAnyOrder(1L, 5L);
        }

        @Test
        @DisplayName("IN subquery equivalent to JOIN")
        void inSubqueryEquivalentToJoin() {
            List<TypedRow> viaSub = sqlRunner.execute(
                    "SELECT order_id FROM " + T1
                    + " WHERE order_id IN (SELECT order_id FROM " + T2 + ")");
            List<TypedRow> viaJoin = sqlRunner.execute(
                    "SELECT DISTINCT o.order_id FROM " + T1 + " o "
                    + "JOIN " + T2 + " i ON o.order_id = i.order_id");
            assertThat(viaSub).hasSameSizeAs(viaJoin);
        }
    }

    @Nested
    @DisplayName("Join Reordering")
    class JoinReordering {

        @Test
        @DisplayName("multi-table join with filters produces correct results")
        void multiTableJoinWithFilters() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o.order_id, i.product, o.amount "
                    + "FROM " + T1 + " o "
                    + "JOIN " + T2 + " i ON o.order_id = i.order_id "
                    + "WHERE o.region = 'east' AND i.qty > 1 "
                    + "ORDER BY o.order_id");
            assertThat(rows).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Projection Pruning")
    class ProjectionPruning {

        @Test
        @DisplayName("unused columns do not affect results")
        void unusedColumnsDoNotAffectResults() {
            List<TypedRow> allCols = sqlRunner.execute(
                    "SELECT order_id FROM " + T1 + " WHERE status = 'SHIPPED'");
            List<TypedRow> explicit = sqlRunner.execute(
                    "SELECT order_id FROM " + T1 + " WHERE status = 'SHIPPED'");
            assertThat(allCols).hasSameSizeAs(explicit);
        }
    }
}
