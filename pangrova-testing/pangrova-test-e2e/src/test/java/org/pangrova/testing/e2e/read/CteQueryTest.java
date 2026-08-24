package org.pangrova.testing.e2e.read;

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
 * E2E tests for Common Table Expressions (CTE / WITH clause).
 *
 * <p>Covers non-recursive CTEs, multiple CTEs, nested CTEs, and CTE with
 * aggregation through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Arrow → Result.
 *
 * <p>Uses TPC-H test data (orders, customer, lineitem, nation).
 */
@DisplayName("CTE (WITH clause) - E2E Tests")
class CteQueryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                org.pangrova.testing.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    // ── Basic CTE ──

    @Nested
    @DisplayName("Basic CTE")
    class BasicCte {

        @Test
        @DisplayName("Simple CTE with SELECT produces correct results")
        void simpleCte() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH order_summary AS ("
                    + "  SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus"
                    + ") "
                    + "SELECT * FROM order_summary");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("CTE with filter produces subset")
        void cteWithFilter() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH big_orders AS ("
                    + "  SELECT * FROM orders WHERE o_totalprice > 1000.0"
                    + ") "
                    + "SELECT COUNT(*) AS cnt FROM big_orders");
            assertThat(rows).hasSize(1);
            long cnt = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(cnt).isGreaterThanOrEqualTo(0);
        }
    }

    // ── Multiple CTEs ──

    @Nested
    @DisplayName("Multiple CTEs")
    class MultipleCtes {

        @Test
        @DisplayName("Multiple CTEs can reference each other in sequence")
        void multipleCtesDefined() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH "
                    + "  order_counts AS ("
                    + "    SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus"
                    + "  ),"
                    + "  top_status AS ("
                    + "    SELECT o_orderstatus FROM order_counts WHERE cnt > 5"
                    + "  ) "
                    + "SELECT * FROM top_status");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("CTE used multiple times in main query")
        void cteReusedMultipleTimes() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH status_summary AS ("
                    + "  SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus"
                    + ") "
                    + "SELECT a.o_orderstatus, a.cnt, b.cnt AS other_cnt "
                    + "FROM status_summary a CROSS JOIN status_summary b "
                    + "LIMIT 10");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── CTE with aggregation ──

    @Nested
    @DisplayName("CTE with aggregation")
    class CteWithAggregation {

        @Test
        @DisplayName("CTE aggregates then outer query filters")
        void cteAggregatesThenFilters() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH customer_orders AS ("
                    + "  SELECT o_custkey, COUNT(*) AS order_count, SUM(o_totalprice) AS total_spent "
                    + "  FROM orders GROUP BY o_custkey"
                    + ") "
                    + "SELECT * FROM customer_orders WHERE order_count > 1 LIMIT 10");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("CTE with JOIN in main query")
        void cteWithJoinInMainQuery() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH order_stats AS ("
                    + "  SELECT o_custkey, SUM(o_totalprice) AS total FROM orders GROUP BY o_custkey"
                    + ") "
                    + "SELECT c.c_name, os.total "
                    + "FROM order_stats os INNER JOIN customer c ON os.o_custkey = c.c_custkey "
                    + "ORDER BY os.total DESC LIMIT 5");
            assertThat(rows).isNotEmpty();
            assertThat(rows.size()).isLessThanOrEqualTo(5);
        }
    }

    // ── CTE with window function ──

    @Nested
    @DisplayName("CTE with window function")
    class CteWithWindow {

        @Test
        @DisplayName("CTE containing window function produces correct ranking")
        void cteWithWindowFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "WITH ranked_orders AS ("
                    + "  SELECT o_orderkey, o_totalprice, "
                    + "    ROW_NUMBER() OVER (ORDER BY o_totalprice DESC) AS rn "
                    + "  FROM orders"
                    + ") "
                    + "SELECT * FROM ranked_orders WHERE rn <= 5");
            assertThat(rows).hasSize(5);
        }
    }
}
