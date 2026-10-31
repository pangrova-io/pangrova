package org.pangrova.testing.integration.e2e.tpcds;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TPC-DS-inspired SQL pattern correctness tests on synthetic data.
 *
 * <p>Validates representative TPC-DS queries through the full pipeline.
 * TPC-DS exercises decision-support workloads with complex analytics:
 * multi-dimensional aggregation, window functions, correlated subqueries,
 * and ad-hoc reporting patterns.</p>
 *
 * <p>Uses inline SQL (no resource files) for a minimal representative set
 * of TPC-DS query patterns against a simplified schema.</p>
 *
 * <p>Requirements validated: 9.3, 14.1</p>
 */
@DisplayName("TPC-DS SQL Pattern Correctness Tests")
class TpcDsCorrectnessTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "tpcds";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

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
        seedTpcDsTables();
    }

    private static void seedTpcDsTables() {
        var runner = new org.pangrova.testing.integration.e2e.runner.SqlTestRunner(framework);

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN_PREFIX + "store_sales ("
                + "ss_sold_date_sk BIGINT, ss_item_sk BIGINT, ss_customer_sk BIGINT, "
                + "ss_store_sk BIGINT, ss_quantity BIGINT, ss_sales_price DOUBLE, "
                + "ss_net_profit DOUBLE)");

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN_PREFIX + "date_dim ("
                + "d_date_sk BIGINT, d_year BIGINT, d_moy BIGINT, d_date VARCHAR)");

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN_PREFIX + "item ("
                + "i_item_sk BIGINT, i_item_id VARCHAR, i_category VARCHAR, "
                + "i_class VARCHAR, i_brand VARCHAR, i_product_name VARCHAR)");

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN_PREFIX + "store ("
                + "s_store_sk BIGINT, s_store_name VARCHAR, s_city VARCHAR, s_state VARCHAR)");

        runner.execute("INSERT INTO " + FQN_PREFIX + "date_dim VALUES "
                + "(1, 2023, 1, '2023-01-15'), (2, 2023, 2, '2023-02-20'), "
                + "(3, 2023, 3, '2023-03-10'), (4, 2022, 12, '2022-12-01')");

        runner.execute("INSERT INTO " + FQN_PREFIX + "item VALUES "
                + "(1, 'ITEM001', 'Electronics', 'Phones', 'BrandA', 'Phone X'), "
                + "(2, 'ITEM002', 'Electronics', 'Laptops', 'BrandB', 'Laptop Y'), "
                + "(3, 'ITEM003', 'Clothing', 'Shirts', 'BrandC', 'Shirt Z'), "
                + "(4, 'ITEM004', 'Clothing', 'Pants', 'BrandD', 'Pants W')");

        runner.execute("INSERT INTO " + FQN_PREFIX + "store VALUES "
                + "(1, 'Downtown', 'NYC', 'NY'), (2, 'Mall', 'LA', 'CA')");

        runner.execute("INSERT INTO " + FQN_PREFIX + "store_sales VALUES "
                + "(1, 1, 100, 1, 2, 599.99, 120.0), "
                + "(1, 2, 101, 1, 1, 999.99, 200.0), "
                + "(2, 3, 100, 2, 3, 29.99, 10.0), "
                + "(2, 4, 102, 2, 1, 49.99, 15.0), "
                + "(3, 1, 101, 1, 1, 599.99, 120.0), "
                + "(3, 2, 103, 2, 2, 1099.99, 250.0), "
                + "(4, 3, 100, 1, 5, 24.99, 8.0)");
    }

    @Nested
    @DisplayName("Store Sales Aggregation")
    class StoreSalesAggregation {

        @Test
        @DisplayName("total sales by store")
        void totalSalesByStore() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT s.s_store_name, SUM(ss.ss_sales_price) AS total_sales, "
                    + "COUNT(*) AS num_transactions "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "store s ON ss.ss_store_sk = s.s_store_sk "
                    + "GROUP BY s.s_store_name "
                    + "ORDER BY total_sales DESC");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("monthly sales trend")
        void monthlySalesTrend() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT d.d_year, d.d_moy, SUM(ss.ss_sales_price) AS monthly_sales "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "date_dim d ON ss.ss_sold_date_sk = d.d_date_sk "
                    + "GROUP BY d.d_year, d.d_moy "
                    + "ORDER BY d.d_year, d.d_moy");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Item Category Analysis")
    class ItemCategoryAnalysis {

        @Test
        @DisplayName("revenue by category")
        void revenueByCategory() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.i_category, SUM(ss.ss_sales_price) AS category_revenue, "
                    + "AVG(ss.ss_net_profit) AS avg_profit "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "item i ON ss.ss_item_sk = i.i_item_sk "
                    + "GROUP BY i.i_category "
                    + "ORDER BY category_revenue DESC");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("top items by sales quantity")
        void topItemsBySalesQuantity() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.i_product_name, SUM(ss.ss_quantity) AS total_qty "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "item i ON ss.ss_item_sk = i.i_item_sk "
                    + "GROUP BY i.i_product_name "
                    + "ORDER BY total_qty DESC "
                    + "LIMIT 5");
            assertThat(rows).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Window Function Patterns")
    class WindowFunctionPatterns {

        @Test
        @DisplayName("running total with window function")
        void runningTotalWithWindow() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT d.d_moy, ss.ss_sales_price, "
                    + "SUM(ss.ss_sales_price) OVER (ORDER BY d.d_moy) AS running_total "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "date_dim d ON ss.ss_sold_date_sk = d.d_date_sk "
                    + "WHERE d.d_year = 2023 "
                    + "ORDER BY d.d_moy");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("rank items by profit within category")
        void rankItemsByProfitWithinCategory() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT i.i_category, i.i_product_name, ss.ss_net_profit, "
                    + "RANK() OVER (PARTITION BY i.i_category ORDER BY ss.ss_net_profit DESC) AS profit_rank "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "JOIN " + FQN_PREFIX + "item i ON ss.ss_item_sk = i.i_item_sk");
            assertThat(rows).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Subquery Patterns")
    class SubqueryPatterns {

        @Test
        @DisplayName("correlated subquery - above average sales")
        void correlatedSubqueryAboveAverage() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT ss_item_sk, ss_sales_price "
                    + "FROM " + FQN_PREFIX + "store_sales ss "
                    + "WHERE ss_sales_price > ("
                    + "  SELECT AVG(ss2.ss_sales_price) "
                    + "  FROM " + FQN_PREFIX + "store_sales ss2"
                    + ")");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("EXISTS subquery - stores with high-value transactions")
        void existsSubquery() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT s_store_name FROM " + FQN_PREFIX + "store s "
                    + "WHERE EXISTS ("
                    + "  SELECT 1 FROM " + FQN_PREFIX + "store_sales ss "
                    + "  WHERE ss.ss_store_sk = s.s_store_sk "
                    + "  AND ss.ss_sales_price > 500"
                    + ")");
            assertThat(rows).isNotEmpty();
        }
    }
}
