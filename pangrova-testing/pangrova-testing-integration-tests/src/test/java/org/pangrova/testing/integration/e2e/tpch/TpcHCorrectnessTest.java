package org.pangrova.testing.integration.e2e.tpch;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TPC-H standard query correctness tests (Q1–Q10).
 *
 * <p>Executes TPC-H benchmark queries through the full pipeline against
 * identical deterministic schema-compatible data in Pangrova and PostgreSQL.
 * Compares every result with the independent reference engine, including
 * ordering and floating-point aggregates. This is not an official TPC-H
 * scale-factor benchmark or a performance acceptance result.</p>
 *
 * <p>Requirements validated: 9.3, 14.1</p>
 */
@DisplayName("TPC-H Correctness Tests (Q1–Q10)")
class TpcHCorrectnessTest extends TpcHTestBase {

    @Nested
    @DisplayName("Q1 - Pricing Summary Report")
    class Q1 {
        @Test
        @DisplayName("aggregates line items by return flag and status")
        void pricingSummaryReport() {
            List<TypedRow> rows = executeQuery("q01.sql");
            assertMatchesReference(rows, "q01.sql",
                    "l_returnflag", "l_linestatus");
        }
    }

    @Nested
    @DisplayName("Q2 - Minimum Cost Supplier")
    class Q2 {
        @Test
        @DisplayName("finds minimum cost supplier for parts in a region")
        void minimumCostSupplier() {
            List<TypedRow> rows = executeQuery("q02.sql");
            assertMatchesReference(rows, "q02.sql",
                    "s_name", "s_address");
        }
    }

    @Nested
    @DisplayName("Q3 - Shipping Priority")
    class Q3 {
        @Test
        @DisplayName("retrieves highest-revenue unshipped orders")
        void shippingPriority() {
            List<TypedRow> rows = executeQuery("q03.sql");
            assertMatchesReference(rows, "q03.sql",
                    "l_orderkey", "revenue");
        }
    }

    @Nested
    @DisplayName("Q4 - Order Priority Checking")
    class Q4 {
        @Test
        @DisplayName("counts orders by priority with late line items")
        void orderPriorityChecking() {
            List<TypedRow> rows = executeQuery("q04.sql");
            assertMatchesReference(rows, "q04.sql",
                    "o_orderpriority", "order_count");
        }
    }

    @Nested
    @DisplayName("Q5 - Local Supplier Volume")
    class Q5 {
        @Test
        @DisplayName("revenue from local suppliers by nation")
        void localSupplierVolume() {
            List<TypedRow> rows = executeQuery("q05.sql");
            assertMatchesReference(rows, "q05.sql",
                    "n_name", "revenue");
            // The fixed fixture contains an exact revenue tie. Keep it in the
            // ordered oracle so a lost secondary key is observable.
            var japan = rows.stream().filter(row -> "JAPAN".equals(row.getString("n_name")))
                    .findFirst().orElseThrow();
            var jordan = rows.stream().filter(row -> "JORDAN".equals(row.getString("n_name")))
                    .findFirst().orElseThrow();
            assertThat(japan.getDouble("revenue")).isEqualTo(jordan.getDouble("revenue"));
            assertThat(rows.indexOf(japan)).isLessThan(rows.indexOf(jordan));
        }
    }

    @Nested
    @DisplayName("Q6 - Forecasting Revenue Change")
    class Q6 {
        @Test
        @DisplayName("computes forecasting revenue scalar aggregate")
        void forecastingRevenueChange() {
            List<TypedRow> rows = executeQuery("q06.sql");
            assertMatchesReference(rows, "q06.sql", "revenue");
        }
    }

    @Nested
    @DisplayName("Q7 - Volume Shipping")
    class Q7 {
        @Test
        @DisplayName("bilateral trade volume between two nations")
        void volumeShipping() {
            List<TypedRow> rows = executeQuery("q07.sql");
            assertMatchesReference(rows, "q07.sql",
                    "supp_nation", "cust_nation", "revenue");
        }
    }

    @Nested
    @DisplayName("Q8 - National Market Share")
    class Q8 {
        @Test
        @DisplayName("computes market share for a given nation")
        void nationalMarketShare() {
            List<TypedRow> rows = executeQuery("q08.sql");
            assertMatchesReference(rows, "q08.sql",
                    "o_year", "mkt_share");
        }
    }

    @Nested
    @DisplayName("Q9 - Product Type Profit Measure")
    class Q9 {
        @Test
        @DisplayName("profit for parts matching a pattern by nation and year")
        void productTypeProfitMeasure() {
            List<TypedRow> rows = executeQuery("q09.sql");
            assertMatchesReference(rows, "q09.sql",
                    "nation", "o_year", "sum_profit");
        }
    }

    @Nested
    @DisplayName("Q10 - Returned Item Reporting")
    class Q10 {
        @Test
        @DisplayName("top customers with returned items")
        void returnedItemReporting() {
            List<TypedRow> rows = executeQuery("q10.sql");
            assertMatchesReference(rows, "q10.sql",
                    "c_custkey", "c_name", "revenue");
        }
    }
}
