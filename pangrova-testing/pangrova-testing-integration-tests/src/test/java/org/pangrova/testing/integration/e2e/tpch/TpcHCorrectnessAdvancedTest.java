package org.pangrova.testing.integration.e2e.tpch;

import org.pangrova.types.value.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TPC-H advanced query correctness tests (Q11–Q22).
 *
 * <p>Covers the more complex TPC-H queries that exercise advanced SQL features:
 * correlated subqueries, multi-level aggregation, CASE expressions, EXISTS/NOT EXISTS,
 * and complex join predicates.</p>
 *
 * <p>Requirements validated: 9.3, 14.1</p>
 */
@DisplayName("TPC-H Correctness Tests (Q11–Q22)")
class TpcHCorrectnessAdvancedTest extends TpcHTestBase {

    @Nested
    @DisplayName("Q11 - Important Stock Identification")
    class Q11 {
        @Test
        @DisplayName("identifies important stock for a nation")
        void importantStockIdentification() {
            List<TypedRow> rows = executeQuery("q11.sql");
            assertMatchesReference(rows, "q11.sql",
                    "ps_partkey", "value");
        }
    }

    @Nested
    @DisplayName("Q12 - Shipping Modes and Order Priority")
    class Q12 {
        @Test
        @DisplayName("counts high/low priority orders by ship mode")
        void shippingModesOrderPriority() {
            List<TypedRow> rows = executeQuery("q12.sql");
            assertMatchesReference(rows, "q12.sql",
                    "l_shipmode", "high_line_count", "low_line_count");
        }
    }

    @Nested
    @DisplayName("Q13 - Customer Distribution")
    class Q13 {
        @Test
        @DisplayName("customer distribution by order count")
        void customerDistribution() {
            List<TypedRow> rows = executeQuery("q13.sql");
            assertMatchesReference(rows, "q13.sql",
                    "c_count", "custdist");
        }
    }

    @Nested
    @DisplayName("Q14 - Promotion Effect")
    class Q14 {
        @Test
        @DisplayName("computes promotion revenue percentage")
        void promotionEffect() {
            List<TypedRow> rows = executeQuery("q14.sql");
            assertMatchesReference(rows, "q14.sql", "promo_revenue");
        }
    }

    @Nested
    @DisplayName("Q15 - Top Supplier")
    class Q15 {
        @Test
        @DisplayName("identifies top supplier by revenue")
        void topSupplier() {
            List<TypedRow> rows = executeQuery("q15.sql");
            assertMatchesReference(rows, "q15.sql",
                    "s_suppkey", "s_name", "total_revenue");
        }
    }

    @Nested
    @DisplayName("Q16 - Parts/Supplier Relationship")
    class Q16 {
        @Test
        @DisplayName("counts suppliers for parts matching criteria")
        void partsSupplierRelationship() {
            List<TypedRow> rows = executeQuery("q16.sql");
            assertMatchesReference(rows, "q16.sql",
                    "p_brand", "p_type", "p_size", "supplier_cnt");
        }
    }

    @Nested
    @DisplayName("Q17 - Small-Quantity Order Revenue")
    class Q17 {
        @Test
        @DisplayName("computes average yearly revenue for small quantities")
        void smallQuantityOrderRevenue() {
            List<TypedRow> rows = executeQuery("q17.sql");
            assertMatchesReference(rows, "q17.sql", "avg_yearly");
        }
    }

    @Nested
    @DisplayName("Q18 - Large Volume Customer")
    class Q18 {
        @Test
        @DisplayName("identifies large volume customers")
        void largeVolumeCustomer() {
            List<TypedRow> rows = executeQuery("q18.sql");
            assertMatchesReference(rows, "q18.sql",
                    "c_name", "c_custkey", "o_orderkey", "o_totalprice");
        }
    }

    @Nested
    @DisplayName("Q19 - Discounted Revenue")
    class Q19 {
        @Test
        @DisplayName("computes discounted revenue for qualifying line items")
        void discountedRevenue() {
            List<TypedRow> rows = executeQuery("q19.sql");
            assertMatchesReference(rows, "q19.sql", "revenue");
        }
    }

    @Nested
    @DisplayName("Q20 - Potential Part Promotion")
    class Q20 {
        @Test
        @DisplayName("identifies suppliers with excess inventory")
        void potentialPartPromotion() {
            List<TypedRow> rows = executeQuery("q20.sql");
            assertMatchesReference(rows, "q20.sql",
                    "s_name", "s_address");
        }
    }

    @Nested
    @DisplayName("Q21 - Suppliers Who Kept Orders Waiting")
    class Q21 {
        @Test
        @DisplayName("identifies suppliers who caused order delays")
        void suppliersKeptOrdersWaiting() {
            List<TypedRow> rows = executeQuery("q21.sql");
            assertMatchesReference(rows, "q21.sql",
                    "s_name", "numwait");
        }
    }

    @Nested
    @DisplayName("Q22 - Global Sales Opportunity")
    class Q22 {
        @Test
        @DisplayName("identifies customers with high balances but no orders")
        void globalSalesOpportunity() {
            List<TypedRow> rows = executeQuery("q22.sql");
            assertMatchesReference(rows, "q22.sql",
                    "cntrycode", "numcust", "totacctbal");
        }
    }
}
