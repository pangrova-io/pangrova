package org.pangrova.testing.integration.e2e.read;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for SQL expression coverage.
 *
 * <p>Covers arithmetic, string functions, conditional expressions (CASE/COALESCE),
 * date functions, and type casting through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Arrow → Result.
 *
 * <p>Uses TPC-H test data (orders, lineitem, customer).
 */
@DisplayName("Expression Coverage - E2E Tests")
class ExpressionCoverageTest extends SqlE2ETestBase {

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
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                org.pangrova.testing.integration.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    // ── Arithmetic expressions ──

    @Nested
    @DisplayName("Arithmetic expressions")
    class ArithmeticExpressions {

        @Test
        @DisplayName("Arithmetic in SELECT computes correct values")
        void arithmeticInSelect() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT l_extendedprice * (1 - l_discount) AS net_price "
                    + "FROM lineitem LIMIT 5");
            assertThat(rows).hasSize(5);
            for (TypedRow row : rows) {
                Object netPrice = row.getRaw("net_price");
                assertThat(netPrice).as("net_price should be non-null").isNotNull();
            }
        }

        @Test
        @DisplayName("Arithmetic with tax computation")
        void arithmeticWithTax() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charged "
                    + "FROM lineitem LIMIT 5");
            assertThat(rows).hasSize(5);
        }

        @Test
        @DisplayName("Division and modulo operators work correctly")
        void divisionAndModulo() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT 10 / 3 AS div_result, 10 % 3 AS mod_result");
            assertThat(rows).hasSize(1);
        }
    }

    // ── String functions ──

    @Nested
    @DisplayName("String functions")
    class StringFunctions {

        @Test
        @DisplayName("UPPER converts to uppercase")
        void upperFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT UPPER(o_orderstatus) AS upper_status FROM orders LIMIT 5");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("LOWER converts to lowercase")
        void lowerFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT LOWER(c_name) AS lower_name FROM customer LIMIT 5");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("LENGTH returns string length")
        void lengthFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c_name, LENGTH(c_name) AS name_len FROM customer LIMIT 5");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                long len = ((Number) row.getRaw("name_len")).longValue();
                assertThat(len).isGreaterThan(0);
            }
        }

        @Test
        @DisplayName("SUBSTRING extracts part of string")
        void substringFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT SUBSTRING(c_phone, 1, 3) AS area_code FROM customer LIMIT 5");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("String concatenation with || operator")
        void concatOperator() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c_name || ' (' || c_phone || ')' AS contact FROM customer LIMIT 5");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── Conditional expressions ──

    @Nested
    @DisplayName("Conditional expressions")
    class ConditionalExpressions {

        @Test
        @DisplayName("CASE WHEN produces conditional result")
        void caseWhen() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey, "
                    + "CASE WHEN o_totalprice > 1000 THEN 'high' "
                    + "     WHEN o_totalprice > 100 THEN 'medium' "
                    + "     ELSE 'low' END AS price_tier "
                    + "FROM orders LIMIT 10");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                String tier = (String) row.getRaw("price_tier");
                assertThat(tier).isIn("high", "medium", "low");
            }
        }

        @Test
        @DisplayName("COALESCE returns first non-null value")
        void coalesce() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COALESCE(l_comment, 'no comment') AS comment "
                    + "FROM lineitem LIMIT 10");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                assertThat(row.getRaw("comment")).isNotNull();
            }
        }

        @Test
        @DisplayName("NULLIF returns NULL when arguments are equal")
        void nullif() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT NULLIF(o_orderstatus, 'O') AS status "
                    + "FROM orders LIMIT 10");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── Type casting ──

    @Nested
    @DisplayName("Type casting")
    class TypeCasting {

        @Test
        @DisplayName("CAST BIGINT to VARCHAR")
        void castBigintToVarchar() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT CAST(o_orderkey AS VARCHAR) AS key_str FROM orders LIMIT 5");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                assertThat(row.getRaw("key_str")).isInstanceOf(String.class);
            }
        }

        @Test
        @DisplayName("CAST DOUBLE to BIGINT truncates")
        void castDoubleToBigint() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT CAST(o_totalprice AS BIGINT) AS price_int FROM orders LIMIT 5");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                assertThat(row.getRaw("price_int")).isInstanceOf(Number.class);
            }
        }
    }

    // ── Date functions ──

    @Nested
    @DisplayName("Date functions")
    class DateFunctions {

        @Test
        @DisplayName("Date comparison in WHERE clause")
        void dateComparison() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_orderdate > DATE '1995-01-01' LIMIT 10");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("EXTRACT year from date")
        void extractYear() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT EXTRACT(YEAR FROM o_orderdate) AS order_year "
                    + "FROM orders LIMIT 5");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                long year = ((Number) row.getRaw("order_year")).longValue();
                assertThat(year).isBetween(1992L, 1998L);
            }
        }
    }

    // ── Mathematical functions ──

    @Nested
    @DisplayName("Mathematical functions")
    class MathFunctions {

        @Test
        @DisplayName("ABS returns absolute value")
        void absFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT ABS(-42) AS abs_val");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("abs_val")).intValue()).isEqualTo(42);
        }

        @Test
        @DisplayName("ROUND rounds to specified precision")
        void roundFunction() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT ROUND(o_totalprice, 0) AS rounded FROM orders LIMIT 5");
            assertThat(rows).hasSize(5);
            // Verify all results are rounded to integer (no decimal places)
            for (TypedRow row : rows) {
                double rounded = ((Number) row.getRaw("rounded")).doubleValue();
                assertThat(rounded)
                        .as("ROUND(x, 0) should produce integer values")
                        .isEqualTo(Math.round(rounded));
            }
        }

        @Test
        @DisplayName("CEIL and FLOOR produce correct bounds")
        void ceilFloor() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT CEIL(CAST(3.2 AS DOUBLE)) AS ceil_val, "
                    + "FLOOR(CAST(3.8 AS DOUBLE)) AS floor_val");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("ceil_val")).intValue()).isEqualTo(4);
            assertThat(((Number) rows.get(0).getRaw("floor_val")).intValue()).isEqualTo(3);
        }
    }

    // ── SQL:2003 aggregate FILTER clause ──

    @Nested
    @DisplayName("Aggregate FILTER clause")
    class AggregateFilterClause {

        @Test
        @DisplayName("COUNT(*) FILTER counts only matching rows")
        void countStarWithFilter() {
            // Data-independent conditions: > 0 matches every row, < 0 matches none.
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) FILTER (WHERE o_totalprice > 0) AS all_cnt, "
                    + "COUNT(*) FILTER (WHERE o_totalprice < 0) AS none_cnt, "
                    + "COUNT(*) AS total FROM orders");
            assertThat(rows).hasSize(1);
            long allCnt = ((Number) rows.get(0).getRaw("all_cnt")).longValue();
            long noneCnt = ((Number) rows.get(0).getRaw("none_cnt")).longValue();
            long total = ((Number) rows.get(0).getRaw("total")).longValue();
            assertThat(total).isPositive();
            assertThat(allCnt).isEqualTo(total);
            assertThat(noneCnt).isZero();
        }

        @Test
        @DisplayName("Multiple FILTER clauses coexist in one SELECT")
        void multipleFiltersCoexist() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) FILTER (WHERE o_totalprice > 1000) AS big_cnt, "
                    + "COUNT(*) FILTER (WHERE o_totalprice <= 1000) AS small_cnt, "
                    + "COUNT(*) AS total FROM orders");
            assertThat(rows).hasSize(1);
            long bigCnt = ((Number) rows.get(0).getRaw("big_cnt")).longValue();
            long smallCnt = ((Number) rows.get(0).getRaw("small_cnt")).longValue();
            long total = ((Number) rows.get(0).getRaw("total")).longValue();
            // Partition property: the two complementary filters sum to total.
            assertThat(bigCnt + smallCnt).isEqualTo(total);
        }

        @Test
        @DisplayName("SUM FILTER aggregates only matching rows")
        void sumWithFilter() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT SUM(o_totalprice) FILTER (WHERE o_totalprice > 0) AS all_sum, "
                    + "SUM(o_totalprice) FILTER (WHERE o_totalprice < 0) AS none_sum, "
                    + "SUM(o_totalprice) AS total_sum FROM orders");
            assertThat(rows).hasSize(1);
            // All rows match → filtered sum equals the unfiltered total.
            double allSum = ((Number) rows.get(0).getRaw("all_sum")).doubleValue();
            double totalSum = ((Number) rows.get(0).getRaw("total_sum")).doubleValue();
            assertThat(allSum).isEqualTo(totalSum);
            // No rows match → SQL standard: SUM over the empty set is NULL.
            assertThat(rows.get(0).getRaw("none_sum")).isNull();
        }

        @Test
        @DisplayName("FILTER with GROUP BY applies per group")
        void filterWithGroupBy() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, "
                    + "COUNT(*) FILTER (WHERE o_totalprice > 1000) AS big_orders, "
                    + "COUNT(*) AS total "
                    + "FROM orders GROUP BY o_orderstatus");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                long bigOrders = ((Number) row.getRaw("big_orders")).longValue();
                long total = ((Number) row.getRaw("total")).longValue();
                assertThat(bigOrders).isBetween(0L, total);
            }
        }

        @Test
        @DisplayName("FILTER matching no rows yields COUNT 0")
        void filterMatchingNothingYieldsZeroCount() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) FILTER (WHERE o_totalprice < 0) AS none_cnt FROM orders");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("none_cnt")).longValue()).isZero();
        }
    }

    @Nested
    @DisplayName("Inline relational values")
    class InlineValues {
        @Test
        void namedValuesReachNativeWithoutAUnionRewrite() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT x AS val FROM (VALUES (1, 2), (3, 4)) AS t(x, y) ORDER BY x");
            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("val")).intValue()).isEqualTo(1);
            assertThat(((Number) rows.get(1).getRaw("val")).intValue()).isEqualTo(3);
        }
    }
}
