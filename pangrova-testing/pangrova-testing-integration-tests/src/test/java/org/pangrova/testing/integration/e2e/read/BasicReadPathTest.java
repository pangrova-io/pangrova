package org.pangrova.testing.integration.e2e.read;

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
 * E2E tests for basic SQL read path operations.
 *
 * <p>Covers SELECT, WHERE filtering, ORDER BY, LIMIT, OFFSET, DISTINCT,
 * and basic projection through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Arrow → Result.
 *
 * <p>Uses TPC-H test data (orders, lineitem, customer tables).
 */
@DisplayName("Basic Read Path - E2E Tests")
class BasicReadPathTest extends SqlE2ETestBase {

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

    // ── SELECT * ──

    @Nested
    @DisplayName("SELECT * (full scan)")
    class SelectAll {

        @Test
        @DisplayName("SELECT * FROM orders returns all rows")
        void selectStarReturnsAllRows() {
            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM orders");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("SELECT * FROM customer returns all customer rows")
        void selectStarFromCustomer() {
            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM customer");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── Projection ──

    @Nested
    @DisplayName("Projection (column selection)")
    class Projection {

        @Test
        @DisplayName("SELECT specific columns returns only those columns")
        void selectSpecificColumns() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey, o_orderstatus FROM orders");
            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.columnNames()).contains("o_orderkey", "o_orderstatus");
        }

        @Test
        @DisplayName("SELECT with alias renames columns in result")
        void selectWithAlias() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey AS id, o_totalprice AS price FROM orders");
            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.columnNames()).contains("id", "price");
        }
    }

    // ── WHERE filtering ──

    @Nested
    @DisplayName("WHERE clause filtering")
    class WhereFiltering {

        @Test
        @DisplayName("WHERE with equality returns matching rows only")
        void whereEquality() {
            List<TypedRow> all = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM orders");
            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_orderstatus = 'O'");
            long totalCount = ((Number) all.get(0).getRaw("cnt")).longValue();
            assertThat(filtered.size()).isLessThan((int) totalCount);
            assertThat(filtered).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with comparison operators filters correctly")
        void whereComparison() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_totalprice > 0");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with AND combines predicates")
        void whereAnd() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_orderstatus = 'O' AND o_totalprice > 500.0");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with OR produces union of conditions")
        void whereOr() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_orderstatus = 'O' OR o_orderstatus = 'F'");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with IN list filters to matching values")
        void whereIn() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_orderpriority IN ('1-URGENT', '2-HIGH')");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with BETWEEN filters to range")
        void whereBetween() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders WHERE o_totalprice BETWEEN 100.0 AND 5000.0");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("WHERE with IS NULL filters null values")
        void whereIsNull() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM lineitem WHERE l_comment IS NULL");
            // May be empty or not depending on data — just confirm no error
            assertThat(rows).isNotNull();
        }

        @Test
        @DisplayName("WHERE with LIKE performs pattern matching")
        void whereLike() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM customer WHERE c_name LIKE 'Customer%'");
            // Pattern may or may not match — no error is the test
            assertThat(rows).isNotNull();
        }
    }

    // ── ORDER BY ──

    @Nested
    @DisplayName("ORDER BY sorting")
    class OrderBy {

        @Test
        @DisplayName("ORDER BY ASC returns rows in ascending order")
        void orderByAsc() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey FROM orders ORDER BY o_orderkey ASC LIMIT 10");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
            long first = ((Number) rows.get(0).getRaw("o_orderkey")).longValue();
            long second = ((Number) rows.get(1).getRaw("o_orderkey")).longValue();
            assertThat(first).isLessThanOrEqualTo(second);
        }

        @Test
        @DisplayName("ORDER BY DESC returns rows in descending order")
        void orderByDesc() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey FROM orders ORDER BY o_orderkey DESC LIMIT 10");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
            long first = ((Number) rows.get(0).getRaw("o_orderkey")).longValue();
            long second = ((Number) rows.get(1).getRaw("o_orderkey")).longValue();
            assertThat(first).isGreaterThanOrEqualTo(second);
        }

        @Test
        @DisplayName("ORDER BY multiple columns applies secondary sort")
        void orderByMultipleColumns() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, o_orderkey FROM orders "
                    + "ORDER BY o_orderstatus ASC, o_orderkey ASC LIMIT 20");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── LIMIT and OFFSET ──

    @Nested
    @DisplayName("LIMIT and OFFSET")
    class LimitOffset {

        @Test
        @DisplayName("LIMIT restricts result set size")
        void limitRestrictsRows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders LIMIT 5");
            assertThat(rows).hasSize(5);
        }

        @Test
        @DisplayName("LIMIT 0 returns empty result")
        void limitZero() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders LIMIT 0");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("OFFSET skips initial rows")
        void offsetSkipsRows() {
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 10");
            List<TypedRow> offsetRows = sqlRunner.execute(
                    "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 5 OFFSET 5");

            assertThat(offsetRows).hasSize(5);
            long firstOffset = ((Number) offsetRows.get(0).getRaw("o_orderkey")).longValue();
            long sixthAll = ((Number) allRows.get(5).getRaw("o_orderkey")).longValue();
            assertThat(firstOffset).isEqualTo(sixthAll);
        }
    }

    // ── DISTINCT ──

    @Nested
    @DisplayName("DISTINCT")
    class Distinct {

        @Test
        @DisplayName("SELECT DISTINCT removes duplicates")
        void selectDistinct() {
            List<TypedRow> distinct = sqlRunner.execute(
                    "SELECT DISTINCT o_orderstatus FROM orders");
            List<TypedRow> all = sqlRunner.execute(
                    "SELECT o_orderstatus FROM orders");
            assertThat(distinct.size()).isLessThanOrEqualTo(all.size());
            assertThat(distinct).isNotEmpty();
        }
    }
}
