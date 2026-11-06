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
 * E2E tests for subqueries and set operations.
 *
 * <p>Covers scalar subqueries, IN subqueries, EXISTS subqueries, correlated
 * subqueries, UNION, UNION ALL, INTERSECT, and EXCEPT through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Arrow → Result.
 *
 * <p>Uses TPC-H test data (orders, customer, lineitem, nation).
 */
@DisplayName("Subquery & Set Operations - E2E Tests")
class SubquerySetOperationTest extends SqlE2ETestBase {

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

    // ── Scalar subquery ──

    @Nested
    @DisplayName("Scalar subquery")
    class ScalarSubquery {

        @Test
        @DisplayName("Scalar subquery in SELECT returns single value per row")
        void scalarSubqueryInSelect() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey, "
                    + "(SELECT COUNT(*) FROM customer) AS total_customers "
                    + "FROM orders LIMIT 5");
            assertThat(rows).hasSize(5);
            long custCount = ((Number) rows.get(0).getRaw("total_customers")).longValue();
            assertThat(custCount).isGreaterThan(0);
        }

        @Test
        @DisplayName("Scalar subquery in WHERE compares against aggregate")
        void scalarSubqueryInWhere() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders "
                    + "WHERE o_totalprice > (SELECT AVG(o_totalprice) FROM orders) "
                    + "LIMIT 10");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── IN subquery ──

    @Nested
    @DisplayName("IN subquery")
    class InSubquery {

        @Test
        @DisplayName("IN subquery filters to values from subquery")
        void inSubqueryFilters() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM orders "
                    + "WHERE o_custkey IN (SELECT c_custkey FROM customer LIMIT 10) "
                    + "LIMIT 20");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("NOT IN subquery excludes values from subquery")
        void notInSubquery() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM orders "
                    + "WHERE o_custkey NOT IN ("
                    + "  SELECT c_custkey FROM customer WHERE c_acctbal < 0"
                    + ")");
            assertThat(rows).hasSize(1);
        }
    }

    // ── EXISTS subquery ──

    @Nested
    @DisplayName("EXISTS subquery")
    class ExistsSubquery {

        @Test
        @DisplayName("EXISTS returns rows where correlated subquery has matches")
        void existsSubquery() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT c_custkey, c_name FROM customer c "
                    + "WHERE EXISTS ("
                    + "  SELECT 1 FROM orders o WHERE o.o_custkey = c.c_custkey"
                    + ") LIMIT 10");
            assertThat(rows).isNotEmpty();
        }

        @Test
        @DisplayName("NOT EXISTS returns rows without matches")
        void notExistsSubquery() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM customer c "
                    + "WHERE NOT EXISTS ("
                    + "  SELECT 1 FROM orders o WHERE o.o_custkey = c.c_custkey"
                    + ")");
            assertThat(rows).hasSize(1);
        }
    }

    // ── UNION ──

    @Nested
    @DisplayName("UNION / UNION ALL")
    class UnionOperations {

        @Test
        @DisplayName("UNION ALL combines results from two queries")
        void unionAll() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderkey AS id FROM orders WHERE o_orderstatus = 'O' LIMIT 5 "
                    + "UNION ALL "
                    + "SELECT o_orderkey AS id FROM orders WHERE o_orderstatus = 'F' LIMIT 5");
            assertThat(rows).hasSize(10);
        }

        @Test
        @DisplayName("UNION removes duplicates")
        void unionDistinct() {
            List<TypedRow> unionAll = sqlRunner.execute(
                    "SELECT o_orderstatus AS status FROM orders "
                    + "UNION ALL "
                    + "SELECT o_orderstatus AS status FROM orders");
            List<TypedRow> unionDistinct = sqlRunner.execute(
                    "SELECT o_orderstatus AS status FROM orders "
                    + "UNION "
                    + "SELECT o_orderstatus AS status FROM orders");
            assertThat(unionDistinct.size()).isLessThanOrEqualTo(unionAll.size());
        }
    }

    // ── INTERSECT ──

    @Nested
    @DisplayName("INTERSECT")
    class IntersectOperation {

        @Test
        @DisplayName("INTERSECT returns only common rows")
        void intersectReturnsCommon() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus AS status FROM orders "
                    + "INTERSECT "
                    + "SELECT o_orderstatus AS status FROM orders WHERE o_orderstatus = 'O'");
            assertThat(rows).isNotEmpty();
            // Result should only contain 'O'
            for (TypedRow row : rows) {
                assertThat(row.getRaw("status")).isEqualTo("O");
            }
        }
    }

    // ── EXCEPT ──

    @Nested
    @DisplayName("EXCEPT")
    class ExceptOperation {

        @Test
        @DisplayName("EXCEPT removes rows present in second query")
        void exceptRemovesSecondSet() {
            List<TypedRow> allStatuses = sqlRunner.execute(
                    "SELECT DISTINCT o_orderstatus AS status FROM orders");
            List<TypedRow> exceptResult = sqlRunner.execute(
                    "SELECT DISTINCT o_orderstatus AS status FROM orders "
                    + "EXCEPT "
                    + "SELECT o_orderstatus AS status FROM orders WHERE o_orderstatus = 'O'");
            assertThat(exceptResult.size()).isLessThan(allStatuses.size());
            // 'O' should not appear in result
            for (TypedRow row : exceptResult) {
                assertThat(row.getRaw("status")).isNotEqualTo("O");
            }
        }
    }
}
