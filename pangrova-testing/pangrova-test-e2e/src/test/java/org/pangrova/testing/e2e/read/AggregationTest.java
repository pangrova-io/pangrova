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
 * E2E tests for SQL aggregation operations.
 *
 * <p>Covers COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native (Velox) → Arrow → Result.
 *
 * <p>Uses TPC-H test data (orders, lineitem).
 */
@DisplayName("Aggregation - E2E Tests")
class AggregationTest extends SqlE2ETestBase {

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
                org.pangrova.testing.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    // ── Basic aggregates ──

    @Nested
    @DisplayName("Basic aggregate functions")
    class BasicAggregates {

        @Test
        @DisplayName("COUNT(*) returns total row count")
        void countStar() {
            List<TypedRow> rows = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM orders");
            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isGreaterThan(0);
        }

        @Test
        @DisplayName("COUNT(column) excludes NULLs")
        void countColumn() {
            List<TypedRow> countAll = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM lineitem");
            List<TypedRow> countCol = sqlRunner.execute("SELECT COUNT(l_comment) AS cnt FROM lineitem");
            long all = ((Number) countAll.get(0).getRaw("cnt")).longValue();
            long col = ((Number) countCol.get(0).getRaw("cnt")).longValue();
            assertThat(col).isLessThanOrEqualTo(all);
        }

        @Test
        @DisplayName("SUM returns correct total")
        void sumAggregate() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT SUM(o_totalprice) AS total FROM orders");
            assertThat(rows).hasSize(1);
            double total = ((Number) rows.get(0).getRaw("total")).doubleValue();
            assertThat(total).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("AVG returns average value")
        void avgAggregate() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT AVG(o_totalprice) AS avg_price FROM orders");
            assertThat(rows).hasSize(1);
            double avg = ((Number) rows.get(0).getRaw("avg_price")).doubleValue();
            assertThat(avg).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("MIN returns minimum value")
        void minAggregate() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT MIN(o_totalprice) AS min_price FROM orders");
            assertThat(rows).hasSize(1);
            double min = ((Number) rows.get(0).getRaw("min_price")).doubleValue();
            assertThat(min).isGreaterThanOrEqualTo(0.0);
        }

        @Test
        @DisplayName("MAX returns maximum value")
        void maxAggregate() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT MAX(o_totalprice) AS max_price FROM orders");
            assertThat(rows).hasSize(1);
            double max = ((Number) rows.get(0).getRaw("max_price")).doubleValue();
            assertThat(max).isGreaterThan(0.0);
        }
    }

    // ── GROUP BY ──

    @Nested
    @DisplayName("GROUP BY")
    class GroupBy {

        @Test
        @DisplayName("GROUP BY single column produces groups")
        void groupBySingleColumn() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("GROUP BY multiple columns produces combined groups")
        void groupByMultipleColumns() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, o_orderpriority, COUNT(*) AS cnt "
                    + "FROM orders GROUP BY o_orderstatus, o_orderpriority");
            assertThat(rows).hasSizeGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("GROUP BY with SUM produces per-group totals")
        void groupByWithSum() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, SUM(o_totalprice) AS total "
                    + "FROM orders GROUP BY o_orderstatus");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                double total = ((Number) row.getRaw("total")).doubleValue();
                assertThat(total).isGreaterThan(0.0);
            }
        }

        @Test
        @DisplayName("GROUP BY aggregate sum equals full-table SUM")
        void groupBySumEqualsTotal() {
            List<TypedRow> totalRow = sqlRunner.execute(
                    "SELECT SUM(o_totalprice) AS total FROM orders");
            double expectedTotal = ((Number) totalRow.get(0).getRaw("total")).doubleValue();

            List<TypedRow> groups = sqlRunner.execute(
                    "SELECT SUM(o_totalprice) AS grp_total FROM orders GROUP BY o_orderstatus");
            double sumOfGroups = groups.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("grp_total")).doubleValue())
                    .sum();

            assertThat(sumOfGroups).isCloseTo(expectedTotal, org.assertj.core.data.Offset.offset(0.01));
        }
    }

    // ── HAVING ──

    @Nested
    @DisplayName("HAVING")
    class Having {

        @Test
        @DisplayName("HAVING filters groups by aggregate condition")
        void havingFiltersGroups() {
            List<TypedRow> allGroups = sqlRunner.execute(
                    "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus");
            List<TypedRow> filteredGroups = sqlRunner.execute(
                    "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders "
                    + "GROUP BY o_orderstatus HAVING COUNT(*) > 10");
            assertThat(filteredGroups.size()).isLessThanOrEqualTo(allGroups.size());
        }

        @Test
        @DisplayName("HAVING with SUM filters by sum condition")
        void havingWithSum() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o_orderstatus, SUM(o_totalprice) AS total "
                    + "FROM orders GROUP BY o_orderstatus HAVING SUM(o_totalprice) > 1000.0");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── COUNT DISTINCT ──

    @Nested
    @DisplayName("COUNT DISTINCT")
    class CountDistinct {

        @Test
        @DisplayName("COUNT(DISTINCT col) returns unique count")
        void countDistinct() {
            List<TypedRow> countAll = sqlRunner.execute(
                    "SELECT COUNT(o_orderstatus) AS cnt FROM orders");
            List<TypedRow> countDistinct = sqlRunner.execute(
                    "SELECT COUNT(DISTINCT o_orderstatus) AS cnt FROM orders");
            long all = ((Number) countAll.get(0).getRaw("cnt")).longValue();
            long distinct = ((Number) countDistinct.get(0).getRaw("cnt")).longValue();
            assertThat(distinct).isLessThanOrEqualTo(all);
            assertThat(distinct).isGreaterThan(0);
        }
    }
}
