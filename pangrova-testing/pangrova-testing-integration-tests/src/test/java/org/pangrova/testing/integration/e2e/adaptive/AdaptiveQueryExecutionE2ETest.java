package org.pangrova.testing.integration.e2e.adaptive;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for Adaptive Query Execution (AQE) runtime adaptation.
 *
 * <p>Validates that PartitionCoalescer and SkewJoinHandler produce correct
 * results when triggered by real shuffle statistics through the full pipeline:
 * SQL → Parser → Planner → Substrait → Native → AdaptiveQueryEvaluator →
 * PartitionCoalescer/SkewJoinHandler → Transport → Client.
 *
 * <p>Uses data distributions designed to trigger AQE decisions:
 * many small partitions (coalescing) and highly skewed keys (skew join).
 */
@DisplayName("Adaptive Query Execution E2E — Runtime Adaptation")
class AdaptiveQueryExecutionE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int ROW_COUNT = 3000;

    private String factTable;
    private String dimTable;
    private String skewTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        factTable = "aqe_fact_" + suffix;
        dimTable = "aqe_dim_" + suffix;
        skewTable = "aqe_skew_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, factTable,
                "id BIGINT, dim_id BIGINT, amount BIGINT, category VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, dimTable,
                "id BIGINT, name VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, skewTable,
                "id BIGINT, skew_key BIGINT, value BIGINT");

        insertFactData();
        insertDimData(50);
        insertSkewedData();
    }

    // -----------------------------------------------------------------------
    // Partition coalescing: many small groups → coalesced partitions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Partition Coalescing")
    class PartitionCoalescingScenarios {

        @Test
        @DisplayName("Aggregation with many small groups produces correct results after coalescing")
        void manySmallGroupsCoalesced() {
            String sql = String.format(
                    "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s GROUP BY category ORDER BY category",
                    CATALOG, SCHEMA, factTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalViaGroup = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("total")).longValue())
                    .sum();
            long totalCount = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();

            assertThat(totalCount).isEqualTo(ROW_COUNT);

            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            long directTotal = ((Number) sqlRunner.execute(directSql)
                    .get(0).getRaw("total")).longValue();
            assertThat(totalViaGroup).isEqualTo(directTotal);
        }

        @Test
        @DisplayName("JOIN with many dim keys produces correct result after coalescing")
        void joinWithManyDimKeysCoalesced() {
            String sql = String.format(
                    "SELECT d.name, SUM(f.amount) AS total "
                    + "FROM %s.%s.%s f "
                    + "JOIN %s.%s.%s d ON f.dim_id = d.id "
                    + "GROUP BY d.name ORDER BY d.name",
                    CATALOG, SCHEMA, factTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(50);
            long joinTotal = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("total")).longValue())
                    .sum();

            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            long directTotal = ((Number) sqlRunner.execute(directSql)
                    .get(0).getRaw("total")).longValue();
            assertThat(joinTotal).isEqualTo(directTotal);
        }
    }

    // -----------------------------------------------------------------------
    // Skew join handling: highly skewed key distribution
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Skew Join Handling")
    class SkewJoinHandlingScenarios {

        @Test
        @DisplayName("Skewed GROUP BY produces correct results despite uneven distribution")
        void skewedGroupByCorrectResults() {
            String sql = String.format(
                    "SELECT skew_key, SUM(value) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s GROUP BY skew_key ORDER BY skew_key",
                    CATALOG, SCHEMA, skewTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalCount = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();
            assertThat(totalCount).isEqualTo(ROW_COUNT);

            String directSql = String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, skewTable);
            long directTotal = ((Number) sqlRunner.execute(directSql)
                    .get(0).getRaw("total")).longValue();
            long groupTotal = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("total")).longValue())
                    .sum();
            assertThat(groupTotal).isEqualTo(directTotal);
        }

        @Test
        @DisplayName("Skewed JOIN produces correct row count")
        void skewedJoinCorrectRowCount() {
            String sql = String.format(
                    "SELECT d.name, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s s "
                    + "JOIN %s.%s.%s d ON s.skew_key = d.id "
                    + "GROUP BY d.name ORDER BY d.name",
                    CATALOG, SCHEMA, skewTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalJoined = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();
            assertThat(totalJoined).isEqualTo(ROW_COUNT);
        }
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers
    // -----------------------------------------------------------------------

    private void insertFactData() {
        int batchSize = 500;
        for (int offset = 0; offset < ROW_COUNT; offset += batchSize) {
            int end = Math.min(offset + batchSize, ROW_COUNT);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s.%s.%s (id, dim_id, amount, category) VALUES ",
                    CATALOG, SCHEMA, factTable));
            for (int i = offset; i < end; i++) {
                if (i > offset) sb.append(", ");
                long id = i + 1;
                long dimId = (i % 50) + 1;
                long amount = (i * 7 + 3) % 500;
                String category = "cat_" + (i % 20);
                sb.append(String.format("(%d, %d, %d, '%s')", id, dimId, amount, category));
            }
            sqlRunner.execute(sb.toString());
        }
    }

    private void insertDimData(int count) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, name) VALUES ",
                CATALOG, SCHEMA, dimTable));
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            long id = i + 1;
            sb.append(String.format("(%d, 'dim_%d')", id, id));
        }
        sqlRunner.execute(sb.toString());
    }

    /**
     * Inserts data with highly skewed key distribution:
     * 80% of rows map to key 1, remaining 20% spread across keys 2-50.
     */
    private void insertSkewedData() {
        int batchSize = 500;
        for (int offset = 0; offset < ROW_COUNT; offset += batchSize) {
            int end = Math.min(offset + batchSize, ROW_COUNT);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s.%s.%s (id, skew_key, value) VALUES ",
                    CATALOG, SCHEMA, skewTable));
            for (int i = offset; i < end; i++) {
                if (i > offset) sb.append(", ");
                long id = i + 1;
                long skewKey = (i < ROW_COUNT * 80 / 100) ? 1 : ((i % 49) + 2);
                long value = (i * 11 + 5) % 1000;
                sb.append(String.format("(%d, %d, %d)", id, skewKey, value));
            }
            sqlRunner.execute(sb.toString());
        }
    }
}
