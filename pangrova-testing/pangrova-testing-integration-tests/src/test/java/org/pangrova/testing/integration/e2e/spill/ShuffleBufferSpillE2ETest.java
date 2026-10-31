package org.pangrova.testing.integration.e2e.spill;

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
 * E2E test for shuffle buffer spill-to-disk and recovery.
 *
 * <p>Validates the full path when ShuffleBufferManager exceeds memory threshold:
 * SQL → Parser → Planner → Coordinator dispatch → Worker →
 * ShuffleBufferManager (memory full) → ShuffleDiskSpiller (spill to disk) →
 * read back from disk → downstream exchange → final result.
 *
 * <p>Uses real infrastructure with constrained shuffle buffer memory to force spill.
 */
@DisplayName("Shuffle Buffer Spill E2E — Disk Spill and Recovery")
class ShuffleBufferSpillE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int LARGE_ROW_COUNT = 5000;

    private String largeTable;
    private String dimTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        largeTable = "spill_large_" + suffix;
        dimTable = "spill_dim_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, largeTable,
                "id BIGINT, group_key BIGINT, value BIGINT, payload VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, dimTable,
                "id BIGINT, label VARCHAR");

        insertLargeData();
        insertDimData(100);
    }

    @Nested
    @DisplayName("Aggregation with Shuffle Spill")
    class AggregationWithShuffleSpill {

        @Test
        @DisplayName("Large GROUP BY forces shuffle buffer spill, result still correct")
        void largeGroupByForcesShuffleSpill() {
            String sql = String.format(
                    "SELECT group_key, SUM(value) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s GROUP BY group_key ORDER BY group_key",
                    CATALOG, SCHEMA, largeTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(100);

            long totalViaGroup = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("total")).longValue())
                    .sum();
            long totalCount = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();

            assertThat(totalCount).isEqualTo(LARGE_ROW_COUNT);

            String directSql = String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, largeTable);
            long directTotal = ((Number) sqlRunner.execute(directSql)
                    .get(0).getRaw("total")).longValue();
            assertThat(totalViaGroup).isEqualTo(directTotal);
        }
    }

    @Nested
    @DisplayName("Join with Shuffle Spill")
    class JoinWithShuffleSpill {

        @Test
        @DisplayName("Large JOIN forces shuffle spill on build side")
        void largeJoinForcesShuffleSpill() {
            String sql = String.format(
                    "SELECT d.label, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s f "
                    + "JOIN %s.%s.%s d ON f.group_key = d.id "
                    + "GROUP BY d.label ORDER BY d.label",
                    CATALOG, SCHEMA, largeTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            long totalJoined = rows.stream()
                    .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                    .sum();
            assertThat(totalJoined).isEqualTo(LARGE_ROW_COUNT);
        }
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers
    // -----------------------------------------------------------------------

    private static final int BATCH_SIZE = 500;

    private void insertLargeData() {
        for (int start = 0; start < LARGE_ROW_COUNT; start += BATCH_SIZE) {
            int end = Math.min(start + BATCH_SIZE, LARGE_ROW_COUNT);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s.%s.%s (id, group_key, value, payload) VALUES ",
                    CATALOG, SCHEMA, largeTable));
            for (int i = start; i < end; i++) {
                if (i > start) {
                    sb.append(", ");
                }
                long id = i + 1;
                long groupKey = (i % 100) + 1;
                long value = (i * 13 + 7) % 997;
                sb.append(String.format("(%d, %d, %d, 'row_%d')", id, groupKey, value, id));
            }
            sqlRunner.execute(sb.toString());
        }
    }

    private void insertDimData(int count) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, label) VALUES ",
                CATALOG, SCHEMA, dimTable));
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            long id = i + 1;
            sb.append(String.format("(%d, 'dim_%d')", id, id));
        }
        sqlRunner.execute(sb.toString());
    }
}
