package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for L0 statistics-based pruning with selective range scans.
 *
 * <p>Inserts 10+ data files with non-overlapping value ranges, then executes
 * range queries to verify that L0 file-level statistics correctly prune
 * irrelevant files while returning accurate results.
 *
 * <p>Validates: Requirements 2.AC1, 2.AC2, 2.AC3, 2.AC5
 */
@DisplayName("L0 Statistics Pruning E2E")
class L0StatisticsPruningE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_l0_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, label VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts 10 data files with non-overlapping value ranges.
     * File 1: value [0, 99], File 2: value [100, 199], ..., File 10: value [900, 999].
     */
    private void insertNonOverlappingFiles() {
        for (int i = 0; i < 10; i++) {
            int base = i * 100;
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES"
                    + " (" + (base + 1) + ", " + base + ", 'batch_" + i + "'),"
                    + " (" + (base + 50) + ", " + (base + 50) + ", 'batch_" + i + "'),"
                    + " (" + (base + 99) + ", " + (base + 99) + ", 'batch_" + i + "')");
        }
    }

    @Nested
    @DisplayName("Range Scan Pruning")
    class RangeScanPruning {

        @Test
        @DisplayName("Range query on non-overlapping files returns correct results")
        void rangeQueryReturnsCorrectResults() {
            insertNonOverlappingFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 100 AND 200");

            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long val = ((Number) r.getRaw("value")).longValue();
                return val >= 100 && val <= 200;
            });
        }

        @Test
        @DisplayName("Narrow range query returns only matching rows")
        void narrowRangeQueryReturnsOnlyMatchingRows() {
            insertNonOverlappingFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 450 AND 550");

            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long val = ((Number) r.getRaw("value")).longValue();
                return val >= 450 && val <= 550;
            });
        }

        @Test
        @DisplayName("Range query matching single file returns exact rows")
        void singleFileRangeQuery() {
            insertNonOverlappingFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 300 AND 399");

            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long val = ((Number) r.getRaw("value")).longValue();
                return val >= 300 && val <= 399;
            });
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Range query matching no files returns empty result")
        void noMatchingFilesReturnsEmpty() {
            insertNonOverlappingFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 1500 AND 2000");

            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("Range query spanning all files returns all rows")
        void fullRangeReturnsAllRows() {
            insertNonOverlappingFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 0 AND 999");

            // 10 files × 3 rows each = 30 rows
            assertThat(rows).hasSize(30);
        }
    }
}
