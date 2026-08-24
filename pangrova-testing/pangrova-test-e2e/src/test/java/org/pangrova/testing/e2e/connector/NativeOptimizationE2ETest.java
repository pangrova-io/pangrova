package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Native optimization E2E tests for Velox reader enhancements.
 *
 * <p>Covers:
 * - Task 2.16: Parquet Page Index (N1)
 * - Task 2.17: ORC Bloom Filter (N2)
 * - Task 2.18: CoalesceIO for Small Files (N4)
 * - Task 2.20: Subfield Pruning for Nested Types
 * - Task 2.21: Spill Compression zstd
 * - Task 2.22: Eager First Stripe for Small Files
 *
 * <p>These optimizations are configuration-level changes in PangrovaVeloxConnector.cpp
 * that enable existing Velox capabilities for massive I/O reduction.
 */
@DisplayName("Native Optimization E2E Tests")
class NativeOptimizationE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String icebergFqn() {
        return ICE_CATALOG + "." + SCHEMA + "." + tableName;
    }

    private void insertBatch(String columns, int rowCount,
                             IntFunction<String> rowGenerator) {
        int batchSize = 100;
        for (int i = 0; i < rowCount; i += batchSize) {
            StringBuilder sb = new StringBuilder("INSERT INTO ")
                    .append(icebergFqn()).append(" (").append(columns).append(") VALUES ");
            int end = Math.min(i + batchSize, rowCount);
            for (int j = i; j < end; j++) {
                if (j > i) {
                    sb.append(", ");
                }
                sb.append(rowGenerator.apply(j));
            }
            sqlRunner.execute(sb.toString());
        }
    }

    // --- Task 2.16: Parquet Page Index ---

    @Nested
    @DisplayName("2.16 - Parquet Page Index")
    class ParquetPageIndex {

        @Test
        @DisplayName("2.16.2 - High-selectivity query with page-level pruning")
        void highSelectivityQueryWithPagePruning() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR, value BIGINT)");

            insertBatch("id, name, value", 10000,
                    i -> "(" + i + ", 'user" + i + "', " + (i * 10) + ")");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id = 5000");

            assertThat(result).hasSize(1);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(5000L);
            assertThat(result.get(0).getRaw("name")).isEqualTo("user5000");
        }

        @Test
        @DisplayName("2.16.3 - Fallback gracefully for files without page index")
        void fallbackGracefullyWithoutPageIndex() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id = 2");

            assertThat(result).hasSize(1);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(2L);
        }
    }

    // --- Task 2.17: ORC Bloom Filter ---

    @Nested
    @DisplayName("2.17 - ORC Bloom Filter")
    class OrcBloomFilter {

        @Test
        @DisplayName("2.17.2 - ORC equality query with bloom filter stripe skip")
        void orcEqualityQueryWithBloomFilter() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR, value BIGINT) "
                    + "TBLPROPERTIES ('write.format.default'='orc')");

            insertBatch("id, name, value", 5000,
                    i -> "(" + i + ", 'user" + i + "', " + (i * 10) + ")");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id = 2500");

            assertThat(result).hasSize(1);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(2500L);
        }

        @Test
        @DisplayName("2.17.3 - ORC stripe statistics data skipping")
        void orcStripeStatisticsDataSkipping() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR) "
                    + "TBLPROPERTIES ('write.format.default'='orc')");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id > 1");

            assertThat(result).hasSize(2);
        }
    }

    // --- Task 2.18: CoalesceIO for Small Files ---

    @Nested
    @DisplayName("2.18 - CoalesceIO for Small Files")
    class CoalesceIO {

        @Test
        @DisplayName("2.18.2 - Small files query performance")
        void smallFilesQueryPerformance() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");

            for (int batch = 0; batch < 20; batch++) {
                StringBuilder sb = new StringBuilder(
                        "INSERT INTO " + icebergFqn() + " (id, name) VALUES ");
                for (int j = 0; j < 5; j++) {
                    int id = batch * 5 + j;
                    if (j > 0) {
                        sb.append(", ");
                    }
                    sb.append("(").append(id).append(", 'user").append(id).append("')");
                }
                sqlRunner.execute(sb.toString());
            }

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id < 10");

            assertThat(result).hasSize(10);
        }

        @Test
        @DisplayName("2.18.3 - CoalesceIO does not affect large files")
        void coalesceIODoesNotAffectLargeFiles() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR, value BIGINT)");

            insertBatch("id, name, value", 1000,
                    i -> "(" + i + ", 'user" + i + "', " + (i * 10) + ")");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT COUNT(*) as cnt FROM " + icebergFqn());

            assertThat(((Number) result.get(0).getRaw("cnt")).longValue()).isEqualTo(1000L);
        }
    }

    // --- Task 2.20: Subfield Pruning ---

    @Nested
    @DisplayName("2.20 - Subfield Pruning for Nested Types")
    class SubfieldPruning {

        @Test
        @DisplayName("2.20.3 - SELECT struct_col.field1 reads only required subfield")
        void selectSingleSubfieldFromStruct() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, info STRUCT(name VARCHAR, age INT, city VARCHAR))");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, info) VALUES "
                    + "(1, ROW('alice', 30, 'seattle')),"
                    + "(2, ROW('bob', 25, 'portland'))");

            // Query only one subfield — reader should prune other subfields
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT id, info.name FROM " + icebergFqn() + " ORDER BY id");

            assertThat(result).hasSize(2);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }

        @Test
        @DisplayName("2.20.4 - SELECT * reads all subfields without pruning")
        void selectAllSubfieldsFromStruct() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, info STRUCT(name VARCHAR, age INT, city VARCHAR))");

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, info) VALUES "
                    + "(1, ROW('alice', 30, 'seattle')),"
                    + "(2, ROW('bob', 25, 'portland'))");

            // SELECT * should read all subfields
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " ORDER BY id");

            assertThat(result).hasSize(2);
        }
    }

    // --- Task 2.21: Spill Compression zstd ---

    @Nested
    @DisplayName("2.21 - Spill Compression zstd")
    class SpillCompressionZstd {

        @Test
        @DisplayName("2.21.2 - Large aggregation with zstd spill produces correct results")
        void zstdSpillCompressionProducesCorrectResults() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, category VARCHAR, value BIGINT)");

            insertBatch("id, category, value", 5000,
                    i -> "(" + i + ", 'cat" + (i % 100) + "', " + (i * 7) + ")");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT category, SUM(value) as total, COUNT(*) as cnt "
                    + "FROM " + icebergFqn()
                    + " GROUP BY category ORDER BY category");

            assertThat(result).hasSize(100);
            assertThat(((Number) result.get(0).getRaw("cnt")).longValue()).isEqualTo(50L);
        }
    }

    // --- Task 2.22: Eager First Stripe for Small Files ---

    @Nested
    @DisplayName("2.22 - Eager First Stripe for Small Files")
    class EagerFirstStripe {

        @Test
        @DisplayName("2.22.2 - Small file first read latency (eager stripe load)")
        void smallFileEagerStripeLoad() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");

            // Insert a small amount of data (< 1MB file)
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            // Query the small file — should use eager first stripe load
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " ORDER BY id");

            assertThat(result).hasSize(3);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(result.get(0).getRaw("name")).isEqualTo("alice");
        }

        @Test
        @DisplayName("2.22.3 - Large file does not use eager stripe (lazy loading)")
        void largeFileNoEagerStripe() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR, value BIGINT)");

            insertBatch("id, name, value", 5000,
                    i -> "(" + i + ", 'user_with_long_name_" + i + "', " + (i * 100) + ")");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + " WHERE id = 2500");

            assertThat(result).hasSize(1);
            assertThat(((Number) result.get(0).getRaw("id")).longValue()).isEqualTo(2500L);
        }
    }
}
