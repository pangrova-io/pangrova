package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for Remote Shuffle Server (gRPC) full pipeline.
 *
 * <p>Validates the complete shuffle path through the real RemoteShuffleServer:
 * SQL → Parser → Planner → Substrait → Coordinator dispatch →
 * Worker → ShuffleBufferSender → gRPC RemoteShuffleServer →
 * PartitionBuffer → gRPC fetch → RemoteShuffleExchangeSource →
 * downstream Worker → final result via Transport → Client.
 *
 * <p>Extends MultiWorkerTestBase which starts a real multi-worker cluster
 * with coordinator, multiple workers, and shuffle server infrastructure.
 */
@Tag("multi-worker")
@DisplayName("Remote Shuffle Server E2E — gRPC Full Pipeline")
class RemoteShuffleServerE2ETest extends MultiWorkerTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String factTable;
    private String dimTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        factTable = "rss_fact_" + suffix;
        dimTable = "rss_dim_" + suffix;

        dataSetup.createTestTable(CATALOG, SCHEMA, factTable,
                "id BIGINT, dim_id BIGINT, amount BIGINT, category VARCHAR");
        dataSetup.createTestTable(CATALOG, SCHEMA, dimTable,
                "id BIGINT, name VARCHAR, region VARCHAR");

        insertFactData(200);
        insertDimData(50);
    }

    // -----------------------------------------------------------------------
    // Hash shuffle through remote shuffle server
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Hash Shuffle via Remote Shuffle Server")
    class HashShuffleViaRemoteServer {

        @Test
        @DisplayName("JOIN with GROUP BY exercises two shuffle stages through gRPC server")
        void joinWithGroupByThroughRemoteShuffleServer() {
            String sql = String.format(
                    "SELECT d.region, SUM(f.amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s f "
                    + "JOIN %s.%s.%s d ON f.dim_id = d.id "
                    + "GROUP BY d.region ORDER BY d.region",
                    CATALOG, SCHEMA, factTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).isNotEmpty();
            Set<String> regions = rows.stream()
                    .map(r -> r.getString("region"))
                    .collect(Collectors.toSet());
            assertThat(regions).hasSameSizeAs(rows);

            rows.forEach(row -> {
                assertThat(row.getLong("total")).isGreaterThan(0);
                assertThat(row.getLong("cnt")).isGreaterThan(0);
            });
        }

        @Test
        @DisplayName("Aggregation SUM through remote shuffle matches direct SUM")
        void aggregationSumMatchesDirectSum() {
            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            List<TypedRow> direct = sqlRunner.execute(directSql);
            long directTotal = direct.get(0).getLong("total");

            String groupedSql = String.format(
                    "SELECT category, SUM(amount) AS cat_total "
                    + "FROM %s.%s.%s GROUP BY category",
                    CATALOG, SCHEMA, factTable);
            List<TypedRow> grouped = sqlRunner.execute(groupedSql);
            long shuffledTotal = grouped.stream()
                    .mapToLong(r -> r.getLong("cat_total"))
                    .sum();

            assertThat(shuffledTotal)
                    .as("Sum through remote shuffle must match direct sum")
                    .isEqualTo(directTotal);
        }
    }

    // -----------------------------------------------------------------------
    // Data integrity through remote shuffle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Data Integrity via Remote Shuffle Server")
    class DataIntegrityViaRemoteServer {

        @Test
        @DisplayName("ORDER BY through remote shuffle preserves global sort")
        void orderByPreservesGlobalSort() {
            String sql = String.format(
                    "SELECT id, amount FROM %s.%s.%s ORDER BY amount ASC",
                    CATALOG, SCHEMA, factTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(200);
            for (int i = 1; i < rows.size(); i++) {
                long prev = rows.get(i - 1).getLong("amount");
                long curr = rows.get(i).getLong("amount");
                assertThat(prev).isLessThanOrEqualTo(curr);
            }
        }

        @Test
        @DisplayName("No row duplication or loss through remote shuffle")
        void noRowDuplicationOrLoss() {
            String sql = String.format(
                    "SELECT id FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(200);
            List<Long> ids = rows.stream()
                    .map(r -> r.getLong("id"))
                    .collect(Collectors.toList());
            assertThat(ids).doesNotHaveDuplicates();
        }
    }

    // -----------------------------------------------------------------------
    // Partition buffer spill through remote shuffle server
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Partition Buffer Pressure via Remote Shuffle Server")
    class PartitionBufferPressure {

        @Test
        @DisplayName("Large shuffle with many partitions completes without data loss")
        void largeShuffleManyPartitions() {
            String sql = String.format(
                    "SELECT dim_id, SUM(amount) AS total "
                    + "FROM %s.%s.%s GROUP BY dim_id ORDER BY dim_id",
                    CATALOG, SCHEMA, factTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(50);
            long totalViaGroup = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();

            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            long directTotal = sqlRunner.execute(directSql)
                    .get(0).getLong("total");

            assertThat(totalViaGroup).isEqualTo(directTotal);
        }
    }

    // -----------------------------------------------------------------------
    // Shuffle config propagation — verifies numPartitions and partition keys
    // reach the worker correctly through the coordinator dispatch pipeline.
    // Task 6 fix: DAGDispatchEngine.buildShuffleConfig() propagates
    // downstream parallelism as numPartitions.
    // Task 8 fix: TaskResultPipelineWirer.preCreatePartitionBuffers()
    // pre-creates partition buffers based on shuffleConfig.numPartitions().
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Shuffle Config Propagation — Coordinator to Worker")
    class ShuffleConfigPropagation {

        @Test
        @DisplayName("Multi-key GROUP BY propagates correct partition count through shuffle config")
        void multiKeyGroupByPropagatesPartitionCount() {
            // This query generates a multi-stage plan:
            // Stage 0: scan fact → partial aggregation → hash shuffle on (category)
            // Stage 1: final aggregation → output
            // The shuffle config must carry numPartitions = stage1.parallelism
            // and partitionKeyIndices for the category column.
            // If partition count is wrong (e.g., hardcoded 0), the shuffle
            // write would produce incorrect partition assignments.
            String sql = String.format(
                    "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt "
                    + "FROM %s.%s.%s "
                    + "GROUP BY category ORDER BY category",
                    CATALOG, SCHEMA, factTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            // 5 distinct categories (cat_0..cat_4)
            assertThat(rows).hasSize(5);

            // Verify partition isolation: each category has correct aggregation
            long totalViaGroup = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            long totalCount = rows.stream()
                    .mapToLong(r -> r.getLong("cnt"))
                    .sum();

            assertThat(totalCount)
                    .as("Total count across all partitions must equal row count")
                    .isEqualTo(200);

            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            long directTotal = sqlRunner.execute(directSql)
                    .get(0).getLong("total");
            assertThat(totalViaGroup)
                    .as("Shuffled aggregation must match direct sum — "
                        + "proves partition config propagated correctly")
                    .isEqualTo(directTotal);
        }

        @Test
        @DisplayName("JOIN exercises two shuffle stages with independent partition configs")
        void joinExercisesTwoShuffleStagesWithIndependentConfigs() {
            // This query generates 3+ stages:
            // Stage 0: scan fact → hash shuffle on dim_id
            // Stage 1: scan dim → hash shuffle on id
            // Stage 2: hash join → output
            // Each shuffle stage gets its own ShuffleConfig with the
            // downstream stage's parallelism as numPartitions.
            String sql = String.format(
                    "SELECT f.id, f.amount, d.name "
                    + "FROM %s.%s.%s f "
                    + "JOIN %s.%s.%s d ON f.dim_id = d.id "
                    + "ORDER BY f.id",
                    CATALOG, SCHEMA, factTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(200);
            // Verify join correctness — each fact row matched a dim row
            rows.forEach(row -> {
                assertThat(row.getString("name")).isNotNull();
                assertThat(row.getLong("amount")).isGreaterThanOrEqualTo(0);
            });

            // Verify global sort preserved through multi-stage shuffle
            for (int i = 1; i < rows.size(); i++) {
                long prev = rows.get(i - 1).getLong("id");
                long curr = rows.get(i).getLong("id");
                assertThat(prev).isLessThanOrEqualTo(curr);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Multi-stage shuffle lifecycle — verifies register/commit/cleanup
    // are called at correct lifecycle points across multiple stages.
    // Task 8 fix: DAGDispatchEngine.onStageFinished() calls
    // shuffleLifecycleManager.commitShuffle() per stage, and
    // cleanupShuffle() when all stages finish.
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-Stage Shuffle Lifecycle")
    class MultiStageShuffleLifecycle {

        @Test
        @DisplayName("Three-stage query completes with correct lifecycle: register → commit → cleanup")
        void threeStageQueryCompletesWithCorrectLifecycle() {
            // JOIN + GROUP BY + ORDER BY generates 3+ shuffle stages.
            // If any lifecycle step fails (register, commit, cleanup),
            // the query would either hang, produce wrong results, or leak resources.
            String sql = String.format(
                    "SELECT d.region, SUM(f.amount) AS total "
                    + "FROM %s.%s.%s f "
                    + "JOIN %s.%s.%s d ON f.dim_id = d.id "
                    + "GROUP BY d.region "
                    + "ORDER BY total DESC",
                    CATALOG, SCHEMA, factTable,
                    CATALOG, SCHEMA, dimTable);

            List<TypedRow> rows = sqlRunner.execute(sql);

            assertThat(rows).hasSize(5); // 5 regions
            // Verify descending sort
            for (int i = 1; i < rows.size(); i++) {
                long prev = rows.get(i - 1).getLong("total");
                long curr = rows.get(i).getLong("total");
                assertThat(prev).isGreaterThanOrEqualTo(curr);
            }

            // Cross-validate: sum of regional totals = direct sum
            long regionalSum = rows.stream()
                    .mapToLong(r -> r.getLong("total"))
                    .sum();
            String directSql = String.format(
                    "SELECT SUM(amount) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, factTable);
            long directTotal = sqlRunner.execute(directSql)
                    .get(0).getLong("total");
            assertThat(regionalSum).isEqualTo(directTotal);
        }

        @Test
        @DisplayName("Sequential queries reuse shuffle infrastructure without resource leak")
        void sequentialQueriesReuseShuffleInfrastructure() {
            // Run two GROUP BY queries back-to-back.
            // If cleanup doesn't release partition buffers, the second query
            // would fail with stale shuffle state or OOM.
            for (int round = 0; round < 2; round++) {
                String sql = String.format(
                        "SELECT category, COUNT(*) AS cnt "
                        + "FROM %s.%s.%s GROUP BY category",
                        CATALOG, SCHEMA, factTable);

                List<TypedRow> rows = sqlRunner.execute(sql);

                assertThat(rows).hasSize(5);
                long totalCount = rows.stream()
                        .mapToLong(r -> r.getLong("cnt"))
                        .sum();
                assertThat(totalCount)
                        .as("Round %d: total count must equal 200", round)
                        .isEqualTo(200);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers
    // -----------------------------------------------------------------------

    private void insertFactData(int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            long id = i + 1;
            long dimId = (i % 50) + 1;
            long amount = (i * 7 + 13) % 1000;
            String category = "cat_" + (i % 5);
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, dim_id, amount, category) "
                    + "VALUES (%d, %d, %d, '%s')",
                    CATALOG, SCHEMA, factTable, id, dimId, amount, category));
        }
    }

    private void insertDimData(int count) {
        for (int i = 0; i < count; i++) {
            long id = i + 1;
            String name = "dim_" + id;
            String region = "region_" + (i % 5);
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name, region) "
                    + "VALUES (%d, '%s', '%s')",
                    CATALOG, SCHEMA, dimTable, id, name, region));
        }
    }
}
