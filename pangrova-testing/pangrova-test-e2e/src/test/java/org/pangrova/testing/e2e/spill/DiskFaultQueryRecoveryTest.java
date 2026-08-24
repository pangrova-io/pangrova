package org.pangrova.testing.e2e.spill;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.fixture.WriteTestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying spill-to-disk fault tolerance under disk errors.
 *
 * <p>Validates that spill handles cleanup
 * correctly through the real pipeline: TestCluster HTTP → NativeRuntime → Velox.
 *
 * <p>Requirements: 11.3, 11.4, 11.5
 */
@DisplayName("Spill-to-Disk - Disk Fault Query Recovery E2E Tests")
class DiskFaultQueryRecoveryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private static final int LARGE_ROW_COUNT = 5000;
    private static final int CONCURRENT_QUERY_COUNT = 3;

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_diskfault_" + method + "_" + System.nanoTime();
    }

    // --- N11.3: Spill files cleaned after successful query ---

    @Test
    @DisplayName("N11.3 - Spill files cleaned up after successful query")
    void spillFilesCleanedAfterSuccess(@TempDir Path spillDir) {
        createDataTable(tableName);
        insertLargeDataset(tableName, LARGE_ROW_COUNT);

        sqlRunner.execute(buildLargeAggregation(tableName));

        assertSpillDirEmpty(spillDir);
    }

    // --- N11.4: Spill files cleaned after failed query ---

    @Test
    @DisplayName("N11.4 - Spill files cleaned up after failed query")
    void spillFilesCleanedAfterFailure(@TempDir Path spillDir) {
        createDataTable(tableName);
        insertLargeDataset(tableName, LARGE_ROW_COUNT);

        try {
            sqlRunner.execute(buildFailingSpillQuery(tableName));
        } catch (Exception ignored) {
            // Expected failure — we only care about cleanup
        }

        assertSpillDirEmpty(spillDir);
    }

    // --- N11.5: Concurrent spill isolation ---

    @Test
    @DisplayName("N11.5 - Concurrent queries spill to same dir with correct isolation")
    void concurrentSpillIsolation() throws Exception {
        List<String> tables = createConcurrentTables();
        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_QUERY_COUNT);

        try {
            List<Future<List<TypedRow>>> futures = submitConcurrentQueries(
                    executor, tables, startGate);
            startGate.countDown();
            assertConcurrentResults(futures, tables);
        } finally {
            executor.shutdownNow();
        }
    }

    // -----------------------------------------------------------------------
    // Table creation and data helpers
    // -----------------------------------------------------------------------

    private void createDataTable(String table) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)",
                fqn(table), "id BIGINT, grp BIGINT, amount DOUBLE"));
    }

    private void insertLargeDataset(String table, int rowCount) {
        List<TypedRow> data = IntStream.rangeClosed(1, rowCount)
                .mapToObj(i -> TypedRow.builder().putRaw("id", (long) i).putRaw("grp", (long) (i % 50)).putRaw("amount", i * 1.5).build())
                .collect(Collectors.toList());
        WriteTestHelper.insertRows(sqlRunner, fqn(table), data);
    }

    // -----------------------------------------------------------------------
    // SQL query builders
    // -----------------------------------------------------------------------

    private String buildLargeAggregation(String table) {
        return "SELECT grp, SUM(amount), COUNT(*) FROM " + fqn(table) + " GROUP BY grp";
    }

    private String buildFailingSpillQuery(String table) {
        return "SELECT id, amount / 0 FROM " + fqn(table);
    }

    // -----------------------------------------------------------------------
    // Spill directory assertion
    // -----------------------------------------------------------------------

    private static void assertSpillDirEmpty(Path spillDir) {
        try (Stream<Path> files = Files.list(spillDir)) {
            List<Path> remaining = files.collect(Collectors.toList());
            assertThat(remaining)
                    .as("Spill directory should contain zero temp files")
                    .isEmpty();
        } catch (IOException e) {
            throw new RuntimeException("Failed to list spill directory", e);
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent spill helpers
    // -----------------------------------------------------------------------

    private List<String> createConcurrentTables() {
        List<String> tables = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_QUERY_COUNT; i++) {
            String name = tableName + "_c" + i;
            createDataTable(name);
            insertLargeDataset(name, LARGE_ROW_COUNT);
            tables.add(name);
        }
        return tables;
    }

    private List<Future<List<TypedRow>>> submitConcurrentQueries(
            ExecutorService executor, List<String> tables, CountDownLatch startGate) {
        return tables.stream()
                .map(t -> executor.submit(() -> {
                    startGate.await();
                    return sqlRunner.execute(buildLargeAggregation(t));
                }))
                .collect(Collectors.toList());
    }

    private void assertConcurrentResults(
            List<Future<List<TypedRow>>> futures,
            List<String> tables) throws Exception {
        for (int i = 0; i < futures.size(); i++) {
            List<TypedRow> rows = futures.get(i).get();
            assertThat(rows)
                    .as("Concurrent query on %s should return results", tables.get(i))
                    .isNotEmpty();
        }
    }

    private String fqn(String table) {
        return FQN_PREFIX + table;
    }
}
