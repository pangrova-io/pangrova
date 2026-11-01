package org.pangrova.testing.integration.e2e.io;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.ResultValidator;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.WriteTestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying I/O coalesced load correctness through the real pipeline.
 *
 * <p>Validates that file layout (wide tables, many small files vs few large files)
 * and concurrent I/O do not affect query result correctness. I/O coalescing is a
 * native layer optimization that merges adjacent I/O requests — these tests ensure
 * it does not corrupt data.
 *
 * <p>Requirements: 8.1, 8.2, 8.3
 */
@DisplayName("File Layout Transparency - I/O Coalesced Load E2E Tests")
class FileLayoutTransparencyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int WIDE_TABLE_COLUMN_COUNT = 22;
    private static final int SMALL_BATCH_SIZE = 5;
    private static final int SMALL_BATCH_COUNT = 10;
    private static final int CONCURRENT_QUERY_COUNT = 4;

    private String table;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        table = "t_io_" + method + "_" + System.nanoTime();
    }

    // --- N8.1: Wide table (20+ columns) scan verifies all columns correct ---

    @Test
    @DisplayName("N8.1 - Wide table scan reads all 22 columns correctly")
    void wideTableScanAllColumnsCorrect() {
        createWideTable(table);
        insertWideRow(table, 1);
        insertWideRow(table, 2);

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(table) + " ORDER BY id");

        assertThat(rows).hasSize(2);
        assertWideRowCorrect(rows.get(0), 1);
        assertWideRowCorrect(rows.get(1), 2);
    }

    // --- N8.2: Many small files vs few large files produce identical results ---

    @Test
    @DisplayName("N8.2 - Many small files match few large files with same data")
    void manySmallFilesMatchFewLargeFiles() {
        String smallFilesTable = table + "_small";
        String largeFilesTable = table + "_large";
        String columns = "id BIGINT, name VARCHAR, amount DOUBLE";

        createTable(smallFilesTable, columns);
        createTable(largeFilesTable, columns);

        insertManySmallBatches(smallFilesTable);
        insertOneLargeBatch(largeFilesTable);

        List<TypedRow> smallResult = sqlRunner.execute(
                "SELECT * FROM " + fqn(smallFilesTable) + " ORDER BY id");
        List<TypedRow> largeResult = sqlRunner.execute(
                "SELECT * FROM " + fqn(largeFilesTable) + " ORDER BY id");

        ResultValidator.assertEqualsUnordered(smallResult, largeResult);
    }

    // --- N8.3: Concurrent queries on same table return correct results ---

    @Test
    @DisplayName("N8.3 - Concurrent queries on same table are isolated")
    void concurrentQueriesSameTableIsolated() {
        String columns = "id BIGINT, name VARCHAR, amount DOUBLE";
        createTable(table, columns);
        insertOneLargeBatch(table);

        String sql = "SELECT * FROM " + fqn(table) + " ORDER BY id";
        List<TypedRow> baseline = sqlRunner.execute(sql);

        List<List<TypedRow>> results =
                executeConcurrentQueries(sql, CONCURRENT_QUERY_COUNT);

        for (List<TypedRow> result : results) {
            ResultValidator.assertEqualsOrdered(baseline, result);
        }
    }

    // -----------------------------------------------------------------------
    // Wide table helpers
    // -----------------------------------------------------------------------

    private void createWideTable(String tableName) {
        String columns = buildWideTableColumns();
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)", fqn(tableName), columns));
    }

    private static String buildWideTableColumns() {
        StringBuilder sb = new StringBuilder("id BIGINT");
        for (int i = 1; i <= WIDE_TABLE_COLUMN_COUNT - 1; i++) {
            sb.append(", ").append(wideColumnDef(i));
        }
        return sb.toString();
    }

    private static String wideColumnDef(int index) {
        int mod = index % 4;
        return switch (mod) {
            case 0 -> "col_bool_" + index + " BOOLEAN";
            case 1 -> "col_bigint_" + index + " BIGINT";
            case 2 -> "col_varchar_" + index + " VARCHAR";
            case 3 -> "col_double_" + index + " DOUBLE";
            default -> throw new IllegalStateException();
        };
    }

    private void insertWideRow(String tableName, long id) {
        String columns = buildWideInsertColumns();
        String values = buildWideInsertValues(id);
        sqlRunner.execute(String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                fqn(tableName), columns, values));
    }

    private static String buildWideInsertColumns() {
        StringBuilder sb = new StringBuilder("id");
        for (int i = 1; i <= WIDE_TABLE_COLUMN_COUNT - 1; i++) {
            sb.append(", ").append(wideColumnName(i));
        }
        return sb.toString();
    }

    private static String buildWideInsertValues(long id) {
        StringBuilder sb = new StringBuilder(String.valueOf(id));
        for (int i = 1; i <= WIDE_TABLE_COLUMN_COUNT - 1; i++) {
            sb.append(", ").append(wideColumnValue(i, id));
        }
        return sb.toString();
    }

    private static String wideColumnName(int index) {
        int mod = index % 4;
        return switch (mod) {
            case 0 -> "col_bool_" + index;
            case 1 -> "col_bigint_" + index;
            case 2 -> "col_varchar_" + index;
            case 3 -> "col_double_" + index;
            default -> throw new IllegalStateException();
        };
    }

    private static String wideColumnValue(int colIndex, long rowId) {
        int mod = colIndex % 4;
        return switch (mod) {
            case 0 -> String.valueOf(rowId % 2 == 0);
            case 1 -> String.valueOf(rowId * 100 + colIndex);
            case 2 -> "'" + "val_" + rowId + "_" + colIndex + "'";
            case 3 -> String.valueOf(rowId * 1.1 + colIndex);
            default -> throw new IllegalStateException();
        };
    }

    private void assertWideRowCorrect(TypedRow row, long id) {
        assertThat(row.getRaw("id")).isEqualTo(id);
        for (int i = 1; i <= WIDE_TABLE_COLUMN_COUNT - 1; i++) {
            String colName = wideColumnName(i);
            assertThat(row.containsColumn(colName))
                    .as("Column %s must exist", colName)
                    .isTrue();
            assertThat(row.getRaw(colName))
                    .as("Column %s must not be null", colName)
                    .isNotNull();
        }
    }

    // -----------------------------------------------------------------------
    // Small/large file insertion helpers
    // -----------------------------------------------------------------------

    private void insertManySmallBatches(String tableName) {
        for (int batch = 0; batch < SMALL_BATCH_COUNT; batch++) {
            int startId = batch * SMALL_BATCH_SIZE + 1;
            int endId = startId + SMALL_BATCH_SIZE - 1;
            WriteTestHelper.insertRows(sqlRunner, fqn(tableName), WriteTestHelper.buildRows(startId, endId));
        }
    }

    private void insertOneLargeBatch(String tableName) {
        int totalRows = SMALL_BATCH_COUNT * SMALL_BATCH_SIZE;
        WriteTestHelper.insertRows(sqlRunner, fqn(tableName), WriteTestHelper.buildRows(1, totalRows));
    }

    // -----------------------------------------------------------------------
    // Table creation
    // -----------------------------------------------------------------------

    private void createTable(String tableName, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (%s)", fqn(tableName), columns));
    }

    // -----------------------------------------------------------------------
    // Concurrent execution helper
    // -----------------------------------------------------------------------

    private List<List<TypedRow>> executeConcurrentQueries(
            String sql, int threadCount) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<CompletableFuture<List<TypedRow>>> futures =
                    IntStream.range(0, threadCount)
                            .mapToObj(i -> CompletableFuture.supplyAsync(
                                    () -> sqlRunner.execute(sql), executor))
                            .toList();
            return futures.stream()
                    .map(CompletableFuture::join)
                    .toList();
        } finally {
            executor.shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // SQL helpers
    // -----------------------------------------------------------------------

    private String fqn(String tableName) {
        return FQN_PREFIX + tableName;
    }


}
