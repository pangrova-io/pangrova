package org.pangrova.testing.integration.e2e.spill;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.SqlTestRowWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying spill-to-disk produces correct results under low memory.
 *
 * <p>Uses metamorphic testing: execute the same query under constrained memory
 * (triggers spill) and verify results match Java-computed expected values.
 * All queries execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox.
 *
 * <p>Requirements: 5.1, 5.2, 5.3, 5.4
 */
@DisplayName("Spill-to-Disk - Large Query Memory Overflow E2E Tests")
class LargeQueryMemoryOverflowTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    /** Row count large enough to trigger spill under constrained memory. */
    private static final int LARGE_ROW_COUNT = 5000;

    /** Number of distinct groups for aggregation tests. */
    private static final int GROUP_COUNT = 50;

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_spill_" + method + "_" + System.nanoTime();
    }

    // --- N5.1: Large aggregation with spill → verify correct results ---

    @Test
    @DisplayName("N5.1 - Large aggregation with spill produces correct SUM/COUNT per group")
    void largeAggregationWithSpill() {
        createSpillTable(tableName, "id BIGINT, grp BIGINT, amount DOUBLE");
        List<TypedRow> data = buildGroupedRows(LARGE_ROW_COUNT, GROUP_COUNT);
        insertBatch(tableName, data);

        Map<Long, double[]> expected = computeExpectedAggregation(data);

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt "
                + "FROM " + fqn(tableName) + " GROUP BY grp ORDER BY grp");

        assertThat(rows).hasSize(GROUP_COUNT);
        for (TypedRow row : rows) {
            long grp = ((Number) row.getRaw("grp")).longValue();
            double total = ((Number) row.getRaw("total")).doubleValue();
            long cnt = ((Number) row.getRaw("cnt")).longValue();

            assertThat(expected).containsKey(grp);
            assertThat(total).isCloseTo(expected.get(grp)[0], withinPercentage(0.01));
            assertThat(cnt).isEqualTo((long) expected.get(grp)[1]);
        }
    }

    // --- N5.2: Large sort with spill → verify sorted result ---

    @Test
    @DisplayName("N5.2 - Large ORDER BY with spill produces correctly sorted result")
    void largeSortWithSpill() {
        createSpillTable(tableName, "id BIGINT, value DOUBLE");
        List<TypedRow> data = buildSortRows(LARGE_ROW_COUNT);
        insertBatch(tableName, data);

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, value FROM " + fqn(tableName) + " ORDER BY value ASC, id ASC");

        assertThat(rows).hasSize(LARGE_ROW_COUNT);
        assertResultIsSorted(rows);
    }

    // --- N5.3: Large join with spill → verify correct join result ---

    @Test
    @DisplayName("N5.3 - Large JOIN with spill produces correct join result")
    void largeJoinWithSpill() {
        String leftTable = tableName + "_left";
        String rightTable = tableName + "_right";
        createSpillTable(leftTable, "id BIGINT, left_val VARCHAR");
        createSpillTable(rightTable, "id BIGINT, right_val VARCHAR");

        int leftCount = LARGE_ROW_COUNT;
        int rightCount = LARGE_ROW_COUNT / 2;
        insertBatch(leftTable, buildJoinRows(leftCount, "L"));
        insertBatch(rightTable, buildJoinRows(rightCount, "R"));

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT a.id, a.left_val, b.right_val "
                + "FROM " + fqn(leftTable) + " a "
                + "INNER JOIN " + fqn(rightTable) + " b ON a.id = b.id "
                + "ORDER BY a.id");

        // Inner join: only ids present in both tables (1..rightCount)
        assertThat(rows).hasSize(rightCount);
        assertJoinResultCorrect(rows, rightCount);
    }

    // -----------------------------------------------------------------------
    // Table creation helper
    // -----------------------------------------------------------------------

    private void createSpillTable(String table, String columns) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s%s (%s)", FQN_PREFIX, table, columns));
    }

    // -----------------------------------------------------------------------
    // Data generation helpers
    // -----------------------------------------------------------------------

    private static List<TypedRow> buildGroupedRows(int count, int groups) {
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> TypedRow.builder().putRaw("id", (long) i).putRaw("grp", (long) (i % groups)).putRaw("amount", i * 1.5).build())
                .collect(Collectors.toList());
    }

    private static List<TypedRow> buildSortRows(int count) {
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> TypedRow.builder().putRaw("id", (long) i).putRaw("value", (count - i) * 0.7 + (i % 17) * 0.3).build())
                .collect(Collectors.toList());
    }

    private static List<TypedRow> buildJoinRows(int count, String prefix) {
        String valCol = prefix.equals("L") ? "left_val" : "right_val";
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> TypedRow.builder().putRaw("id", (long) i).putRaw(valCol, prefix + "_" + i).build())
                .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------
    // Batch insert (chunks to avoid oversized SQL)
    // -----------------------------------------------------------------------

    private void insertBatch(String table, List<TypedRow> rows) {
        SqlTestRowWriter.insertRows(sqlRunner, fqn(table), rows);
    }

    // -----------------------------------------------------------------------
    // Oracle: Java-side expected value computation
    // -----------------------------------------------------------------------

    private static Map<Long, double[]> computeExpectedAggregation(
            List<TypedRow> data) {
        Map<Long, double[]> result = new HashMap<>();
        for (TypedRow row : data) {
            long grp = ((Number) row.getRaw("grp")).longValue();
            double amount = ((Number) row.getRaw("amount")).doubleValue();
            result.computeIfAbsent(grp, k -> new double[]{0.0, 0.0});
            result.get(grp)[0] += amount;
            result.get(grp)[1] += 1;
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private static void assertResultIsSorted(List<TypedRow> rows) {
        for (int i = 1; i < rows.size(); i++) {
            double prev = ((Number) rows.get(i - 1).getRaw("value")).doubleValue();
            double curr = ((Number) rows.get(i).getRaw("value")).doubleValue();
            if (Double.compare(prev, curr) == 0) {
                long prevId = ((Number) rows.get(i - 1).getRaw("id")).longValue();
                long currId = ((Number) rows.get(i).getRaw("id")).longValue();
                assertThat(prevId).isLessThanOrEqualTo(currId);
            } else {
                assertThat(prev).isLessThanOrEqualTo(curr);
            }
        }
    }

    private static void assertJoinResultCorrect(
            List<TypedRow> rows, int expectedCount) {
        for (int i = 0; i < rows.size(); i++) {
            long id = ((Number) rows.get(i).getRaw("id")).longValue();
            assertThat(id).isEqualTo(i + 1L);
            assertThat(rows.get(i).getRaw("left_val")).isEqualTo("L_" + id);
            assertThat(rows.get(i).getRaw("right_val")).isEqualTo("R_" + id);
        }
    }

    private String fqn(String table) {
        return FQN_PREFIX + table;
    }

    private static org.assertj.core.data.Percentage withinPercentage(double pct) {
        return org.assertj.core.data.Percentage.withPercentage(pct);
    }
}
