package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Iceberg v2 position delete filtering through the real pipeline.
 *
 * <p>Each test creates an Iceberg v2 table with merge-on-read mode, performs
 * INSERT → DELETE → SELECT, and verifies that PositionDeleteFilter in the
 * native layer correctly excludes deleted rows.
 *
 * <p>Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
 */
@DisplayName("Iceberg Position Delete - Row Delete By Position E2E Tests")
class RowDeleteByPositionTest extends IcebergDeleteTestBase {

    private static final String TABLE_COLUMNS = "id BIGINT, name VARCHAR, amount DOUBLE";

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_pos_del_" + method + "_" + System.nanoTime();
    }

    // --- N1.1: INSERT 10 rows → DELETE 3 → SELECT * → verify 7 remain ---

    @Test
    @DisplayName("N1.1 - Delete partial orders then query returns only remaining rows")
    void deletePartialOrdersThenQuery() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildOrderRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (2, 5, 8)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        Set<Long> expectedIds = Set.of(1L, 3L, 4L, 6L, 7L, 9L, 10L);
        assertThat(rows).hasSize(7);
        assertThat(extractIds(rows)).containsExactlyElementsOf(
                expectedIds.stream().sorted().collect(Collectors.toList()));
    }

    // --- N1.2: INSERT 100 rows → 2 DELETE statements → SELECT * → verify union ---

    @Test
    @DisplayName("N1.2 - Batch delete with two statements then query verifies union")
    void batchDeleteThenQuery() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildOrderRows(1, 100));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id <= 10");
        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id >= 91");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        assertThat(rows).hasSize(80);
        List<Long> ids = extractIds(rows);
        assertThat(ids.get(0)).isEqualTo(11L);
        assertThat(ids.get(ids.size() - 1)).isEqualTo(90L);
    }

    // --- N1.3: DELETE targeting non-existent rows → SELECT * → all valid rows ---

    @Test
    @DisplayName("N1.3 - Delete non-existent rows is silently ignored")
    void deleteNonExistentRowsIgnored() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildOrderRows(1, 5));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (99, 100, 200)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        assertThat(rows).hasSize(5);
        assertThat(extractIds(rows)).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    // --- N1.4: INSERT → DELETE → SELECT WHERE predicate → verify intersection ---

    @Test
    @DisplayName("N1.4 - Delete with predicate intersection filters correctly")
    void deleteWithPredicateIntersection() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildOrderRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (3, 6, 9)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " WHERE amount > 500.0 ORDER BY id");

        // Rows with amount > 500: id 6,7,8,9,10 (amount = id * 100)
        // After delete of 6,9: remaining with amount > 500 = 7,8,10
        assertThat(rows).hasSize(3);
        assertThat(extractIds(rows)).containsExactly(7L, 8L, 10L);
    }

    // --- N1.5: INSERT → DELETE → SELECT ORDER BY + LIMIT → verify pagination ---

    @Test
    @DisplayName("N1.5 - Delete then paginate remaining rows correctly")
    void deleteThenPaginateRemaining() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildOrderRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (1, 3, 5, 7, 9)");

        List<TypedRow> page = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id LIMIT 3");

        // Remaining: 2,4,6,8,10 → first 3 = 2,4,6
        assertThat(page).hasSize(3);
        assertThat(extractIds(page)).containsExactly(2L, 4L, 6L);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<TypedRow> buildOrderRows(int startId, int endId) {
        return LongStream.rangeClosed(startId, endId)
                .mapToObj(id -> TypedRow.builder().putRaw("id", id).putRaw("name", "order_" + id).putRaw("amount", (double) (id * 100)).build())
                .collect(Collectors.toList());
    }

    private static List<Long> extractIds(List<TypedRow> rows) {
        return rows.stream()
                .map(r -> ((Number) r.getRaw("id")).longValue())
                .collect(Collectors.toList());
    }
}
