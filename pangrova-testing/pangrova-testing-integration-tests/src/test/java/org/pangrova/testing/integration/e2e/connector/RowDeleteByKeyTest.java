package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
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
 * E2E tests for Iceberg v2 equality delete filtering through the real pipeline.
 *
 * <p>Each test creates an Iceberg v2 table with merge-on-read mode, performs
 * INSERT → DELETE WHERE col = val → SELECT, and verifies that EqualityDeleteFilter
 * in the native layer correctly excludes matching rows.
 *
 * <p>Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
 */
@DisplayName("Iceberg Equality Delete - Row Delete By Key E2E Tests")
class RowDeleteByKeyTest extends IcebergDeleteTestBase {

    private static final String TABLE_COLUMNS = "id BIGINT, name VARCHAR, amount DOUBLE";

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_eq_del_" + method + "_" + System.nanoTime();
    }

    // --- N2.1: INSERT → DELETE WHERE col = val → SELECT * → verify removed ---

    @Test
    @DisplayName("N2.1 - Delete by business key then query returns only non-matching rows")
    void deleteByBusinessKeyThenQuery() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount = 500.0");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // Row with id=5 has amount=500.0 → deleted
        assertThat(rows).hasSize(9);
        assertThat(extractIds(rows)).doesNotContain(5L);
    }

    // --- N2.2: Multiple DELETE with overlapping keys → SELECT * → verify union ---

    @Test
    @DisplayName("N2.2 - Overlapping key deletes then query verifies union of deleted keys")
    void overlappingKeyDeletesThenQuery() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount IN (100.0, 200.0, 300.0)");
        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount IN (200.0, 300.0, 400.0)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // Union of deleted: amount in {100,200,300,400} → ids {1,2,3,4}
        Set<Long> expectedIds = Set.of(5L, 6L, 7L, 8L, 9L, 10L);
        assertThat(rows).hasSize(6);
        assertThat(extractIds(rows)).containsExactlyElementsOf(
                expectedIds.stream().sorted().collect(Collectors.toList()));
    }

    // --- N2.3: DELETE WHERE col1 = v1 AND col2 = v2 → only full-match removed ---

    @Test
    @DisplayName("N2.3 - Composite key delete removes only full-match rows")
    void compositeKeyDeleteOnlyFullMatch() {
        String compositeColumns = "id BIGINT, category VARCHAR, region VARCHAR, value DOUBLE";
        createIcebergV2Table(tableName, compositeColumns);

        insertRows(tableName, List.of(
                TypedRow.builder().putRaw("id", 1L).putRaw("category", "electronics").putRaw("region", "east").putRaw("value", 10.0).build(),
                TypedRow.builder().putRaw("id", 2L).putRaw("category", "electronics").putRaw("region", "west").putRaw("value", 20.0).build(),
                TypedRow.builder().putRaw("id", 3L).putRaw("category", "clothing").putRaw("region", "east").putRaw("value", 30.0).build(),
                TypedRow.builder().putRaw("id", 4L).putRaw("category", "clothing").putRaw("region", "west").putRaw("value", 40.0).build(),
                TypedRow.builder().putRaw("id", 5L).putRaw("category", "electronics").putRaw("region", "east").putRaw("value", 50.0).build()));

        sqlRunner.execute("DELETE FROM " + fqn(tableName)
                + " WHERE category = 'electronics' AND region = 'east'");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // Only full-match (electronics + east) removed: ids 1, 5
        // Partial matches retained: id 2 (electronics+west), id 3 (clothing+east)
        assertThat(rows).hasSize(3);
        assertThat(extractIds(rows)).containsExactly(2L, 3L, 4L);
    }

    // --- N2.4: DELETE → INSERT new data → SELECT * → new data not affected ---

    @Test
    @DisplayName("N2.4 - New insert not affected by old delete files")
    void newInsertNotAffectedByOldDelete() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 5));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount = 300.0");

        // Insert new row with same amount value as deleted row
        insertRows(tableName, List.of(
                TypedRow.builder().putRaw("id", 100L).putRaw("name", "new_order_100").putRaw("amount", 300.0).build()));

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // Original id=3 (amount=300) deleted, but new id=100 (amount=300) retained
        assertThat(rows).hasSize(5);
        assertThat(extractIds(rows)).containsExactly(1L, 2L, 4L, 5L, 100L);
    }

    // --- N2.5: DELETE WHERE varchar_col = 'value' → verify VARCHAR matching ---

    @Test
    @DisplayName("N2.5 - VARCHAR key delete matches correctly")
    void varcharKeyDeleteMatching() {
        createIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName)
                + " WHERE name = 'order_3'");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        assertThat(rows).hasSize(9);
        assertThat(extractIds(rows)).doesNotContain(3L);
        assertThat(extractIds(rows)).containsExactly(
                1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<TypedRow> buildRows(int startId, int endId) {
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
