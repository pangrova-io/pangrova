package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: Iceberg ORC format + v2 delete files (position + equality).
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * VeloxDwrfReader + RowDeleteFilter → transport → Client.
 *
 * <p>Existing delete tests ({@code RowDeleteByPositionTest}, {@code RowDeleteByKeyTest})
 * use Parquet (Iceberg default). This test verifies the same delete filter logic
 * works correctly when the underlying data file format is ORC, exercising the
 * VeloxDwrfReader → RowDeleteFilter integration path.
 *
 * <p>Requirements: 3.5, 1.1-1.5, 2.1-2.5
 */
@DisplayName("Iceberg ORC + Delete Files E2E Tests")
class IcebergOrcDeleteE2ETest extends IcebergDeleteTestBase {

    private static final String TABLE_COLUMNS = "id BIGINT, name VARCHAR, amount DOUBLE";

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_orc_del_" + method + "_" + System.nanoTime();
    }

    // --- ORC + Position Delete ---

    @Test
    @DisplayName("ORC position delete: deleted rows excluded from scan")
    void orcPositionDeleteExcludesRows() {
        createOrcIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (2, 5, 8)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        assertThat(rows).hasSize(7);
        assertThat(extractIds(rows)).containsExactly(1L, 3L, 4L, 6L, 7L, 9L, 10L);
    }

    // --- ORC + Equality Delete ---

    @Test
    @DisplayName("ORC equality delete: matching key rows excluded from scan")
    void orcEqualityDeleteExcludesMatchingKeys() {
        createOrcIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 10));

        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount = 300.0");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // id=3 has amount=300.0 → deleted
        assertThat(rows).hasSize(9);
        assertThat(extractIds(rows)).doesNotContain(3L);
    }

    // --- ORC + Mixed Deletes (position + equality across snapshots) ---

    @Test
    @DisplayName("ORC mixed deletes: position + equality across snapshots")
    void orcMixedDeletesAcrossSnapshots() {
        createOrcIcebergV2Table(tableName, TABLE_COLUMNS);
        insertRows(tableName, buildRows(1, 20));

        // Snapshot 1: position delete (by id)
        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE id IN (1, 5, 10)");
        // Snapshot 2: equality delete (by value predicate)
        sqlRunner.execute("DELETE FROM " + fqn(tableName) + " WHERE amount >= 1800.0");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(tableName) + " ORDER BY id");

        // Deleted by position: {1, 5, 10}
        // Deleted by equality (amount >= 1800): {18, 19, 20}
        // Remaining: {2,3,4,6,7,8,9,11,12,13,14,15,16,17} = 14 rows
        assertThat(rows).hasSize(14);
        assertThat(extractIds(rows)).containsExactly(
                2L, 3L, 4L, 6L, 7L, 8L, 9L, 11L, 12L, 13L, 14L, 15L, 16L, 17L);
    }

    // -----------------------------------------------------------------------
    // ORC table creation
    // -----------------------------------------------------------------------

    /**
     * Creates an Iceberg v2 table with ORC format and merge-on-read delete mode.
     */
    private void createOrcIcebergV2Table(String table, String columns) {
        String ddl = String.format(
                "CREATE TABLE %s%s (%s) TBLPROPERTIES ("
                        + "'format-version'='2', "
                        + "'write.delete.mode'='merge-on-read', "
                        + "'write.format.default'='orc')",
                FQN_PREFIX, table, columns);
        sqlRunner.executeDdl(ddl);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<TypedRow> buildRows(int startId, int endId) {
        return LongStream.rangeClosed(startId, endId)
                .mapToObj(id -> TypedRow.builder()
                        .putRaw("id", id)
                        .putRaw("name", "item_" + id)
                        .putRaw("amount", (double) (id * 100))
                        .build())
                .collect(Collectors.toList());
    }

    private static List<Long> extractIds(List<TypedRow> rows) {
        return rows.stream()
                .map(r -> ((Number) r.getRaw("id")).longValue())
                .collect(Collectors.toList());
    }
}
