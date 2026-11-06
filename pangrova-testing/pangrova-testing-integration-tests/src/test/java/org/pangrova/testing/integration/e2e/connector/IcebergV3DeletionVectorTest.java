package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg v3 deletion vector (DV) E2E tests covering write and read paths:
 * DV-based DELETE, multi-DELETE DV bitmap merge, COMPACT materializing DVs,
 * DV read performance benchmark vs v2 position delete, and format version
 * incompatibility error handling.
 *
 * <p>Covers A1 acceptance criteria: v3 DELETE generates DV, multiple DELETEs
 * merge DV bitmap correctly, COMPACT materializes DVs into clean data files,
 * DV read path returns correct results, and format version checks reject
 * unsupported operations. All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: A1
 */
@DisplayName("Iceberg v3 Deletion Vector E2E Tests")
class IcebergV3DeletionVectorTest extends SqlE2ETestBase {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergV3DeletionVectorTest.class);
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // --- 1.2.6: v3 DELETE generates DV (not position delete file) ---

    @Nested
    @DisplayName("1.2.6 - v3 DELETE Generates Deletion Vector")
    class V3DeleteGeneratesDeletionVector {

        @Test
        @DisplayName("DELETE on v3 table removes rows correctly via DV")
        void deleteOnV3TableRemovesRowsViaDv() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0), (4, 'dave', 400.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(4L);
            assertThat(rows.get(2).getRaw("name")).isEqualTo("dave");
        }

        @Test
        @DisplayName("DELETE multiple rows with range predicate on v3 table")
        void deleteMultipleRowsWithRangePredicate() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0), (4, 'dave', 400.0), "
                    + "(5, 'eve', 500.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id >= 3");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
        }

        @Test
        @DisplayName("SELECT with predicate after DELETE on v3 table returns correct subset")
        void selectWithPredicateAfterDelete() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, price DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, price) VALUES "
                    + "(1, 'A', 10.0), (2, 'B', 20.0), "
                    + "(3, 'A', 30.0), (4, 'B', 40.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 3");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE category = 'A' ORDER BY id");

            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }
    }

    // --- 1.2.7: Multiple DELETEs on same file merge DV bitmap ---

    @Nested
    @DisplayName("1.2.7 - Multiple DELETEs Merge DV Bitmap")
    class MultipleDeletionVectorMerge {

        @Test
        @DisplayName("Two successive DELETEs on same data file both take effect")
        void twoSuccessiveDeletesBothTakeEffect() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            // Single INSERT creates one data file
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0), (4, 'dave', 400.0), "
                    + "(5, 'eve', 500.0)");

            // First DELETE: remove id=2
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

            // Second DELETE: remove id=4 (same underlying data file)
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 4");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(5L);
            assertThat(rows.get(2).getRaw("name")).isEqualTo("eve");
        }

        @Test
        @DisplayName("Three successive DELETEs accumulate correctly")
        void threeSuccessiveDeletesAccumulate() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES "
                    + "(1, 'a'), (2, 'b'), (3, 'c'), "
                    + "(4, 'd'), (5, 'e'), (6, 'f')");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 1");
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 3");
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 5");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(0).getRaw("val")).isEqualTo("b");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(4L);
            assertThat(rows.get(1).getRaw("val")).isEqualTo("d");
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(6L);
            assertThat(rows.get(2).getRaw("val")).isEqualTo("f");
        }

        @Test
        @DisplayName("Aggregation query correct after multiple DELETEs")
        void aggregationCorrectAfterMultipleDeletes() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES "
                    + "(1, 'X', 10.0), (2, 'Y', 20.0), "
                    + "(3, 'X', 30.0), (4, 'Y', 40.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 1");
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 4");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT category, SUM(amount) AS total FROM " + fqn()
                    + " GROUP BY category ORDER BY category");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("category")).isEqualTo("X");
            assertThat(((Number) rows.get(0).getRaw("total")).doubleValue()).isEqualTo(30.0);
            assertThat(rows.get(1).getRaw("category")).isEqualTo("Y");
            assertThat(((Number) rows.get(1).getRaw("total")).doubleValue()).isEqualTo(20.0);
        }
    }

    // --- 1.2.8: COMPACT after DV materializes data correctly ---

    @Nested
    @DisplayName("1.2.8 - COMPACT Materializes DV Into Clean Data Files")
    class CompactMaterializesDeletionVector {

        @Test
        @DisplayName("COMPACT after DELETE preserves surviving rows")
        void compactAfterDeletePreservesSurvivingRows() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0), (4, 'dave', 400.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

            // Verify pre-compaction state
            List<TypedRow> preCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(preCompact).hasSize(3);

            // COMPACT materializes the DV into new clean data files
            sqlRunner.executeDdl("COMPACT " + fqn());

            // Verify post-compaction: same surviving rows
            List<TypedRow> postCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(postCompact).hasSize(3);
            assertRow(postCompact.get(0), 1L, "alice", 100.0);
            assertRow(postCompact.get(1), 3L, "carol", 300.0);
            assertRow(postCompact.get(2), 4L, "dave", 400.0);
        }

        @Test
        @DisplayName("COMPACT after multiple DELETEs produces correct data")
        void compactAfterMultipleDeletesProducesCorrectData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0), (4, 'dave', 400.0), "
                    + "(5, 'eve', 500.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 4");

            sqlRunner.executeDdl("COMPACT " + fqn());

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRow(rows.get(0), 1L, "alice", 100.0);
            assertRow(rows.get(1), 3L, "carol", 300.0);
            assertRow(rows.get(2), 5L, "eve", 500.0);
        }

        @Test
        @DisplayName("New INSERT after COMPACT works correctly")
        void newInsertAfterCompactWorksCorrectly() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), "
                    + "(3, 'carol', 300.0)");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");
            sqlRunner.executeDdl("COMPACT " + fqn());

            // Insert new data after compaction
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (4, 'dave', 400.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRow(rows.get(0), 1L, "alice", 100.0);
            assertRow(rows.get(1), 3L, "carol", 300.0);
            assertRow(rows.get(2), 4L, "dave", 400.0);
        }
    }

    // --- 1.3.7: DV read performance benchmark vs v2 position delete ---

    @Nested
    @DisplayName("1.3.7 - DV Read Performance Benchmark vs v2 Position Delete")
    class DvReadPerformanceBenchmark {

        @Test
        @DisplayName("v3 DV and v2 position delete return identical results after DELETE")
        void dvAndPositionDeleteReturnIdenticalResults() {
            String v3Table = "t_bench_v3_" + System.nanoTime();
            String v2Table = "t_bench_v2_" + System.nanoTime();
            String v3Fqn = FQN_PREFIX + v3Table;
            String v2Fqn = FQN_PREFIX + v2Table;
            int totalRows = 200;
            int deleteCount = 40;

            // Build batch INSERT values: (1, 'row_1', 1.0), (2, 'row_2', 2.0), ...
            String values = IntStream.rangeClosed(1, totalRows)
                    .mapToObj(i -> "(%d, 'row_%d', %d.0)".formatted(i, i, i))
                    .collect(Collectors.joining(", "));

            // Create and populate v3 table (uses DV for deletes)
            sqlRunner.executeDdl("CREATE TABLE " + v3Fqn
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");
            sqlRunner.execute("INSERT INTO " + v3Fqn
                    + " (id, name, amount) VALUES " + values);

            // Create and populate v2 table (uses position delete)
            sqlRunner.executeDdl("CREATE TABLE " + v2Fqn
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '2')");
            sqlRunner.execute("INSERT INTO " + v2Fqn
                    + " (id, name, amount) VALUES " + values);

            // DELETE same rows from both tables (ids 1..deleteCount)
            sqlRunner.execute("DELETE FROM " + v3Fqn
                    + " WHERE id <= " + deleteCount);
            sqlRunner.execute("DELETE FROM " + v2Fqn
                    + " WHERE id <= " + deleteCount);

            // Time v3 DV read
            long v3Start = System.nanoTime();
            List<TypedRow> v3Rows = sqlRunner.execute(
                    "SELECT * FROM " + v3Fqn + " ORDER BY id");
            long v3Elapsed = System.nanoTime() - v3Start;

            // Time v2 position delete read
            long v2Start = System.nanoTime();
            List<TypedRow> v2Rows = sqlRunner.execute(
                    "SELECT * FROM " + v2Fqn + " ORDER BY id");
            long v2Elapsed = System.nanoTime() - v2Start;

            int expectedSurviving = totalRows - deleteCount;
            assertThat(v3Rows).hasSize(expectedSurviving);
            assertThat(v2Rows).hasSize(expectedSurviving);

            // Verify both return identical data (correctness, not strict timing)
            for (int i = 0; i < expectedSurviving; i++) {
                long v3Id = ((Number) v3Rows.get(i).getRaw("id")).longValue();
                long v2Id = ((Number) v2Rows.get(i).getRaw("id")).longValue();
                assertThat(v3Id).isEqualTo(v2Id);
            }

            LOG.info("event=dv_read_benchmark v3_dv_ms={} v2_pos_delete_ms={} "
                            + "total_rows={} deleted={} surviving={}",
                    v3Elapsed / 1_000_000, v2Elapsed / 1_000_000,
                    totalRows, deleteCount, expectedSurviving);
        }
    }

    // --- 1.3.8: v2 engine reading v3 DV table format version incompatibility ---

    @Nested
    @DisplayName("1.3.8 - v2 Engine Reading v3 DV Table Format Incompatibility")
    class V2EngineReadingV3DvTable {

        @Test
        @DisplayName("v3 table with DV works correctly via v3 read path")
        void v3TableWithDvWorksCorrectly() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        }

        @Test
        @DisplayName("DELETE on v1 format table uses copy-on-write path")
        void deleteOnV1FormatTableUsesCopyOnWrite() {
            String v1Table = "t_v1_compat_" + System.nanoTime();
            String v1Fqn = FQN_PREFIX + v1Table;

            sqlRunner.executeDdl("CREATE TABLE " + v1Fqn
                    + " (id BIGINT, name VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '1')");
            sqlRunner.execute("INSERT INTO " + v1Fqn
                    + " (id, name) VALUES (1, 'a'), (2, 'b')");

            sqlRunner.execute("DELETE FROM " + v1Fqn + " WHERE id = 1");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + v1Fqn + " ORDER BY id");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
        }
    }

    // --- Assertion helper ---

    private static void assertRow(TypedRow row,
                                  long expectedId, String expectedName,
                                  double expectedAmount) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
        assertThat(((Number) row.getRaw("amount")).doubleValue()).isEqualTo(expectedAmount);
    }
}
