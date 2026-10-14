package org.pangrova.testing.e2e.streaming;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for the test harness's Dynamic Table read rewrite.
 *
 * <p>The E2E harness installs a refresh executor that points reads at the
 * defining source query. These assertions check SQL rewrite and source-row
 * visibility across changes. They do not exercise a materialized refresh,
 * physical split selection, or {@code RefreshCoordinator}.</p>
 *
 * <p>Three scenarios:</p>
 * <ol>
 *   <li>Append-only source changes remain visible through a rewritten read.</li>
 *   <li>Deleted source rows disappear from a rewritten read.</li>
 *   <li>CDC-mode declarations use the same harness rewrite.</li>
 * </ol>
 */
@DisplayName("Dynamic Table read rewrite against changing source data")
class DynamicTableReadRewriteE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PAIMON_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private static void insertIcebergBatch(String source, int batch) {
        StringJoiner values = new StringJoiner(", ");
        for (int i = 1; i <= 10; i++) {
            int id = (batch - 1) * 10 + i;
            values.add("(" + id + ", " + batch + ", " + (id * 1.5) + ")");
        }
        sqlRunner.execute("INSERT INTO " + source + " VALUES " + values);
    }

    private static void insertCdcSnapshot(String source, int snapshot) {
        StringJoiner values = new StringJoiner(", ");
        for (int i = 1; i <= 5; i++) {
            int id = (snapshot - 1) * 5 + i;
            values.add("(" + id + ", 'event_" + snapshot + "', 'payload_" + id + "')");
        }
        sqlRunner.execute("INSERT INTO " + source + " VALUES " + values);
    }

    // -----------------------------------------------------------------------
    // Scenario 1: Iceberg source rewrite after append-only changes
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Iceberg source rewrite")
    class IcebergSourceRewrite {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".incr_delta_source";
        private static final String DT_FULL = ICE_CATALOG + "." + SCHEMA + ".incr_delta_dt";

        @BeforeAll
        static void createSource() {
            isDockerReady();
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, batch_num INT, value DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT_FULL);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, batch_num INT, value DOUBLE)");
        }

        @Test
        @DisplayName("Source reads show all 6 Iceberg batches after append")
        void rewriteIncludesNewBatch() {
            // Insert 5 batches into source (one INSERT/snapshot per batch).
            for (int batch = 1; batch <= 5; batch++) {
                insertIcebergBatch(SOURCE, batch);
            }

            // The E2E harness registers a source-query rewrite on CREATE DT.
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Verify the rewrite exposes all 50 source rows.
            List<TypedRow> initialRows = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(initialRows).hasSize(50);

            // Verify all 5 batch numbers present
            List<Object> initialBatches = initialRows.stream()
                    .map(r -> r.getRaw("batch_num"))
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(initialBatches).containsExactly(1, 2, 3, 4, 5);

            // INSERT 1 more batch (batch 6) — creates a new snapshot
            insertIcebergBatch(SOURCE, 6);

            // The fixture is a live source rewrite: the new rows are visible
            // before the explicit refresh command.
            assertThat(sqlRunner.execute("SELECT * FROM " + DT_FULL)).hasSize(60);

            // Exercise the SQL refresh command; the fixture keeps the source rewrite.
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify the rewritten read now shows all 60 source rows.
            List<TypedRow> afterRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterRefresh).hasSize(60);

            // Verify all 6 batch numbers present
            List<Object> allBatches = afterRefresh.stream()
                    .map(r -> r.getRaw("batch_num"))
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(allBatches).containsExactly(1, 2, 3, 4, 5, 6);

            // Verify batch 6 data is correct
            List<TypedRow> batch6Rows = afterRefresh.stream()
                    .filter(r -> Integer.valueOf(6).equals(((Number) r.getRaw("batch_num")).intValue()))
                    .toList();
            assertThat(batch6Rows).hasSize(10);

            // Verify data integrity: SUM of all values matches expected
            double expectedSum = 0;
            for (int id = 1; id <= 60; id++) {
                expectedSum += id * 1.5;
            }
            double actualSum = afterRefresh.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("value")).doubleValue())
                    .sum();
            assertThat(actualSum).isEqualTo(expectedSum);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Iceberg source rewrite after non-append changes
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Iceberg source rewrite after deletes")
    class IcebergDeleteRewrite {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".incr_fallback_source";
        private static final String DT_FULL = ICE_CATALOG + "." + SCHEMA + ".incr_fallback_dt";

        @BeforeAll
        static void createSource() {
            isDockerReady();
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, name VARCHAR(100), amount DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT_FULL);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, name VARCHAR(100), amount DOUBLE)");
        }

        @Test
        @DisplayName("Source reads omit deleted rows")
        void rewriteOmitsDeletedRows() {
            // Seed source with initial data
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (1, 'Alice', 100.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (2, 'Bob', 200.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (3, 'Charlie', 300.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (4, 'Diana', 400.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (5, 'Eve', 500.0)");

            // CREATE DT installs the fixture's source-query rewrite.
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(initial).hasSize(5);

            // DELETE some source rows; the rewritten read must reflect them.
            sqlRunner.execute("DELETE FROM " + SOURCE + " WHERE id IN (2, 4)");

            // Exercise the SQL refresh command after a non-append source change.
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify results reflect deleted rows.
            List<TypedRow> afterDelete = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterDelete).hasSize(3);
            assertThat(afterDelete).extracting(r -> r.getRaw("name"))
                    .containsExactly("Alice", "Charlie", "Eve");

            // Verify amounts are correct
            double totalAmount = afterDelete.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("amount")).doubleValue())
                    .sum();
            assertThat(totalAmount).isEqualTo(100.0 + 300.0 + 500.0);
        }

        @Test
        @DisplayName("Source reads show appended rows and omit deleted rows")
        void rewritePreservesSurvivorsAfterMixedChanges() {
            // Initial data
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (1, 'Alice', 100.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (2, 'Bob', 200.0)");

            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(initial).hasSize(2);

            // Mixed changes: INSERT new + DELETE old
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (3, 'Charlie', 300.0)");
            sqlRunner.execute("DELETE FROM " + SOURCE + " WHERE id = 1");

            // Exercise the SQL refresh command after mixed source changes.
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            List<TypedRow> afterMixed = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterMixed).hasSize(2);
            assertThat(afterMixed).extracting(r -> r.getRaw("name"))
                    .containsExactly("Bob", "Charlie");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Paimon source rewrite with a CDC-mode declaration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — Paimon source rewrite in CDC mode")
    class PaimonCdcModeRewrite {

        private static final String SOURCE = PAIMON_CATALOG + "." + SCHEMA + ".incr_cdc_source";
        private static final String DT_FULL = PAIMON_CATALOG + "." + SCHEMA + ".incr_cdc_dt";

        @BeforeAll
        static void createSource() {
            isDockerReady();
            cdcAvailableFlag = true;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.paimon(
                            PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, event_type VARCHAR(50), payload VARCHAR(200))");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT_FULL);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, event_type VARCHAR(50), payload VARCHAR(200))");
        }

        @Test
        @DisplayName("Rewritten read includes rows from a fourth source snapshot")
        void rewriteIncludesFourthSnapshot() {
            // Write 3 Paimon source snapshots (one INSERT per batch).
            for (int snap = 1; snap <= 3; snap++) {
                insertCdcSnapshot(SOURCE, snap);
            }

            // CREATE DT with CDC mode installs the fixture's source rewrite.
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' REFRESH_MODE = CDC "
                    + "AS SELECT * FROM " + SOURCE);

            // Verify initial state: 15 rows from 3 snapshots
            List<TypedRow> initial = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(initial).hasSize(15);

            // Capture full-scan baseline for comparison
            List<TypedRow> baselineBeforeDelta = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(baselineBeforeDelta).hasSize(15);

            // Write one more Paimon source snapshot (snapshot 4).
            insertCdcSnapshot(SOURCE, 4);

            // Exercise the SQL refresh command after the fourth source snapshot.
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify the rewritten read shows all 20 source rows.
            List<TypedRow> afterDelta = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterDelta).hasSize(20);

            // Verify new snapshot data is present
            List<TypedRow> snap4Rows = afterDelta.stream()
                    .filter(r -> "event_4".equals(r.getRaw("event_type")))
                    .toList();
            assertThat(snap4Rows).hasSize(5);

            // Verify results match a direct source read.
            List<TypedRow> fullScanBaseline = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(afterDelta).hasSameSizeAs(fullScanBaseline);

            // Verify data integrity: every row in DT matches source
            for (int i = 0; i < afterDelta.size(); i++) {
                TypedRow dtRow = afterDelta.get(i);
                TypedRow srcRow = fullScanBaseline.get(i);
                assertThat(dtRow.getRaw("id")).isEqualTo(srcRow.getRaw("id"));
                assertThat(dtRow.getRaw("event_type")).isEqualTo(srcRow.getRaw("event_type"));
                assertThat(dtRow.getRaw("payload")).isEqualTo(srcRow.getRaw("payload"));
            }
        }

        @Test
        @DisplayName("Repeated reads expose successive source changes")
        void rewrittenReadsExposeSuccessiveChanges() {
            // Initial snapshot
            sqlRunner.execute("INSERT INTO " + SOURCE
                    + " VALUES (1, 'init', 'first_event')");

            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' REFRESH_MODE = CDC "
                    + "AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(initial).hasSize(1);

            // Delta 1: add 2 rows
            sqlRunner.execute("INSERT INTO " + SOURCE
                    + " VALUES (2, 'delta_1', 'second_event')");
            sqlRunner.execute("INSERT INTO " + SOURCE
                    + " VALUES (3, 'delta_1', 'third_event')");
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            List<TypedRow> afterDelta1 = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterDelta1).hasSize(3);

            // Delta 2: add 2 more rows
            sqlRunner.execute("INSERT INTO " + SOURCE
                    + " VALUES (4, 'delta_2', 'fourth_event')");
            sqlRunner.execute("INSERT INTO " + SOURCE
                    + " VALUES (5, 'delta_2', 'fifth_event')");
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            List<TypedRow> afterDelta2 = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterDelta2).hasSize(5);

            // Verify all event types present
            assertThat(afterDelta2).extracting(r -> r.getRaw("event_type"))
                    .containsExactly("init", "delta_1", "delta_1", "delta_2", "delta_2");

            // Verify matches source
            List<TypedRow> source = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(afterDelta2).hasSameSizeAs(source);
        }
    }
}
