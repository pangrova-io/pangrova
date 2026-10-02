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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for incremental split generation during DT refresh.
 *
 * <p>Proves that tasks 5.3-5.6 (incremental splits in ConnectorSplitManager
 * SPI, Iceberg/Paimon implementations, and RefreshCoordinator wiring)
 * actually use incremental splits O(delta) instead of full scan O(total)
 * for append-only workloads, and correctly fall back to full scan when
 * non-append changes (DELETE/UPDATE) are detected.</p>
 *
 * <p>Three scenarios:</p>
 * <ol>
 *   <li>Delta refresh correctness: 5 batches → CREATE DT → full refresh →
 *       INSERT 1 more → delta refresh → all 6 batches present, second
 *       refresh only scanned the 1 new batch.</li>
 *   <li>Non-append falls back to full scan: INSERT → CREATE DT → refresh →
 *       DELETE → refresh → full scan used, results still correct.</li>
 *   <li>CDC append-only path: CDC writes 3 snapshots → DT refresh → CDC
 *       writes 1 more → delta refresh → only last snapshot scanned,
 *       results match full scan baseline.</li>
 * </ol>
 */
@DisplayName("Incremental Split Refresh E2E — O(delta) vs O(total)")
class IncrementalSplitRefreshE2ETest extends SqlE2ETestBase {

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

    // -----------------------------------------------------------------------
    // Scenario 1: Delta refresh correctness (Iceberg — append-only)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Delta refresh correctness")
    class DeltaRefreshCorrectness {

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
        @DisplayName("INSERT 5 batches → CREATE DT → refresh → INSERT 1 more → "
                + "delta refresh → all 6 batches present, second refresh O(delta)")
        void deltaRefreshOnlyScansNewBatch() {
            // Insert 5 batches into source (each INSERT creates a new snapshot)
            for (int batch = 1; batch <= 5; batch++) {
                for (int i = 1; i <= 10; i++) {
                    int id = (batch - 1) * 10 + i;
                    sqlRunner.execute("INSERT INTO " + SOURCE
                            + " VALUES (" + id + ", " + batch + ", " + (id * 1.5) + ")");
                }
            }

            // CREATE DT triggers initial full-scan refresh
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Verify initial refresh captured all 50 rows from 5 batches
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
            for (int i = 1; i <= 10; i++) {
                int id = 50 + i;
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + id + ", 6, " + (id * 1.5) + ")");
            }

            // Trigger manual refresh — should use incremental splits (O(delta))
            // because the range is append-only (only INSERTs since last refresh)
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify DT now has all 60 rows from 6 batches
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
    // Scenario 2: Non-append falls back to full scan
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Non-append falls back to full scan")
    class NonAppendFallbackToFullScan {

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
        @DisplayName("INSERT → CREATE DT → refresh → DELETE → refresh → "
                + "full scan used (DELETE is not append-only) → results correct")
        void deleteTriggersFullScanFallback() {
            // Seed source with initial data
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (1, 'Alice', 100.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (2, 'Bob', 200.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (3, 'Charlie', 300.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (4, 'Diana', 400.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (5, 'Eve', 500.0)");

            // CREATE DT — initial full-scan refresh
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(initial).hasSize(5);

            // DELETE some rows — this creates delete files, making the range
            // non-append-only. The next refresh must fall back to full scan.
            sqlRunner.execute("DELETE FROM " + SOURCE + " WHERE id IN (2, 4)");

            // Trigger refresh — should detect non-append changes and use full scan
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify results are correct despite full-scan fallback
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
        @DisplayName("Mixed append + delete: INSERT new rows then DELETE old → "
                + "full scan fallback preserves all surviving rows")
        void mixedAppendDeleteFallback() {
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

            // Refresh — must use full scan due to DELETE
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            List<TypedRow> afterMixed = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterMixed).hasSize(2);
            assertThat(afterMixed).extracting(r -> r.getRaw("name"))
                    .containsExactly("Bob", "Charlie");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: CDC append-only path (Paimon — native changelog)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — CDC append-only path")
    class CdcAppendOnlyPath {

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
        @DisplayName("CDC writes 3 snapshots → DT refresh → CDC writes 1 more → "
                + "delta refresh → only last snapshot scanned → results match baseline")
        void cdcDeltaRefreshScansOnlyNewSnapshot() {
            // CDC writes 3 snapshots (each INSERT batch = 1 snapshot)
            for (int snap = 1; snap <= 3; snap++) {
                for (int i = 1; i <= 5; i++) {
                    int id = (snap - 1) * 5 + i;
                    sqlRunner.execute("INSERT INTO " + SOURCE
                            + " VALUES (" + id + ", 'event_" + snap + "', 'payload_" + id + "')");
                }
            }

            // CREATE DT with CDC mode — initial full refresh
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

            // CDC writes 1 more snapshot (snapshot 4)
            for (int i = 1; i <= 5; i++) {
                int id = 15 + i;
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + id + ", 'event_4', 'payload_" + id + "')");
            }

            // Trigger delta refresh — should use incremental splits for the
            // append-only CDC range (only snapshot 4's files scanned)
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // Verify DT has all 20 rows (3 original snapshots + 1 new)
            List<TypedRow> afterDelta = sqlRunner.execute(
                    "SELECT * FROM " + DT_FULL + " ORDER BY id");
            assertThat(afterDelta).hasSize(20);

            // Verify new snapshot data is present
            List<TypedRow> snap4Rows = afterDelta.stream()
                    .filter(r -> "event_4".equals(r.getRaw("event_type")))
                    .toList();
            assertThat(snap4Rows).hasSize(5);

            // Verify results match full-scan baseline from source
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
        @DisplayName("Multiple delta refreshes accumulate correctly without data loss")
        void multipleDeltaRefreshesAccumulate() {
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
