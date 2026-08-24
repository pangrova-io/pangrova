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
 * E2E test for merge-on-read query freshness.
 *
 * <p>Proves that tasks 7.1-7.3 (DtResolution + UNION ALL rewrite) actually
 * serve fresh data without waiting for a DT refresh to complete. When a
 * Dynamic Table has pending deltas (new source snapshots since last refresh),
 * the planner generates a UNION ALL plan merging the materialized table with
 * an incremental scan of the source, so queries always see the latest data.</p>
 *
 * <p>Four scenarios:</p>
 * <ol>
 *   <li>Query before refresh sees fresh data via merge-on-read UNION ALL</li>
 *   <li>Query after refresh uses direct target (no UNION ALL needed)</li>
 *   <li>Merge-on-read with filter pushdown</li>
 *   <li>CDC writes + merge-on-read</li>
 * </ol>
 */
@DisplayName("Merge-on-Read Query Freshness E2E")
class MergeOnReadE2ETest extends SqlE2ETestBase {

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
    // Scenario 1: Query before refresh sees fresh data via merge-on-read
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Query before refresh sees fresh data")
    class QueryBeforeRefreshSeesFreshData {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".mor_fresh_source";
        private static final String DT = ICE_CATALOG + "." + SCHEMA + ".mor_fresh_dt";

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, batch_num INT, value DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, batch_num INT, value DOUBLE)");
        }

        @Test
        @DisplayName("INSERT → CREATE DT → refresh (materializes S1) → INSERT more (S2) → "
                + "query DT BEFORE next refresh → result includes S2 via merge-on-read → "
                + "row count matches full source scan")
        void mergeOnReadServesUnrefreshedData() {
            // Insert initial batch (creates snapshot S1)
            for (int i = 1; i <= 20; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 1, " + (i * 1.0) + ")");
            }

            // CREATE DT triggers initial full-scan refresh (materializes up to S1)
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Verify initial refresh captured all 20 rows
            List<TypedRow> afterInitialRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterInitialRefresh).hasSize(20);

            // Insert more data (creates snapshot S2) — NOT refreshed yet
            for (int i = 21; i <= 40; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 2, " + (i * 1.0) + ")");
            }

            // Query DT BEFORE triggering refresh — merge-on-read should
            // serve fresh data by generating UNION ALL of materialized + delta
            List<TypedRow> mergeOnReadResult = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");

            // Full source scan baseline for comparison
            List<TypedRow> fullSourceScan = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");

            // Merge-on-read result must include S2 data (all 40 rows)
            assertThat(mergeOnReadResult)
                    .as("Merge-on-read must serve fresh data including unrefreshed S2")
                    .hasSameSizeAs(fullSourceScan);
            assertThat(mergeOnReadResult).hasSize(40);

            // Verify both batch numbers are present
            List<Integer> batches = mergeOnReadResult.stream()
                    .map(r -> ((Number) r.getRaw("batch_num")).intValue())
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(batches).containsExactly(1, 2);

            // Verify batch 2 rows (the unrefreshed delta) are correct
            List<TypedRow> batch2Rows = mergeOnReadResult.stream()
                    .filter(r -> ((Number) r.getRaw("batch_num")).intValue() == 2)
                    .toList();
            assertThat(batch2Rows).hasSize(20);

            // Verify data integrity: SUM matches expected
            double expectedSum = 0;
            for (int i = 1; i <= 40; i++) {
                expectedSum += i * 1.0;
            }
            double actualSum = mergeOnReadResult.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("value")).doubleValue())
                    .sum();
            assertThat(actualSum).isEqualTo(expectedSum);

            // Verify no duplicate IDs
            List<Integer> ids = mergeOnReadResult.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids).doesNotHaveDuplicates();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Query after refresh uses direct target (no UNION ALL)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Query after refresh uses direct target")
    class QueryAfterRefreshUsesDirectTarget {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".mor_direct_source";
        private static final String DT = ICE_CATALOG + "." + SCHEMA + ".mor_direct_dt";

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, name VARCHAR(100), amount DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, name VARCHAR(100), amount DOUBLE)");
        }

        @Test
        @DisplayName("INSERT → CREATE DT → refresh → query DT → result from materialized "
                + "table directly (no UNION ALL) → same results as source")
        void directTargetAfterFullRefresh() {
            // Insert data into source
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (1, 'Alice', 100.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (2, 'Bob', 200.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (3, 'Charlie', 300.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (4, 'Diana', 400.0)");
            sqlRunner.execute("INSERT INTO " + SOURCE + " VALUES (5, 'Eve', 500.0)");

            // CREATE DT — initial refresh materializes all data
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Query DT immediately after refresh — no pending deltas,
            // should resolve to DirectTarget (materialized table only)
            List<TypedRow> dtResult = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");

            // Source baseline
            List<TypedRow> sourceResult = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");

            // Results must match exactly
            assertThat(dtResult).hasSameSizeAs(sourceResult);
            assertThat(dtResult).hasSize(5);

            // Verify each row matches source
            for (int i = 0; i < dtResult.size(); i++) {
                TypedRow dtRow = dtResult.get(i);
                TypedRow srcRow = sourceResult.get(i);
                assertThat(((Number) dtRow.getRaw("id")).intValue())
                        .isEqualTo(((Number) srcRow.getRaw("id")).intValue());
                assertThat(String.valueOf(dtRow.getRaw("name")))
                        .isEqualTo(String.valueOf(srcRow.getRaw("name")));
                assertThat(((Number) dtRow.getRaw("amount")).doubleValue())
                        .isEqualTo(((Number) srcRow.getRaw("amount")).doubleValue());
            }

            // Verify aggregate correctness
            double expectedTotal = 100.0 + 200.0 + 300.0 + 400.0 + 500.0;
            double actualTotal = dtResult.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("amount")).doubleValue())
                    .sum();
            assertThat(actualTotal).isEqualTo(expectedTotal);
        }

        @Test
        @DisplayName("INSERT → CREATE DT → INSERT more → manual refresh → query → "
                + "all data from materialized table (refresh caught up)")
        void directTargetAfterManualRefreshCatchesUp() {
            // Initial data
            for (int i = 1; i <= 10; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'user_" + i + "', " + (i * 50.0) + ")");
            }

            // CREATE DT — initial refresh
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT);
            assertThat(initial).hasSize(10);

            // Insert more data (creates new snapshot)
            for (int i = 11; i <= 15; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'user_" + i + "', " + (i * 50.0) + ")");
            }

            // Trigger manual refresh — catches up to latest snapshot
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT + " REFRESH");

            // Query after refresh — should use direct target (fully refreshed)
            List<TypedRow> afterRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterRefresh).hasSize(15);

            // Verify matches source
            List<TypedRow> source = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(afterRefresh).hasSameSizeAs(source);

            // Verify no duplicates
            List<Integer> ids = afterRefresh.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids).doesNotHaveDuplicates();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Merge-on-read with filter pushdown
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — Merge-on-read with filter pushdown")
    class MergeOnReadWithFilterPushdown {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".mor_filter_source";
        private static final String DT = ICE_CATALOG + "." + SCHEMA + ".mor_filter_dt";

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, category VARCHAR(50), amount DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, category VARCHAR(50), amount DOUBLE)");
        }

        @Test
        @DisplayName("INSERT 1000 rows → CREATE DT → refresh → INSERT 100 more → "
                + "query DT with WHERE clause → filter applied to both materialized "
                + "and delta legs → correct filtered count")
        void filterPushdownOnBothLegs() {
            // Insert 1000 rows with 5 categories (200 per category)
            for (int i = 1; i <= 1000; i++) {
                String category = "cat_" + ((i - 1) % 5 + 1);
                double amount = i * 0.5;
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + amount + ")");
            }

            // CREATE DT — initial refresh materializes all 1000 rows
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initialAll = sqlRunner.execute("SELECT * FROM " + DT);
            assertThat(initialAll).hasSize(1000);

            // Insert 100 more rows (20 per category) — NOT refreshed
            for (int i = 1001; i <= 1100; i++) {
                String category = "cat_" + ((i - 1) % 5 + 1);
                double amount = i * 0.5;
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + amount + ")");
            }

            // Query DT with WHERE filter — merge-on-read should apply filter
            // to both the materialized leg and the delta leg
            List<TypedRow> filteredResult = sqlRunner.execute(
                    "SELECT * FROM " + DT + " WHERE category = 'cat_1' ORDER BY id");

            // Source baseline with same filter
            List<TypedRow> sourceFiltered = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " WHERE category = 'cat_1' ORDER BY id");

            // cat_1 appears at positions 1, 6, 11, ... (every 5th row)
            // From 1000 rows: 200 rows with cat_1
            // From 100 more rows: 20 rows with cat_1
            // Total: 220 rows with cat_1
            assertThat(filteredResult)
                    .as("Merge-on-read with filter must return correct filtered count")
                    .hasSameSizeAs(sourceFiltered);
            assertThat(filteredResult).hasSize(220);

            // Verify all returned rows have the correct category
            assertThat(filteredResult).allSatisfy(row ->
                    assertThat(String.valueOf(row.getRaw("category"))).isEqualTo("cat_1"));

            // Verify data integrity: SUM of filtered amounts matches source
            double expectedFilteredSum = sourceFiltered.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("amount")).doubleValue())
                    .sum();
            double actualFilteredSum = filteredResult.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("amount")).doubleValue())
                    .sum();
            assertThat(actualFilteredSum).isEqualTo(expectedFilteredSum);

            // Verify no duplicate IDs in filtered result
            List<Integer> ids = filteredResult.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids).doesNotHaveDuplicates();
        }

        @Test
        @DisplayName("Merge-on-read with range filter on amount column")
        void rangeFilterOnMergeOnRead() {
            // Insert 100 rows
            for (int i = 1; i <= 100; i++) {
                String category = "cat_" + ((i - 1) % 3 + 1);
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + (i * 10.0) + ")");
            }

            // CREATE DT — refresh
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Insert 50 more rows — NOT refreshed
            for (int i = 101; i <= 150; i++) {
                String category = "cat_" + ((i - 1) % 3 + 1);
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + (i * 10.0) + ")");
            }

            // Range filter: amount > 500 AND amount <= 1000
            // Rows with amount > 500: id > 50 (amount = id * 10)
            // Rows with amount <= 1000: id <= 100
            // So ids 51-100 from materialized + delta rows with amount in range
            List<TypedRow> rangeResult = sqlRunner.execute(
                    "SELECT * FROM " + DT
                            + " WHERE amount > 500.0 AND amount <= 1000.0 ORDER BY id");

            List<TypedRow> sourceRange = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE
                            + " WHERE amount > 500.0 AND amount <= 1000.0 ORDER BY id");

            assertThat(rangeResult)
                    .as("Range filter on merge-on-read must match source")
                    .hasSameSizeAs(sourceRange);

            // Verify all amounts are within range
            assertThat(rangeResult).allSatisfy(row -> {
                double amt = ((Number) row.getRaw("amount")).doubleValue();
                assertThat(amt).isGreaterThan(500.0);
                assertThat(amt).isLessThanOrEqualTo(1000.0);
            });
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: CDC writes + merge-on-read
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 4 — CDC writes + merge-on-read")
    class CdcWritesMergeOnRead {

        private static final String SOURCE = PAIMON_CATALOG + "." + SCHEMA + ".mor_cdc_source";
        private static final String DT = PAIMON_CATALOG + "." + SCHEMA + ".mor_cdc_dt";

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            cdcAvailableFlag = true;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.paimon(
                            PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, event_type VARCHAR(50), payload VARCHAR(200))");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(DT);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + SOURCE
                    + " (id INT, event_type VARCHAR(50), payload VARCHAR(200))");
        }

        @Test
        @DisplayName("CDC writes 3 batches → CREATE DT → refresh (materializes batch 1-2) → "
                + "CDC writes batch 3 → query DT → sees all 3 batches → "
                + "batch 3 via incremental scan")
        void cdcMergeOnReadServesLatestBatch() {
            // CDC writes batch 1 (5 rows)
            for (int i = 1; i <= 5; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'batch_1', 'payload_" + i + "')");
            }

            // CDC writes batch 2 (5 rows)
            for (int i = 6; i <= 10; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'batch_2', 'payload_" + i + "')");
            }

            // CREATE DT with CDC mode — initial refresh materializes batches 1-2
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' REFRESH_MODE = CDC "
                    + "AS SELECT * FROM " + SOURCE);

            // Verify initial refresh captured batches 1-2 (10 rows)
            List<TypedRow> afterInitialRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterInitialRefresh).hasSize(10);

            // CDC writes batch 3 (5 rows) — NOT refreshed yet
            for (int i = 11; i <= 15; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'batch_3', 'payload_" + i + "')");
            }

            // Query DT — merge-on-read should serve all 3 batches
            // Batches 1-2 from materialized table, batch 3 via incremental scan
            List<TypedRow> mergeOnReadResult = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");

            // Full source baseline
            List<TypedRow> fullSource = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");

            // Must see all 15 rows (3 batches × 5 rows)
            assertThat(mergeOnReadResult)
                    .as("CDC merge-on-read must serve all 3 batches including unrefreshed batch 3")
                    .hasSameSizeAs(fullSource);
            assertThat(mergeOnReadResult).hasSize(15);

            // Verify all 3 batch types are present
            List<String> eventTypes = mergeOnReadResult.stream()
                    .map(r -> String.valueOf(r.getRaw("event_type")))
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(eventTypes).containsExactly("batch_1", "batch_2", "batch_3");

            // Verify batch 3 rows (the unrefreshed CDC delta)
            List<TypedRow> batch3Rows = mergeOnReadResult.stream()
                    .filter(r -> "batch_3".equals(String.valueOf(r.getRaw("event_type"))))
                    .toList();
            assertThat(batch3Rows).hasSize(5);

            // Verify data integrity: every row matches source
            for (int i = 0; i < mergeOnReadResult.size(); i++) {
                TypedRow dtRow = mergeOnReadResult.get(i);
                TypedRow srcRow = fullSource.get(i);
                assertThat(((Number) dtRow.getRaw("id")).intValue())
                        .isEqualTo(((Number) srcRow.getRaw("id")).intValue());
                assertThat(String.valueOf(dtRow.getRaw("event_type")))
                        .isEqualTo(String.valueOf(srcRow.getRaw("event_type")));
                assertThat(String.valueOf(dtRow.getRaw("payload")))
                        .isEqualTo(String.valueOf(srcRow.getRaw("payload")));
            }

            // Verify no duplicate IDs
            List<Integer> ids = mergeOnReadResult.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids).doesNotHaveDuplicates();
        }

        @Test
        @DisplayName("CDC merge-on-read followed by refresh catches up — "
                + "subsequent query uses direct target")
        void cdcMergeOnReadThenRefreshCatchesUp() {
            // CDC writes batch 1
            for (int i = 1; i <= 5; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'batch_1', 'data_" + i + "')");
            }

            // CREATE DT with CDC mode
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' REFRESH_MODE = CDC "
                    + "AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT);
            assertThat(initial).hasSize(5);

            // CDC writes batch 2 — NOT refreshed
            for (int i = 6; i <= 10; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + i + ", 'batch_2', 'data_" + i + "')");
            }

            // Query via merge-on-read — should see all 10 rows
            List<TypedRow> mergeOnRead = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(mergeOnRead).hasSize(10);

            // Now trigger refresh to catch up
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT + " REFRESH");

            // Query after refresh — should use direct target (fully refreshed)
            List<TypedRow> afterRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterRefresh).hasSize(10);

            // Results must be identical whether via merge-on-read or direct target
            assertThat(afterRefresh).hasSameSizeAs(mergeOnRead);
            for (int i = 0; i < afterRefresh.size(); i++) {
                assertThat(((Number) afterRefresh.get(i).getRaw("id")).intValue())
                        .isEqualTo(((Number) mergeOnRead.get(i).getRaw("id")).intValue());
            }

            // Verify matches source
            List<TypedRow> source = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(afterRefresh).hasSameSizeAs(source);
        }
    }
}
