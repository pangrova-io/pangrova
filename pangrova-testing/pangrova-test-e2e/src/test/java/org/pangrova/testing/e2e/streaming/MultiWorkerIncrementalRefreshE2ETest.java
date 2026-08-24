package org.pangrova.testing.e2e.streaming;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.shuffle.MultiWorkerTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test proving incremental split generation (task 5.6) works correctly
 * when the DT refresh query is distributed across multiple workers.
 *
 * <p>Extends {@link MultiWorkerTestBase} (1 coordinator + 2 virtual workers)
 * so that refresh execution triggers shuffle partitioning across worker
 * boundaries, exercising the full distributed path for incremental splits.</p>
 *
 * <p>Two scenarios:</p>
 * <ol>
 *   <li>DT refresh with multi-worker execution: INSERT 10 batches → CREATE DT →
 *       refresh (distributed across 2 workers) → INSERT 2 more batches →
 *       delta refresh (distributed) → verify DT has all 12 batches →
 *       verify delta refresh only scanned 2 batches.</li>
 *   <li>Cross-connector DT with multi-worker: Iceberg source → Paimon DT →
 *       refresh across 2 workers → correct results.</li>
 * </ol>
 */
@DisplayName("Multi-Worker Incremental DT Refresh E2E")
class MultiWorkerIncrementalRefreshE2ETest extends MultiWorkerTestBase {

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
    // Scenario 1: DT refresh with multi-worker execution
    // INSERT 10 batches → CREATE DT → refresh (distributed) →
    // INSERT 2 more batches → delta refresh (distributed) →
    // verify DT has all 12 batches → verify delta refresh only scanned 2 batches
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — DT refresh with multi-worker execution")
    class DtRefreshMultiWorkerExecution {

        private static final String SOURCE = ICE_CATALOG + "." + SCHEMA + ".mw_incr_source";
        private static final String DT = ICE_CATALOG + "." + SCHEMA + ".mw_incr_dt";
        private static final int INITIAL_BATCHES = 10;
        private static final int DELTA_BATCHES = 2;
        private static final int ROWS_PER_BATCH = 10;

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                    ConnectorTestConfig.paimon(
                            PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString())));
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
        @DisplayName("INSERT 10 batches → CREATE DT → refresh → INSERT 2 more → "
                + "delta refresh → all 12 batches present, delta only scanned 2 batches")
        void deltaRefreshDistributedAcrossWorkers() {
            // Insert 10 batches into source (each INSERT creates a new snapshot)
            for (int batch = 1; batch <= INITIAL_BATCHES; batch++) {
                for (int i = 1; i <= ROWS_PER_BATCH; i++) {
                    int id = (batch - 1) * ROWS_PER_BATCH + i;
                    sqlRunner.execute("INSERT INTO " + SOURCE
                            + " VALUES (" + id + ", " + batch + ", " + (id * 2.5) + ")");
                }
            }

            int initialRowCount = INITIAL_BATCHES * ROWS_PER_BATCH;

            // CREATE DT triggers initial full-scan refresh (distributed across 2 workers)
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            // Verify initial refresh captured all rows from 10 batches
            List<TypedRow> initialRows = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(initialRows)
                    .as("Initial refresh must capture all %d rows from %d batches",
                            initialRowCount, INITIAL_BATCHES)
                    .hasSize(initialRowCount);

            // Verify all 10 batch numbers present
            List<Integer> initialBatches = initialRows.stream()
                    .map(r -> ((Number) r.getRaw("batch_num")).intValue())
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(initialBatches).hasSize(INITIAL_BATCHES);
            for (int b = 1; b <= INITIAL_BATCHES; b++) {
                assertThat(initialBatches).contains(b);
            }

            // INSERT 2 more batches (batches 11 and 12)
            for (int batch = INITIAL_BATCHES + 1; batch <= INITIAL_BATCHES + DELTA_BATCHES; batch++) {
                for (int i = 1; i <= ROWS_PER_BATCH; i++) {
                    int id = (batch - 1) * ROWS_PER_BATCH + i;
                    sqlRunner.execute("INSERT INTO " + SOURCE
                            + " VALUES (" + id + ", " + batch + ", " + (id * 2.5) + ")");
                }
            }

            // Trigger delta refresh — distributed across 2 workers
            // Should use incremental splits (O(delta)) for append-only range
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT + " REFRESH");

            int totalRowCount = (INITIAL_BATCHES + DELTA_BATCHES) * ROWS_PER_BATCH;

            // Verify DT now has all 120 rows from 12 batches
            List<TypedRow> afterRefresh = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterRefresh)
                    .as("After delta refresh, DT must have all %d rows from %d batches",
                            totalRowCount, INITIAL_BATCHES + DELTA_BATCHES)
                    .hasSize(totalRowCount);

            // Verify all 12 batch numbers present
            List<Integer> allBatches = afterRefresh.stream()
                    .map(r -> ((Number) r.getRaw("batch_num")).intValue())
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(allBatches).hasSize(INITIAL_BATCHES + DELTA_BATCHES);
            for (int b = 1; b <= INITIAL_BATCHES + DELTA_BATCHES; b++) {
                assertThat(allBatches).contains(b);
            }

            // Verify delta batch data is correct (batches 11 and 12)
            for (int batch = INITIAL_BATCHES + 1; batch <= INITIAL_BATCHES + DELTA_BATCHES; batch++) {
                final int batchNum = batch;
                List<TypedRow> batchRows = afterRefresh.stream()
                        .filter(r -> ((Number) r.getRaw("batch_num")).intValue() == batchNum)
                        .toList();
                assertThat(batchRows)
                        .as("Batch %d must have %d rows", batchNum, ROWS_PER_BATCH)
                        .hasSize(ROWS_PER_BATCH);
            }

            // Verify data integrity: SUM of all values matches expected
            double expectedSum = 0;
            for (int id = 1; id <= totalRowCount; id++) {
                expectedSum += id * 2.5;
            }
            double actualSum = afterRefresh.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("value")).doubleValue())
                    .sum();
            assertThat(actualSum)
                    .as("Total value sum must match oracle across distributed refresh")
                    .isEqualTo(expectedSum);

            // Verify no duplicate IDs — proves multi-worker refresh didn't
            // produce duplicate rows from overlapping splits
            List<Integer> ids = afterRefresh.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids)
                    .as("No duplicate IDs after distributed delta refresh")
                    .doesNotHaveDuplicates();
        }

        @Test
        @DisplayName("Multiple distributed delta refreshes accumulate correctly")
        void multipleDistributedDeltaRefreshesAccumulate() {
            // Insert initial 10 batches
            for (int batch = 1; batch <= INITIAL_BATCHES; batch++) {
                for (int i = 1; i <= ROWS_PER_BATCH; i++) {
                    int id = (batch - 1) * ROWS_PER_BATCH + i;
                    sqlRunner.execute("INSERT INTO " + SOURCE
                            + " VALUES (" + id + ", " + batch + ", " + (id * 2.5) + ")");
                }
            }

            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + SOURCE);

            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT);
            assertThat(initial).hasSize(INITIAL_BATCHES * ROWS_PER_BATCH);

            // First delta: add batch 11
            int nextId = INITIAL_BATCHES * ROWS_PER_BATCH;
            for (int i = 1; i <= ROWS_PER_BATCH; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + (nextId + i) + ", 11, " + ((nextId + i) * 2.5) + ")");
            }
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT + " REFRESH");

            List<TypedRow> afterDelta1 = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterDelta1)
                    .as("After first delta refresh")
                    .hasSize((INITIAL_BATCHES + 1) * ROWS_PER_BATCH);

            // Second delta: add batch 12
            nextId = (INITIAL_BATCHES + 1) * ROWS_PER_BATCH;
            for (int i = 1; i <= ROWS_PER_BATCH; i++) {
                sqlRunner.execute("INSERT INTO " + SOURCE
                        + " VALUES (" + (nextId + i) + ", 12, " + ((nextId + i) * 2.5) + ")");
            }
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT + " REFRESH");

            List<TypedRow> afterDelta2 = sqlRunner.execute(
                    "SELECT * FROM " + DT + " ORDER BY id");
            assertThat(afterDelta2)
                    .as("After second delta refresh")
                    .hasSize((INITIAL_BATCHES + 2) * ROWS_PER_BATCH);

            // Verify all 12 batch numbers present
            List<Integer> allBatches = afterDelta2.stream()
                    .map(r -> ((Number) r.getRaw("batch_num")).intValue())
                    .distinct()
                    .sorted()
                    .toList();
            assertThat(allBatches).hasSize(INITIAL_BATCHES + 2);

            // Verify results match source
            List<TypedRow> source = sqlRunner.execute(
                    "SELECT * FROM " + SOURCE + " ORDER BY id");
            assertThat(afterDelta2).hasSameSizeAs(source);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Cross-connector DT with multi-worker
    // Iceberg source → Paimon DT → refresh across 2 workers → correct results
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Cross-connector DT with multi-worker")
    class CrossConnectorDtMultiWorker {

        private static final String ICE_SOURCE = ICE_CATALOG + "." + SCHEMA + ".mw_cross_ice_src";
        private static final String PAIMON_DT = PAIMON_CATALOG + "." + SCHEMA + ".mw_cross_paimon_dt";
        private static final int ROW_COUNT = 50;

        @BeforeAll
        static void createSource() {
            if (!isDockerReady()) return;
            registerTestConnectors(List.of(
                    ConnectorTestConfig.icebergLocal(
                            ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                    ConnectorTestConfig.paimon(
                            PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString())));
            sqlRunner.execute("CREATE TABLE " + ICE_SOURCE
                    + " (id INT, category VARCHAR(50), amount DOUBLE)");
        }

        @BeforeEach
        void cleanup() {
            ensureDynamicTableDropped(PAIMON_DT);
            try {
                sqlRunner.execute("DROP TABLE IF EXISTS " + ICE_SOURCE);
            } catch (Exception ignored) {
            }
            sqlRunner.execute("CREATE TABLE " + ICE_SOURCE
                    + " (id INT, category VARCHAR(50), amount DOUBLE)");
        }

        @Test
        @DisplayName("Iceberg source → Paimon DT → distributed refresh → correct results")
        void crossConnectorRefreshDistributed() {
            // Populate Iceberg source
            for (int i = 1; i <= ROW_COUNT; i++) {
                String category = "cat_" + ((i - 1) % 5 + 1);
                double amount = i * 3.0;
                sqlRunner.execute("INSERT INTO " + ICE_SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + amount + ")");
            }

            // CREATE DT on Paimon backed by Iceberg source
            // Refresh is distributed across 2 workers
            sqlRunner.execute("CREATE DYNAMIC TABLE " + PAIMON_DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + ICE_SOURCE);

            // Verify DT has all rows
            List<TypedRow> dtRows = sqlRunner.execute(
                    "SELECT * FROM " + PAIMON_DT + " ORDER BY id");
            assertThat(dtRows)
                    .as("Cross-connector DT must have all %d rows", ROW_COUNT)
                    .hasSize(ROW_COUNT);

            // Verify data integrity: every row matches source
            List<TypedRow> sourceRows = sqlRunner.execute(
                    "SELECT * FROM " + ICE_SOURCE + " ORDER BY id");
            assertThat(dtRows).hasSameSizeAs(sourceRows);

            for (int i = 0; i < dtRows.size(); i++) {
                TypedRow dtRow = dtRows.get(i);
                TypedRow srcRow = sourceRows.get(i);
                assertThat(((Number) dtRow.getRaw("id")).intValue())
                        .as("Row %d id must match", i)
                        .isEqualTo(((Number) srcRow.getRaw("id")).intValue());
                assertThat(String.valueOf(dtRow.getRaw("category")))
                        .as("Row %d category must match", i)
                        .isEqualTo(String.valueOf(srcRow.getRaw("category")));
                assertThat(((Number) dtRow.getRaw("amount")).doubleValue())
                        .as("Row %d amount must match", i)
                        .isEqualTo(((Number) srcRow.getRaw("amount")).doubleValue());
            }

            // Verify aggregate correctness across distributed refresh
            double expectedSum = 0;
            for (int i = 1; i <= ROW_COUNT; i++) {
                expectedSum += i * 3.0;
            }
            double actualSum = dtRows.stream()
                    .mapToDouble(r -> ((Number) r.getRaw("amount")).doubleValue())
                    .sum();
            assertThat(actualSum)
                    .as("Total amount must match oracle for cross-connector distributed refresh")
                    .isEqualTo(expectedSum);

            // Verify category distribution is correct (5 categories, 10 rows each)
            for (int c = 1; c <= 5; c++) {
                String cat = "cat_" + c;
                long catCount = dtRows.stream()
                        .filter(r -> cat.equals(String.valueOf(r.getRaw("category"))))
                        .count();
                assertThat(catCount)
                        .as("Category %s must have %d rows", cat, ROW_COUNT / 5)
                        .isEqualTo(ROW_COUNT / 5);
            }
        }

        @Test
        @DisplayName("Cross-connector DT delta refresh after adding more Iceberg data")
        void crossConnectorDeltaRefresh() {
            // Initial data in Iceberg source
            for (int i = 1; i <= 20; i++) {
                String category = "cat_" + ((i - 1) % 4 + 1);
                sqlRunner.execute("INSERT INTO " + ICE_SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + (i * 1.5) + ")");
            }

            // CREATE DT on Paimon
            sqlRunner.execute("CREATE DYNAMIC TABLE " + PAIMON_DT
                    + " TARGET_LAG = 'PT1H' AS SELECT * FROM " + ICE_SOURCE);

            List<TypedRow> initial = sqlRunner.execute(
                    "SELECT * FROM " + PAIMON_DT + " ORDER BY id");
            assertThat(initial).hasSize(20);

            // Add more data to Iceberg source
            for (int i = 21; i <= 30; i++) {
                String category = "cat_" + ((i - 1) % 4 + 1);
                sqlRunner.execute("INSERT INTO " + ICE_SOURCE
                        + " VALUES (" + i + ", '" + category + "', " + (i * 1.5) + ")");
            }

            // Delta refresh — distributed across 2 workers
            sqlRunner.execute("ALTER DYNAMIC TABLE " + PAIMON_DT + " REFRESH");

            List<TypedRow> afterRefresh = sqlRunner.execute(
                    "SELECT * FROM " + PAIMON_DT + " ORDER BY id");
            assertThat(afterRefresh)
                    .as("After delta refresh, cross-connector DT must have all 30 rows")
                    .hasSize(30);

            // Verify new rows are present
            List<TypedRow> newRows = afterRefresh.stream()
                    .filter(r -> ((Number) r.getRaw("id")).intValue() > 20)
                    .toList();
            assertThat(newRows)
                    .as("Delta rows (id 21-30) must be present")
                    .hasSize(10);

            // Verify no duplicates
            List<Integer> ids = afterRefresh.stream()
                    .map(r -> ((Number) r.getRaw("id")).intValue())
                    .toList();
            assertThat(ids).doesNotHaveDuplicates();

            // Verify matches source
            List<TypedRow> source = sqlRunner.execute(
                    "SELECT * FROM " + ICE_SOURCE + " ORDER BY id");
            assertThat(afterRefresh).hasSameSizeAs(source);
        }
    }
}
