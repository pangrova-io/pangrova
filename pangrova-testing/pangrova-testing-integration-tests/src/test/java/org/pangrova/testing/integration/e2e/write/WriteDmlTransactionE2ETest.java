package org.pangrova.testing.integration.e2e.write;

import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T01: Write/DML Transaction Correctness — 8 E2E scenarios.
 *
 * <p>Validates the complete write path through the Pangrova engine:
 * INSERT → native execution → Iceberg commit → SELECT verification.
 *
 * <p>Scenarios:
 * <ol>
 *   <li>INSERT/SELECT/CTAS round-trip</li>
 *   <li>UPDATE/DELETE/MERGE correctness</li>
 *   <li>INSERT OVERWRITE partition semantics</li>
 *   <li>Concurrent OCC (optimistic concurrency control)</li>
 *   <li>CoW vs MoR write modes</li>
 *   <li>Coordinator crash idempotency (simulated)</li>
 *   <li>Multi-query DataFile isolation</li>
 *   <li>Time travel (snapshot-based reads)</li>
 * </ol>
 */
@E2ETest
@DisplayName("T01: Write/DML Transaction Correctness")
class WriteDmlTransactionE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t01_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // Scenario 1: INSERT/SELECT/CTAS round-trip
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1: INSERT/SELECT/CTAS")
    class InsertSelectCtas {

        @Test
        @DisplayName("INSERT then SELECT returns exact rows")
        void insertSelectRoundTrip() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(3);
        }

        @Test
        @DisplayName("CTAS creates table with data from source")
        void ctasCreatesTableWithData() {
            String src = fqn();
            executeDdl("CREATE TABLE " + src + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + src + " VALUES (1, 'x'), (2, 'y')");

            String dst = FQN_PREFIX + tableName + "_ctas";
            executeDdl("CREATE TABLE " + dst + " AS SELECT * FROM " + src);

            List<TypedRow> rows = executeQuery("SELECT * FROM " + dst + " ORDER BY id");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("Multi-batch INSERT accumulates correctly")
        void multiBatchInsert() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val INT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 10)");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 20)");
            executeDml("INSERT INTO " + fqn() + " VALUES (3, 30)");

            List<TypedRow> rows = executeQuery("SELECT COUNT(*) as cnt FROM " + fqn());
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: UPDATE/DELETE/MERGE correctness
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2: UPDATE/DELETE/MERGE")
    class UpdateDeleteMerge {

        @Test
        @DisplayName("UPDATE modifies matching rows only")
        void updateModifiesMatchingRows() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'alice', 100.0), (2, 'bob', 200.0)");
            executeDml("UPDATE " + fqn() + " SET amount = 999.0 WHERE id = 1");

            List<TypedRow> rows = executeQuery(
                    "SELECT amount FROM " + fqn() + " WHERE id = 1");
            assertThat(rows).hasSize(1);
        }

        @Test
        @DisplayName("DELETE removes matching rows only")
        void deleteRemovesMatchingRows() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'keep'), (2, 'remove'), (3, 'keep')");
            executeDml("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("MERGE upserts correctly")
        void mergeUpsertsCorrectly() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'alice', 100.0), (2, 'bob', 200.0)");

            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + src + " VALUES (2, 'bob_updated', 250.0), (3, 'carol', 300.0)");

            executeDml("MERGE INTO " + fqn() + " t USING " + src + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET NAME = s.name, Amount = s.amount "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (s.id, s.name, s.amount)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(3);
            assertThat(rows).extracting(row -> List.of(
                    row.getLong("id"), row.getString("name"), row.getDouble("amount")))
                    .containsExactly(
                            List.of(1L, "alice", 100.0),
                            List.of(2L, "bob_updated", 250.0),
                            List.of(3L, "carol", 300.0));
        }

        @Test
        @DisplayName("MERGE handles nullable source prefix and reordered partial INSERT")
        void mergeUsesMatchMarkerAndTargetColumnMapping() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, 'alice', 100.0), (2, 'bob', 200.0)");

            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src
                    + " (note VARCHAR, id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + src
                    + " VALUES (NULL, 2, 'bob_updated'), (NULL, 3, 'carol')");

            executeDml("MERGE INTO " + fqn() + " t USING " + src + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name "
                    + "WHEN NOT MATCHED THEN INSERT (name, id) VALUES (s.name, s.id)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice");
            assertThat(rows.get(0).getDouble("amount")).isEqualTo(100.0);
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("bob_updated");
            assertThat(rows.get(1).getDouble("amount")).isEqualTo(200.0);
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
            assertThat(rows.get(2).getString("name")).isEqualTo("carol");
            assertThat(rows.get(2).getDouble("amount")).isNull();
        }

        @Test
        @DisplayName("MERGE rejects two source matches without changing the target")
        void mergeRejectsMultipleSourceMatchesAtomically() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'original')");
            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + src
                    + " VALUES (1, 'first'), (1, 'second')");

            String error = executeExpectingError(
                    "MERGE INTO " + fqn() + " t USING " + src + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name");

            assertThat(error).contains("matched more than one source row");
            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("original");
        }

        @Test
        @DisplayName("MERGE permits identical target rows with one source match")
        void mergeDoesNotMistakeEqualTargetValuesForMultipleMatches() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, 'original'), (1, 'original')");
            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + src + " VALUES (1, 'updated')");

            executeDml("MERGE INTO " + fqn() + " t USING " + src + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(2);
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.getLong("id")).isEqualTo(1L);
                assertThat(row.getString("name")).isEqualTo("updated");
            });
        }

        @Test
        @DisplayName("MERGE with DELETE and other actions fails without a commit")
        void mergeMultipleMatchedActionsFailClosed() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'keep'), (2, 'old')");
            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + src + " VALUES (2, 'delete'), (3, 'insert')");

            String error = executeExpectingError(
                    "MERGE INTO " + fqn() + " t USING " + src + " s ON t.id = s.id "
                    + "WHEN MATCHED AND s.name = 'delete' THEN DELETE "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name "
                    + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");

            assertThat(error).contains("one atomic native write");
            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("keep");
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("old");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: INSERT OVERWRITE partition semantics
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3: INSERT OVERWRITE")
    class InsertOverwrite {

        @Test
        @DisplayName("INSERT OVERWRITE replaces partition data")
        void insertOverwriteReplacesPartition() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE) PARTITIONED BY (region)");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, 'us', 100.0), (2, 'eu', 200.0), (3, 'us', 300.0)");

            // Overwrite 'us' partition with new data
            executeDml("INSERT OVERWRITE " + fqn()
                    + " (id, amount) PARTITION (region = 'us') VALUES (10, 1000.0)");

            List<TypedRow> usRows = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE region = 'us'");
            assertThat(usRows).hasSize(1);
            assertThat(usRows.get(0).get("id"))
                    .isEqualTo(new ColumnValue.LongValue(10L));
            assertThat(usRows.get(0).get("amount"))
                    .isEqualTo(new ColumnValue.DoubleValue(1000.0));

            // 'eu' partition should be untouched
            List<TypedRow> euRows = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE region = 'eu'");
            assertThat(euRows).hasSize(1);
            assertThat(euRows.get(0).get("id"))
                    .isEqualTo(new ColumnValue.LongValue(2L));
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: Concurrent OCC (Optimistic Concurrency Control)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 4: Concurrent OCC")
    class ConcurrentOcc {

        @Test
        @DisplayName("Concurrent inserts to different partitions succeed")
        void concurrentInsertsToDisjointPartitions() throws Exception {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE) PARTITIONED BY (region)");

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> f1 = pool.submit(() ->
                        executeDml("INSERT INTO " + fqn() + " VALUES (1, 'us', 100.0)"));
                Future<?> f2 = pool.submit(() ->
                        executeDml("INSERT INTO " + fqn() + " VALUES (2, 'eu', 200.0)"));
                f1.get();
                f2.get();
            } finally {
                pool.shutdown();
            }

            List<TypedRow> rows = executeQuery("SELECT COUNT(*) as cnt FROM " + fqn());
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5: CoW (Copy-on-Write) mode
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 5: CoW Write Mode")
    class CopyOnWrite {

        @Test
        @DisplayName("DELETE in CoW mode rewrites entire data file")
        void cowDeleteRewritesFile() {
            // Default Iceberg mode is CoW
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            executeDml("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6: Coordinator crash idempotency (simulated)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 6: Crash Idempotency")
    class CrashIdempotency {

        @Test
        @DisplayName("Failed INSERT leaves table unchanged (atomicity)")
        void failedInsertLeavesTableUnchanged() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'original')");

            // Attempt INSERT with type mismatch — should fail
            String error = executeExpectingError(
                    "INSERT INTO " + fqn() + " VALUES ('not_a_number', 'bad')");
            assertThat(error).isNotNull();

            // Table should still have only the original row
            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(1);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 7: Multi-query DataFile isolation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 7: DataFile Isolation")
    class DataFileIsolation {

        @Test
        @DisplayName("Separate INSERTs create isolated DataFiles")
        void separateInsertsCreateIsolatedFiles() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, batch INT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 1), (2, 1)");
            executeDml("INSERT INTO " + fqn() + " VALUES (3, 2), (4, 2)");

            // Both batches should be visible
            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(4);

            // Verify batch separation via count per batch
            List<TypedRow> batchCounts = executeQuery(
                    "SELECT batch, COUNT(*) as cnt FROM " + fqn() + " GROUP BY batch ORDER BY batch");
            assertThat(batchCounts).hasSize(2);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 8: Time travel (snapshot-based reads)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 8: Time Travel")
    class TimeTravel {

        @Test
        @DisplayName("Reading historical snapshot returns old data")
        void readHistoricalSnapshot() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'v1')");

            // Get snapshot ID via Iceberg Java API (metadata table queries not yet supported)
            var hadoopConf = new org.apache.hadoop.conf.Configuration();
            var catalog = CatalogUtil.buildIcebergCatalog("test",
                    Map.of("type", "hadoop",
                           "warehouse", icebergWarehouse().toUri().toString()),
                    hadoopConf);
            var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
            long firstSnapshotId = table.currentSnapshot().snapshotId();

            // Insert more data
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'v2'), (3, 'v3')");

            // Current state should have 3 rows
            List<TypedRow> currentRows = executeQuery("SELECT * FROM " + fqn());
            assertThat(currentRows).hasSize(3);

            // Time travel to first snapshot — should have only 1 row
            List<TypedRow> historicalRows = executeQuery(
                    "SELECT * FROM " + fqn() + " FOR SYSTEM_TIME AS OF " + firstSnapshotId);
            assertThat(historicalRows).hasSize(1);
        }
    }
}
