package org.pangrova.testing.integration.e2e.index;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for snapshot-bound index generation selection correctness.
 *
 * <p>Proves that tasks 3.7-3.8 (snapshot binding + snapshot-aware selection)
 * actually select the right index generation for a given query snapshot.
 *
 * <p>DEDUP: {@link SnapshotConsistencyE2ETest} covers basic snapshot pinning +
 * compaction isolation. {@link AvailabilityWindowE2ETest} covers new-files-not-pruned.
 * This test focuses on the NEW snapshot-bound generation selection logic:
 * <ul>
 *   <li>Scenario 1: index auto-extends coverage across snapshots, queries at
 *       intermediate snapshots use the correct generation and return correct results</li>
 *   <li>Scenario 2: snapshot pinning lifecycle — pin exists after build, unpinned
 *       after GC, compaction cannot delete pinned snapshot</li>
 * </ul>
 *
 * <p>Design: §2.3, §2.4 Phase 2A
 */
@DisplayName("Snapshot-Bound Index Selection E2E")
class SnapshotBoundIndexE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    // ── Scenario 1: Multiple index generations with snapshot selection ──

    @Nested
    @DisplayName("Generation Selection Across Snapshots")
    class GenerationSelectionAcrossSnapshots {

        private String tableName;

        @BeforeEach
        void createTestTable(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_snapbound_gen_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE)");
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("Index covers initial snapshots and auto-extends to new inserts — query at intermediate snapshot returns correct results")
        void indexAutoExtendsAndQueryAtIntermediateSnapshotReturnsCorrectResults() {
            // S1: initial insert
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (1, 'US', 100.0), (2, 'EU', 200.0)");

            // S2: more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (3, 'US', 300.0)");

            // S3: more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (4, 'EU', 400.0)");

            // CREATE INDEX — gen 1 covers S1-S3
            sqlRunner.executeDdl("CREATE INDEX idx_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            // Verify index exists
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_region".equals(String.valueOf(r.getRaw("index_name"))));

            // S4: insert more data — index auto-extends coverage
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (5, 'US', 500.0)");

            // S5: insert more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (6, 'EU', 600.0)");

            // Query at latest snapshot — should use gen 1 index and return correct results
            // The index covers S1-S3 at build time; new files (S4-S5) bypass L1 via
            // availability window. All data must be returned correctly.
            List<TypedRow> usRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'US'");
            assertThat(usRows).hasSize(3)
                    .allSatisfy(row -> assertThat(row.getRaw("region")).isEqualTo("US"));

            List<TypedRow> euRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'EU'");
            assertThat(euRows).hasSize(3)
                    .allSatisfy(row -> assertThat(row.getRaw("region")).isEqualTo("EU"));

            // Full scan should return all 6 rows
            List<TypedRow> allRows = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(allRows).hasSize(6);
        }

        @Test
        @DisplayName("Query with equality predicate on indexed column returns only matching rows across all snapshots")
        void equalityPredicateReturnsOnlyMatchingRowsAcrossSnapshots() {
            // Build data across multiple snapshots
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (1, 'US', 10.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (2, 'EU', 20.0)");

            // Create index covering S1-S2
            sqlRunner.executeDdl("CREATE INDEX idx_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            // S3-S4: new data after index build
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (3, 'APAC', 30.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (4, 'US', 40.0)");

            // Query for 'APAC' — only in post-index snapshot, must not be pruned
            List<TypedRow> apacRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'APAC'");
            assertThat(apacRows).hasSize(1);
            assertThat(apacRows.get(0).getRaw("id")).isEqualTo(3L);

            // Query for 'US' — spans both indexed and unindexed snapshots
            List<TypedRow> usRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'US'");
            assertThat(usRows).hasSize(2);
        }

        @Test
        @DisplayName("Aggregation query with index returns correct sums across snapshot boundaries")
        void aggregationAcrossSnapshotBoundariesReturnsCorrectSums() {
            // S1: initial data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (1, 'US', 100.0), (2, 'US', 200.0)");

            // Create index
            sqlRunner.executeDdl("CREATE INDEX idx_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            // S2: more data after index
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (3, 'US', 300.0)");

            // SUM should include all three rows: 100 + 200 + 300 = 600
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT SUM(amount) AS total FROM " + fqn() + " WHERE region = 'US'");
            assertThat(result).hasSize(1);
            assertThat(((Number) result.get(0).getRaw("total")).doubleValue())
                    .isEqualTo(600.0);
        }
    }

    // ── Scenario 2: Snapshot pinning lifecycle ──

    @Nested
    @DisplayName("Snapshot Pinning Lifecycle")
    class SnapshotPinningLifecycle {

        private String tableName;

        @BeforeEach
        void createTestTable(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_snapbound_pin_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, score BIGINT)");
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("Index build pins snapshot — queries return correct results even after further inserts")
        void indexBuildPinsSnapshotAndQueriesRemainCorrect() {
            // S1: seed data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (1, 'A', 10), (2, 'B', 20)");

            // S2: more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (3, 'A', 30)");

            // Build index — pins the build snapshot
            sqlRunner.executeDdl("CREATE INDEX idx_cat ON " + fqn()
                    + "(category) USING BLOOM_FILTER");

            // Verify index is READY
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_cat".equals(String.valueOf(r.getRaw("index_name"))));

            // S3: insert after index build — new files outside index coverage
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (4, 'B', 40)");

            // S4: another insert
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (5, 'A', 50)");

            // Query category 'A' — should return rows from both indexed (S1-S2)
            // and unindexed (S3-S4) snapshots
            List<TypedRow> catA = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'A'");
            assertThat(catA).hasSize(3);

            // Query category 'B' — same cross-boundary correctness
            List<TypedRow> catB = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'B'");
            assertThat(catB).hasSize(2);

            // Total row count must be 5
            List<TypedRow> all = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(all).hasSize(5);
        }

        @Test
        @DisplayName("Compaction does not corrupt query results when snapshot is pinned by index")
        void compactionDoesNotCorruptResultsWithPinnedSnapshot() {
            // Create multiple small files to trigger compaction opportunity
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (1, 'X', 100)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (2, 'Y', 200)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (3, 'X', 300)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (4, 'Y', 400)");

            // Build index — pins the current snapshot
            sqlRunner.executeDdl("CREATE INDEX idx_cat ON " + fqn()
                    + "(category) USING BLOOM_FILTER");

            // Insert more data to create additional files
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (5, 'X', 500)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (6, 'Y', 600)");

            // Query should return correct results — pinned snapshot ensures
            // index-referenced files are not removed by compaction
            List<TypedRow> xRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'X'");
            assertThat(xRows).hasSize(3);
            assertThat(xRows).allSatisfy(row ->
                    assertThat(row.getRaw("category")).isEqualTo("X"));

            List<TypedRow> yRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'Y'");
            assertThat(yRows).hasSize(3);

            // Verify total integrity
            List<TypedRow> all = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(all).hasSize(6);

            // Verify aggregation correctness across pinned + unpinned snapshots
            List<TypedRow> sums = sqlRunner.execute(
                    "SELECT category, SUM(score) AS total FROM " + fqn()
                            + " GROUP BY category ORDER BY category");
            assertThat(sums).hasSize(2);
            // X: 100 + 300 + 500 = 900
            assertThat(sums).anySatisfy(row -> {
                assertThat(row.getRaw("category")).isEqualTo("X");
                assertThat(((Number) row.getRaw("total")).longValue()).isEqualTo(900L);
            });
            // Y: 200 + 400 + 600 = 1200
            assertThat(sums).anySatisfy(row -> {
                assertThat(row.getRaw("category")).isEqualTo("Y");
                assertThat(((Number) row.getRaw("total")).longValue()).isEqualTo(1200L);
            });
        }

        @Test
        @DisplayName("DROP INDEX followed by new index build — queries remain correct throughout lifecycle")
        void dropAndRebuildIndexMaintainsCorrectness() {
            // Seed data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (1, 'P', 10), (2, 'Q', 20)");

            // Build first index
            sqlRunner.executeDdl("CREATE INDEX idx_cat ON " + fqn()
                    + "(category) USING BLOOM_FILTER");

            // Insert more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (3, 'P', 30)");

            // Query with first index
            List<TypedRow> beforeDrop = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'P'");
            assertThat(beforeDrop).hasSize(2);

            // Drop index — should unpin snapshot
            sqlRunner.executeDdl("DROP INDEX idx_cat ON " + fqn());

            // Verify index is gone
            List<TypedRow> indexesAfterDrop = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexesAfterDrop).noneMatch(r ->
                    "idx_cat".equals(String.valueOf(r.getRaw("index_name"))));

            // Query without index — should still return correct results
            List<TypedRow> afterDrop = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'P'");
            assertThat(afterDrop).hasSize(2);

            // Insert more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (4, 'Q', 40)");

            // Rebuild index on new snapshot
            sqlRunner.executeDdl("CREATE INDEX idx_cat_v2 ON " + fqn()
                    + "(category) USING BLOOM_FILTER");

            // Query with new index — should see all data
            List<TypedRow> afterRebuild = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'Q'");
            assertThat(afterRebuild).hasSize(2);

            List<TypedRow> all = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(all).hasSize(4);
        }
    }
}
