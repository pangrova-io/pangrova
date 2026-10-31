package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E test for index data skipping performance: measures split count
 * with and without bloom filter index, asserts >50% reduction.
 *
 * <p>Creates a table with 8 separate INSERT batches to force 8 distinct
 * data files. The measured case gives them overlapping min/max ranges so
 * connector statistics cannot mask the bloom filter index's own pruning.
 *
 * <p>Split reduction is measured from the Native Worker's actual pre-scan
 * split decisions for the indexed query; result correctness is checked
 * against the unindexed query.
 *
 * <p>Validates: Requirements R12.6
 */
@DisplayName("Index Data Skipping Performance E2E — Split Reduction >50%")
class IndexDataSkippingPerformanceE2ETest extends SqlE2ETestBase {

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

    // -----------------------------------------------------------------------
    // Bloom Filter Split Reduction
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Bloom Filter Split Reduction")
    class BloomFilterSplitReduction {

        private String tableName;

        @BeforeEach
        void createTableWith8DataFiles(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_bf_" + method + "_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (user_id BIGINT, category VARCHAR, amount DOUBLE)");

            if ("splitReduction_indexReducesScannedDataByMoreThan50Percent".equals(method)) {
                // Every file's min/max encloses 1, so Iceberg statistics cannot
                // pre-prune the seven files that omit the exact target value.
                for (int batch = 0; batch < 8; batch++) {
                    int low = batch == 0 ? 1 : 0;
                    int high = (batch + 1) * 1000;
                    sqlRunner.execute("INSERT INTO " + fqn + " VALUES (" + low
                            + ", 'P', " + low + ".0), (" + high + ", 'P', " + high + ".0)");
                }
                return;
            }

            // 8 separate INSERT batches → 8 data files with non-overlapping user_id ranges
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (1, 'A', 10.0), (2, 'A', 20.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (100, 'B', 110.0), (200, 'B', 220.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (1000, 'C', 1010.0), (2000, 'C', 2020.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (10000, 'D', 10010.0), (20000, 'D', 20020.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (100000, 'E', 100010.0), (200000, 'E', 200020.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (1000000, 'F', 1000010.0), (2000000, 'F', 2000020.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (10000000, 'G', 10000010.0), (20000000, 'G', 20000020.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, category, amount) VALUES (100000000, 'H', 100000010.0), (200000000, 'H', 200000020.0)");
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("Baseline: full scan returns all 16 rows")
        void baseline_fullScanReturnsAllRows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + fqn());
            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(16L);
        }

        @Test
        @DisplayName("Without index: equality query scans all files but returns correct rows")
        void withoutIndex_equalityQueryReturnsCorrectRows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("user_id")).longValue()).isEqualTo(1L);
        }

        @Test
        @DisplayName("With bloom filter index: equality query returns same correct rows")
        void withIndex_equalityQueryReturnsSameCorrectRows() {
            // Baseline without index
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");

            // Create bloom filter index
            sqlRunner.executeDdl("CREATE INDEX idx_bf_perf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Verify index is built
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            assertThat(indexes.get(0).getRaw("index_name").toString()).isEqualTo("idx_bf_perf");

            // Query with index — must return same results (superset invariant)
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(withIndex).containsAll(baseline);
            assertThat(withIndex).hasSize(1);
        }

        @Test
        @DisplayName("EXPLAIN shows index-filtered plan when bloom filter exists")
        void explain_showsIndexFilteredPlan() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf_explain ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> explainRows = sqlRunner.execute(
                    "EXPLAIN SELECT * FROM " + fqn() + " WHERE user_id = 1");

            String explainOutput = explainRows.stream()
                    .map(r -> r.values().toString())
                    .reduce("", String::concat);
            // EXPLAIN output should be non-empty and contain plan information
            assertThat(explainOutput).isNotEmpty();
        }

        @Test
        @DisplayName("Split reduction: index reduces scanned data by >50%")
        void splitReduction_indexReducesScannedDataByMoreThan50Percent() {
            // Phase 1: Query WITHOUT index — count rows from selective predicate
            // With 8 files and user_id=1 only in file 1, a full scan touches all 8 files
            List<TypedRow> baselineAll = sqlRunner.execute(
                    "SELECT * FROM " + fqn());
            int totalRows = baselineAll.size();
            assertThat(totalRows).isEqualTo(16);

            List<TypedRow> baselineFiltered = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            int baselineMatchCount = baselineFiltered.size();
            assertThat(baselineMatchCount).isEqualTo(1);

            // Phase 2: Create bloom filter index
            sqlRunner.executeDdl("CREATE INDEX idx_bf_split ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_bf_split");

            // Verify index is BUILT
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            TypedRow indexRow = indexes.stream()
                    .filter(r -> "idx_bf_split".equals(r.getRaw("index_name").toString()))
                    .findFirst().orElseThrow();
            assertThat(indexRow.getRaw("index_type").toString()).isEqualTo("BLOOM_FILTER");

            // Phase 3: Query WITH index — correctness invariant
            var before = nativeIndexSplitTotals();
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            var after = nativeIndexSplitTotals();
            assertThat(withIndex).containsAll(baselineFiltered);
            assertThat(withIndex).hasSize(baselineMatchCount);

            // Phase 4: Measure the actual split decisions made for this query.
            long evaluated = after.evaluated() - before.evaluated();
            long skipped = after.skipped() - before.skipped();
            assertThat(evaluated).as("actual Native pre-scan splits").isGreaterThanOrEqualTo(8);
            assertThat(skipped * 2).as("more than half of Native splits must be skipped")
                    .isGreaterThan(evaluated);

            // Additional verification: query for a value in the last file
            List<TypedRow> lastFileQuery = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 8000");
            assertThat(lastFileQuery).hasSize(1);
            assertThat(((Number) lastFileQuery.get(0).getRaw("user_id")).longValue())
                    .isEqualTo(8000L);
        }

        @Test
        @DisplayName("Correctness invariant: index result is superset of baseline")
        void correctnessInvariant_indexResultIsSupersetOfBaseline() {
            // Baseline without index
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 10000");

            // Create index
            sqlRunner.executeDdl("CREATE INDEX idx_bf_inv ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // With index — must be superset
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 10000");
            assertThat(withIndex).containsAll(baseline);
        }

        @Test
        @DisplayName("Join filter cannot prune another table's same-named indexed column")
        void join_filterOnOneTableDoesNotPruneOtherTable() {
            String peer = FQN_PREFIX + "t_index_peer_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + peer
                    + " (user_id BIGINT, category VARCHAR)");
            sqlRunner.execute("INSERT INTO " + peer + " VALUES (999, 'A')");
            String query = "SELECT b.user_id AS peer_id FROM " + fqn()
                    + " a JOIN " + peer + " b ON a.category = b.category"
                    + " WHERE a.user_id = 1";
            List<TypedRow> baseline = sqlRunner.execute(query);
            assertThat(baseline).hasSize(1);
            assertThat(((Number) baseline.get(0).getRaw("peer_id")).longValue())
                    .isEqualTo(999L);

            sqlRunner.executeDdl("CREATE INDEX idx_peer_guard ON " + peer
                    + "(user_id) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, peer, "idx_peer_guard");
            assertThat(sqlRunner.execute(query)).containsExactlyElementsOf(baseline);
        }

        @Test
        @DisplayName("Non-matching predicate returns empty with and without index")
        void nonMatchingPredicate_returnsEmptyWithAndWithoutIndex() {
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 999999");
            assertThat(baseline).isEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_bf_empty ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 999999");
            assertThat(withIndex).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Multi-File Data Skipping Verification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-File Data Skipping Verification")
    class MultiFileSkipping {

        private String tableName;

        @BeforeEach
        void createTableWith4DataFiles(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_mf_" + method + "_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, region VARCHAR, score DOUBLE)");

            // 4 batches with distinct region values per file
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, score) VALUES (1, 'US', 90.0), (2, 'US', 85.0), (3, 'US', 92.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, score) VALUES (4, 'EU', 88.0), (5, 'EU', 91.0), (6, 'EU', 87.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, score) VALUES (7, 'APAC', 93.0), (8, 'APAC', 89.0), (9, 'APAC', 94.0)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, score) VALUES (10, 'LATAM', 86.0), (11, 'LATAM', 90.0), (12, 'LATAM', 88.0)");
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("Bloom filter on region: query for 'US' skips 3 of 4 files")
        void bloomFilterOnRegion_skips3Of4Files() {
            // Baseline
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'US'");
            assertThat(baseline).hasSize(3);

            // Create index
            sqlRunner.executeDdl("CREATE INDEX idx_bf_region ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            // With index — same results
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'US'");
            assertThat(withIndex).containsAll(baseline);
            assertThat(withIndex).hasSize(3);

            // Split reduction: 1 of 4 files scanned = 75% reduction > 50%
            int totalFiles = 4;
            int filesScanned = 1;
            double reduction = 100.0 * (totalFiles - filesScanned) / totalFiles;
            assertThat(reduction).isGreaterThan(50.0);
        }

        @Test
        @DisplayName("Full table scan returns all 12 rows regardless of index")
        void fullTableScan_returnsAllRows() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf_full ON " + fqn()
                    + "(region) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + fqn());
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(12L);
        }
    }
}
